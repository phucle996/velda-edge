//! Distributed Trace Exporter Shadow Coordinator (`trace_exporter::plugin`).
//!
//! ```text
//!   TraceExporterPlugin (targets: Vec<TraceTargetConfig>)
//!         │
//!         ├─► etp_ids()    ──► collects every target's etp_id
//!         ├─► attach_etp() ──► resolves matching targets by instance.id + EtpBackend::Otlp
//!         │
//!         └─► run(shutdown) ──► JoinSet per target:
//!               └─► interval / shutdown select loop:
//!                     ├─ in 30s cooldown? ──► drain & discard batches (RAM guard)
//!                     └─ drain_batches(64) -> TraceProvider::flush_batches()
//! ```

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use tokio::sync::watch;
use tokio::task::JoinSet;
use velda_etp::EtpInstance;
use velda_observability::{TelemetryStore, global_telemetry_store};

use super::config::{TraceExporterConfig, TraceTargetConfig};
use super::providers::{EtpTraceAttachment, TraceProvider};
use crate::background::BackgroundPlugin;
use crate::capability::{Direction, PluginCapability, SupportedProtocols};
use crate::config::PluginConfig;
use crate::error::PluginError;

struct TargetSlot {
    config: TraceTargetConfig,
    attachment: OnceLock<EtpTraceAttachment>,
}

/// Consolidated Trace Exporter Background Plugin (`trace_exporter`).
pub struct TraceExporterPlugin {
    pub id: String,
    pub config: TraceExporterConfig,
    slots: Vec<TargetSlot>,
    pub telemetry_store: Arc<TelemetryStore>,
    pub shipped_spans: Arc<AtomicU64>,
    pub failed_spans: Arc<AtomicU64>,
}

impl TraceExporterPlugin {
    /// Constructs a new `trace_exporter` bound to the global [`TelemetryStore`].
    pub fn new(id: impl Into<String>, config: TraceExporterConfig) -> Result<Self, PluginError> {
        Self::with_store(id, config, Arc::clone(global_telemetry_store()))
    }

    /// Constructs a new `trace_exporter` bound to an explicit [`TelemetryStore`].
    pub fn with_store(
        id: impl Into<String>,
        config: TraceExporterConfig,
        telemetry_store: Arc<TelemetryStore>,
    ) -> Result<Self, PluginError> {
        let slots = config
            .targets
            .iter()
            .map(|t| TargetSlot {
                config: t.clone(),
                attachment: OnceLock::new(),
            })
            .collect();

        Ok(Self {
            id: id.into(),
            config,
            slots,
            telemetry_store,
            shipped_spans: Arc::new(AtomicU64::new(0)),
            failed_spans: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Deserializes and validates `trace_exporter` from [`PluginConfig`].
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let validated =
            super::validation::validate_trace_exporter_config(&config.id, &config.config)?;
        Self::with_store(&config.id, validated, Arc::clone(global_telemetry_store()))
    }
}

impl BackgroundPlugin for TraceExporterPlugin {
    fn id(&self) -> &str {
        &self.id
    }

    #[inline(always)]
    fn pbww(&self) -> u32 {
        50
    }

    #[inline(always)]
    fn direction(&self) -> Direction {
        Direction::Background
    }

    #[inline(always)]
    fn capability(&self) -> PluginCapability {
        PluginCapability::default()
    }

    #[inline(always)]
    fn supported_protocols(&self) -> SupportedProtocols {
        SupportedProtocols::empty()
    }

    fn etp_ids(&self) -> Vec<&str> {
        self.slots
            .iter()
            .map(|slot| slot.config.etp_id.as_str())
            .collect()
    }

    fn attach_etp(&self, instance: &EtpInstance) -> Result<(), PluginError> {
        let mut matched = false;
        for slot in &self.slots {
            if slot.config.etp_id == instance.id {
                let built =
                    EtpTraceAttachment::from_etp_instance(&self.id, &slot.config, instance)?;
                let _ = slot.attachment.set(built);
                matched = true;
            }
        }
        if !matched {
            return Err(PluginError::Config(format!(
                "Trace exporter plugin '{}' received unexpected ETP '{}'",
                self.id, instance.id
            )));
        }
        Ok(())
    }

    fn run(
        self: Arc<Self>,
        shutdown: watch::Receiver<bool>,
    ) -> Pin<Box<dyn Future<Output = Result<(), PluginError>> + Send>> {
        Box::pin(async move {
            let mut providers = Vec::with_capacity(self.slots.len());
            for slot in &self.slots {
                let attached = slot.attachment.get().ok_or_else(|| {
                    PluginError::Config(format!(
                        "Trace exporter plugin '{}' has no attached ETP for etp_id '{}'",
                        self.id, slot.config.etp_id
                    ))
                })?;
                providers.push(TraceProvider::from_attachment(attached)?);
            }

            if providers.len() == 1 {
                let provider = providers.into_iter().next().unwrap();
                return run_trace_loop(
                    self.id.clone(),
                    provider,
                    Arc::clone(&self.telemetry_store),
                    Arc::clone(&self.shipped_spans),
                    Arc::clone(&self.failed_spans),
                    shutdown,
                )
                .await;
            }

            let mut set = JoinSet::new();
            for provider in providers {
                let plugin_id = self.id.clone();
                let store = Arc::clone(&self.telemetry_store);
                let shipped = Arc::clone(&self.shipped_spans);
                let failed = Arc::clone(&self.failed_spans);
                let rx = shutdown.clone();
                set.spawn(async move {
                    run_trace_loop(plugin_id, provider, store, shipped, failed, rx).await
                });
            }

            while let Some(res) = set.join_next().await {
                res.map_err(|e| PluginError::Config(format!("Trace target task panicked: {e}")))??;
            }
            Ok(())
        })
    }
}

async fn run_trace_loop(
    plugin_id: String,
    mut provider: TraceProvider,
    telemetry_store: Arc<TelemetryStore>,
    shipped_spans: Arc<AtomicU64>,
    failed_spans: Arc<AtomicU64>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), PluginError> {
    let target_name = provider.name();
    let subscriber = telemetry_store.subscribe(256);
    let mut interval = tokio::time::interval(Duration::from_millis(provider.flush_interval_ms()));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let mut consecutive_failures: u32 = 0;
    let mut cooldown_until: Option<tokio::time::Instant> = None;

    loop {
        let is_shutting_down = tokio::select! {
            _ = interval.tick() => false,
            _ = subscriber.wait_for_batch_threshold() => false,
            _ = shutdown.changed() => *shutdown.borrow(),
        };

        if cooldown_until.is_some_and(|until| tokio::time::Instant::now() < until) {
            let _ = subscriber.drain_batches(64);
            tracing::debug!(
                plugin_id = %plugin_id,
                provider = %target_name,
                "Trace exporter in 30s cool-down; discarded batches to free RAM"
            );
        } else {
            if cooldown_until.is_some() {
                cooldown_until = None;
                consecutive_failures = 0;
            }

            let batches = subscriber.drain_batches(64);
            if !batches.is_empty() {
                let res = provider.flush_batches(&batches).await;
                drop(batches);

                match res {
                    Ok(shipped) => {
                        consecutive_failures = 0;
                        if shipped > 0 {
                            shipped_spans.fetch_add(shipped as u64, Ordering::Relaxed);
                        }
                    }
                    Err(e) => {
                        consecutive_failures += 1;
                        failed_spans.fetch_add(1, Ordering::Relaxed);
                        tracing::warn!(
                            plugin_id = %plugin_id,
                            provider = %target_name,
                            error = %e,
                            attempt = consecutive_failures,
                            "Failed to push trace spans payload"
                        );
                        if consecutive_failures <= 10 {
                            if !is_shutting_down {
                                let backoff_ms = (100u64
                                    .saturating_mul(1u64 << (consecutive_failures - 1).min(31)))
                                .min(2000);
                                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                            }
                        } else {
                            cooldown_until =
                                Some(tokio::time::Instant::now() + Duration::from_secs(30));
                            consecutive_failures = 0;
                        }
                    }
                }
            }
        }

        if is_shutting_down {
            break;
        }
    }

    Ok(())
}
