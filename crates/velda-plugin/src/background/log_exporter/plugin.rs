//! Access Log Exporter Shadow Worker Coordinator (`log_exporter::plugin`).
//!
//! ```text
//!   LogExporterPlugin (targets: [Local | Etp])
//!         │
//!         ├─► etp_ids()    ──► collects every target's etp_id (mode == "etp")
//!         ├─► attach_etp() ──► resolves matching targets by instance.id + EtpBackend
//!         │
//!         └─► run(shutdown) ──► spawns JoinSet of run_target_loop per target
//!               │
//!               └─► run_target_loop(mut provider):
//!                     ├─► provider.prepare().await
//!                     └─► tick / shutdown select loop:
//!                           ├─ in 30s cooldown? ──► drain & discard batches (RAM guard)
//!                           └─ drain_batches() -> provider.flush_batches(&batches)
//!                                 ├─ Ok(n)  ──► ++shipped_records, reset failures
//!                                 └─ Err(e) ──► ++failed_pushes -> 30s cooldown (> 10)
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

use super::config::{EtpLogTargetConfig, LocalLogTargetConfig, LogExporterConfig, LogTargetConfig};
use super::providers::{EtpLogAttachment, LogProvider};
use crate::background::BackgroundPlugin;
use crate::capability::{Direction, PluginCapability, SupportedProtocols};
use crate::config::PluginConfig;
use crate::error::PluginError;

enum TargetSlot {
    Local(LocalLogTargetConfig),
    Etp {
        config: Box<EtpLogTargetConfig>,
        attachment: OnceLock<EtpLogAttachment>,
    },
}

/// Consolidated Access Log Exporter Background Plugin (`log_exporter`).
pub struct LogExporterPlugin {
    pub id: String,
    pub config: LogExporterConfig,
    slots: Vec<TargetSlot>,
    pub telemetry_store: Arc<TelemetryStore>,
    pub shipped_records: Arc<AtomicU64>,
    pub failed_pushes: Arc<AtomicU64>,
}

impl LogExporterPlugin {
    /// Constructs a new `log_exporter` bound to the global [`TelemetryStore`].
    pub fn new(id: impl Into<String>, config: LogExporterConfig) -> Result<Self, PluginError> {
        Self::with_store(id, config, Arc::clone(global_telemetry_store()))
    }

    /// Constructs a new `log_exporter` bound to an explicit [`TelemetryStore`].
    pub fn with_store(
        id: impl Into<String>,
        config: LogExporterConfig,
        telemetry_store: Arc<TelemetryStore>,
    ) -> Result<Self, PluginError> {
        let slots = config
            .targets
            .iter()
            .map(|t| match t {
                LogTargetConfig::Local(local) => TargetSlot::Local(local.clone()),
                LogTargetConfig::Etp(etp) => TargetSlot::Etp {
                    config: etp.clone(),
                    attachment: OnceLock::new(),
                },
            })
            .collect();

        Ok(Self {
            id: id.into(),
            config,
            slots,
            telemetry_store,
            shipped_records: Arc::new(AtomicU64::new(0)),
            failed_pushes: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Deserializes and validates `log_exporter` from [`PluginConfig`].
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let validated =
            super::validation::validate_log_exporter_config(&config.id, &config.config)?;
        Self::with_store(&config.id, validated, Arc::clone(global_telemetry_store()))
    }
}

impl BackgroundPlugin for LogExporterPlugin {
    fn id(&self) -> &str {
        &self.id
    }

    #[inline(always)]
    fn pbww(&self) -> u32 {
        80
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
            .filter_map(|slot| match slot {
                TargetSlot::Local(_) => None,
                TargetSlot::Etp { config, .. } => Some(config.etp_id.as_str()),
            })
            .collect()
    }

    fn attach_etp(&self, instance: &EtpInstance) -> Result<(), PluginError> {
        let mut matched = false;
        for slot in &self.slots {
            if let TargetSlot::Etp { config, attachment } = slot
                && config.etp_id == instance.id
            {
                let built = EtpLogAttachment::from_etp_instance(&self.id, config, instance)?;
                let _ = attachment.set(built);
                matched = true;
            }
        }
        if !matched {
            return Err(PluginError::Config(format!(
                "Log exporter plugin '{}' received unexpected ETP '{}'",
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
                let provider = match slot {
                    TargetSlot::Local(cfg) => LogProvider::from_local(cfg.clone()),
                    TargetSlot::Etp { config, attachment } => {
                        let attached = attachment.get().ok_or_else(|| {
                            PluginError::Config(format!(
                                "Log exporter plugin '{}' has no attached ETP for etp_id '{}'",
                                self.id, config.etp_id
                            ))
                        })?;
                        LogProvider::from_attachment(attached)?
                    }
                };
                providers.push(provider);
            }

            if providers.len() == 1 {
                let provider = providers.into_iter().next().unwrap();
                return run_target_loop(
                    self.id.clone(),
                    provider,
                    Arc::clone(&self.telemetry_store),
                    Arc::clone(&self.shipped_records),
                    Arc::clone(&self.failed_pushes),
                    shutdown,
                )
                .await;
            }

            let mut set = JoinSet::new();
            for provider in providers {
                let plugin_id = self.id.clone();
                let store = Arc::clone(&self.telemetry_store);
                let shipped = Arc::clone(&self.shipped_records);
                let failed = Arc::clone(&self.failed_pushes);
                let rx = shutdown.clone();
                set.spawn(async move {
                    run_target_loop(plugin_id, provider, store, shipped, failed, rx).await
                });
            }

            while let Some(res) = set.join_next().await {
                res.map_err(|e| PluginError::Config(format!("Log target task panicked: {e}")))??;
            }
            Ok(())
        })
    }
}

async fn run_target_loop(
    plugin_id: String,
    mut provider: LogProvider,
    telemetry_store: Arc<TelemetryStore>,
    shipped_records: Arc<AtomicU64>,
    failed_pushes: Arc<AtomicU64>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), PluginError> {
    provider.prepare().await?;

    let target_name = provider.name();
    let max_batches = provider.max_drain_batches();
    let subscriber = telemetry_store.subscribe(max_batches.max(256));
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
            let _ = subscriber.drain_batches(max_batches);
            tracing::debug!(
                plugin_id = %plugin_id,
                provider = %target_name,
                "Log provider in 30s cool-down; discarded batches to protect memory"
            );
        } else {
            if cooldown_until.is_some() {
                cooldown_until = None;
                consecutive_failures = 0;
                tracing::info!(
                    plugin_id = %plugin_id,
                    provider = %target_name,
                    "Log provider cool-down ended; resuming exports"
                );
            }

            let batches = subscriber.drain_batches(max_batches);
            if !batches.is_empty() {
                let res = provider.flush_batches(&batches).await;
                drop(batches);

                match res {
                    Ok(shipped) => {
                        consecutive_failures = 0;
                        if shipped > 0 {
                            shipped_records.fetch_add(shipped as u64, Ordering::Relaxed);
                        }
                    }
                    Err(e) => {
                        consecutive_failures += 1;
                        failed_pushes.fetch_add(1, Ordering::Relaxed);
                        tracing::warn!(
                            plugin_id = %plugin_id,
                            provider = %target_name,
                            error = %e,
                            attempt = consecutive_failures,
                            "Failed to export log batch"
                        );
                        if consecutive_failures > 10 {
                            tracing::warn!(
                                plugin_id = %plugin_id,
                                provider = %target_name,
                                "Log provider failed 10 consecutive times; entering 30s cool-down"
                            );
                            cooldown_until =
                                Some(tokio::time::Instant::now() + Duration::from_secs(30));
                            consecutive_failures = 0;
                        } else if provider.uses_exponential_backoff() && !is_shutting_down {
                            let backoff_ms = (100u64
                                .saturating_mul(1u64 << (consecutive_failures - 1).min(31)))
                            .min(2000);
                            tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                        }
                    }
                }
            }
        }

        if is_shutting_down {
            tracing::info!(
                plugin_id = %plugin_id,
                provider = %target_name,
                "Log target shutting down"
            );
            break;
        }
    }

    Ok(())
}
