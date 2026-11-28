//! Consolidated Metrics Exporter Coordinator (`metrics_exporter::plugin`).
//!
//! ```text
//!   MetricsExporterPlugin (targets: [Pull | Push])
//!         │
//!         ├─► etp_ids()    ──► collects every target's etp_id (mode == "push")
//!         ├─► attach_etp() ──► resolves matching targets by instance.id + EtpBackend (Otlp | Statsd)
//!         │
//!         └─► run(shutdown) ──► JoinSet per target:
//!               ├─► Pull   ──► PrometheusExporter::run_server() (GET /metrics + status_mask)
//!               ├─► Otlp   ──► interval tick -> delta_since/cumulative -> status_mask -> OtlpEtp
//!               └─► Statsd ──► interval tick -> delta_since -> status_mask -> StatsdEtp
//! ```

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use tokio::sync::watch;
use tokio::task::JoinSet;
use velda_etp::EtpInstance;
use velda_observability::{MetricsStore, global_metrics};

use super::config::{
    MetricsExporterConfig, MetricsPushTargetConfig, MetricsTargetConfig, PrometheusPullConfig,
};
use super::providers::{EtpMetricsAttachment, MetricsProvider};
use crate::background::BackgroundPlugin;
use crate::capability::{Direction, PluginCapability, SupportedProtocols};
use crate::config::PluginConfig;
use crate::error::PluginError;

enum TargetSlot {
    Pull(PrometheusPullConfig),
    Push {
        config: MetricsPushTargetConfig,
        attachment: OnceLock<EtpMetricsAttachment>,
    },
}

/// Consolidated Metrics Exporter Background Plugin (`metrics_exporter`).
pub struct MetricsExporterPlugin {
    pub id: String,
    pub config: MetricsExporterConfig,
    slots: Vec<TargetSlot>,
    pub metrics_store: Arc<MetricsStore>,
    pub flushed_count: Arc<AtomicU64>,
    pub failed_count: Arc<AtomicU64>,
}

impl MetricsExporterPlugin {
    /// Constructs a new `metrics_exporter` bound to the global [`MetricsStore`].
    pub fn new(id: impl Into<String>, config: MetricsExporterConfig) -> Result<Self, PluginError> {
        Self::with_store(id, config, global_metrics())
    }

    /// Constructs a new `metrics_exporter` bound to an explicit [`MetricsStore`].
    pub fn with_store(
        id: impl Into<String>,
        config: MetricsExporterConfig,
        metrics_store: Arc<MetricsStore>,
    ) -> Result<Self, PluginError> {
        let slots = config
            .targets
            .iter()
            .map(|t| match t {
                MetricsTargetConfig::Pull(pull) => TargetSlot::Pull(pull.clone()),
                MetricsTargetConfig::Push(push) => TargetSlot::Push {
                    config: push.clone(),
                    attachment: OnceLock::new(),
                },
            })
            .collect();

        Ok(Self {
            id: id.into(),
            config,
            slots,
            metrics_store,
            flushed_count: Arc::new(AtomicU64::new(0)),
            failed_count: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Deserializes and validates `metrics_exporter` from [`PluginConfig`].
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let validated =
            super::validation::validate_metrics_exporter_config(&config.id, &config.config)?;
        Self::with_store(&config.id, validated, global_metrics())
    }
}

impl BackgroundPlugin for MetricsExporterPlugin {
    fn id(&self) -> &str {
        &self.id
    }

    #[inline(always)]
    fn pbww(&self) -> u32 {
        40
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
                TargetSlot::Pull(_) => None,
                TargetSlot::Push { config, .. } => Some(config.etp_id.as_str()),
            })
            .collect()
    }

    fn attach_etp(&self, instance: &EtpInstance) -> Result<(), PluginError> {
        let mut matched = false;
        for slot in &self.slots {
            if let TargetSlot::Push { config, attachment } = slot
                && config.etp_id == instance.id
            {
                let built = EtpMetricsAttachment::from_etp_instance(&self.id, config, instance)?;
                let _ = attachment.set(built);
                matched = true;
            }
        }
        if !matched {
            return Err(PluginError::Config(format!(
                "Metrics exporter plugin '{}' received unexpected ETP '{}'",
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
                    TargetSlot::Pull(cfg) => {
                        MetricsProvider::from_pull(cfg.clone(), Arc::clone(&self.metrics_store))?
                    }
                    TargetSlot::Push { config, attachment } => {
                        let attached = attachment.get().ok_or_else(|| {
                            PluginError::Config(format!(
                                "Metrics exporter plugin '{}' has no attached ETP for etp_id '{}'",
                                self.id, config.etp_id
                            ))
                        })?;
                        MetricsProvider::from_attachment(attached)?
                    }
                };
                providers.push(provider);
            }

            if providers.len() == 1 {
                let provider = providers.into_iter().next().unwrap();
                return run_metrics_target(
                    self.id.clone(),
                    provider,
                    Arc::clone(&self.metrics_store),
                    Arc::clone(&self.flushed_count),
                    Arc::clone(&self.failed_count),
                    shutdown,
                )
                .await;
            }

            let mut set = JoinSet::new();
            for provider in providers {
                let plugin_id = self.id.clone();
                let store = Arc::clone(&self.metrics_store);
                let flushed = Arc::clone(&self.flushed_count);
                let failed = Arc::clone(&self.failed_count);
                let rx = shutdown.clone();
                set.spawn(async move {
                    run_metrics_target(plugin_id, provider, store, flushed, failed, rx).await
                });
            }

            while let Some(res) = set.join_next().await {
                res.map_err(|e| {
                    PluginError::Config(format!("Metrics target task panicked: {e}"))
                })??;
            }
            Ok(())
        })
    }
}

async fn run_metrics_target(
    plugin_id: String,
    provider: MetricsProvider,
    store: Arc<MetricsStore>,
    flushed_count: Arc<AtomicU64>,
    failed_count: Arc<AtomicU64>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), PluginError> {
    match provider {
        MetricsProvider::Pull(exporter) => exporter.run_server(shutdown).await,
        MetricsProvider::Otlp(mut otlp) => {
            let mut interval = tokio::time::interval(otlp.interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut consecutive_failures: u32 = 0;
            let mut cooldown_until: Option<tokio::time::Instant> = None;

            loop {
                let is_shutting_down = tokio::select! {
                    _ = interval.tick() => false,
                    _ = shutdown.changed() => *shutdown.borrow(),
                };

                if cooldown_until.is_some_and(|until| tokio::time::Instant::now() < until) {
                    tracing::debug!(
                        plugin_id = %plugin_id,
                        etp_id = %otlp.etp.config.id,
                        "OTLP metrics push in 30s cool-down; skipping cycle"
                    );
                } else {
                    if cooldown_until.is_some() {
                        cooldown_until = None;
                        consecutive_failures = 0;
                    }
                    match otlp.push_tick(&store).await {
                        Ok(()) => {
                            consecutive_failures = 0;
                            flushed_count.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(e) => {
                            consecutive_failures += 1;
                            failed_count.fetch_add(1, Ordering::Relaxed);
                            tracing::warn!(
                                plugin_id = %plugin_id,
                                etp_id = %otlp.etp.config.id,
                                error = %e,
                                attempt = consecutive_failures,
                                "Failed to push OTLP metrics payload"
                            );
                            if consecutive_failures <= 10 {
                                if !is_shutting_down {
                                    let backoff_ms = (100u64.saturating_mul(
                                        1u64 << (consecutive_failures - 1).min(31),
                                    ))
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

                if is_shutting_down {
                    break;
                }
            }
            Ok(())
        }
        MetricsProvider::Statsd(mut statsd) => {
            let mut interval = tokio::time::interval(statsd.interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    biased;
                    _ = shutdown.changed() => {
                        if *shutdown.borrow() {
                            break;
                        }
                    }
                    _ = interval.tick() => {
                        match statsd.push_tick(&store).await {
                            Ok(sent) => {
                                flushed_count.fetch_add(sent as u64, Ordering::Relaxed);
                            }
                            Err(e) => {
                                failed_count.fetch_add(1, Ordering::Relaxed);
                                tracing::warn!(
                                    plugin_id = %plugin_id,
                                    etp_id = %statsd.etp.config.id,
                                    error = %e,
                                    "StatsD flush failed; retaining previous snapshot"
                                );
                            }
                        }
                    }
                }
            }
            Ok(())
        }
    }
}
