//! StatsD / DogStatsD Metrics Exporter Background Coordinator (`statsd_exporter::plugin`).
//!
//! ```text
//!   Startup wiring (wire_plugin_etps / with_etp) attaches Arc<StatsdEtp> into OnceLock
//!         │
//!         ▼
//!   run(shutdown) ──► resolve effective prefix & dialect from config + etp.config
//!         │            -> pre-warm thread-local StatsD dialect template
//!         ▼
//!   Interval / shutdown select loop:
//!     interval.tick() ──► snapshot = metrics.snapshot(); delta = snapshot.delta_since(&prev)
//!         │
//!         └─► wire::flush_snapshot(&etp, prefix, dialect, max_packet_size, &delta, &mut buf)
//!               ├─ Ok(_)  ──► prev_snapshot = snapshot
//!               └─ Err(e) ──► warn; keep prev_snapshot so unsent deltas accumulate
//!
//!   Coordinator Struct Layout:
//!   ┌────────────────────────────────────────────────────────────────────────────┐
//!   │ StatsdExporterPlugin                                                       │
//!   │   id: String | config: StatsdExporterConfig (etp_id, prefix, dialect)      │
//!   │   etp: OnceLock<Arc<StatsdEtp>> | metrics: Arc<MetricsStore>              │
//!   └────────────────────────────────────────────────────────────────────────────┘
//! ```

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use tokio::sync::watch;
use velda_etp::statsd::{StatsdEtp, StatsdFlavor};
use velda_etp::{EtpBackend, EtpInstance};
use velda_observability::{MetricsSnapshot, MetricsStore};

use super::config::StatsdExporterConfig;
use super::dialect::{StatsdDialect, format_statsd_snapshot};
use super::wire;
use crate::background::BackgroundPlugin;
use crate::capability::{Direction, PluginCapability, SupportedProtocols};
use crate::error::PluginError;

/// Background Logistics Service exporting gateway telemetry via StatsD / DogStatsD UDP protocol.
pub struct StatsdExporterPlugin {
    pub id: String,
    pub config: StatsdExporterConfig,
    pub etp: OnceLock<Arc<StatsdEtp>>,
    pub metrics: Arc<MetricsStore>,
}

impl StatsdExporterPlugin {
    /// Creates a new exporter plugin instance with an explicit [`MetricsStore`].
    pub fn new(
        id: impl Into<String>,
        config: StatsdExporterConfig,
        metrics: Arc<MetricsStore>,
    ) -> Result<Self, PluginError> {
        Ok(Self {
            id: id.into(),
            config,
            etp: OnceLock::new(),
            metrics,
        })
    }

    /// Creates a new exporter plugin instance pre-attached to an explicit [`StatsdEtp`].
    pub fn with_etp(
        id: impl Into<String>,
        config: StatsdExporterConfig,
        etp: Arc<StatsdEtp>,
        metrics: Arc<MetricsStore>,
    ) -> Result<Self, PluginError> {
        let plugin = Self::new(id, config, metrics)?;
        let _ = plugin.etp.set(etp);
        Ok(plugin)
    }

    /// Instantiates and validates exporter plugin from declarative configuration.
    pub fn from_config(config: &crate::config::PluginConfig) -> Result<Self, PluginError> {
        let cfg = super::validation::validate_statsd_exporter_config(&config.id, &config.config)
            .map_err(|e| PluginError::Config(format!("Invalid statsd_exporter config: {e}")))?;

        Self::new(
            config.id.clone(),
            cfg,
            velda_observability::global_metrics(),
        )
    }

    /// Runs the periodic delta-snapshot UDP export loop until shutdown is signaled.
    pub async fn run_service(
        self: Arc<Self>,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<(), PluginError> {
        let etp = self.etp.get().ok_or_else(|| {
            PluginError::Config(format!(
                "StatsD plugin '{}' has no attached StatsdEtp for etp_id '{}'",
                self.id, self.config.etp_id
            ))
        })?;

        let prefix = match self.config.prefix.trim() {
            "" => etp
                .config
                .prefix
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .unwrap_or("velda"),
            explicit => explicit,
        };

        let dialect = match self
            .config
            .dialect
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            Some(raw) => raw.parse::<StatsdDialect>().map_err(PluginError::Config)?,
            None => match etp.config.flavor {
                StatsdFlavor::Standard => StatsdDialect::Standard,
                StatsdFlavor::DogStatsd => StatsdDialect::DogStatsd,
            },
        };

        tracing::info!(
            id = %self.id,
            etp_id = %etp.config.id,
            endpoint = %etp.config.endpoint,
            prefix,
            interval_secs = self.config.interval_secs,
            dialect = ?dialect,
            max_packet_size = self.config.max_packet_size,
            "Starting StatsD metrics exporter via StatsdEtp pool"
        );

        let mut packet_buffer = Vec::with_capacity(self.config.max_packet_size + 256);
        let mut prev_snapshot = MetricsSnapshot::zero();

        // Pre-warm the thread-local dialect template once before the export loop starts.
        let _ = format_statsd_snapshot(
            &prev_snapshot,
            prefix,
            dialect,
            &mut packet_buffer,
            self.config.max_packet_size,
            &mut |_| {},
        );

        let mut interval = tokio::time::interval(Duration::from_secs(self.config.interval_secs));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                biased;
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        tracing::info!(
                            id = %self.id,
                            "StatsD exporter received shutdown signal; stopping service loop"
                        );
                        break;
                    }
                }
                _ = interval.tick() => {
                    let snapshot = self.metrics.snapshot();
                    let delta_snapshot = snapshot.delta_since(&prev_snapshot);

                    match wire::flush_snapshot(
                        etp,
                        prefix,
                        dialect,
                        self.config.max_packet_size,
                        &delta_snapshot,
                        &mut packet_buffer,
                    )
                    .await
                    {
                        Ok(_) => {
                            prev_snapshot = snapshot;
                        }
                        Err(e) => {
                            tracing::warn!(
                                id = %self.id,
                                etp_id = %etp.config.id,
                                error = %e,
                                "Failed to transmit batched StatsD UDP datagrams"
                            );
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

impl BackgroundPlugin for StatsdExporterPlugin {
    fn id(&self) -> &str {
        &self.id
    }

    #[inline(always)]
    fn pbww(&self) -> u32 {
        20
    }

    fn direction(&self) -> Direction {
        Direction::Background
    }

    fn capability(&self) -> PluginCapability {
        PluginCapability::default()
    }

    fn supported_protocols(&self) -> SupportedProtocols {
        SupportedProtocols::empty()
    }

    #[inline(always)]
    fn etp_id(&self) -> Option<&str> {
        Some(&self.config.etp_id)
    }

    fn attach_etp(&self, instance: &EtpInstance) -> Result<(), PluginError> {
        match &instance.backend {
            EtpBackend::Statsd(pool) => {
                let _ = self.etp.set(Arc::clone(pool));
                Ok(())
            }
            _ => Err(PluginError::Config(format!(
                "StatsD plugin '{}' requires a 'statsd' ETP for etp_id '{}', got '{}'",
                self.id, self.config.etp_id, instance.kind
            ))),
        }
    }

    fn run(
        self: Arc<Self>,
        shutdown: watch::Receiver<bool>,
    ) -> Pin<Box<dyn Future<Output = Result<(), PluginError>> + Send>> {
        Box::pin(async move { self.run_service(shutdown).await })
    }
}
