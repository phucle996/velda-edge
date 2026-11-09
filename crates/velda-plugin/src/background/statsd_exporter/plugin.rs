//! StatsD / DogStatsD Metrics Exporter Background Service.
//!
//! Drives the periodic telemetry push loop on dedicated Shadow Logistics workers,
//! extracting lock-free atomic snapshots and streaming UDP datagrams to the configured daemon.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;

use velda_observability::MetricsStore;
use velda_sync::StatsdExporterConfig;

use super::formatter::{StatsdDialect, format_statsd_snapshot};
use super::transport::StatsdTransport;
use crate::background::BackgroundPlugin;
use crate::error::PluginError;

/// Background Logistics Service exporting gateway telemetry via StatsD / DogStatsD UDP protocol.
pub struct StatsdExporterPlugin {
    id: String,
    config: StatsdExporterConfig,
    metrics: Arc<MetricsStore>,
    dialect: StatsdDialect,
}

impl StatsdExporterPlugin {
    /// Creates a new exporter plugin instance with explicit metrics store.
    pub fn new(
        id: impl Into<String>,
        config: StatsdExporterConfig,
        metrics: Arc<MetricsStore>,
    ) -> Self {
        let dialect = StatsdDialect::from_dialect_str(&config.dialect);
        Self {
            id: id.into(),
            config,
            metrics,
            dialect,
        }
    }

    /// Instantiates and validates exporter plugin from declarative configuration.
    pub fn from_config(
        config: &velda_sync::post_sync::plugin::PluginConfig,
    ) -> Result<Self, PluginError> {
        let cfg = velda_sync::validate_statsd_exporter_config(&config.id, &config.config)
            .map_err(|e| PluginError::Config(format!("Invalid statsd_exporter config: {e}")))?;

        Ok(Self::new(
            config.id.clone(),
            cfg,
            velda_observability::global_metrics(),
        ))
    }

    /// Runs the periodic export loop until shutdown is signaled.
    pub async fn run_service(
        self: Arc<Self>,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<(), PluginError> {
        tracing::info!(
            id = %self.id,
            endpoint = %self.config.endpoint,
            interval_secs = self.config.interval_secs,
            dialect = ?self.dialect,
            max_packet_size = self.config.max_packet_size,
            "Starting StatsD metrics exporter on Shadow Logistics Worker"
        );

        // 1. Establish UDP transport via Core SocketManager
        let transport = StatsdTransport::connect(&self.config.endpoint, &self.id).await?;
        tracing::info!(
            id = %self.id,
            destination = %transport.destination(),
            "StatsD exporter UDP transport connected successfully"
        );

        // 2. Pre-allocate reusable scratch buffers and packet arena (zero-allocation hot loop)
        let mut packet_buffer = Vec::with_capacity(self.config.max_packet_size + 256);
        let mut packet_pool: Vec<Vec<u8>> = (0..64)
            .map(|_| Vec::with_capacity(self.config.max_packet_size + 256))
            .collect();
        let mut interval = tokio::time::interval(Duration::from_secs(self.config.interval_secs));
        // Avoid burst tick on startup
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        // 3. Main execution loop
        loop {
            tokio::select! {
                biased;
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        tracing::info!(id = %self.id, "StatsD exporter received shutdown signal; stopping service loop");
                        break;
                    }
                }
                _ = interval.tick() => {
                    let snapshot = self.metrics.snapshot();
                    let mut packet_count = 0;

                    // Format snapshot into MTU-bounded chunks using recycled buffer pool
                    let (_packets_count, _metrics_count) = format_statsd_snapshot(
                        &snapshot,
                        &self.config.prefix,
                        self.dialect,
                        &mut packet_buffer,
                        self.config.max_packet_size,
                        |packet| {
                            if packet_count < packet_pool.len() {
                                packet_pool[packet_count].clear();
                                packet_pool[packet_count].extend_from_slice(packet);
                            } else {
                                let mut buf = Vec::with_capacity(self.config.max_packet_size + 256);
                                buf.extend_from_slice(packet);
                                packet_pool.push(buf);
                            }
                            packet_count += 1;
                        },
                    );

                    // Transmit ALL datagrams in a single vectorized sendmmsg syscall!
                    if packet_count > 0 {
                        let res = transport.send_batch(&packet_pool[..packet_count]).await;
                        if let Err(e) = res {
                            tracing::warn!(
                                id = %self.id,
                                error = %e,
                                "Failed to transmit batched StatsD UDP datagrams via sendmmsg"
                            );
                        }
                    }
                }
            }
        }

        transport.release();
        Ok(())
    }
}

impl BackgroundPlugin for StatsdExporterPlugin {
    fn id(&self) -> &str {
        &self.id
    }

    fn run(
        self: Arc<Self>,
        shutdown: watch::Receiver<bool>,
    ) -> Pin<Box<dyn Future<Output = Result<(), PluginError>> + Send>> {
        Box::pin(async move { self.run_service(shutdown).await })
    }
}
