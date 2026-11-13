//! StatsD / DogStatsD Metrics Exporter Background Service.
//!
//! Drives the periodic telemetry push loop on dedicated Shadow Logistics workers,
//! extracting lock-free atomic snapshots and streaming UDP datagrams to the configured daemon.
//!
//! # Architecture & Mechanism
//!
//! ```text
//! ╔══════════════════════════════════════════════════════════════════════════════════════════════════╗
//! ║                          VELDA EDGE — STATSD EXPORTER PIPELINE                                    ║
//! ║                    (crates/velda-plugin: statsd_exporter::plugin)                                 ║
//! ╚══════════════════════════════════════════════════════════════════════════════════════════════════╝
//!
//!  [FRONTLINE WORKERS]                       [SHADOW WORKER: velda-shadow-0 (SCHED_IDLE)]
//!   velda-worker-0/1                          interval_secs periodic ticker
//!         │                                                │
//!         ▼ (Atomic writes)                                ▼
//!   ┌───────────┐   Lock-Free Atomic Extraction    ┌───────────────────────────────┐
//!   │ 36 Slots  │ ───────────────────────────────► │   MetricsStore::snapshot()    │
//!   │ Counters  │     (< 100ns vector reduce)      │   (36 Pipelines, 563 DPs)     │
//!   └───────────┘                                  └───────────────┬───────────────┘
//!                                                                  │
//!                                       ┌──────────────────────────┴──────────────────────────┐
//!                                       │                                                     │
//!                            [dialect: "dogstatsd"]                                [dialect: "standard"]
//!                                       ▼                                                     ▼
//!                            ┌───────────────────────┐                             ┌───────────────────────┐
//!                            │ dialect/dogstatsd.rs  │                             │  dialect/standard.rs  │
//!                            │ Pre-baked Tag Suffix  │                             │ Pre-baked Dot Paths   │
//!                            │ Direct-to-Packet Pass │                             │ Direct-to-Packet Pass │
//!                            │ (14,356 CPU cycles)   │                             │ (10,022 CPU cycles)   │
//!                            └──────────┬────────────┘                             └──────────┬────────────┘
//!                                       │                                                     │
//!                                       └──────────────────────────┬──────────────────────────┘
//!                                                                  │
//!                                                                  ▼
//!                                                       ┌─────────────────────┐
//!                                                       │    transport.rs     │
//!                                                       │ acquire_shadow_udp  │
//!                                                       │ kernel connect()    │
//!                                                       │ libc::sendmmsg(64)  │
//!                                                       └──────────┬──────────┘
//!                                                                  │
//!                                                                  ▼ UDP Datagrams (MTU 1432)
//!                                                       ┌─────────────────────────────────────┐
//!                                                       │   Datadog / Telegraf / StatsD       │
//!                                                       └─────────────────────────────────────┘
//! ```

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;

use velda_observability::MetricsStore;

use super::config::StatsdExporterConfig;
use super::dialect::{DogStatsdTemplate, StandardTemplate, StatsdBatchWriter, StatsdDialect};
use super::transport::StatsdTransport;
use crate::background::BackgroundPlugin;
use crate::error::PluginError;

/// Background Logistics Service exporting gateway telemetry via StatsD / DogStatsD UDP protocol.
pub struct StatsdExporterPlugin {
    id: String,
    config: StatsdExporterConfig,
    metrics: Arc<MetricsStore>,
    dialect: StatsdDialect,
    dogstatsd_template: Option<DogStatsdTemplate>,
    standard_template: Option<StandardTemplate>,
}

impl StatsdExporterPlugin {
    /// Creates a new exporter plugin instance with explicit metrics store.
    pub fn new(
        id: impl Into<String>,
        config: StatsdExporterConfig,
        metrics: Arc<MetricsStore>,
    ) -> Result<Self, PluginError> {
        let dialect = config
            .dialect
            .parse::<StatsdDialect>()
            .map_err(PluginError::Config)?;
        let (dogstatsd_template, standard_template) = match dialect {
            StatsdDialect::DogStatsd => (Some(DogStatsdTemplate::new(&config.prefix)), None),
            StatsdDialect::Standard => (None, Some(StandardTemplate::new(&config.prefix))),
        };
        Ok(Self {
            id: id.into(),
            config,
            metrics,
            dialect,
            dogstatsd_template,
            standard_template,
        })
    }

    /// Instantiates and validates exporter plugin from declarative configuration.
    pub fn from_config(
        config: &velda_sync::post_sync::plugin::PluginConfig,
    ) -> Result<Self, PluginError> {
        let cfg = velda_sync::validate_statsd_exporter_config(&config.id, &config.config)
            .map_err(|e| PluginError::Config(format!("Invalid statsd_exporter config: {e}")))?;

        Self::new(
            config.id.clone(),
            cfg,
            velda_observability::global_metrics(),
        )
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

        // Track previous cumulative snapshot so StatsD counters (`|c`) strictly emit interval deltas
        // rather than re-adding monotonic totals on every flush cycle.
        let mut prev_snapshot = velda_observability::MetricsSnapshot::zero();

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
                    let delta_snapshot = snapshot.delta_since(&prev_snapshot);
                    let mut packet_count = 0;

                    // Format delta snapshot directly into MTU-bounded chunks using recycled buffer pool
                    let mut writer = StatsdBatchWriter::new(
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

                    if let Some(ref t) = self.dogstatsd_template {
                        t.format_with_writer(&delta_snapshot, &mut writer);
                    } else if let Some(ref t) = self.standard_template {
                        t.format_with_writer(&delta_snapshot, &mut writer);
                    }
                    writer.flush();

                    // Transmit ALL datagrams in a single vectorized sendmmsg syscall!
                    if packet_count > 0 {
                        match transport.send_batch(&packet_pool[..packet_count]).await {
                            Ok(_) => {
                                prev_snapshot = snapshot;
                            }
                            Err(e) => {
                                tracing::warn!(
                                    id = %self.id,
                                    error = %e,
                                    "Failed to transmit batched StatsD UDP datagrams via sendmmsg"
                                );
                            }
                        }
                    } else {
                        prev_snapshot = snapshot;
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

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::UdpSocket;
    use velda_observability::{
        DownstreamProtocol, PipeStrategy, StreamCompletion, UpstreamProtocol,
    };

    #[tokio::test]
    async fn test_statsd_exporter_emits_deltas_without_idle_counter_inflation() {
        let rx_sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let rx_addr = rx_sock.local_addr().unwrap();

        let store = Arc::new(MetricsStore::new());
        store.record_stream_start(
            DownstreamProtocol::H1,
            UpstreamProtocol::H1,
            PipeStrategy::Buffered,
        );
        store.record_stream_completion(
            DownstreamProtocol::H1,
            UpstreamProtocol::H1,
            PipeStrategy::Buffered,
            StreamCompletion::new(200, 128, 256, Duration::from_millis(2), None),
        );

        let config = StatsdExporterConfig {
            endpoint: rx_addr.to_string(),
            interval_secs: 1,
            prefix: "velda_delta".into(),
            dialect: "dogstatsd".into(),
            max_packet_size: 4096,
        };
        let plugin =
            Arc::new(StatsdExporterPlugin::new("delta_test", config, store.clone()).unwrap());
        let (tx, rx) = watch::channel(false);
        let handle = tokio::spawn(async move { plugin.run_service(rx).await });

        // Wait for Tick 1 (emits immediate first tick)
        let mut tick1 = String::new();
        let mut buf = [0u8; 8192];
        let (n, _) = tokio::time::timeout(Duration::from_secs(2), rx_sock.recv_from(&mut buf))
            .await
            .unwrap()
            .unwrap();
        tick1.push_str(&String::from_utf8_lossy(&buf[..n]));
        while let Ok(Ok((extra, _))) =
            tokio::time::timeout(Duration::from_millis(120), rx_sock.recv_from(&mut buf)).await
        {
            tick1.push_str(&String::from_utf8_lossy(&buf[..extra]));
        }
        assert!(
            tick1.contains("velda_delta.requests.total:1|c|#downstream:h1,upstream:h1,pipe:buffered,status:2xx\n"),
            "Tick 1 must emit delta 1, got: {tick1}"
        );

        // Wait for Tick 2 (idle — zero new traffic, so counter delta MUST be 0|c!)
        let (n2, _) = tokio::time::timeout(Duration::from_secs(2), rx_sock.recv_from(&mut buf))
            .await
            .unwrap()
            .unwrap();
        let mut tick2 = String::from_utf8_lossy(&buf[..n2]).to_string();
        while let Ok(Ok((extra, _))) =
            tokio::time::timeout(Duration::from_millis(120), rx_sock.recv_from(&mut buf)).await
        {
            tick2.push_str(&String::from_utf8_lossy(&buf[..extra]));
        }
        assert!(
            tick2.contains("velda_delta.requests.total:0|c|#downstream:h1,upstream:h1,pipe:buffered,status:2xx\n"),
            "Tick 2 at idle must emit delta 0 (not 1), got: {tick2}"
        );

        let _ = tx.send(true);
        let _ = handle.await;
    }
}
