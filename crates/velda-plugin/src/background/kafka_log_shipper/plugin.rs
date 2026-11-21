//! Kafka Log Shipper Background Coordinator (`kafka_log_shipper::plugin`).
//!
//! Subscribes to core-local log streams via [`TelemetryStore`], encodes access log records
//! into Kafka RecordBatch v2 format, and produces them directly to partition leader brokers.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::sync::watch;
use velda_observability::{TelemetryRecord, TelemetryStore, global_telemetry_store};

use crate::background::BackgroundPlugin;
use crate::capability::{Direction, PluginCapability, SupportedProtocols};
use crate::config::PluginConfig;
use crate::error::PluginError;

use super::client::KafkaClusterClient;
use super::config::KafkaLogShipperConfig;

/// Background Kafka Log Shipper Plugin.
pub struct KafkaLogShipperPlugin {
    pub id: String,
    pub config: KafkaLogShipperConfig,
    telemetry_store: Arc<TelemetryStore>,
    shipped_records: Arc<AtomicU64>,
    failed_pushes: Arc<AtomicU64>,
}

impl KafkaLogShipperPlugin {
    /// Constructs a new shipper bound to the global singleton [`TelemetryStore`].
    pub fn new(id: impl Into<String>, config: KafkaLogShipperConfig) -> Result<Self, PluginError> {
        Self::with_store(id, config, Arc::clone(global_telemetry_store()))
    }

    /// Constructs a new shipper bound to an explicit [`TelemetryStore`].
    pub fn with_store(
        id: impl Into<String>,
        config: KafkaLogShipperConfig,
        telemetry_store: Arc<TelemetryStore>,
    ) -> Result<Self, PluginError> {
        Ok(Self {
            id: id.into(),
            config,
            telemetry_store,
            shipped_records: Arc::new(AtomicU64::new(0)),
            failed_pushes: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Instantiates and validates this plugin from declarative [`PluginConfig`].
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let validated =
            super::validation::validate_kafka_log_shipper_config(&config.id, &config.config)
                .map_err(|e| PluginError::Config(e.to_string()))?;

        Self::with_store(&config.id, validated, Arc::clone(global_telemetry_store()))
    }

    /// Returns cumulative total of successfully shipped log records.
    pub fn shipped_records(&self) -> u64 {
        self.shipped_records.load(Ordering::Relaxed)
    }

    /// Returns cumulative total of failed push operations.
    pub fn failed_pushes(&self) -> u64 {
        self.failed_pushes.load(Ordering::Relaxed)
    }
}

impl BackgroundPlugin for KafkaLogShipperPlugin {
    fn id(&self) -> &str {
        &self.id
    }

    fn weight(&self) -> u32 {
        75
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

    fn run(
        self: Arc<Self>,
        mut shutdown: watch::Receiver<bool>,
    ) -> Pin<Box<dyn Future<Output = Result<(), PluginError>> + Send>> {
        Box::pin(async move {
            let subscriber = self.telemetry_store.subscribe(256);
            let mut interval = tokio::time::interval(Duration::from_millis(
                self.config.flush_interval_ms.max(100),
            ));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            let mut client = Some(KafkaClusterClient::new(&self.config)?);

            // Attempt initial metadata discovery upon startup
            if let Some(c) = client.as_mut()
                && let Err(e) = c.refresh_metadata().await
            {
                tracing::warn!(
                    plugin_id = %self.id,
                    error = %e,
                    topic = %self.config.topic,
                    "Could not fetch Kafka topic metadata on startup (will retry during push)"
                );
            }

            let mut records_buf: Vec<TelemetryRecord> =
                Vec::with_capacity(self.config.max_batch_size);
            let mut consecutive_failures: u32 = 0;
            let mut cooldown_until: Option<tokio::time::Instant> = None;

            loop {
                let is_shutting_down = tokio::select! {
                    _ = interval.tick() => false,
                    _ = shutdown.changed() => true,
                };

                // 1. Cool-down state: discard incoming batches to prevent RAM buildup
                if cooldown_until.is_some_and(|until| tokio::time::Instant::now() < until) {
                    let _ = subscriber.drain_batches(64);
                    tracing::debug!(
                        plugin_id = %self.id,
                        "Kafka log shipper in 30s cool-down; discarded batches to protect memory"
                    );
                    if is_shutting_down {
                        break;
                    }
                    continue;
                }

                // 2. Cool-down expired: re-instantiate client and reset failure count
                if cooldown_until.is_some() {
                    cooldown_until = None;
                    consecutive_failures = 0;
                    client = Some(KafkaClusterClient::new(&self.config)?);
                    tracing::info!(
                        plugin_id = %self.id,
                        "Kafka cool-down period ended, reconnecting client"
                    );
                }

                // 3. Normal drain and push
                let batches = subscriber.drain_batches(64);

                if !batches.is_empty() {
                    records_buf.clear();
                    for b in &batches {
                        records_buf.extend_from_slice(b.as_ref());
                        if records_buf.len() >= self.config.max_batch_size {
                            break;
                        }
                    }

                    drop(batches);

                    let rec_count = records_buf.len();
                    if rec_count > 0 {
                        if client.is_none() {
                            client = Some(KafkaClusterClient::new(&self.config)?);
                        }

                        let cl = client.as_mut().unwrap();
                        match cl.push_records(&records_buf).await {
                            Ok(shipped) => {
                                consecutive_failures = 0;
                                self.shipped_records
                                    .fetch_add(shipped as u64, Ordering::Relaxed);
                            }
                            Err(e) => {
                                consecutive_failures += 1;
                                self.failed_pushes.fetch_add(1, Ordering::Relaxed);
                                cl.close_all_connections();

                                tracing::warn!(
                                    plugin_id = %self.id,
                                    error = %e,
                                    records = rec_count,
                                    attempt = consecutive_failures,
                                    "Failed to push batch to Kafka (dropped without retry)"
                                );

                                if consecutive_failures > 10 {
                                    tracing::warn!(
                                        plugin_id = %self.id,
                                        "Kafka shipper failed 10 consecutive times; entering 30s cool-down"
                                    );
                                    client = None;
                                    cooldown_until =
                                        Some(tokio::time::Instant::now() + Duration::from_secs(30));
                                    consecutive_failures = 0;
                                }
                            }
                        }
                    }
                }

                if is_shutting_down {
                    tracing::info!(
                        plugin_id = %self.id,
                        shipped = self.shipped_records(),
                        failed = self.failed_pushes(),
                        "Kafka log shipper received shutdown signal, exiting loop"
                    );
                    break;
                }
            }

            Ok(())
        })
    }
}
