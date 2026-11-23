//! Kafka Log Shipper Background Coordinator (`kafka_log_shipper::plugin`).
//!
//! ```text
//!   Startup wiring (wire_plugin_etps / with_etp) attaches Arc<KafkaEtp> into OnceLock
//!         │
//!         ▼
//!   run(shutdown) ──► KafkaWireState::new(Arc::clone(etp), &self.config)
//!         │            └─► initial wire_state.refresh_metadata().await (warn on miss)
//!         ▼
//!   Interval / shutdown select loop:
//!     ├─ in 30s cooldown? ──► drain & discard batches (RAM guard)
//!     └─ drain_batches(64) -> records_buf -> wire_state.produce_records(&records_buf)
//!           ├─ Ok(shipped) ──► ++shipped_records
//!           └─ Err(e)      ──► ++failed_pushes (> 10 consecutive -> 30s cooldown)
//!
//!   Coordinator Struct Layout:
//!   ┌────────────────────────────────────────────────────────────────────────────┐
//!   │ KafkaLogShipperPlugin                                                      │
//!   │   id: String | config: KafkaLogShipperConfig (etp_id, topic, acks, codec)  │
//!   │   etp: OnceLock<Arc<KafkaEtp>> | telemetry_store: Arc<TelemetryStore>      │
//!   │   shipped_records: Arc<AtomicU64> | failed_pushes: Arc<AtomicU64>          │
//!   └────────────────────────────────────────────────────────────────────────────┘
//! ```

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use tokio::sync::watch;
use velda_etp::kafka::KafkaEtp;
use velda_etp::{EtpBackend, EtpInstance};
use velda_observability::{TelemetryRecord, TelemetryStore, global_telemetry_store};

use crate::background::BackgroundPlugin;
use crate::capability::{Direction, PluginCapability, SupportedProtocols};
use crate::config::PluginConfig;
use crate::error::PluginError;

use super::config::KafkaLogShipperConfig;
use super::wire::KafkaWireState;

/// Background Kafka Log Shipper Plugin.
pub struct KafkaLogShipperPlugin {
    pub id: String,
    pub config: KafkaLogShipperConfig,
    pub etp: OnceLock<Arc<KafkaEtp>>,
    pub telemetry_store: Arc<TelemetryStore>,
    pub shipped_records: Arc<AtomicU64>,
    pub failed_pushes: Arc<AtomicU64>,
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
            etp: OnceLock::new(),
            telemetry_store,
            shipped_records: Arc::new(AtomicU64::new(0)),
            failed_pushes: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Constructs a new shipper pre-attached to an explicit [`KafkaEtp`] and [`TelemetryStore`].
    pub fn with_etp(
        id: impl Into<String>,
        config: KafkaLogShipperConfig,
        etp: Arc<KafkaEtp>,
        telemetry_store: Arc<TelemetryStore>,
    ) -> Result<Self, PluginError> {
        let plugin = Self::with_store(id, config, telemetry_store)?;
        let _ = plugin.etp.set(etp);
        Ok(plugin)
    }

    /// Instantiates and validates this plugin from declarative [`PluginConfig`].
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let validated =
            super::validation::validate_kafka_log_shipper_config(&config.id, &config.config)?;
        Self::with_store(&config.id, validated, Arc::clone(global_telemetry_store()))
    }
}

impl BackgroundPlugin for KafkaLogShipperPlugin {
    fn id(&self) -> &str {
        &self.id
    }

    #[inline(always)]
    fn pbww(&self) -> u32 {
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

    #[inline(always)]
    fn etp_id(&self) -> Option<&str> {
        Some(&self.config.etp_id)
    }

    fn attach_etp(&self, instance: &EtpInstance) -> Result<(), PluginError> {
        match &instance.backend {
            EtpBackend::Kafka(pool) => {
                let _ = self.etp.set(Arc::clone(pool));
                Ok(())
            }
            _ => Err(PluginError::Config(format!(
                "Kafka plugin '{}' requires a 'kafka' ETP for etp_id '{}', got '{}'",
                self.id, self.config.etp_id, instance.kind
            ))),
        }
    }

    fn run(
        self: Arc<Self>,
        mut shutdown: watch::Receiver<bool>,
    ) -> Pin<Box<dyn Future<Output = Result<(), PluginError>> + Send>> {
        Box::pin(async move {
            let etp = self.etp.get().ok_or_else(|| {
                PluginError::Config(format!(
                    "Kafka plugin '{}' has no attached KafkaEtp for etp_id '{}'",
                    self.id, self.config.etp_id
                ))
            })?;

            let mut wire_state = KafkaWireState::new(Arc::clone(etp), &self.config)?;
            if let Err(e) = wire_state.refresh_metadata().await {
                tracing::warn!(
                    plugin_id = %self.id,
                    error = %e,
                    topic = %self.config.topic,
                    "Could not fetch Kafka topic metadata on startup (will retry during push)"
                );
            }

            let subscriber = self.telemetry_store.subscribe(256);
            let mut interval = tokio::time::interval(Duration::from_millis(
                self.config.flush_interval_ms.max(100),
            ));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            let mut records_buf: Vec<TelemetryRecord> =
                Vec::with_capacity(self.config.max_batch_size);
            let mut consecutive_failures: u32 = 0;
            let mut cooldown_until: Option<tokio::time::Instant> = None;

            loop {
                let is_shutting_down = tokio::select! {
                    _ = interval.tick() => false,
                    _ = shutdown.changed() => true,
                };

                if cooldown_until.is_some_and(|until| tokio::time::Instant::now() < until) {
                    let _ = subscriber.drain_batches(64);
                    tracing::debug!(
                        plugin_id = %self.id,
                        "Kafka log shipper in 30s cool-down; discarded batches to protect memory"
                    );
                } else {
                    if cooldown_until.is_some() {
                        cooldown_until = None;
                        consecutive_failures = 0;
                        tracing::info!(
                            plugin_id = %self.id,
                            "Kafka cool-down period ended, resuming produce cycles"
                        );
                    }

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
                            match wire_state.produce_records(&records_buf).await {
                                Ok(shipped) => {
                                    consecutive_failures = 0;
                                    self.shipped_records
                                        .fetch_add(shipped as u64, Ordering::Relaxed);
                                }
                                Err(e) => {
                                    consecutive_failures += 1;
                                    self.failed_pushes.fetch_add(1, Ordering::Relaxed);
                                    tracing::warn!(
                                        plugin_id = %self.id,
                                        error = %e,
                                        records = rec_count,
                                        attempt = consecutive_failures,
                                        "Failed to produce batch to Kafka"
                                    );
                                    if consecutive_failures > 10 {
                                        tracing::warn!(
                                            plugin_id = %self.id,
                                            "Kafka shipper failed 10 consecutive times; entering 30s cool-down"
                                        );
                                        cooldown_until = Some(
                                            tokio::time::Instant::now() + Duration::from_secs(30),
                                        );
                                        consecutive_failures = 0;
                                    }
                                }
                            }
                        }
                    }
                }

                if is_shutting_down {
                    tracing::info!(
                        plugin_id = %self.id,
                        shipped = self.shipped_records.load(Ordering::Relaxed),
                        failed = self.failed_pushes.load(Ordering::Relaxed),
                        "Kafka log shipper received shutdown signal, exiting loop"
                    );
                    break;
                }
            }

            Ok(())
        })
    }
}
