//! S3-Compatible Background Log Shipper Coordinator (`s3_log_shipper::plugin`).
//!
//! ```text
//!   Startup wiring (wire_plugin_etps / with_etp) attaches Arc<S3Etp> into OnceLock
//!         │
//!         ▼
//!   run(shutdown) ──► wire::S3WireState::new(Arc::clone(etp), &self.config)
//!         │
//!         └─► Interval / shutdown select loop:
//!               ├─ in 30s cooldown? ──► drain & discard batches (RAM guard)
//!               └─ drain_batches(max) -> format_ndjson -> wire::upload_batch(&mut state, &buf)
//!                     ├─ 2xx      ──► ++shipped_records
//!                     └─ err/!2xx ──► ++failed_pushes (> 10 consecutive -> 30s cooldown)
//!
//!   Coordinator Struct Layout:
//!   ┌────────────────────────────────────────────────────────────────────────────┐
//!   │ S3LogShipperPlugin                                                         │
//!   │   id: String | config: S3LogShipperConfig (etp_id, prefix, compression)    │
//!   │   etp: OnceLock<Arc<S3Etp>> | telemetry_store: Arc<TelemetryStore>         │
//!   │   shipped_records: Arc<AtomicU64> | failed_pushes: Arc<AtomicU64>          │
//!   └────────────────────────────────────────────────────────────────────────────┘
//! ```

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use tokio::sync::watch;
use velda_etp::s3::S3Etp;
use velda_etp::{EtpBackend, EtpInstance};
use velda_observability::{TelemetryStore, global_telemetry_store};

use crate::background::BackgroundPlugin;
use crate::capability::{Direction, PluginCapability, SupportedProtocols};
use crate::config::PluginConfig;
use crate::error::PluginError;

use super::config::S3LogShipperConfig;
use super::format::format_ndjson;
use super::wire::{self, S3WireState};

/// Background S3 / MinIO / R2 Object Storage Log Shipper.
pub struct S3LogShipperPlugin {
    pub id: String,
    pub config: S3LogShipperConfig,
    pub etp: OnceLock<Arc<S3Etp>>,
    pub telemetry_store: Arc<TelemetryStore>,
    pub shipped_records: Arc<AtomicU64>,
    pub failed_pushes: Arc<AtomicU64>,
}

impl S3LogShipperPlugin {
    /// Constructs a new shipper bound to the global singleton [`TelemetryStore`].
    pub fn new(id: impl Into<String>, config: S3LogShipperConfig) -> Result<Self, PluginError> {
        Self::with_store(id, config, Arc::clone(global_telemetry_store()))
    }

    /// Constructs a new shipper bound to an explicit [`TelemetryStore`].
    pub fn with_store(
        id: impl Into<String>,
        config: S3LogShipperConfig,
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

    /// Constructs a new shipper pre-attached to an explicit [`S3Etp`] and [`TelemetryStore`].
    pub fn with_etp(
        id: impl Into<String>,
        config: S3LogShipperConfig,
        etp: Arc<S3Etp>,
        telemetry_store: Arc<TelemetryStore>,
    ) -> Result<Self, PluginError> {
        let plugin = Self::with_store(id, config, telemetry_store)?;
        let _ = plugin.etp.set(etp);
        Ok(plugin)
    }

    /// Instantiates and validates this plugin from declarative [`PluginConfig`].
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let validated =
            super::validation::validate_s3_log_shipper_config(&config.id, &config.config)?;
        Self::with_store(&config.id, validated, Arc::clone(global_telemetry_store()))
    }
}

impl BackgroundPlugin for S3LogShipperPlugin {
    fn id(&self) -> &str {
        &self.id
    }

    #[inline(always)]
    fn pbww(&self) -> u32 {
        65
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
            EtpBackend::S3(pool) => {
                let _ = self.etp.set(Arc::clone(pool));
                Ok(())
            }
            _ => Err(PluginError::Config(format!(
                "S3 plugin '{}' requires an 's3' ETP for etp_id '{}', got '{}'",
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
                    "S3 plugin '{}' has no attached S3Etp for etp_id '{}'",
                    self.id, self.config.etp_id
                ))
            })?;

            let mut wire_state = S3WireState::new(Arc::clone(etp), &self.config);
            let max_batches = self.config.max_drain_batches.clamp(1, 4096);
            let subscriber = self.telemetry_store.subscribe(max_batches);
            let mut interval =
                tokio::time::interval(Duration::from_millis(self.config.flush_interval_ms.max(10)));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            let mut payload_buf: Vec<u8> = Vec::with_capacity(256 * 1024);
            let mut consecutive_failures = 0u32;
            let mut cooldown_until: Option<tokio::time::Instant> = None;

            loop {
                let is_shutting_down = tokio::select! {
                    _ = interval.tick() => false,
                    _ = shutdown.changed() => *shutdown.borrow(),
                };

                if cooldown_until.is_some_and(|until| tokio::time::Instant::now() < until) {
                    let _ = subscriber.drain_batches(max_batches);
                    tracing::debug!(
                        plugin_id = %self.id,
                        "S3 log shipper in 30s cool-down; discarded batches"
                    );
                } else {
                    if cooldown_until.is_some() {
                        cooldown_until = None;
                        consecutive_failures = 0;
                        tracing::info!(
                            plugin_id = %self.id,
                            "S3 log shipper cool-down ended; resuming uploads"
                        );
                    }

                    let batches = subscriber.drain_batches(max_batches);
                    if !batches.is_empty() {
                        payload_buf.clear();
                        let encoded = format_ndjson(&batches, &mut payload_buf);
                        drop(batches);

                        if encoded > 0 && !payload_buf.is_empty() {
                            match wire::upload_batch(&mut wire_state, &payload_buf).await {
                                Ok(status) if (200..300).contains(&status) => {
                                    consecutive_failures = 0;
                                    self.shipped_records
                                        .fetch_add(encoded as u64, Ordering::Relaxed);
                                }
                                Ok(status) => {
                                    consecutive_failures += 1;
                                    self.failed_pushes.fetch_add(1, Ordering::Relaxed);
                                    tracing::warn!(
                                        plugin_id = %self.id,
                                        status,
                                        records = encoded,
                                        attempt = consecutive_failures,
                                        "S3 PUT returned non-2xx status"
                                    );
                                }
                                Err(e) => {
                                    consecutive_failures += 1;
                                    self.failed_pushes.fetch_add(1, Ordering::Relaxed);
                                    tracing::warn!(
                                        plugin_id = %self.id,
                                        error = %e,
                                        records = encoded,
                                        attempt = consecutive_failures,
                                        "Failed to upload log object to S3"
                                    );
                                }
                            }

                            if consecutive_failures > 10 {
                                tracing::warn!(
                                    plugin_id = %self.id,
                                    "S3 shipper failed 10 consecutive times; entering 30s cool-down"
                                );
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
        })
    }
}
