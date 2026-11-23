//! Grafana Loki Log Shipper Background Coordinator (`loki_log_shipper::plugin`).
//!
//! ```text
//!   Startup wiring (wire_plugin_etps / with_etp) attaches Arc<LokiEtp> into OnceLock
//!         │
//!         ▼
//!   run(shutdown) ──► wire::bake_wire_template(&etp.config)
//!         │           + LokiJsonFormatter (Http) | LokiProtoFormatter (Grpc)
//!         └─► Interval / shutdown select loop:
//!               ├─ in 30s cooldown? ──► drain & discard batches (RAM guard)
//!               └─ drain_batches(max_batch_size) -> format into payload_buf
//!                     └─► wire::push_logs(&etp, &template, &mut head_buf, &payload_buf, ...)
//!                           ├─ Ok  ──► ++shipped_records, reset failure counter
//!                           └─ Err ──► ++failed_pushes (> 10 -> 30s cooldown)
//!
//!   Coordinator Struct Layout:
//!   ┌────────────────────────────────────────────────────────────────────────────┐
//!   │ LokiLogShipperPlugin                                                       │
//!   │   id: String | config: LokiLogShipperConfig (etp_id)                       │
//!   │   etp: OnceLock<Arc<LokiEtp>> | telemetry_store: Arc<TelemetryStore>       │
//!   │   shipped_records: Arc<AtomicU64> | failed_pushes: Arc<AtomicU64>          │
//!   └────────────────────────────────────────────────────────────────────────────┘
//! ```

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use tokio::sync::watch;
use velda_etp::loki::{LokiEtp, LokiProtocol};
use velda_etp::{EtpBackend, EtpInstance};
use velda_observability::{TelemetryStore, global_telemetry_store};

use crate::background::BackgroundPlugin;
use crate::capability::{Direction, PluginCapability, SupportedProtocols};
use crate::config::PluginConfig;
use crate::error::PluginError;

pub use super::config::LokiLogShipperConfig;
use super::json::LokiJsonFormatter;
use super::proto::LokiProtoFormatter;
use super::wire;

/// Background Grafana Loki Log Shipper Coordinator Plugin.
pub struct LokiLogShipperPlugin {
    pub id: String,
    pub config: LokiLogShipperConfig,
    pub etp: OnceLock<Arc<LokiEtp>>,
    pub telemetry_store: Arc<TelemetryStore>,
    pub shipped_records: Arc<AtomicU64>,
    pub failed_pushes: Arc<AtomicU64>,
}

enum LokiFormatter {
    Json(LokiJsonFormatter),
    Proto(LokiProtoFormatter),
}

impl LokiLogShipperPlugin {
    /// Constructs a new Loki log shipper bound to the global singleton [`TelemetryStore`].
    pub fn new(id: impl Into<String>, config: LokiLogShipperConfig) -> Result<Self, PluginError> {
        Self::with_store(id, config, Arc::clone(global_telemetry_store()))
    }

    /// Constructs a new shipper bound to an explicit [`TelemetryStore`] without an attached ETP yet.
    pub fn with_store(
        id: impl Into<String>,
        config: LokiLogShipperConfig,
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

    /// Constructs a shipper pre-bound to an explicit [`LokiEtp`] and [`TelemetryStore`].
    pub fn with_etp(
        id: impl Into<String>,
        config: LokiLogShipperConfig,
        etp: Arc<LokiEtp>,
        telemetry_store: Arc<TelemetryStore>,
    ) -> Result<Self, PluginError> {
        let plugin = Self::with_store(id, config, telemetry_store)?;
        let _ = plugin.etp.set(etp);
        Ok(plugin)
    }

    /// Instantiates and validates this plugin from declarative [`PluginConfig`].
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let validated =
            super::validation::validate_loki_log_shipper_config(&config.id, &config.config)?;
        Self::with_store(&config.id, validated, Arc::clone(global_telemetry_store()))
    }
}

impl BackgroundPlugin for LokiLogShipperPlugin {
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

    #[inline(always)]
    fn etp_id(&self) -> Option<&str> {
        Some(&self.config.etp_id)
    }

    fn attach_etp(&self, instance: &EtpInstance) -> Result<(), PluginError> {
        match &instance.backend {
            EtpBackend::Loki(pool) => {
                let _ = self.etp.set(Arc::clone(pool));
                Ok(())
            }
            _ => Err(PluginError::Config(format!(
                "Loki plugin '{}' requires a 'loki' ETP for etp_id '{}', got '{}'",
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
                    "Loki plugin '{}' has no attached LokiEtp for etp_id '{}'",
                    self.id, self.config.etp_id
                ))
            })?;

            let wire_template = wire::bake_wire_template(&etp.config)?;
            let mut formatter = match etp.config.protocol {
                LokiProtocol::Http => LokiFormatter::Json(LokiJsonFormatter::new(
                    &self.config.service_name,
                    &self.config.labels,
                )),
                LokiProtocol::Grpc => LokiFormatter::Proto(LokiProtoFormatter::new(
                    &self.config.service_name,
                    &self.config.labels,
                )),
            };

            let subscriber = self.telemetry_store.subscribe(256);
            let mut interval =
                tokio::time::interval(Duration::from_millis(self.config.flush_interval_ms.max(50)));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            let request_timeout = Duration::from_millis(self.config.request_timeout_ms);
            let mut head_buf = Vec::with_capacity(512);
            let mut payload_buf = Vec::with_capacity(32 * 1024);
            let mut resp_buf = Vec::with_capacity(1024);
            let mut consecutive_failures: u32 = 0;
            let mut cooldown_until: Option<tokio::time::Instant> = None;

            loop {
                let is_shutting_down = tokio::select! {
                    _ = interval.tick() => false,
                    _ = shutdown.changed() => *shutdown.borrow(),
                };

                if cooldown_until.is_some_and(|until| tokio::time::Instant::now() < until) {
                    let _ = subscriber.drain_batches(64);
                    tracing::debug!(
                        plugin_id = %self.id,
                        "Loki log shipper in 30s cool-down; discarded batches to protect memory"
                    );
                } else {
                    if cooldown_until.is_some() {
                        cooldown_until = None;
                        consecutive_failures = 0;
                        tracing::info!(
                            plugin_id = %self.id,
                            "Loki cool-down period ended, resuming batch pushes"
                        );
                    }

                    let batches = subscriber.drain_batches(64);
                    if !batches.is_empty() {
                        let record_count = match &mut formatter {
                            LokiFormatter::Json(f) => f.format_batches(&batches, &mut payload_buf),
                            LokiFormatter::Proto(f) => f.format_batches(&batches, &mut payload_buf),
                        };
                        drop(batches);

                        if record_count > 0 && !payload_buf.is_empty() {
                            match wire::push_logs(
                                etp,
                                &wire_template,
                                &mut head_buf,
                                &payload_buf,
                                &mut resp_buf,
                                request_timeout,
                            )
                            .await
                            {
                                Ok(()) => {
                                    consecutive_failures = 0;
                                    self.shipped_records
                                        .fetch_add(record_count as u64, Ordering::Relaxed);
                                }
                                Err(err) => {
                                    consecutive_failures += 1;
                                    self.failed_pushes.fetch_add(1, Ordering::Relaxed);
                                    tracing::warn!(
                                        plugin_id = %self.id,
                                        error = %err,
                                        records = record_count,
                                        attempt = consecutive_failures,
                                        "Failed to push batch to Loki"
                                    );
                                    if consecutive_failures > 10 {
                                        tracing::warn!(
                                            plugin_id = %self.id,
                                            "Loki shipper failed 10 consecutive times; entering 30s cool-down"
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
                    tracing::info!(plugin_id = %self.id, "Loki log shipper shutting down");
                    break;
                }
            }

            Ok(())
        })
    }
}
