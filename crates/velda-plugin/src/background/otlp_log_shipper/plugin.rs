//! OpenTelemetry Log Shipper Background Coordinator (`otlp_log_shipper::plugin`).
//!
//! ```text
//!   Startup wiring (wire_plugin_etps / with_etp) attaches Arc<OtlpEtp> into OnceLock
//!         │
//!         ▼
//!   run(shutdown) ──► wire::bake_wire_template(&etp.config)
//!         │
//!         └─► Interval / shutdown select loop:
//!               ├─ in 30s cooldown? ──► drain & discard batches (RAM guard)
//!               └─ drain_batches(64) -> encode (OtlpJsonEnvelope | OtlpProtoWireEnvelope)
//!                     └─► wire::push_logs(&etp, &template, &mut head_buf, &payload, ...)
//!                           ├─ Ok(())  ──► ++shipped_records, reset failures
//!                           └─ Err(e)  ──► ++failed_records, backoff (..2000ms) -> 30s cooldown (>10)
//!
//!   Coordinator Struct Layout:
//!   ┌────────────────────────────────────────────────────────────────────────────┐
//!   │ OtlpLogShipperPlugin                                                       │
//!   │   id: String | config: OtlpLogShipperConfig (etp_id, service_name)         │
//!   │   etp: OnceLock<Arc<OtlpEtp>> | telemetry_store: Arc<TelemetryStore>       │
//!   │   shipped_records: Arc<AtomicU64> | failed_records: Arc<AtomicU64>         │
//!   └────────────────────────────────────────────────────────────────────────────┘
//! ```

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use tokio::sync::watch;
use velda_etp::otlp::{OtlpEtp, OtlpProtocol};
use velda_etp::{EtpBackend, EtpInstance};
use velda_observability::{TelemetryStore, global_telemetry_store};

use super::config::OtlpLogShipperConfig;
use super::json::{OtlpJsonEnvelope, format_otlp_records_json};
use super::proto::OtlpProtoWireEnvelope;
use super::wire;
use crate::background::BackgroundPlugin;
use crate::capability::{Direction, PluginCapability, SupportedProtocols};
use crate::config::PluginConfig;
use crate::error::PluginError;

/// Background OpenTelemetry Log Shipper Plugin.
pub struct OtlpLogShipperPlugin {
    pub id: String,
    pub config: OtlpLogShipperConfig,
    pub etp: OnceLock<Arc<OtlpEtp>>,
    pub telemetry_store: Arc<TelemetryStore>,
    pub shipped_records: Arc<AtomicU64>,
    pub failed_records: Arc<AtomicU64>,
}

impl OtlpLogShipperPlugin {
    /// Constructs a new OTLP log shipper using the global singleton telemetry store.
    pub fn new(id: impl Into<String>, config: OtlpLogShipperConfig) -> Result<Self, PluginError> {
        Self::with_store(id, config, Arc::clone(global_telemetry_store()))
    }

    /// Constructs a new shipper bound to an explicit [`TelemetryStore`].
    pub fn with_store(
        id: impl Into<String>,
        config: OtlpLogShipperConfig,
        telemetry_store: Arc<TelemetryStore>,
    ) -> Result<Self, PluginError> {
        Ok(Self {
            id: id.into(),
            config,
            etp: OnceLock::new(),
            telemetry_store,
            shipped_records: Arc::new(AtomicU64::new(0)),
            failed_records: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Constructs a new shipper pre-attached to an explicit [`OtlpEtp`] and [`TelemetryStore`].
    pub fn with_etp(
        id: impl Into<String>,
        config: OtlpLogShipperConfig,
        etp: Arc<OtlpEtp>,
        telemetry_store: Arc<TelemetryStore>,
    ) -> Result<Self, PluginError> {
        let plugin = Self::with_store(id, config, telemetry_store)?;
        let _ = plugin.etp.set(etp);
        Ok(plugin)
    }

    /// Instantiates and validates this plugin from declarative [`PluginConfig`].
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let validated =
            super::validation::validate_otlp_log_shipper_config(&config.id, &config.config)?;
        Self::with_store(&config.id, validated, Arc::clone(global_telemetry_store()))
    }
}

impl BackgroundPlugin for OtlpLogShipperPlugin {
    fn id(&self) -> &str {
        &self.id
    }

    #[inline(always)]
    fn pbww(&self) -> u32 {
        70
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
    fn direction(&self) -> Direction {
        Direction::Background
    }

    #[inline(always)]
    fn etp_id(&self) -> Option<&str> {
        Some(&self.config.etp_id)
    }

    fn attach_etp(&self, instance: &EtpInstance) -> Result<(), PluginError> {
        match &instance.backend {
            EtpBackend::Otlp(pool) => {
                let _ = self.etp.set(Arc::clone(pool));
                Ok(())
            }
            _ => Err(PluginError::Config(format!(
                "OTLP Logs plugin '{}' requires an 'otlp' ETP for etp_id '{}', got '{}'",
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
                    "OTLP Logs plugin '{}' has no attached OtlpEtp for etp_id '{}'",
                    self.id, self.config.etp_id
                ))
            })?;

            let wire_template = wire::bake_wire_template(&etp.config)?;
            let use_json = matches!(etp.config.protocol, OtlpProtocol::HttpJson);
            let json_envelope = use_json.then(|| OtlpJsonEnvelope::new(&self.config.service_name));
            let proto_envelope =
                (!use_json).then(|| OtlpProtoWireEnvelope::new(&self.config.service_name));

            let request_timeout = Duration::from_millis(self.config.request_timeout_ms);
            let subscriber = self.telemetry_store.subscribe(256);
            let mut interval =
                tokio::time::interval(Duration::from_millis(self.config.flush_interval_ms.max(50)));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            tracing::info!(
                plugin_id = %self.id,
                etp_id = %etp.config.id,
                endpoint = %etp.config.endpoint,
                protocol = ?etp.config.protocol,
                flush_interval_ms = self.config.flush_interval_ms,
                "Starting OpenTelemetry Log Shipper via OtlpEtp pool"
            );

            let mut head_buf = Vec::with_capacity(512);
            let mut payload_buf = Vec::with_capacity(32 * 1024);
            let mut proto_records_buf = Vec::with_capacity(32 * 1024);
            let mut resp_buf = Vec::with_capacity(1024);
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
                        "OTLP log shipper in 30s cool-down; discarded batches to free RAM"
                    );
                } else {
                    if cooldown_until.is_some() {
                        cooldown_until = None;
                        consecutive_failures = 0;
                        tracing::info!(
                            plugin_id = %self.id,
                            "OTLP log shipper cool-down ended; resuming exports"
                        );
                    }

                    let batches = subscriber.drain_batches(64);
                    if !batches.is_empty() {
                        let record_count = if let Some(ref env) = json_envelope {
                            format_otlp_records_json(&batches, env, &mut payload_buf)
                        } else if let Some(ref env) = proto_envelope {
                            env.format_records_wire(
                                &batches,
                                &mut proto_records_buf,
                                &mut payload_buf,
                            )
                        } else {
                            0
                        };
                        drop(batches);

                        if record_count > 0 {
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
                                Err(e) => {
                                    consecutive_failures += 1;
                                    self.failed_records
                                        .fetch_add(record_count as u64, Ordering::Relaxed);
                                    tracing::warn!(
                                        plugin_id = %self.id,
                                        etp_id = %etp.config.id,
                                        error = %e,
                                        records = record_count,
                                        attempt = consecutive_failures,
                                        "Failed to push OTLP logs payload"
                                    );

                                    if consecutive_failures <= 10 {
                                        if !is_shutting_down {
                                            let backoff_ms = (100u64.saturating_mul(
                                                1u64 << (consecutive_failures - 1).min(31),
                                            ))
                                            .min(2000);
                                            tokio::time::sleep(Duration::from_millis(backoff_ms))
                                                .await;
                                        }
                                    } else {
                                        tracing::warn!(
                                            plugin_id = %self.id,
                                            "OTLP log shipper failed 10 consecutive times; entering 30s cool-down"
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
                    break;
                }
            }

            Ok(())
        })
    }
}
