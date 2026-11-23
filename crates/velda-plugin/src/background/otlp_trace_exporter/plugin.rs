//! OpenTelemetry Trace Exporter Background Coordinator (`otlp_trace_exporter::plugin`).
//!
//! ```text
//!   Startup wiring (wire_plugin_etps / with_etp) attaches Arc<OtlpEtp> into OnceLock
//!         │
//!         ▼
//!   run(shutdown) ──► wire::bake_wire_template(&etp.config)
//!         │
//!         └─► Interval / shutdown select loop:
//!               ├─ in 30s cooldown? ──► drain & discard batches (RAM guard)
//!               └─ drain_batches(64) -> encode (OtlpTraceJsonEnvelope | OtlpTraceProtoEnvelope)
//!                     └─► wire::push_traces(&etp, &template, &mut head_buf, payload, ...)
//!                           ├─ Ok(())  ──► ++shipped_spans, reset failures
//!                           └─ Err(e)  ──► ++failed_spans, backoff (..2000ms) -> 30s cooldown (>10)
//!
//!   Coordinator Struct Layout:
//!   ┌────────────────────────────────────────────────────────────────────────────┐
//!   │ OtlpTraceExporterPlugin                                                    │
//!   │   id: String | config: OtlpTraceExporterConfig (etp_id, service_name)      │
//!   │   etp: OnceLock<Arc<OtlpEtp>> | telemetry_store: Arc<TelemetryStore>       │
//!   │   shipped_spans: Arc<AtomicU64> | failed_spans: Arc<AtomicU64>             │
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

use super::config::OtlpTraceExporterConfig;
use super::json::OtlpTraceJsonEnvelope;
use super::proto::OtlpTraceProtoEnvelope;
use super::wire;
use crate::background::BackgroundPlugin;
use crate::capability::{Direction, PluginCapability, SupportedProtocols};
use crate::config::PluginConfig;
use crate::error::PluginError;

/// Background OpenTelemetry Trace Exporter Plugin.
pub struct OtlpTraceExporterPlugin {
    pub id: String,
    pub config: OtlpTraceExporterConfig,
    pub etp: OnceLock<Arc<OtlpEtp>>,
    pub telemetry_store: Arc<TelemetryStore>,
    pub shipped_spans: Arc<AtomicU64>,
    pub failed_spans: Arc<AtomicU64>,
}

impl OtlpTraceExporterPlugin {
    /// Constructs a new OTLP trace exporter using the global singleton telemetry store.
    pub fn new(
        id: impl Into<String>,
        config: OtlpTraceExporterConfig,
    ) -> Result<Self, PluginError> {
        Self::with_store(id, config, Arc::clone(global_telemetry_store()))
    }

    /// Constructs a new exporter bound to an explicit [`TelemetryStore`].
    pub fn with_store(
        id: impl Into<String>,
        config: OtlpTraceExporterConfig,
        telemetry_store: Arc<TelemetryStore>,
    ) -> Result<Self, PluginError> {
        Ok(Self {
            id: id.into(),
            config,
            etp: OnceLock::new(),
            telemetry_store,
            shipped_spans: Arc::new(AtomicU64::new(0)),
            failed_spans: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Constructs a new exporter pre-attached to an explicit [`OtlpEtp`] and [`TelemetryStore`].
    pub fn with_etp(
        id: impl Into<String>,
        config: OtlpTraceExporterConfig,
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
            super::validation::validate_otlp_trace_exporter_config(&config.id, &config.config)?;
        Self::with_store(&config.id, validated, Arc::clone(global_telemetry_store()))
    }
}

impl BackgroundPlugin for OtlpTraceExporterPlugin {
    fn id(&self) -> &str {
        &self.id
    }

    #[inline(always)]
    fn pbww(&self) -> u32 {
        50
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
                "OTLP Traces plugin '{}' requires an 'otlp' ETP for etp_id '{}', got '{}'",
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
                    "OTLP Traces plugin '{}' has no attached OtlpEtp for etp_id '{}'",
                    self.id, self.config.etp_id
                ))
            })?;

            let wire_template = wire::bake_wire_template(&etp.config)?;
            let use_json = matches!(etp.config.protocol, OtlpProtocol::HttpJson);
            let json_envelope =
                use_json.then(|| OtlpTraceJsonEnvelope::new(&self.config.service_name));
            let mut proto_envelope =
                (!use_json).then(|| OtlpTraceProtoEnvelope::new(&self.config.service_name));

            let request_timeout = Duration::from_millis(self.config.request_timeout_ms);
            let subscriber = self.telemetry_store.subscribe(256);
            let mut interval =
                tokio::time::interval(Duration::from_millis(self.config.flush_interval_ms.max(10)));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            tracing::info!(
                plugin_id = %self.id,
                etp_id = %etp.config.id,
                endpoint = %etp.config.endpoint,
                protocol = ?etp.config.protocol,
                flush_interval_ms = self.config.flush_interval_ms,
                "Starting OpenTelemetry Trace Exporter via OtlpEtp pool"
            );

            let mut head_buf = Vec::with_capacity(512);
            let mut json_buf = String::with_capacity(if use_json { 32 * 1024 } else { 0 });
            let mut proto_buf = Vec::with_capacity(if use_json { 0 } else { 32 * 1024 });
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
                        "OTLP trace exporter in 30s cool-down; discarded batches to free RAM"
                    );
                } else {
                    if cooldown_until.is_some() {
                        cooldown_until = None;
                        consecutive_failures = 0;
                        tracing::info!(
                            plugin_id = %self.id,
                            "OTLP trace exporter cool-down ended; resuming exports"
                        );
                    }

                    let batches = subscriber.drain_batches(64);
                    if !batches.is_empty() {
                        let (span_count, payload_is_json) = if let Some(ref env) = json_envelope {
                            (env.format_spans_json(&batches, &mut json_buf), true)
                        } else if let Some(ref mut env) = proto_envelope {
                            (env.format_spans_wire(&batches, &mut proto_buf), false)
                        } else {
                            (0, false)
                        };
                        drop(batches);

                        if span_count > 0 {
                            let payload = if payload_is_json {
                                json_buf.as_bytes()
                            } else {
                                &proto_buf
                            };

                            match wire::push_traces(
                                etp,
                                &wire_template,
                                &mut head_buf,
                                payload,
                                &mut resp_buf,
                                request_timeout,
                            )
                            .await
                            {
                                Ok(()) => {
                                    consecutive_failures = 0;
                                    self.shipped_spans
                                        .fetch_add(span_count as u64, Ordering::Relaxed);
                                }
                                Err(e) => {
                                    consecutive_failures += 1;
                                    self.failed_spans
                                        .fetch_add(span_count as u64, Ordering::Relaxed);
                                    tracing::warn!(
                                        plugin_id = %self.id,
                                        etp_id = %etp.config.id,
                                        error = %e,
                                        spans = span_count,
                                        attempt = consecutive_failures,
                                        "Failed to push OTLP trace spans payload"
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
                                            "OTLP trace exporter failed 10 consecutive times; entering 30s cool-down"
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
