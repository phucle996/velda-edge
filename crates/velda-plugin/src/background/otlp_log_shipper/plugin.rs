//! OpenTelemetry Log Shipper Background Plugin (`otlp_log_shipper`).
//!
//! Subscribes to reference-counted 64-byte [`AccessLogRecord`] batches from
//! [`velda_observability::LogStore`] across all protocols (HTTP/1, H2, H3, gRPC, Raw L4)
//! and ships them to an OpenTelemetry Collector over persistent HTTP/1.1 Keep-Alive (`/v1/logs`)
//! or HTTP/2 gRPC (`/opentelemetry.proto.collector.logs.v1.LogsService/Export`).
//!
//! # Hardware & Architecture Rationale
//! - **Zero Hot-Path Contention**: Log emission occurs into lock-free SPSC rings on frontline workers.
//!   This shipper runs asynchronously on shadow workers (`SCHED_IDLE`), draining batches via
//!   [`LogStore::subscribe`].
//! - **Reference-Counted Lifetime**: Drained batches are held as `Arc<[AccessLogRecord]>`. Dropping
//!   the slice after formatting decrements strong counts atomically, returning memory to the allocator
//!   without mutex contention when all registered shippers finish reading.
//! - **Warm Persistent Channels**: Reuses established TCP/TLS connections across export cycles
//!   to eliminate handshake latency storms and socket churn.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use http::HeaderMap;
use tokio::sync::watch;
use velda_observability::{LogStore, global_log_store};
use velda_sync::post_sync::plugin::PluginConfig;
pub use velda_sync::post_sync::plugin::otlp_log_shipper::OtlpLogShipperConfig;

use super::client::OtlpLogClient;
use super::client_grpc::OtlpLogsGrpcClient;
use super::format::{OtlpJsonEnvelope, format_otlp_records_json};
use super::wire::OtlpProtoWireEnvelope;
use crate::background::BackgroundPlugin;
use crate::capability::{Direction, PluginCapability, SupportedProtocols};
use crate::error::PluginError;

/// Background OpenTelemetry Log Shipper Plugin.
pub struct OtlpLogShipperPlugin {
    pub id: String,
    pub config: OtlpLogShipperConfig,
    log_store: Arc<LogStore>,
    custom_headers: HeaderMap,
    json_envelope: OtlpJsonEnvelope,
    shipped_records: Arc<AtomicU64>,
    failed_records: Arc<AtomicU64>,
}

impl OtlpLogShipperPlugin {
    /// Constructs a new OTLP log shipper using the global singleton log store.
    pub fn new(id: impl Into<String>, config: OtlpLogShipperConfig) -> Result<Self, PluginError> {
        Self::with_store(id, config, global_log_store())
    }

    /// Constructs a new shipper bound to an explicit [`LogStore`] (useful for isolated unit testing).
    pub fn with_store(
        id: impl Into<String>,
        config: OtlpLogShipperConfig,
        log_store: Arc<LogStore>,
    ) -> Result<Self, PluginError> {
        let mut custom_headers = HeaderMap::new();
        for (k, v) in &config.headers {
            let header_name = http::header::HeaderName::from_bytes(k.as_bytes())
                .map_err(|e| PluginError::Config(format!("Invalid header name '{k}': {e}")))?;
            let header_value = http::header::HeaderValue::from_str(v)
                .map_err(|e| PluginError::Config(format!("Invalid header value '{v}': {e}")))?;
            custom_headers.insert(header_name, header_value);
        }

        let json_envelope = OtlpJsonEnvelope::new(&config.service_name);

        Ok(Self {
            id: id.into(),
            config,
            log_store,
            custom_headers,
            json_envelope,
            shipped_records: Arc::new(AtomicU64::new(0)),
            failed_records: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Instantiates and validates this plugin from declarative [`PluginConfig`].
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let validated = velda_sync::validate_otlp_log_shipper_config(&config.id, &config.config)
            .map_err(|e| PluginError::Config(e.to_string()))?;

        Self::with_store(&config.id, validated, global_log_store())
    }

    /// Returns cumulative total of successfully shipped log records.
    pub fn shipped_records(&self) -> u64 {
        self.shipped_records.load(Ordering::Relaxed)
    }

    /// Returns cumulative total of log records that failed to ship due to network or collector errors.
    pub fn failed_records(&self) -> u64 {
        self.failed_records.load(Ordering::Relaxed)
    }
}

impl BackgroundPlugin for OtlpLogShipperPlugin {
    fn id(&self) -> &str {
        &self.id
    }

    fn run(
        self: Arc<Self>,
        mut shutdown: watch::Receiver<bool>,
    ) -> Pin<Box<dyn Future<Output = Result<(), PluginError>> + Send>> {
        Box::pin(async move {
            let is_grpc = self.config.protocol.eq_ignore_ascii_case("grpc")
                || self.config.endpoint.starts_with("grpc://")
                || self.config.endpoint.starts_with("grpcs://");

            let subscriber = self.log_store.subscribe(256);
            let mut interval =
                tokio::time::interval(Duration::from_millis(self.config.flush_interval_ms.max(50)));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            let connect_timeout = Duration::from_millis(self.config.connect_timeout_ms);
            let request_timeout = Duration::from_millis(self.config.request_timeout_ms);

            tracing::info!(
                plugin_id = %self.id,
                endpoint = %self.config.endpoint,
                is_grpc = is_grpc,
                flush_interval_ms = self.config.flush_interval_ms,
                "Starting OpenTelemetry Log Shipper background loop"
            );

            if is_grpc {
                let mut grpc_client = OtlpLogsGrpcClient::new(
                    &self.config.endpoint,
                    connect_timeout,
                    request_timeout,
                )?;
                let mut proto_buf = Vec::with_capacity(32 * 1024);
                let mut records_buf = Vec::with_capacity(32 * 1024);
                let proto_wire_envelope = OtlpProtoWireEnvelope::new(&self.config.service_name);

                loop {
                    let is_shutting_down = tokio::select! {
                        _ = interval.tick() => false,
                        _ = shutdown.changed() => true,
                    };

                    self.log_store.flush_all_lanes(self.config.max_batch_size);
                    let batches = subscriber.drain_batches(64);

                    if !batches.is_empty() {
                        let record_count = proto_wire_envelope.format_records_wire(
                            &batches,
                            &mut records_buf,
                            &mut proto_buf,
                        );
                        drop(batches);

                        if record_count > 0 {
                            match grpc_client
                                .send_logs(&proto_buf, &self.custom_headers)
                                .await
                            {
                                Ok(()) => {
                                    self.shipped_records
                                        .fetch_add(record_count as u64, Ordering::Relaxed);
                                    tracing::debug!(
                                        plugin_id = %self.id,
                                        records = record_count,
                                        "Exported OTLP logs batch via gRPC"
                                    );
                                }
                                Err(e) => {
                                    self.failed_records
                                        .fetch_add(record_count as u64, Ordering::Relaxed);
                                    tracing::warn!(
                                        plugin_id = %self.id,
                                        error = %e,
                                        "Failed to send OTLP logs payload via gRPC"
                                    );
                                }
                            }
                        }
                    }

                    if is_shutting_down {
                        tracing::info!(
                            plugin_id = %self.id,
                            "OTLP log shipper shutting down (gRPC)"
                        );
                        break;
                    }
                }
            } else {
                let mut http_client =
                    OtlpLogClient::new(&self.config.endpoint, connect_timeout, request_timeout)?;
                let mut json_buf = Vec::with_capacity(32 * 1024);

                loop {
                    let is_shutting_down = tokio::select! {
                        _ = interval.tick() => false,
                        _ = shutdown.changed() => true,
                    };

                    self.log_store.flush_all_lanes(self.config.max_batch_size);
                    let batches = subscriber.drain_batches(64);

                    if !batches.is_empty() {
                        let record_count =
                            format_otlp_records_json(&batches, &self.json_envelope, &mut json_buf);
                        drop(batches);

                        if record_count > 0 {
                            match http_client.send_logs(&json_buf, &self.custom_headers).await {
                                Ok(status) if (200..300).contains(&status) => {
                                    self.shipped_records
                                        .fetch_add(record_count as u64, Ordering::Relaxed);
                                    tracing::debug!(
                                        plugin_id = %self.id,
                                        records = record_count,
                                        status = status,
                                        "Exported OTLP logs batch via HTTP"
                                    );
                                }
                                Ok(status) => {
                                    self.failed_records
                                        .fetch_add(record_count as u64, Ordering::Relaxed);
                                    tracing::warn!(
                                        plugin_id = %self.id,
                                        status = status,
                                        "OTLP logs collector rejected HTTP payload"
                                    );
                                }
                                Err(e) => {
                                    self.failed_records
                                        .fetch_add(record_count as u64, Ordering::Relaxed);
                                    tracing::warn!(
                                        plugin_id = %self.id,
                                        error = %e,
                                        "Failed to send OTLP logs payload via HTTP"
                                    );
                                }
                            }
                        }
                    }

                    if is_shutting_down {
                        tracing::info!(
                            plugin_id = %self.id,
                            "OTLP log shipper shutting down (HTTP)"
                        );
                        break;
                    }
                }
            }

            Ok(())
        })
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
}
