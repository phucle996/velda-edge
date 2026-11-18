//! OpenTelemetry Trace Exporter Background Plugin (`otlp_trace_exporter::plugin`).
//!
//! ```text
//! ┌────────────────────────────────────────────────────────────────────────────────────────┐
//! │                        VELDA EDGE DATA PLANE ARCHITECTURE                              │
//! │                                                                                        │
//! │   FRONTLINE TRAFFIC WORKERS (Cores 0..N-1, SCHED_OTHER)                                │
//! │   ┌────────────────────────────────────────────────────────────────────────────────┐   │
//! │   │ Ingress -> Route Matching -> Upstream Proxying -> Downstream Completion       │   │
//! │   │                                                                                │   │
//! │   │ [Hot-Path Telemetry Hook]                                                      │   │
//! │   │ 64-byte SpanRecord ──► Core-Local SPSC Span Ring (Non-temporal push)           │   │
//! │   └───────────────────────────────────────┬────────────────────────────────────────┘   │
//! │                                           │ Atomic In-Memory Hand-off                  │
//! │                                           ▼                                            │
//! │   SHADOW LOGISTICS WORKER (velda-shadow-{i}, SCHED_IDLE, Assigned Core i)              │
//! │   ┌────────────────────────────────────────────────────────────────────────────────┐   │
//! │   │ TraceStore::flush_all_lanes() ──► Batched Arc<[SpanRecord]> Slices             │   │
//! │   │                                                                                │   │
//! │   │ OtlpTraceExporterPlugin::run()                                                 │   │
//! │   │  ├── is_grpc?                                                                  │   │
//! │   │  │    ├── YES ──► proto::OtlpTraceProtoEnvelope + proto::OtlpTracesGrpcClient  │   │
//! │   │  │    └── NO  ──► json::OtlpTraceJsonEnvelope   + json::OtlpTraceHttpClient    │   │
//! │   │  └── Batch Drop: Atomic ref-count decrement (deallocates when count reaches 0) │   │
//! │   └────────────────────────────────────────────────────────────────────────────────┘   │
//! └────────────────────────────────────────────────────────────────────────────────────────┘
//! ```

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::config::PluginConfig;
use http::HeaderMap;
use tokio::sync::watch;
use velda_observability::{TraceStore, global_trace_store};

pub use super::config::OtlpTraceExporterConfig;
use super::json::{OtlpTraceHttpClient, OtlpTraceJsonEnvelope};
use super::proto::{OtlpTraceProtoEnvelope, OtlpTracesGrpcClient};
use crate::background::BackgroundPlugin;
use crate::error::PluginError;

/// Background OpenTelemetry Trace Exporter Plugin.
pub struct OtlpTraceExporterPlugin {
    pub id: String,
    pub config: OtlpTraceExporterConfig,
    trace_store: Arc<TraceStore>,
    custom_headers: HeaderMap,
    pub shipped_spans: AtomicU64,
    pub failed_spans: AtomicU64,
}

impl OtlpTraceExporterPlugin {
    /// Constructs a new exporter bound to an explicit [`TraceStore`].
    pub fn with_store(
        id: impl Into<String>,
        config: OtlpTraceExporterConfig,
        trace_store: Arc<TraceStore>,
    ) -> Result<Self, PluginError> {
        let mut custom_headers = HeaderMap::new();
        for (k, v) in &config.headers {
            let header_name = http::header::HeaderName::from_bytes(k.as_bytes())
                .map_err(|e| PluginError::Config(format!("Invalid header name '{k}': {e}")))?;
            let header_value = http::header::HeaderValue::from_str(v)
                .map_err(|e| PluginError::Config(format!("Invalid header value '{v}': {e}")))?;
            custom_headers.insert(header_name, header_value);
        }

        Ok(Self {
            id: id.into(),
            config,
            trace_store,
            custom_headers,
            shipped_spans: AtomicU64::new(0),
            failed_spans: AtomicU64::new(0),
        })
    }

    /// Instantiates and validates this plugin from declarative [`PluginConfig`].
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let validated =
            super::validation::validate_otlp_trace_exporter_config(&config.id, &config.config)
                .map_err(|e| PluginError::Config(e.to_string()))?;

        Self::with_store(&config.id, validated, global_trace_store().clone())
    }
}

impl BackgroundPlugin for OtlpTraceExporterPlugin {
    fn id(&self) -> &str {
        &self.id
    }

    #[inline(always)]
    fn weight(&self) -> u32 {
        50
    }

    fn run(
        self: Arc<Self>,
        mut shutdown: watch::Receiver<bool>,
    ) -> Pin<Box<dyn Future<Output = Result<(), PluginError>> + Send>> {
        Box::pin(async move {
            let is_grpc = self.config.protocol.eq_ignore_ascii_case("grpc")
                || self.config.endpoint.starts_with("grpc://")
                || self.config.endpoint.starts_with("grpcs://");

            let subscriber = self.trace_store.subscribe(256);
            let mut interval =
                tokio::time::interval(Duration::from_millis(self.config.flush_interval_ms.max(10)));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            let connect_timeout = Duration::from_millis(self.config.connect_timeout_ms);
            let request_timeout = Duration::from_millis(self.config.request_timeout_ms);

            tracing::info!(
                plugin_id = %self.id,
                endpoint = %self.config.endpoint,
                is_grpc = is_grpc,
                flush_interval_ms = self.config.flush_interval_ms,
                "Starting OpenTelemetry Trace Exporter background loop"
            );

            if is_grpc {
                // gRPC HTTP/2 Protobuf Wire path
                let mut grpc_client = Some(OtlpTracesGrpcClient::new(
                    &self.config.endpoint,
                    connect_timeout,
                    request_timeout,
                    &self.config.tls,
                )?);
                let mut proto_buf = Vec::with_capacity(32 * 1024);
                let mut proto_wire_envelope =
                    OtlpTraceProtoEnvelope::new(&self.config.service_name);
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
                            "OTLP gRPC trace exporter in cool-down; discarded batches to free RAM"
                        );
                        if is_shutting_down {
                            break;
                        }
                        continue;
                    }

                    if cooldown_until.is_some() {
                        cooldown_until = None;
                        grpc_client = Some(OtlpTracesGrpcClient::new(
                            &self.config.endpoint,
                            connect_timeout,
                            request_timeout,
                            &self.config.tls,
                        )?);
                        consecutive_failures = 0;
                    }

                    let batches = subscriber.drain_batches(64);

                    if !batches.is_empty() {
                        let span_count =
                            proto_wire_envelope.format_spans_wire(&batches, &mut proto_buf);
                        // Natural reference count decrement:
                        // As batches is dropped, Rust automatically decrements the Arc strong count.
                        // When all subscribers drop, the underlying allocation is freed!
                        drop(batches);

                        if span_count > 0 {
                            if grpc_client.is_none() {
                                grpc_client = Some(OtlpTracesGrpcClient::new(
                                    &self.config.endpoint,
                                    connect_timeout,
                                    request_timeout,
                                    &self.config.tls,
                                )?);
                            }

                            let client = grpc_client.as_mut().unwrap();
                            match client.send_traces(&proto_buf, &self.custom_headers).await {
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
                                        error = %e,
                                        dropped_spans = span_count,
                                        "Failed to export trace spans via gRPC"
                                    );

                                    if consecutive_failures <= 10 {
                                        let backoff_ms = (100u64.saturating_mul(
                                            1u64 << (consecutive_failures - 1).min(31),
                                        ))
                                        .min(2000);
                                        tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                                    } else {
                                        tracing::warn!(
                                            plugin_id = %self.id,
                                            "Shipper failed 10 consecutive times; tearing down connection and entering 30s cool-down"
                                        );
                                        grpc_client = None;
                                        cooldown_until = Some(
                                            tokio::time::Instant::now() + Duration::from_secs(30),
                                        );
                                        consecutive_failures = 0;
                                    }
                                }
                            }
                        }
                    }

                    if is_shutting_down {
                        tracing::info!(plugin_id = %self.id, "OTLP Trace Exporter loop shutting down");
                        break;
                    }
                }
            } else {
                // HTTP/1.1 JSON path
                let mut http_client = Some(OtlpTraceHttpClient::new(
                    &self.config.endpoint,
                    connect_timeout,
                    request_timeout,
                    &self.config.tls,
                )?);
                let mut json_buf = String::with_capacity(32 * 1024);
                let json_envelope = OtlpTraceJsonEnvelope::new(&self.config.service_name);
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
                            "OTLP HTTP trace exporter in cool-down; discarded batches to free RAM"
                        );
                        if is_shutting_down {
                            break;
                        }
                        continue;
                    }

                    if cooldown_until.is_some() {
                        cooldown_until = None;
                        http_client = Some(OtlpTraceHttpClient::new(
                            &self.config.endpoint,
                            connect_timeout,
                            request_timeout,
                            &self.config.tls,
                        )?);
                        consecutive_failures = 0;
                    }

                    let batches = subscriber.drain_batches(64);

                    if !batches.is_empty() {
                        let span_count = json_envelope.format_spans_json(&batches, &mut json_buf);
                        drop(batches);

                        if span_count > 0 {
                            if http_client.is_none() {
                                http_client = Some(OtlpTraceHttpClient::new(
                                    &self.config.endpoint,
                                    connect_timeout,
                                    request_timeout,
                                    &self.config.tls,
                                )?);
                            }

                            let client = http_client.as_mut().unwrap();
                            match client
                                .send_traces_json(&json_buf, &self.custom_headers)
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
                                        error = %e,
                                        dropped_spans = span_count,
                                        "Failed to export trace spans via HTTP"
                                    );

                                    if consecutive_failures <= 10 {
                                        let backoff_ms = (100u64.saturating_mul(
                                            1u64 << (consecutive_failures - 1).min(31),
                                        ))
                                        .min(2000);
                                        tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                                    } else {
                                        tracing::warn!(
                                            plugin_id = %self.id,
                                            "Shipper failed 10 consecutive times; tearing down connection and entering 30s cool-down"
                                        );
                                        http_client = None;
                                        cooldown_until = Some(
                                            tokio::time::Instant::now() + Duration::from_secs(30),
                                        );
                                        consecutive_failures = 0;
                                    }
                                }
                            }
                        }
                    }

                    if is_shutting_down {
                        tracing::info!(plugin_id = %self.id, "OTLP Trace Exporter loop shutting down");
                        break;
                    }
                }
            }

            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_exponential_backoff_progression() {
        for failures in 1..=10 {
            let backoff_ms = (100u64.saturating_mul(1u64 << (failures - 1).min(31))).min(2000);
            match failures {
                1 => assert_eq!(backoff_ms, 100),
                2 => assert_eq!(backoff_ms, 200),
                3 => assert_eq!(backoff_ms, 400),
                4 => assert_eq!(backoff_ms, 800),
                5 => assert_eq!(backoff_ms, 1600),
                6..=10 => assert_eq!(backoff_ms, 2000),
                _ => unreachable!(),
            }
        }
    }

    #[tokio::test]
    async fn test_otlp_trace_exporter_shutdown() {
        let store = Arc::new(TraceStore::new());
        let config = OtlpTraceExporterConfig {
            protocol: "http".to_string(),
            endpoint: "http://127.0.0.1:4318/v1/traces".to_string(),
            service_name: "test-service".to_string(),
            flush_interval_ms: 50,
            max_batch_size: 100,
            connect_timeout_ms: 1000,
            request_timeout_ms: 1000,
            headers: HashMap::new(),
            tls: Default::default(),
        };

        let plugin = Arc::new(
            OtlpTraceExporterPlugin::with_store("otlp_trace_test", config, store).unwrap(),
        );
        let (tx, rx) = watch::channel(false);

        let handle = tokio::spawn(async move { plugin.run(rx).await });

        tx.send(true).unwrap();
        let res = handle.await.unwrap();
        assert!(res.is_ok());
    }
}
