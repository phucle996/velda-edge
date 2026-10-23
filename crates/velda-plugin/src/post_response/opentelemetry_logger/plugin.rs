//! OpenTelemetry Logger Post-Response Plugin (`opentelemetry_logger`).
//!
//! Provides lock-free, zero-blocking OpenTelemetry structured access logging during `post_response`.
//! Extracts response status, method, path, latency, route, client IP, and W3C trace identifiers,
//! dispatching [`OtlpLogItem`] records into a bounded channel for asynchronous Shadow Worker
//! batch flushing without performing synchronous I/O or stalling Data Plane traffic workers.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use http::HeaderMap;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use velda_core::L7Response;
use velda_sync::post_sync::plugin::PluginConfig;

use super::client::OtlpLogClient;
use super::client_grpc::OtlpLogsGrpcClient;
use super::proto::format_otlp_logs_proto;
use super::record::{OtlpLogItem, format_otlp_logs_json};
use crate::capability::{AccessLevel, PluginCapability, SupportedProtocols};
use crate::error::PluginError;
use crate::header::{
    X_REQUEST_ID, X_RESPONSE_TIME, X_ROUTE_ID, X_SPAN_ID, X_TRACE_ID, X_UPSTREAM_ADDR,
};
use crate::hook::try_dispatch_shadow;
use crate::post_response::PostResponsePlugin;

/// Declarative configuration for [`OpentelemetryLoggerPlugin`].
///
/// # Strict Invariant: 100% Explicit Declaration
/// Every parameter must be explicitly declared in the configuration JSON.
/// Zero defaults are permitted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpentelemetryLoggerConfig {
    /// OTLP transport protocol (must be explicitly declared as `"http"` or `"grpc"`).
    pub protocol: String,
    /// OTLP collector endpoint URL (e.g. `http://127.0.0.1:4318/v1/logs` or `grpc://127.0.0.1:4317`).
    pub endpoint: String,
    /// Batch size threshold triggering an immediate flush (between 1 and 4096).
    pub batch_size: usize,
    /// Maximum duration in milliseconds before flushing pending records (between 10 and 60000).
    pub flush_interval_ms: u64,
    /// Bounded capacity of the lock-free log dispatch queue (between 128 and 262144).
    pub buffer_capacity: usize,
    /// TCP and TLS connection establishment timeout in milliseconds (100 to 60000).
    pub connect_timeout_ms: u64,
    /// Request export completion timeout in milliseconds (100 to 60000).
    pub request_timeout_ms: u64,
    /// Service name reported in OpenTelemetry Resource attributes.
    pub service_name: String,
    /// Custom HTTP or gRPC headers (use `{}` if none).
    pub headers: HashMap<String, String>,
}

/// Post-Response OpenTelemetry Structured Logger Plugin.
pub struct OpentelemetryLoggerPlugin {
    pub id: String,
    pub config: OpentelemetryLoggerConfig,
    sender: mpsc::Sender<OtlpLogItem>,
    dropped_count: Arc<AtomicU64>,
    logged_count: Arc<AtomicU64>,
}

impl OpentelemetryLoggerPlugin {
    /// Constructs a new `OpentelemetryLoggerPlugin` and starts the asynchronous Shadow flusher.
    pub fn new(id: impl Into<String>, config: OpentelemetryLoggerConfig) -> Self {
        let (plugin, rx) = Self::new_with_receiver(id, config);
        let endpoint = plugin.config.endpoint.clone();
        let protocol = plugin.config.protocol.clone();
        let batch_size = plugin.config.batch_size;
        let flush_interval = Duration::from_millis(plugin.config.flush_interval_ms.max(10));
        let service_name = plugin.config.service_name.clone();

        let connect_timeout = Duration::from_millis(plugin.config.connect_timeout_ms);
        let request_timeout = Duration::from_millis(plugin.config.request_timeout_ms);

        let mut custom_headers = HeaderMap::new();
        for (k, v) in &plugin.config.headers {
            if let Ok(name) = http::header::HeaderName::from_bytes(k.as_bytes())
                && let Ok(val) = http::header::HeaderValue::from_str(v)
            {
                custom_headers.insert(name, val);
            }
        }

        let flusher_task = Box::pin(async move {
            Self::run_flusher_loop(
                rx,
                protocol,
                endpoint,
                batch_size,
                flush_interval,
                connect_timeout,
                request_timeout,
                service_name,
                custom_headers,
            )
            .await;
        });

        // Priority 1: Dispatch onto Shadow Worker runtime via non-blocking try_dispatch_shadow
        if let Err(task) = try_dispatch_shadow(flusher_task) {
            // Priority 2: Fallback to Tokio current runtime if Shadow Worker hook is uninitialized (e.g. testing)
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                handle.spawn(task);
            }
        }

        plugin
    }

    /// Constructs an `OpentelemetryLoggerPlugin` returning the receiving channel end for test inspection.
    pub fn new_with_receiver(
        id: impl Into<String>,
        config: OpentelemetryLoggerConfig,
    ) -> (Self, mpsc::Receiver<OtlpLogItem>) {
        let (sender, receiver) = mpsc::channel(config.buffer_capacity.max(16));
        (
            Self {
                id: id.into(),
                config,
                sender,
                dropped_count: Arc::new(AtomicU64::new(0)),
                logged_count: Arc::new(AtomicU64::new(0)),
            },
            receiver,
        )
    }

    /// Deserializes and validates this plugin from declarative [`PluginConfig`].
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let parsed: OpentelemetryLoggerConfig = serde_json::from_value(config.config.clone())
            .map_err(|e| {
                PluginError::Config(format!("Failed to parse opentelemetry_logger config: {e}"))
            })?;
        Ok(Self::new(&config.id, parsed))
    }

    /// Executes the background batching loop on the Shadow Worker.
    #[allow(clippy::too_many_arguments)]
    async fn run_flusher_loop(
        mut rx: mpsc::Receiver<OtlpLogItem>,
        protocol: String,
        endpoint: String,
        batch_size: usize,
        flush_interval: Duration,
        connect_timeout: Duration,
        request_timeout: Duration,
        service_name: String,
        custom_headers: HeaderMap,
    ) {
        let is_grpc = protocol.eq_ignore_ascii_case("grpc")
            || endpoint.starts_with("grpc://")
            || endpoint.starts_with("grpcs://");

        let mut batch = Vec::with_capacity(batch_size);
        let mut ticker = tokio::time::interval(flush_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        if is_grpc {
            let mut client = match OtlpLogsGrpcClient::new(
                &endpoint,
                connect_timeout,
                request_timeout,
            ) {
                Ok(c) => c,
                Err(e) => {
                    tracing::error!(endpoint = %endpoint, error = %e, "Failed to initialize OTLP gRPC log client");
                    return;
                }
            };

            loop {
                tokio::select! {
                    item = rx.recv() => {
                        match item {
                            Some(record) => {
                                batch.push(record);
                                if batch.len() >= batch_size {
                                    Self::flush_batch_grpc(&mut client, &mut batch, &service_name, &custom_headers).await;
                                }
                            }
                            None => {
                                if !batch.is_empty() {
                                    Self::flush_batch_grpc(&mut client, &mut batch, &service_name, &custom_headers).await;
                                }
                                break;
                            }
                        }
                    }
                    _ = ticker.tick() => {
                        if !batch.is_empty() {
                            Self::flush_batch_grpc(&mut client, &mut batch, &service_name, &custom_headers).await;
                        }
                    }
                }
            }
        } else {
            let mut client = match OtlpLogClient::new(&endpoint, connect_timeout, request_timeout) {
                Ok(c) => c,
                Err(e) => {
                    tracing::error!(endpoint = %endpoint, error = %e, "Failed to initialize OTLP HTTP log client");
                    return;
                }
            };

            loop {
                tokio::select! {
                    item = rx.recv() => {
                        match item {
                            Some(record) => {
                                batch.push(record);
                                if batch.len() >= batch_size {
                                    Self::flush_batch_http(&mut client, &mut batch, &service_name, &custom_headers).await;
                                }
                            }
                            None => {
                                if !batch.is_empty() {
                                    Self::flush_batch_http(&mut client, &mut batch, &service_name, &custom_headers).await;
                                }
                                break;
                            }
                        }
                    }
                    _ = ticker.tick() => {
                        if !batch.is_empty() {
                            Self::flush_batch_http(&mut client, &mut batch, &service_name, &custom_headers).await;
                        }
                    }
                }
            }
        }
    }

    /// Flushes a batch of log records to the OTLP collector via HTTP/JSON.
    async fn flush_batch_http(
        client: &mut OtlpLogClient,
        batch: &mut Vec<OtlpLogItem>,
        service_name: &str,
        custom_headers: &HeaderMap,
    ) {
        let json_str = format_otlp_logs_json(batch, service_name);
        batch.clear();

        if let Err(e) = client.send_logs(json_str.as_bytes(), custom_headers).await {
            tracing::warn!(error = %e, "Failed to flush OTLP logs batch to collector via HTTP");
        }
    }

    /// Flushes a batch of log records to the OTLP collector via gRPC/Protobuf.
    async fn flush_batch_grpc(
        client: &mut OtlpLogsGrpcClient,
        batch: &mut Vec<OtlpLogItem>,
        service_name: &str,
        custom_headers: &HeaderMap,
    ) {
        let proto_bytes = format_otlp_logs_proto(batch, service_name);
        batch.clear();

        if let Err(e) = client.send_logs(&proto_bytes, custom_headers).await {
            tracing::warn!(error = %e, "Failed to flush OTLP logs batch to collector via gRPC");
        }
    }

    /// Returns cumulative number of records dropped due to queue saturation.
    pub fn dropped_count(&self) -> u64 {
        self.dropped_count.load(Ordering::Relaxed)
    }

    /// Returns cumulative number of successfully queued log records.
    pub fn logged_count(&self) -> u64 {
        self.logged_count.load(Ordering::Relaxed)
    }
}

impl PostResponsePlugin for OpentelemetryLoggerPlugin {
    fn id(&self) -> &str {
        &self.id
    }

    fn on_response(&self, req_headers: &velda_core::HeaderWorkspace, resp: &mut L7Response) {
        let now_nano = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);

        let status = resp.status.as_u16();

        let method = req_headers
            .get("x-original-method")
            .or_else(|| req_headers.get("x-forwarded-method"))
            .and_then(|v| v.to_str().ok())
            .unwrap_or("GET")
            .to_string();

        let path = req_headers
            .get("x-original-path")
            .or_else(|| req_headers.get("x-forwarded-uri"))
            .and_then(|v| v.to_str().ok())
            .unwrap_or("/")
            .to_string();

        let client_ip = req_headers
            .get("x-forwarded-for")
            .or_else(|| req_headers.get("x-real-ip"))
            .and_then(|v| v.to_str().ok())
            .map(|s| s.split(',').next().unwrap_or("").trim().to_string());

        let route_id = resp
            .headers
            .get(&X_ROUTE_ID)
            .or_else(|| req_headers.get(&X_ROUTE_ID))
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        let upstream = resp
            .headers
            .get(&X_UPSTREAM_ADDR)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        let latency_ms = resp
            .headers
            .get(&X_RESPONSE_TIME)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<f64>().ok());

        let trace_id = req_headers
            .get(&X_TRACE_ID)
            .or_else(|| req_headers.get(&X_REQUEST_ID))
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        let span_id = req_headers
            .get(&X_SPAN_ID)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        let item = OtlpLogItem {
            time_unix_nano: now_nano,
            status,
            method,
            path,
            client_ip,
            route_id,
            upstream,
            latency_ms,
            trace_id,
            span_id,
        };

        match self.sender.try_send(item) {
            Ok(()) => {
                self.logged_count.fetch_add(1, Ordering::Relaxed);
            }
            Err(_) => {
                self.dropped_count.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    #[inline(always)]
    fn capability(&self) -> PluginCapability {
        PluginCapability::header_only(AccessLevel::Read)
    }

    #[inline(always)]
    fn supported_protocols(&self) -> SupportedProtocols {
        SupportedProtocols::ALL_L7
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{HeaderValue, StatusCode};

    #[tokio::test]
    async fn test_opentelemetry_logger_capture() {
        let config = OpentelemetryLoggerConfig {
            protocol: "http".to_string(),
            endpoint: "http://127.0.0.1:4318/v1/logs".to_string(),
            batch_size: 10,
            flush_interval_ms: 100,
            buffer_capacity: 100,
            connect_timeout_ms: 5000,
            request_timeout_ms: 10000,
            service_name: "test-gateway".to_string(),
            headers: HashMap::new(),
        };

        let (plugin, mut rx) = OpentelemetryLoggerPlugin::new_with_receiver("otel_log", config);

        let mut req_headers = velda_core::HeaderWorkspace::empty();
        req_headers.insert("x-original-method", HeaderValue::from_static("POST"));
        req_headers.insert("x-original-path", HeaderValue::from_static("/api/orders"));
        req_headers.insert(
            X_REQUEST_ID,
            HeaderValue::from_static("4bf92f3577b34da6a3ce929d0e0e4736"),
        );
        req_headers.insert(X_SPAN_ID, HeaderValue::from_static("00f067aa0ba902b7"));

        let mut resp = L7Response::empty(StatusCode::CREATED);
        resp.headers
            .insert(X_ROUTE_ID, HeaderValue::from_static("orders-route"));
        resp.headers
            .insert(X_UPSTREAM_ADDR, HeaderValue::from_static("10.0.1.2:8080"));
        resp.headers
            .insert(X_RESPONSE_TIME, HeaderValue::from_static("3.45"));

        plugin.on_response(&req_headers, &mut resp);
        assert_eq!(plugin.logged_count(), 1);
        assert_eq!(plugin.dropped_count(), 0);

        let received = rx.recv().await.expect("Item received");
        assert_eq!(received.status, 201);
        assert_eq!(received.method, "POST");
        assert_eq!(received.path, "/api/orders");
        assert_eq!(received.route_id.as_deref(), Some("orders-route"));
        assert_eq!(received.upstream.as_deref(), Some("10.0.1.2:8080"));
        assert_eq!(received.latency_ms, Some(3.45));
        assert_eq!(
            received.trace_id.as_deref(),
            Some("4bf92f3577b34da6a3ce929d0e0e4736")
        );
        assert_eq!(received.span_id.as_deref(), Some("00f067aa0ba902b7"));
    }
}
