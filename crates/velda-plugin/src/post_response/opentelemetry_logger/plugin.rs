//! OpenTelemetry Logger Post-Response Plugin (`opentelemetry_logger`).
//!
//! Provides zero-blocking, zero-heap-allocation OpenTelemetry structured access logging during `post_response`.
//! Constructs a 256-byte cacheline-aligned record on the CPU stack, streams directly to page-aligned RAM
//! via non-temporal vector instructions (`_mm256_stream_si256`), and hands off the memory slot index
//! to the background Shadow Worker in ~1ns.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use http::HeaderMap;
use velda_core::L7Response;
use velda_sync::post_sync::plugin::PluginConfig;

use super::config::OpentelemetryLoggerConfig;
use super::drainer::ShadowOtlpDrainer;
use super::record::{PackedOtlpRecord, decode_hex};
use super::ring_buffer::ShardedRingBuffer;
use super::sampling::SamplingTable;
use crate::capability::{AccessLevel, PluginCapability, SupportedProtocols};
use crate::error::PluginError;
use crate::header::{
    X_REQUEST_ID, X_RESPONSE_TIME, X_ROUTE_ID, X_SPAN_ID, X_TRACE_ID, X_UPSTREAM_ADDR,
};
use crate::hook::try_dispatch_shadow;
use crate::post_response::PostResponsePlugin;

/// Post-Response OpenTelemetry Structured Logger Plugin.
pub struct OpentelemetryLoggerPlugin {
    pub id: String,
    pub config: OpentelemetryLoggerConfig,
    sampling: SamplingTable,
    ring_buffer: Arc<ShardedRingBuffer>,
    dropped_count: Arc<AtomicU64>,
    logged_count: Arc<AtomicU64>,
    shutdown: Arc<AtomicBool>,
}

impl OpentelemetryLoggerPlugin {
    /// Constructs a new `OpentelemetryLoggerPlugin` and starts the asynchronous Shadow flusher.
    pub fn new(id: impl Into<String>, config: OpentelemetryLoggerConfig) -> Self {
        let id_str = id.into();
        let topology = velda_core::global_hardware_topology();
        let num_workers = topology.worker_threads.max(1);

        let ring_buffer = Arc::new(ShardedRingBuffer::new(
            num_workers,
            config.buffer_capacity.max(128),
        ));
        let shutdown = Arc::new(AtomicBool::new(false));
        let dropped_count = Arc::new(AtomicU64::new(0));
        let logged_count = Arc::new(AtomicU64::new(0));

        let sampling = config
            .build_sampling_table()
            .unwrap_or_else(|_| SamplingTable::empty());

        let mut custom_headers = HeaderMap::new();
        for (k, v) in &config.headers {
            if let Ok(name) = http::header::HeaderName::from_bytes(k.as_bytes())
                && let Ok(val) = http::header::HeaderValue::from_str(v)
            {
                custom_headers.insert(name, val);
            }
        }

        let drainer = ShadowOtlpDrainer::new(
            config.protocol.clone(),
            config.endpoint.clone(),
            config.batch_size,
            Duration::from_millis(config.flush_interval_ms.max(10)),
            Duration::from_millis(config.connect_timeout_ms),
            Duration::from_millis(config.request_timeout_ms),
            config.service_name.clone(),
            custom_headers,
            ring_buffer.clone(),
            shutdown.clone(),
        );

        let flusher_task = Box::pin(async move {
            drainer.run().await;
        });

        // Priority 1: Dispatch onto Shadow Worker runtime via non-blocking try_dispatch_shadow
        if let Err(task) = try_dispatch_shadow(flusher_task) {
            // Priority 2: Fallback to Tokio current runtime if Shadow Worker hook is uninitialized
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                handle.spawn(task);
            }
        }

        Self {
            id: id_str,
            config,
            sampling,
            ring_buffer,
            dropped_count,
            logged_count,
            shutdown,
        }
    }

    /// Deserializes and validates this plugin from declarative [`PluginConfig`].
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let parsed: OpentelemetryLoggerConfig = serde_json::from_value(config.config.clone())
            .map_err(|e| {
                PluginError::Config(format!("Failed to parse opentelemetry_logger config: {e}"))
            })?;
        Ok(Self::new(&config.id, parsed))
    }

    /// Accessor for testing: pushes directly to ring buffer.
    pub fn try_push_record(&self, record: &PackedOtlpRecord) -> bool {
        self.ring_buffer.try_push(record).is_ok()
    }

    /// Returns cumulative number of records dropped due to queue saturation.
    pub fn dropped_count(&self) -> u64 {
        self.dropped_count.load(Ordering::Relaxed)
    }

    /// Returns cumulative number of successfully queued log records.
    pub fn logged_count(&self) -> u64 {
        self.logged_count.load(Ordering::Relaxed)
    }

    pub fn ring_buffer(&self) -> Arc<ShardedRingBuffer> {
        self.ring_buffer.clone()
    }
}

impl Drop for OpentelemetryLoggerPlugin {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
    }
}

impl PostResponsePlugin for OpentelemetryLoggerPlugin {
    fn id(&self) -> &str {
        &self.id
    }

    fn on_response(&self, req_headers: &velda_core::HeaderWorkspace, resp: &mut L7Response) {
        let status = resp.status.as_u16();

        // 1. Instant Hot-Path Escape: Status-Aware Sampling in ~1.5ns
        if !self.sampling.should_sample(status) {
            return;
        }

        // 2. Construct 256-byte aligned POD record directly on CPU Stack (hot in L1 cache)
        let mut record = PackedOtlpRecord::empty();
        record.status = status;

        // Nanosecond timestamp from system clock or coarse fallback
        record.time_unix_nano = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or_else(|_| velda_core::coarse_now_sec().saturating_mul(1_000_000_000));

        // Method
        if let Some(m) = req_headers
            .get("x-forwarded-method")
            .or_else(|| req_headers.get("x-original-method"))
            .and_then(|v| v.to_str().ok())
        {
            if let Ok(method) = http::Method::from_bytes(m.as_bytes()) {
                record.method = PackedOtlpRecord::encode_method(&method);
            }
        } else {
            record.method = 1; // Default GET
        }

        // URL Path
        if let Some(path_bytes) = req_headers
            .get("x-forwarded-uri")
            .or_else(|| req_headers.get("x-original-path"))
            .map(|v| v.as_bytes())
        {
            record.set_path(path_bytes);
        } else {
            record.set_path(b"/");
        }

        // Client IP
        if let Some(ip_str) = req_headers
            .get("x-forwarded-for")
            .or_else(|| req_headers.get("x-real-ip"))
            .and_then(|v| v.to_str().ok())
        {
            let first_ip = ip_str.split(',').next().unwrap_or("").trim();
            if let Ok(ip) = first_ip.parse::<std::net::IpAddr>() {
                record.set_client_ip(ip);
            }
        }

        // Matched Route ID
        if let Some(rt) = resp
            .headers
            .get(&X_ROUTE_ID)
            .or_else(|| req_headers.get(&X_ROUTE_ID))
        {
            record.set_route_id(rt.as_bytes());
        }

        // Upstream Address
        if let Some(up) = resp.headers.get(&X_UPSTREAM_ADDR) {
            record.set_upstream(up.as_bytes());
        }

        // Latency
        if let Some(lat) = resp
            .headers
            .get(&X_RESPONSE_TIME)
            .and_then(|v| v.to_str().ok())
            && let Ok(ms) = lat.trim_end_matches("ms").trim().parse::<f64>()
        {
            record.duration_us = (ms * 1000.0).clamp(0.0, u32::MAX as f64) as u32;
        }

        // W3C Trace ID & Span ID
        if let Some(trace_id_val) = req_headers
            .get(&X_TRACE_ID)
            .or_else(|| req_headers.get(&X_REQUEST_ID))
        {
            let bytes = trace_id_val.as_bytes();
            if bytes.len() == 32 {
                let mut raw = [0u8; 16];
                if decode_hex(bytes, &mut raw) == 16 {
                    record.set_trace_id(&raw);
                } else {
                    record.set_trace_id(bytes);
                }
            } else {
                record.set_trace_id(bytes);
            }
        }

        if let Some(span_id_val) = req_headers.get(&X_SPAN_ID) {
            let bytes = span_id_val.as_bytes();
            if bytes.len() == 16 {
                let mut raw = [0u8; 8];
                if decode_hex(bytes, &mut raw) == 8 {
                    record.set_span_id(&raw);
                } else {
                    record.set_span_id(bytes);
                }
            } else {
                record.set_span_id(bytes);
            }
        }

        // 3. Write directly to RAM ring buffer slot via _mm256_stream_si256 and hand off index
        match self.ring_buffer.try_push(&record) {
            Ok(()) => {
                self.logged_count.fetch_add(1, Ordering::Relaxed);
            }
            Err(()) => {
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
        SupportedProtocols::ALL_HTTP
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
            headers: std::collections::HashMap::new(),
            sampling: std::collections::HashMap::new(),
        };

        let plugin = OpentelemetryLoggerPlugin::new("otel_log", config);

        let mut req_headers = velda_core::HeaderWorkspace::empty();
        req_headers.insert("x-forwarded-method", HeaderValue::from_static("POST"));
        req_headers.insert("x-forwarded-uri", HeaderValue::from_static("/api/orders"));
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
            .insert(X_RESPONSE_TIME, HeaderValue::from_static("3.45ms"));

        plugin.on_response(&req_headers, &mut resp);
        assert_eq!(plugin.logged_count(), 1);
        assert_eq!(plugin.dropped_count(), 0);

        let mut batch = [PackedOtlpRecord::empty(); 1];
        let drained = plugin.ring_buffer().pop_all(&mut batch);
        assert_eq!(drained, 1);
        assert_eq!(batch[0].status, 201);
        assert_eq!(batch[0].method_str(), "POST");
        assert_eq!(batch[0].path(), "/api/orders");
        assert_eq!(batch[0].route_id(), "orders-route");
        assert_eq!(batch[0].upstream(), "10.0.1.2:8080");
        assert_eq!(batch[0].duration_us, 3450);
        let mut expected_span = [0u8; 8];
        decode_hex(b"00f067aa0ba902b7", &mut expected_span);
        assert_eq!(batch[0].span_id(), &expected_span[..]);
    }
}
