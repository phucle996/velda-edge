//! OpenTelemetry Span Reporter Post-Response Plugin (`opentelemetry_tracer`).
//!
//! Provides zero-blocking, zero-heap-allocation OpenTelemetry span reporting during `post_response`.
//! Reconstructs end-to-end edge request spans from lifecycle metadata headers,
//! writes 256-byte cacheline-aligned [`PackedOtlpSpan`] records to page-aligned RAM slots
//! via non-temporal vector instructions (`_mm256_stream_si256`), and hands off the slot index
//! to the background Shadow Worker in ~1ns without stalling Data Plane frontline traffic workers.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use http::HeaderMap;
use velda_core::L7Response;
use velda_sync::post_sync::plugin::PluginConfig;

use super::config::OpentelemetryTracerConfig;
use super::drainer::ShadowOtlpTracerDrainer;
use super::ring_buffer::ShardedRingBuffer;
use super::sampling::SamplingTable;
use super::span::{PackedOtlpSpan, decode_hex};
use crate::capability::{AccessLevel, PluginCapability, SupportedProtocols};
use crate::error::PluginError;
use crate::header::{
    TRACEPARENT, X_PARENT_SPAN_ID, X_REQUEST_ID, X_RESPONSE_TIME, X_ROUTE_ID, X_SPAN_ID,
    X_TRACE_ID, X_UPSTREAM_ADDR,
};
use crate::post_response::PostResponsePlugin;

/// Post-Response OpenTelemetry Tracer Plugin.
pub struct OpentelemetryTracerPlugin {
    pub id: String,
    pub config: OpentelemetryTracerConfig,
    sampling: SamplingTable,
    ring_buffer: Arc<ShardedRingBuffer>,
    dropped_count: Arc<AtomicU64>,
    logged_count: Arc<AtomicU64>,
    shutdown: Arc<AtomicBool>,
}

impl OpentelemetryTracerPlugin {
    /// Constructs a new `OpentelemetryTracerPlugin` and starts the asynchronous Shadow flusher.
    pub fn new(id: impl Into<String>, config: OpentelemetryTracerConfig) -> Self {
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

        let drainer = ShadowOtlpTracerDrainer::new(
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

        drainer.dispatch_to_shadow();

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
        let parsed: OpentelemetryTracerConfig = serde_json::from_value(config.config.clone())
            .map_err(|e| {
                PluginError::Config(format!("Failed to parse opentelemetry_tracer config: {e}"))
            })?;
        Ok(Self::new(&config.id, parsed))
    }

    /// Returns cumulative number of records dropped due to ring buffer saturation.
    pub fn dropped_count(&self) -> u64 {
        self.dropped_count.load(Ordering::Relaxed)
    }

    /// Returns cumulative number of successfully queued span records.
    pub fn logged_count(&self) -> u64 {
        self.logged_count.load(Ordering::Relaxed)
    }

    /// Returns reference to internal ring buffer container for testing and inspection.
    pub fn ring_buffer(&self) -> &Arc<ShardedRingBuffer> {
        &self.ring_buffer
    }
}

impl Drop for OpentelemetryTracerPlugin {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
    }
}

impl PostResponsePlugin for OpentelemetryTracerPlugin {
    fn id(&self) -> &str {
        &self.id
    }

    fn on_response(&self, req_headers: &velda_core::HeaderWorkspace, resp: &mut L7Response) {
        let status = resp.status.as_u16();

        // 1. Direct-index status-aware sampling check (Instant escape in ~1.4ns)
        if !self.sampling.should_sample(status) {
            return;
        }

        // 2. Construct 256-byte aligned POD span directly on CPU Stack (hot in L1 cache)
        let mut span = PackedOtlpSpan::empty();
        span.status_code = status;
        span.otlp_status = if status < 400 { 1 } else { 2 };
        span.span_kind = 2; // SPAN_KIND_SERVER

        // Timestamps (Nanoseconds from system clock or coarse fallback)
        let now_nano = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or_else(|_| velda_core::coarse_now_sec().saturating_mul(1_000_000_000));
        span.end_time_unix_nano = now_nano;

        if let Some(lat) = resp
            .headers
            .get(&X_RESPONSE_TIME)
            .and_then(|v| v.to_str().ok())
            && let Ok(ms) = lat.trim_end_matches("ms").trim().parse::<f64>()
        {
            let dur_nanos = (ms * 1_000_000.0).clamp(0.0, u64::MAX as f64) as u64;
            span.start_time_unix_nano = now_nano.saturating_sub(dur_nanos);
        } else {
            span.start_time_unix_nano = now_nano;
        }

        // Method
        if let Some(m) = req_headers
            .get("x-forwarded-method")
            .or_else(|| req_headers.get("x-original-method"))
            .and_then(|v| v.to_str().ok())
        {
            if let Ok(method) = http::Method::from_bytes(m.as_bytes()) {
                span.method = PackedOtlpSpan::encode_method(&method);
            }
        } else {
            span.method = 1; // Default GET
        }

        // URL Path
        if let Some(path_bytes) = req_headers
            .get("x-forwarded-uri")
            .or_else(|| req_headers.get("x-original-path"))
            .map(|v| v.as_bytes())
        {
            span.set_path(path_bytes);
        } else {
            span.set_path(b"/");
        }

        // Client IP
        if let Some(ip_str) = req_headers
            .get("x-forwarded-for")
            .or_else(|| req_headers.get("x-real-ip"))
            .and_then(|v| v.to_str().ok())
        {
            let first_ip = ip_str.split(',').next().unwrap_or("").trim();
            if let Ok(ip) = first_ip.parse::<std::net::IpAddr>() {
                span.set_client_ip(ip);
            }
        }

        // Matched Route ID
        if let Some(rt) = resp
            .headers
            .get(&X_ROUTE_ID)
            .or_else(|| req_headers.get(&X_ROUTE_ID))
        {
            span.set_route_id(rt.as_bytes());
        }

        // Upstream Target Address
        if let Some(up) = resp.headers.get(&X_UPSTREAM_ADDR) {
            span.set_upstream(up.as_bytes());
        }

        // 3. W3C Trace ID & Span ID Resolution (Zero-copy hex decode from HeaderWorkspace)
        if let Some(t_val) = req_headers
            .get(&X_TRACE_ID)
            .or_else(|| req_headers.get(&X_REQUEST_ID))
        {
            let bytes = t_val.as_bytes();
            if bytes.len() == 32 {
                let mut raw = [0u8; 16];
                if decode_hex(bytes, &mut raw) == 16 {
                    span.set_trace_id(&raw);
                } else {
                    span.set_trace_id(bytes);
                }
            } else {
                span.set_trace_id(bytes);
            }
        }

        if let Some(s_val) = req_headers.get(&X_SPAN_ID) {
            let bytes = s_val.as_bytes();
            if bytes.len() == 16 {
                let mut raw = [0u8; 8];
                if decode_hex(bytes, &mut raw) == 8 {
                    span.set_span_id(&raw);
                } else {
                    span.set_span_id(bytes);
                }
            } else {
                span.set_span_id(bytes);
            }
        }

        // 4. Parent Span ID (Enables distributed hierarchical trace tree linking)
        if let Some(p_val) = req_headers.get(&X_PARENT_SPAN_ID) {
            let bytes = p_val.as_bytes();
            if bytes.len() == 16 {
                let mut raw = [0u8; 8];
                if decode_hex(bytes, &mut raw) == 8 {
                    span.set_parent_span_id(&raw);
                }
            }
        }

        // Fallback: Parse directly from TRACEPARENT if X_TRACE_ID / X_SPAN_ID were omitted
        if (span.trace_id == [0; 16] || span.span_id == [0; 8])
            && let Some(tp_val) = req_headers.get(&TRACEPARENT)
        {
            let bytes = tp_val.as_bytes();
            if bytes.len() >= 53 && bytes.starts_with(b"00-") {
                if span.trace_id == [0; 16] {
                    let mut raw = [0u8; 16];
                    if decode_hex(&bytes[3..35], &mut raw) == 16 {
                        span.set_trace_id(&raw);
                    }
                }
                if span.span_id == [0; 8] {
                    let mut raw = [0u8; 8];
                    if decode_hex(&bytes[36..52], &mut raw) == 8 {
                        span.set_span_id(&raw);
                    }
                }
            }
        }

        // 5. Stream 256-byte POD span directly to SPSC ring buffer in RAM via AVX2 non-temporal store
        match self.ring_buffer.try_push(&span) {
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
        SupportedProtocols::ALL_L7
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{HeaderValue, StatusCode};
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_opentelemetry_tracer_capture() {
        let config = OpentelemetryTracerConfig {
            protocol: "http".to_string(),
            endpoint: "http://127.0.0.1:4318/v1/traces".to_string(),
            batch_size: 10,
            flush_interval_ms: 100,
            buffer_capacity: 100,
            connect_timeout_ms: 5000,
            request_timeout_ms: 10000,
            service_name: "test-gateway".to_string(),
            headers: HashMap::new(),
            sampling: HashMap::new(),
        };

        let plugin = OpentelemetryTracerPlugin::new("otel_tracer", config);

        let mut req_headers = velda_core::HeaderWorkspace::empty();
        req_headers.insert("x-original-method", HeaderValue::from_static("GET"));
        req_headers.insert("x-original-path", HeaderValue::from_static("/api/users"));
        req_headers.insert(
            X_TRACE_ID,
            HeaderValue::from_static("4bf92f3577b34da6a3ce929d0e0e4736"),
        );
        req_headers.insert(X_SPAN_ID, HeaderValue::from_static("00f067aa0ba902b7"));
        req_headers.insert(
            X_PARENT_SPAN_ID,
            HeaderValue::from_static("5fb397be34d23b0f"),
        );

        let mut resp = L7Response::empty(StatusCode::OK);
        resp.headers
            .insert(X_ROUTE_ID, HeaderValue::from_static("users-route"));
        resp.headers
            .insert(X_UPSTREAM_ADDR, HeaderValue::from_static("10.0.0.5:8080"));
        resp.headers
            .insert(X_RESPONSE_TIME, HeaderValue::from_static("2.10"));

        plugin.on_response(&req_headers, &mut resp);
        assert_eq!(plugin.logged_count(), 1);
        assert_eq!(plugin.dropped_count(), 0);

        let mut batch = vec![PackedOtlpSpan::empty(); 10];
        let drained = plugin.ring_buffer().pop_all(&mut batch);
        assert_eq!(drained, 1);

        let received = &batch[0];
        assert_eq!(received.status_code, 200);
        assert_eq!(received.path(), "/api/users");
        assert_eq!(received.method_str(), "GET");
        assert_eq!(received.route_id(), "users-route");
        assert_eq!(received.upstream(), "10.0.0.5:8080");
        assert_eq!(received.has_parent, 1);

        let mut trace_hex = [0u8; 32];
        for (i, b) in received.trace_id.iter().enumerate() {
            let s = format!("{:02x}", b);
            trace_hex[i * 2] = s.as_bytes()[0];
            trace_hex[i * 2 + 1] = s.as_bytes()[1];
        }
        assert_eq!(
            std::str::from_utf8(&trace_hex).unwrap(),
            "4bf92f3577b34da6a3ce929d0e0e4736"
        );

        let mut span_hex = [0u8; 16];
        for (i, b) in received.span_id.iter().enumerate() {
            let s = format!("{:02x}", b);
            span_hex[i * 2] = s.as_bytes()[0];
            span_hex[i * 2 + 1] = s.as_bytes()[1];
        }
        assert_eq!(std::str::from_utf8(&span_hex).unwrap(), "00f067aa0ba902b7");

        let mut parent_hex = [0u8; 16];
        for (i, b) in received.parent_span_id.iter().enumerate() {
            let s = format!("{:02x}", b);
            parent_hex[i * 2] = s.as_bytes()[0];
            parent_hex[i * 2 + 1] = s.as_bytes()[1];
        }
        assert_eq!(
            std::str::from_utf8(&parent_hex).unwrap(),
            "5fb397be34d23b0f"
        );
    }
}
