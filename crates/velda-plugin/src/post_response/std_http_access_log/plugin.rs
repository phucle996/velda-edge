//! Standard Stream HTTP Access Log Plugin (`std_http_access_log`).
//!
//! Provides hardware-harmonized, zero-allocation request access logging during the `post_response` phase.
//! Features status-aware sampling with instant hot-path escape, cacheline-aligned 256-byte vector records,
//! per-lane SPSC ring buffer queues, and single-syscall batch draining to `stdout` or `stderr`.
//!
//! # Micro-Architectural Guarantees
//! - **Instant Hot-Path Escape**: Status codes not explicitly declared in sampling config escape in ~1.5ns
//!   with zero allocations and zero contention.
//! - **Zero Serving I/O**: Frontline workers never write to disk or standard streams; records are enqueued
//!   via SIMD vector registers into dedicated SPSC lane shards.
//! - **Direct Standard Stream Output**: Shadow logistics workers drain batches using slot-filling serialization
//!   flushing via single `libc::write` syscalls to `stdout` (fd 1) or `stderr` (fd 2).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use velda_core::L7Response;
use velda_sync::post_sync::plugin::PluginConfig;

use super::config::{StdHttpAccessLogConfig, TargetStream};
use super::drainer::ShadowLogDrainer;
use super::record::{LogFormat, PackedLogRecord};
use super::ring_buffer::ShardedRingBuffer;
use super::sampling::SamplingTable;
use crate::capability::{AccessLevel, PluginCapability, SupportedProtocols};
use crate::error::PluginError;
use crate::header::{X_REQUEST_ID, X_RESPONSE_TIME, X_ROUTE_ID, X_UPSTREAM_ADDR};
use crate::post_response::PostResponsePlugin;

/// Standard Stream HTTP Access Log Plugin.
pub struct StdHttpAccessLogPlugin {
    pub id: String,
    pub config: StdHttpAccessLogConfig,
    pub ring_buffer: Arc<ShardedRingBuffer>,
    dropped_count: Arc<AtomicU64>,
    logged_count: Arc<AtomicU64>,
    shutdown: Arc<AtomicBool>,
}

impl StdHttpAccessLogPlugin {
    /// Constructs a new `StdHttpAccessLogPlugin` and spawns a background Shadow Logistics Drainer.
    pub fn new(id: impl Into<String>, config: StdHttpAccessLogConfig) -> Self {
        let workers = velda_core::global_hardware_topology().worker_threads.max(1);
        let ring_buffer = Arc::new(ShardedRingBuffer::new(workers, config.buffer_capacity));
        let shutdown = Arc::new(AtomicBool::new(false));

        let plugin = Self {
            id: id.into(),
            config: config.clone(),
            ring_buffer: ring_buffer.clone(),
            dropped_count: Arc::new(AtomicU64::new(0)),
            logged_count: Arc::new(AtomicU64::new(0)),
            shutdown: shutdown.clone(),
        };

        // Spawn background shadow worker drainer if inside an active Tokio runtime
        if let Ok(_handle) = tokio::runtime::Handle::try_current() {
            let drainer =
                ShadowLogDrainer::new(config.target, config.format, ring_buffer, shutdown);
            drainer.spawn();
        }

        plugin
    }

    /// Constructs an instance for isolated testing without background task spawning.
    pub fn new_for_test(id: impl Into<String>, config: StdHttpAccessLogConfig) -> Self {
        let workers = 2;
        let ring_buffer = Arc::new(ShardedRingBuffer::new(workers, config.buffer_capacity));
        Self {
            id: id.into(),
            config,
            ring_buffer,
            dropped_count: Arc::new(AtomicU64::new(0)),
            logged_count: Arc::new(AtomicU64::new(0)),
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Deserializes and constructs an instance directly from a [`PluginConfig`].
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let cfg = velda_sync::validate_std_http_access_log_config(&config.id, &config.config)
            .map_err(|e| {
                PluginError::Config(format!(
                    "Invalid std_http_access_log configuration for '{}': {e}",
                    config.id
                ))
            })?;

        let target = TargetStream::parse(&cfg.target).map_err(PluginError::Config)?;
        let format = LogFormat::parse(&cfg.format);
        let sampling = SamplingTable::from_map(&cfg.sampling).map_err(PluginError::Config)?;

        Ok(Self::new(
            &config.id,
            StdHttpAccessLogConfig {
                target,
                format,
                include_user_agent: cfg.include_user_agent,
                include_request_id: cfg.include_request_id,
                include_route_id: cfg.include_route_id,
                include_upstream: cfg.include_upstream,
                include_latency: cfg.include_latency,
                buffer_capacity: cfg.buffer_capacity,
                sampling,
            },
        ))
    }

    /// Number of successfully enqueued log entries.
    #[inline(always)]
    pub fn logged_count(&self) -> u64 {
        self.logged_count.load(Ordering::Relaxed)
    }

    /// Number of dropped log entries due to ring buffer saturation.
    #[inline(always)]
    pub fn dropped_count(&self) -> u64 {
        self.dropped_count.load(Ordering::Relaxed)
    }

    /// Helper for test verification: drains enqueued records into a memory buffer.
    pub fn drain_to_buffer(&self, buf: &mut Vec<u8>) -> usize {
        let drainer = ShadowLogDrainer::new(
            self.config.target,
            self.config.format,
            self.ring_buffer.clone(),
            self.shutdown.clone(),
        );
        drainer.drain_to_buffer(buf)
    }
}

impl Drop for StdHttpAccessLogPlugin {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
    }
}

impl PostResponsePlugin for StdHttpAccessLogPlugin {
    #[inline(always)]
    fn id(&self) -> &str {
        &self.id
    }

    fn on_response(&self, req_headers: &velda_core::HeaderWorkspace, resp: &mut L7Response) {
        let status = resp.status.as_u16();

        // Instant Hot-Path Escape: If status is not sampled, abort in ~1.5ns
        if !self.config.sampling.should_sample(status) {
            return;
        }

        // Construct 256-byte aligned record directly on CPU stack
        let mut record = PackedLogRecord::empty();
        record.status = status;
        record.version = PackedLogRecord::encode_version(resp.version);

        // Fast timestamp from system clock
        record.timestamp_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        // Content length (O(1) read from HeaderWorkspace arithmetic register / numeric_entries)
        let cl_raw = resp
            .headers
            .get_u64(http::header::CONTENT_LENGTH)
            .or_else(|| {
                if !resp.body.is_empty() {
                    Some(resp.body.len() as u64)
                } else {
                    None
                }
            })
            .unwrap_or(0);
        record.content_length = cl_raw.min(u32::MAX as u64) as u32;

        // Latency in microseconds
        if self.config.include_latency
            && let Some(val) = resp
                .headers
                .get(&X_RESPONSE_TIME)
                .and_then(|v| v.to_str().ok())
            && let Ok(ms) = val.trim_end_matches("ms").trim().parse::<f64>()
        {
            record.duration_us = (ms * 1000.0).clamp(0.0, u32::MAX as f64) as u32;
        }

        // Method
        if let Some(m) = req_headers
            .get("x-forwarded-method")
            .and_then(|v| v.to_str().ok())
        {
            if let Ok(method) = http::Method::from_bytes(m.as_bytes()) {
                record.method = PackedLogRecord::encode_method(&method);
            }
        } else {
            record.method = 1; // Default GET
        }

        // Canonical Request ID
        if self.config.include_request_id
            && let Some(id) = req_headers.get(&X_REQUEST_ID)
        {
            record.set_request_id(id.as_bytes());
        }

        // Matched Route ID
        if self.config.include_route_id
            && let Some(rt) = resp
                .headers
                .get(&X_ROUTE_ID)
                .or_else(|| req_headers.get(&X_ROUTE_ID))
        {
            record.set_route_id(rt.as_bytes());
        }

        // Target Upstream Address
        if self.config.include_upstream
            && let Some(up) = resp.headers.get(&X_UPSTREAM_ADDR)
        {
            record.set_upstream(up.as_bytes());
        }

        // Client User-Agent Header (up to 192 bytes inline)
        if self.config.include_user_agent
            && let Some(ua) = req_headers
                .get(http::header::USER_AGENT)
                .or_else(|| req_headers.get("user-agent"))
        {
            record.set_user_agent(ua.as_bytes());
        }

        // Non-blocking lock-free enqueue via hardware SIMD registers
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
    use http::{HeaderMap, HeaderValue, StatusCode, Version};
    use velda_core::Body;

    #[test]
    fn test_access_log_extracts_headers_status_and_user_agent() {
        let mut sampling = SamplingTable::empty();
        sampling.set_rate(200, 1.0); // 100% sample for 200

        let config = StdHttpAccessLogConfig {
            sampling,
            buffer_capacity: 64,
            ..Default::default()
        };
        let plugin = StdHttpAccessLogPlugin::new_for_test("std_http_access_log", config);

        let mut headers = HeaderMap::new();
        headers.insert("x-route-id", HeaderValue::from_static("orders"));
        headers.insert("x-upstream-addr", HeaderValue::from_static("10.0.1.5:8080"));
        headers.insert("x-response-time", HeaderValue::from_static("14.5ms"));
        headers.insert("content-length", HeaderValue::from_static("256"));

        let mut resp = L7Response::new(
            StatusCode::OK,
            Version::HTTP_2,
            headers,
            Body::Bytes(bytes::Bytes::from_static(b"response-body")),
        );

        let mut req_headers = velda_core::HeaderWorkspace::default();
        req_headers.insert("x-request-id", HeaderValue::from_static("req-12345"));
        req_headers.insert(
            "user-agent",
            HeaderValue::from_static(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0.0.0",
            ),
        );
        req_headers.insert("x-forwarded-method", HeaderValue::from_static("POST"));

        plugin.on_response(&req_headers, &mut resp);

        assert_eq!(plugin.logged_count(), 1);
        assert_eq!(plugin.dropped_count(), 0);

        let mut out = Vec::new();
        let drained = plugin.drain_to_buffer(&mut out);
        assert_eq!(drained, 1);

        let json = std::str::from_utf8(&out).unwrap();
        assert!(json.contains("\"status\":200"));
        assert!(json.contains("\"method\":\"POST\""));
        assert!(json.contains("\"version\":\"HTTP/2.0\""));
        assert!(json.contains("\"latency_us\":14500"));
        assert!(json.contains("\"request_id\":\"req-12345\""));
        assert!(json.contains("\"route_id\":\"orders\""));
        assert!(json.contains("\"upstream\":\"10.0.1.5:8080\""));
        assert!(json.contains("Mozilla/5.0"));
    }

    #[test]
    fn test_unsampled_status_escapes_without_logging() {
        let mut sampling = SamplingTable::empty();
        sampling.set_rate(502, 1.0); // Only 502 is sampled

        let config = StdHttpAccessLogConfig {
            sampling,
            ..Default::default()
        };
        let plugin = StdHttpAccessLogPlugin::new_for_test("std_http_access_log", config);

        let mut resp = L7Response::empty(StatusCode::OK); // 200 is unconfigured
        let req_headers = velda_core::HeaderWorkspace::default();

        plugin.on_response(&req_headers, &mut resp);

        // 200 was not configured -> 0 logged, 0 dropped
        assert_eq!(plugin.logged_count(), 0);
        assert_eq!(plugin.dropped_count(), 0);
    }

    #[test]
    fn test_access_log_buffer_saturation_drops_non_blocking() {
        let mut sampling = SamplingTable::empty();
        sampling.set_rate(200, 1.0);

        let config = StdHttpAccessLogConfig {
            sampling,
            buffer_capacity: 64, // min capacity
            ..Default::default()
        };
        let plugin = StdHttpAccessLogPlugin::new_for_test("std_http_access_log", config);

        let mut resp = L7Response::empty(StatusCode::OK);
        let req_headers = velda_core::HeaderWorkspace::default();

        // Fill buffer to capacity (each shard has 64 capacity)
        for _ in 0..128 {
            plugin.on_response(&req_headers, &mut resp);
        }

        // Saturate further
        for _ in 0..10 {
            plugin.on_response(&req_headers, &mut resp);
        }

        // Drops must have occurred without panic or blocking
        assert!(plugin.dropped_count() > 0);
    }
}
