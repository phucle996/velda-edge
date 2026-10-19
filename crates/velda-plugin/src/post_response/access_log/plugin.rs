//! Post-Response Access Log Plugin (`access_log`).
//!
//! Provides lock-free, zero-blocking request access logging during the `post_response` phase.
//! Extracts response status, content size, and lifecycle metadata headers, dispatching
//! structured records into a bounded channel for asynchronous background draining without
//! performing synchronous disk I/O or blocking traffic worker threads.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use velda_core::L7Response;
use velda_sync::post_sync::plugin::PluginConfig;

use crate::capability::{AccessLevel, PluginCapability, SupportedProtocols};
use crate::error::PluginError;
use crate::post_response::PostResponsePlugin;

/// Formatting output style for serialized access log entries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LogFormat {
    #[default]
    Json,
    Text,
}

impl LogFormat {
    pub fn parse(s: &str) -> Self {
        match s.trim().to_ascii_lowercase().as_str() {
            "text" | "combined" | "clf" => Self::Text,
            _ => Self::Json,
        }
    }
}

/// Configuration settings for the `access_log` plugin.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccessLogConfig {
    /// Extracts and records the `x-request-id` header if present.
    pub include_request_id: bool,
    /// Extracts and records the `x-route-id` or `x-velda-route` header if present.
    pub include_route_id: bool,
    /// Extracts and records the `x-upstream-addr` or `x-upstream-id` header if present.
    pub include_upstream: bool,
    /// Extracts and records the `x-response-time` or `x-upstream-response-time` header if present.
    pub include_latency: bool,
    /// Output serialization format (JSON or Text).
    pub format: LogFormat,
    /// Bounded capacity of the lock-free log dispatch queue.
    pub buffer_capacity: usize,
}

impl Default for AccessLogConfig {
    fn default() -> Self {
        Self {
            include_request_id: true,
            include_route_id: true,
            include_upstream: true,
            include_latency: true,
            format: LogFormat::Json,
            buffer_capacity: 65536,
        }
    }
}

/// Strongly typed, structured record emitted by the `access_log` hook on every response.
#[derive(Debug, Clone, PartialEq)]
pub struct LogRecord {
    pub timestamp_ms: u64,
    pub status: u16,
    pub version: &'static str,
    pub content_length: Option<u64>,
    pub request_id: Option<String>,
    pub route_id: Option<String>,
    pub upstream: Option<String>,
    pub latency_ms: Option<f64>,
}

impl LogRecord {
    /// Serializes this record into a structured JSON string.
    pub fn to_json_string(&self) -> String {
        serde_json::json!({
            "timestamp": self.timestamp_ms,
            "status": self.status,
            "version": self.version,
            "content_length": self.content_length,
            "request_id": self.request_id,
            "route_id": self.route_id,
            "upstream": self.upstream,
            "latency_ms": self.latency_ms,
        })
        .to_string()
    }

    /// Serializes this record into a human-readable, single-line log format.
    pub fn to_text_string(&self) -> String {
        format!(
            "[{}] status={} version={} bytes={} req_id={} route={} upstream={} latency_ms={}",
            self.timestamp_ms,
            self.status,
            self.version,
            self.content_length.unwrap_or(0),
            self.request_id.as_deref().unwrap_or("-"),
            self.route_id.as_deref().unwrap_or("-"),
            self.upstream.as_deref().unwrap_or("-"),
            self.latency_ms
                .map_or("-".to_string(), |l| format!("{:.2}", l)),
        )
    }
}

/// Post-Response Access Log Plugin.
pub struct AccessLogPlugin {
    pub id: String,
    pub config: AccessLogConfig,
    sender: tokio::sync::mpsc::Sender<LogRecord>,
    dropped_count: Arc<AtomicU64>,
    logged_count: Arc<AtomicU64>,
}

impl AccessLogPlugin {
    /// Constructs a new `AccessLogPlugin` and spawns a background draining task if in a Tokio runtime.
    pub fn new(id: impl Into<String>, config: AccessLogConfig) -> Self {
        let (plugin, mut rx) = Self::new_with_receiver(id, config);

        // Spawn background writer task if an active Tokio runtime is available
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                while let Some(_record) = rx.recv().await {
                    // Drained asynchronously; in full deployment, buffered batching writes to stdout/file.
                }
            });
        }

        plugin
    }

    /// Constructs an `AccessLogPlugin` returning the receiving channel end for test verification.
    pub fn new_with_receiver(
        id: impl Into<String>,
        config: AccessLogConfig,
    ) -> (Self, tokio::sync::mpsc::Receiver<LogRecord>) {
        let (tx, rx) = tokio::sync::mpsc::channel(config.buffer_capacity);
        let plugin = Self {
            id: id.into(),
            config,
            sender: tx,
            dropped_count: Arc::new(AtomicU64::new(0)),
            logged_count: Arc::new(AtomicU64::new(0)),
        };
        (plugin, rx)
    }

    /// Deserializes and constructs an instance directly from a [`PluginConfig`].
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let defaults = AccessLogConfig::default();

        if config.config.is_null() {
            return Ok(Self::new(&config.id, defaults));
        }

        let raw = config.config.as_object().ok_or_else(|| {
            PluginError::Config(format!("Plugin '{}' config must be an object", config.id))
        })?;

        let include_request_id = raw
            .get("include_request_id")
            .and_then(|v| v.as_bool())
            .unwrap_or(defaults.include_request_id);

        let include_route_id = raw
            .get("include_route_id")
            .and_then(|v| v.as_bool())
            .unwrap_or(defaults.include_route_id);

        let include_upstream = raw
            .get("include_upstream")
            .and_then(|v| v.as_bool())
            .unwrap_or(defaults.include_upstream);

        let include_latency = raw
            .get("include_latency")
            .and_then(|v| v.as_bool())
            .unwrap_or(defaults.include_latency);

        let format = raw
            .get("format")
            .and_then(|v| v.as_str())
            .map(LogFormat::parse)
            .unwrap_or(defaults.format);

        let buffer_capacity = raw
            .get("buffer_capacity")
            .and_then(|v| v.as_u64())
            .map(|v| v as usize)
            .unwrap_or(defaults.buffer_capacity);

        Ok(Self::new(
            &config.id,
            AccessLogConfig {
                include_request_id,
                include_route_id,
                include_upstream,
                include_latency,
                format,
                buffer_capacity,
            },
        ))
    }

    /// Number of successfully enqueued log entries.
    #[inline(always)]
    pub fn logged_count(&self) -> u64 {
        self.logged_count.load(Ordering::Relaxed)
    }

    /// Number of dropped log entries due to buffer saturation.
    #[inline(always)]
    pub fn dropped_count(&self) -> u64 {
        self.dropped_count.load(Ordering::Relaxed)
    }
}

impl PostResponsePlugin for AccessLogPlugin {
    #[inline(always)]
    fn id(&self) -> &str {
        &self.id
    }

    fn on_response(&self, resp: &mut L7Response) {
        let status = resp.status.as_u16();

        let version = match resp.version {
            http::Version::HTTP_10 => "HTTP/1.0",
            http::Version::HTTP_11 => "HTTP/1.1",
            http::Version::HTTP_2 => "HTTP/2.0",
            http::Version::HTTP_3 => "HTTP/3.0",
            _ => "HTTP/?",
        };

        let content_length = resp
            .headers
            .get(http::header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok())
            .or_else(|| {
                if let velda_core::Body::Bytes(ref b) = resp.body {
                    Some(b.len() as u64)
                } else {
                    None
                }
            });

        let request_id = if self.config.include_request_id {
            resp.headers
                .get("x-request-id")
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string())
        } else {
            None
        };

        let route_id = if self.config.include_route_id {
            resp.headers
                .get("x-route-id")
                .or_else(|| resp.headers.get("x-velda-route"))
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string())
        } else {
            None
        };

        let upstream = if self.config.include_upstream {
            resp.headers
                .get("x-upstream-addr")
                .or_else(|| resp.headers.get("x-upstream-id"))
                .or_else(|| resp.headers.get("x-upstream"))
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string())
        } else {
            None
        };

        let latency_ms = if self.config.include_latency {
            resp.headers
                .get("x-response-time")
                .or_else(|| resp.headers.get("x-upstream-response-time"))
                .or_else(|| resp.headers.get("x-latency-ms"))
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.trim_end_matches("ms").trim().parse::<f64>().ok())
        } else {
            None
        };

        let timestamp_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        let record = LogRecord {
            timestamp_ms,
            status,
            version,
            content_length,
            request_id,
            route_id,
            upstream,
            latency_ms,
        };

        // Non-blocking try_send: guarantees zero worker blocking under heavy load
        match self.sender.try_send(record) {
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
        SupportedProtocols::ALL_HTTP
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{HeaderMap, HeaderValue, StatusCode, Version};
    use serde_json::json;
    use velda_core::Body;

    #[test]
    fn test_access_log_extracts_headers_and_status() {
        let config = AccessLogConfig {
            buffer_capacity: 10,
            ..Default::default()
        };
        let (plugin, mut rx) = AccessLogPlugin::new_with_receiver("access_log", config);

        let mut headers = HeaderMap::new();
        headers.insert("x-request-id", HeaderValue::from_static("req-12345"));
        headers.insert("x-route-id", HeaderValue::from_static("route-orders"));
        headers.insert(
            "x-upstream-addr",
            HeaderValue::from_static("10.0.1.50:8080"),
        );
        headers.insert("x-response-time", HeaderValue::from_static("14.5ms"));
        headers.insert("content-length", HeaderValue::from_static("256"));

        let mut resp = L7Response::new(
            StatusCode::OK,
            Version::HTTP_2,
            headers,
            Body::Bytes(bytes::Bytes::from_static(b"response-body")),
        );

        plugin.on_response(&mut resp);

        assert_eq!(plugin.logged_count(), 1);
        assert_eq!(plugin.dropped_count(), 0);

        let record = rx.try_recv().expect("Should receive emitted record");
        assert_eq!(record.status, 200);
        assert_eq!(record.version, "HTTP/2.0");
        assert_eq!(record.content_length, Some(256));
        assert_eq!(record.request_id.as_deref(), Some("req-12345"));
        assert_eq!(record.route_id.as_deref(), Some("route-orders"));
        assert_eq!(record.upstream.as_deref(), Some("10.0.1.50:8080"));
        assert_eq!(record.latency_ms, Some(14.5));
    }

    #[test]
    fn test_access_log_buffer_saturation_drops_non_blocking() {
        // Tiny capacity of 1 record
        let config = AccessLogConfig {
            buffer_capacity: 1,
            ..Default::default()
        };
        let (plugin, _rx) = AccessLogPlugin::new_with_receiver("access_log", config);

        let mut resp = L7Response::empty(StatusCode::OK);

        // First record fills capacity
        plugin.on_response(&mut resp);
        assert_eq!(plugin.logged_count(), 1);
        assert_eq!(plugin.dropped_count(), 0);

        // Second record immediately drops without blocking
        plugin.on_response(&mut resp);
        assert_eq!(plugin.logged_count(), 1);
        assert_eq!(plugin.dropped_count(), 1);
    }

    #[test]
    fn test_access_log_format_serialization() {
        let record = LogRecord {
            timestamp_ms: 1776543210000,
            status: 200,
            version: "HTTP/1.1",
            content_length: Some(1024),
            request_id: Some("req-abc".into()),
            route_id: Some("route-xyz".into()),
            upstream: Some("127.0.0.1:8081".into()),
            latency_ms: Some(2.35),
        };

        let json_str = record.to_json_string();
        assert!(json_str.contains("\"status\":200"));
        assert!(json_str.contains("\"request_id\":\"req-abc\""));

        let text_str = record.to_text_string();
        assert!(text_str.contains("status=200"));
        assert!(text_str.contains("req_id=req-abc"));
        assert!(text_str.contains("latency_ms=2.35"));
    }

    #[test]
    fn test_access_log_from_config() {
        let config = PluginConfig {
            id: "access_log".into(),
            phase: String::new(),
            config: json!({
                "include_request_id": true,
                "include_route_id": false,
                "include_upstream": true,
                "include_latency": false,
                "format": "text",
                "buffer_capacity": 1024
            }),
        };

        let plugin = AccessLogPlugin::from_config(&config).expect("valid config");
        assert_eq!(plugin.id, "access_log");
        assert!(plugin.config.include_request_id);
        assert!(!plugin.config.include_route_id);
        assert!(plugin.config.include_upstream);
        assert!(!plugin.config.include_latency);
        assert_eq!(plugin.config.format, LogFormat::Text);
        assert_eq!(plugin.config.buffer_capacity, 1024);
    }
}
