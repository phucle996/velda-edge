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

use crate::header::{X_REQUEST_ID, X_RESPONSE_TIME, X_ROUTE_ID, X_UPSTREAM_ADDR};

use super::record::{LogFormat, LogRecord};
use crate::capability::{AccessLevel, PluginCapability, SupportedProtocols};
use crate::error::PluginError;
use crate::post_response::PostResponsePlugin;

/// Configuration settings for the `access_log` plugin.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccessLogConfig {
    /// Extracts and records the canonical `x-request-id` header from incoming request workspace.
    pub include_request_id: bool,
    /// Extracts and records the canonical `x-route-id` header from response.
    pub include_route_id: bool,
    /// Extracts and records the canonical `x-upstream-addr` header from response.
    pub include_upstream: bool,
    /// Extracts and records the canonical `x-response-time` header from response.
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
        let cfg =
            velda_sync::validate_access_log_config(&config.id, &config.config).map_err(|e| {
                PluginError::Config(format!(
                    "Invalid access_log configuration for '{}': {e}",
                    config.id
                ))
            })?;

        let format = LogFormat::parse(&cfg.format);

        Ok(Self::new(
            &config.id,
            AccessLogConfig {
                include_request_id: cfg.include_request_id,
                include_route_id: cfg.include_route_id,
                include_upstream: cfg.include_upstream,
                include_latency: cfg.include_latency,
                format,
                buffer_capacity: cfg.buffer_capacity,
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

    fn on_response(&self, req_headers: &http::HeaderMap, resp: &mut L7Response) {
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
            req_headers
                .get(&X_REQUEST_ID)
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string())
        } else {
            None
        };

        let route_id = if self.config.include_route_id {
            resp.headers
                .get(&X_ROUTE_ID)
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string())
        } else {
            None
        };

        let upstream = if self.config.include_upstream {
            resp.headers
                .get(&X_UPSTREAM_ADDR)
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string())
        } else {
            None
        };

        let latency_ms = if self.config.include_latency {
            resp.headers
                .get(&X_RESPONSE_TIME)
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
    use velda_core::Body;

    #[test]
    fn test_access_log_extracts_headers_and_status() {
        let config = AccessLogConfig {
            buffer_capacity: 10,
            ..Default::default()
        };
        let (plugin, mut rx) = AccessLogPlugin::new_with_receiver("access_log", config);

        let mut headers = HeaderMap::new();
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
        let mut req_headers = HeaderMap::new();
        req_headers.insert("x-request-id", HeaderValue::from_static("req-12345"));

        plugin.on_response(&req_headers, &mut resp);

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
        let config = AccessLogConfig {
            buffer_capacity: 1,
            ..Default::default()
        };
        let (plugin, _rx) = AccessLogPlugin::new_with_receiver("access_log", config);

        let mut resp = L7Response::empty(StatusCode::OK);
        let req_headers = HeaderMap::new();

        // First record fills capacity
        plugin.on_response(&req_headers, &mut resp);
        assert_eq!(plugin.logged_count(), 1);
        assert_eq!(plugin.dropped_count(), 0);

        // Second record immediately drops without blocking
        plugin.on_response(&req_headers, &mut resp);
        assert_eq!(plugin.logged_count(), 1);
        assert_eq!(plugin.dropped_count(), 1);
    }

    #[test]
    fn test_from_config_strict_validation() {
        let valid_json = serde_json::json!({
            "format": "json",
            "include_request_id": true,
            "include_route_id": true,
            "include_upstream": true,
            "include_latency": true,
            "buffer_capacity": 4096
        });

        let p_cfg = PluginConfig {
            id: "log_test".into(),
            phase: "post_response".into(),
            config: valid_json,
        };

        let plugin = AccessLogPlugin::from_config(&p_cfg).unwrap();
        assert_eq!(plugin.config.format, LogFormat::Json);
        assert_eq!(plugin.config.buffer_capacity, 4096);
        assert!(plugin.config.include_request_id);

        let invalid_json = serde_json::json!({
            "format": "json"
            // Missing other fields
        });
        let bad_cfg = PluginConfig {
            id: "log_bad".into(),
            phase: "post_response".into(),
            config: invalid_json,
        };
        assert!(AccessLogPlugin::from_config(&bad_cfg).is_err());
    }
}
