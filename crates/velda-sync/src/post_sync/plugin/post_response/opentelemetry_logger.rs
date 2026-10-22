//! OpenTelemetry Logger Schema & Semantic Validation (`phase: "post_response"`).

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::SyncError;

/// Declarative configuration for the OpenTelemetry Logger plugin.
///
/// # Strict Invariant: 100% Explicit Declaration
/// Every parameter must be explicitly declared in the configuration JSON.
/// Zero defaults are permitted. Missing fields fail fast at configuration sync time.
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
    /// Optional HTTP/gRPC custom headers (use `{}` if no custom headers are required).
    pub headers: HashMap<String, String>,
}

/// Validates OpenTelemetry Logger configuration strictly according to production standards.
pub fn validate_opentelemetry_logger_config(
    plugin_id: &str,
    config_val: &serde_json::Value,
) -> Result<OpentelemetryLoggerConfig, SyncError> {
    let cfg: OpentelemetryLoggerConfig =
        serde_json::from_value(config_val.clone()).map_err(|e| SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}' has malformed or incomplete opentelemetry_logger config: {e}"
            ),
        })?;

    // 1. Validate protocol
    let protocol = cfg.protocol.trim().to_ascii_lowercase();
    if protocol != "http" && protocol != "grpc" {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': protocol must be explicitly set to 'http' or 'grpc', found '{protocol}'"
            ),
        });
    }

    // 2. Validate endpoint
    let trimmed = cfg.endpoint.trim();
    let valid_prefix = if protocol == "grpc" {
        trimmed.starts_with("http://")
            || trimmed.starts_with("https://")
            || trimmed.starts_with("grpc://")
            || trimmed.starts_with("grpcs://")
    } else {
        trimmed.starts_with("http://") || trimmed.starts_with("https://")
    };

    if !valid_prefix {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': endpoint '{trimmed}' is invalid for protocol '{protocol}'"
            ),
        });
    }

    // 3. Validate batch_size
    if !(1..=4096).contains(&cfg.batch_size) {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': batch_size ({}) must be between 1 and 4096",
                cfg.batch_size
            ),
        });
    }

    // 4. Validate flush_interval_ms
    if !(10..=60000).contains(&cfg.flush_interval_ms) {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': flush_interval_ms ({}) must be between 10 and 60000 ms",
                cfg.flush_interval_ms
            ),
        });
    }

    // 5. Validate buffer_capacity
    if !(128..=262144).contains(&cfg.buffer_capacity) {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': buffer_capacity ({}) must be between 128 and 262144",
                cfg.buffer_capacity
            ),
        });
    }

    // 6. Validate connect_timeout_ms (100ms ..= 60,000ms)
    if !(100..=60000).contains(&cfg.connect_timeout_ms) {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': connect_timeout_ms ({}) must be between 100 and 60,000 ms",
                cfg.connect_timeout_ms
            ),
        });
    }

    // 7. Validate request_timeout_ms (100ms ..= 60,000ms)
    if !(100..=60000).contains(&cfg.request_timeout_ms) {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': request_timeout_ms ({}) must be between 100 and 60,000 ms",
                cfg.request_timeout_ms
            ),
        });
    }

    // 8. Validate service_name
    if cfg.service_name.trim().is_empty() {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!("Plugin '{plugin_id}': service_name cannot be empty"),
        });
    }

    Ok(cfg)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_opentelemetry_logger_config() {
        let val = serde_json::json!({
            "protocol": "http",
            "endpoint": "http://127.0.0.1:4318/v1/logs",
            "batch_size": 256,
            "flush_interval_ms": 200,
            "buffer_capacity": 65536,
            "connect_timeout_ms": 5000,
            "request_timeout_ms": 10000,
            "service_name": "velda-edge",
            "headers": {}
        });
        let res = validate_opentelemetry_logger_config("otel_log", &val);
        assert!(res.is_ok());
    }

    #[test]
    fn test_missing_batch_size_fails() {
        let val = serde_json::json!({
            "protocol": "http",
            "endpoint": "http://127.0.0.1:4318/v1/logs",
            "flush_interval_ms": 200,
            "buffer_capacity": 65536,
            "connect_timeout_ms": 5000,
            "request_timeout_ms": 10000,
            "service_name": "velda-edge",
            "headers": {}
        });
        let res = validate_opentelemetry_logger_config("otel_log", &val);
        assert!(res.is_err(), "Must fail when batch_size is omitted");
    }

    #[test]
    fn test_missing_protocol_fails() {
        let val = serde_json::json!({
            "endpoint": "http://127.0.0.1:4318/v1/logs",
            "batch_size": 256,
            "flush_interval_ms": 200,
            "buffer_capacity": 65536,
            "connect_timeout_ms": 5000,
            "request_timeout_ms": 10000,
            "service_name": "velda-edge",
            "headers": {}
        });
        let res = validate_opentelemetry_logger_config("otel_log", &val);
        assert!(res.is_err(), "Must fail when protocol is omitted");
    }

    #[test]
    fn test_missing_timeouts_fails() {
        let val = serde_json::json!({
            "protocol": "http",
            "endpoint": "http://127.0.0.1:4318/v1/logs",
            "batch_size": 256,
            "flush_interval_ms": 200,
            "buffer_capacity": 65536,
            "service_name": "velda-edge",
            "headers": {}
        });
        let res = validate_opentelemetry_logger_config("otel_log", &val);
        assert!(res.is_err(), "Must fail when timeouts are omitted");
    }
}
