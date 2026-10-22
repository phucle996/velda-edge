//! OpenTelemetry Metrics Exporter Schema & Semantic Validation (`phase: "background"`).

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::SyncError;

/// Declarative configuration for the OpenTelemetry Metrics Exporter plugin.
///
/// # Strict Invariant: 100% Explicit Declaration
/// Every parameter must be explicitly declared in the configuration JSON.
/// Zero defaults are permitted. Missing fields fail fast at configuration sync time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OtlpMetricsExporterConfig {
    /// OTLP transport protocol (must be explicitly declared as `"http"` or `"grpc"`).
    pub protocol: String,
    /// OTLP collector endpoint URL (e.g. `http://127.0.0.1:4318/v1/metrics` or `grpc://127.0.0.1:4317`).
    pub endpoint: String,
    /// Periodic push interval in seconds (between 1 and 3600).
    pub interval_secs: u64,
    /// TCP and TLS connection establishment timeout in milliseconds (100 to 60000).
    pub connect_timeout_ms: u64,
    /// Request export completion timeout in milliseconds (100 to 60000).
    pub request_timeout_ms: u64,
    /// Service name reported in OpenTelemetry Resource attributes.
    pub service_name: String,
    /// Optional HTTP/gRPC custom headers (use `{}` if no custom headers are required).
    pub headers: HashMap<String, String>,
}

/// Validates OTLP Metrics Exporter configuration strictly according to production standards.
pub fn validate_otlp_metrics_exporter_config(
    plugin_id: &str,
    config_val: &serde_json::Value,
) -> Result<OtlpMetricsExporterConfig, SyncError> {
    let cfg: OtlpMetricsExporterConfig =
        serde_json::from_value(config_val.clone()).map_err(|e| SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}' has malformed or incomplete otlp_metrics_exporter config: {e}"
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

    // 2. Validate endpoint URL
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

    // 3. Validate interval_secs
    if !(1..=3600).contains(&cfg.interval_secs) {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': interval_secs ({}) must be between 1 and 3600 seconds",
                cfg.interval_secs
            ),
        });
    }

    // 4. Validate connect_timeout_ms (100ms ..= 60,000ms)
    if !(100..=60000).contains(&cfg.connect_timeout_ms) {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': connect_timeout_ms ({}) must be between 100 and 60,000 ms",
                cfg.connect_timeout_ms
            ),
        });
    }

    // 5. Validate request_timeout_ms (100ms ..= 60,000ms)
    if !(100..=60000).contains(&cfg.request_timeout_ms) {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': request_timeout_ms ({}) must be between 100 and 60,000 ms",
                cfg.request_timeout_ms
            ),
        });
    }

    // 6. Validate service_name
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
    fn test_valid_otlp_metrics_exporter_config() {
        let val = serde_json::json!({
            "protocol": "http",
            "endpoint": "http://127.0.0.1:4318/v1/metrics",
            "interval_secs": 15,
            "connect_timeout_ms": 5000,
            "request_timeout_ms": 10000,
            "service_name": "edge-gateway",
            "headers": {}
        });
        let res = validate_otlp_metrics_exporter_config("metrics_exp", &val);
        assert!(res.is_ok());
    }

    #[test]
    fn test_missing_protocol_fails() {
        let val = serde_json::json!({
            "endpoint": "http://127.0.0.1:4318/v1/metrics",
            "interval_secs": 10,
            "connect_timeout_ms": 5000,
            "request_timeout_ms": 10000,
            "service_name": "edge-gateway",
            "headers": {}
        });
        let res = validate_otlp_metrics_exporter_config("metrics_exp", &val);
        assert!(res.is_err(), "Must fail when protocol is omitted");
    }

    #[test]
    fn test_missing_headers_fails() {
        let val = serde_json::json!({
            "protocol": "grpc",
            "endpoint": "grpc://127.0.0.1:4317",
            "interval_secs": 10,
            "connect_timeout_ms": 5000,
            "request_timeout_ms": 10000,
            "service_name": "edge-gateway"
        });
        let res = validate_otlp_metrics_exporter_config("metrics_exp", &val);
        assert!(res.is_err(), "Must fail when headers is omitted");
    }

    #[test]
    fn test_missing_timeouts_fails() {
        let val = serde_json::json!({
            "protocol": "grpc",
            "endpoint": "grpc://127.0.0.1:4317",
            "interval_secs": 10,
            "service_name": "edge-gateway",
            "headers": {}
        });
        let res = validate_otlp_metrics_exporter_config("metrics_exp", &val);
        assert!(res.is_err(), "Must fail when timeouts are omitted");
    }
}
