//! OpenTelemetry Log Shipper Schema & Validation (`phase: "background"`).
//!
//! Validates configuration for [`OtlpLogShipperPlugin`], which drains reference-counted
//! 64-byte [`velda_observability::AccessLogRecord`] batches across all protocols and ships them
//! to OpenTelemetry Collectors over persistent HTTP/1.1 or HTTP/2 gRPC connections.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::SyncError;

/// Declarative configuration for the OpenTelemetry Log Shipper background plugin (`otlp_log_shipper`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OtlpLogShipperConfig {
    /// OTLP transport protocol (`"http"` or `"grpc"`).
    pub protocol: String,
    /// OTLP collector endpoint URL (e.g. `http://127.0.0.1:4318/v1/logs` or `grpc://127.0.0.1:4317`).
    pub endpoint: String,
    /// Logical service name reported in OpenTelemetry Resource attributes.
    pub service_name: String,
    /// Periodic batch flush interval in milliseconds (100 to 60,000 ms).
    pub flush_interval_ms: u64,
    /// Maximum records grouped in a single OTLP export payload (1 to 10,000).
    pub max_batch_size: usize,
    /// TCP and TLS connection establishment timeout in milliseconds (100 to 60,000 ms).
    pub connect_timeout_ms: u64,
    /// Request export completion timeout in milliseconds (100 to 60,000 ms).
    pub request_timeout_ms: u64,
    /// Optional HTTP/gRPC custom headers (use `{}` if no custom headers are required).
    pub headers: HashMap<String, String>,
}

/// Validates [`OtlpLogShipperConfig`] at configuration compilation time.
pub fn validate_otlp_log_shipper_config(
    plugin_id: &str,
    config_val: &serde_json::Value,
) -> Result<OtlpLogShipperConfig, SyncError> {
    let cfg: OtlpLogShipperConfig =
        serde_json::from_value(config_val.clone()).map_err(|e| SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}' has malformed or incomplete otlp_log_shipper config: {e}"
            ),
        })?;

    let protocol = cfg.protocol.trim().to_ascii_lowercase();
    if protocol != "http" && protocol != "grpc" {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': protocol must be explicitly set to 'http' or 'grpc', found '{protocol}'"
            ),
        });
    }

    let endpoint = cfg.endpoint.trim();
    if !endpoint.starts_with("http://")
        && !endpoint.starts_with("https://")
        && !endpoint.starts_with("grpc://")
    {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': endpoint '{endpoint}' must start with 'http://', 'https://', or 'grpc://'"
            ),
        });
    }

    if cfg.service_name.trim().is_empty() {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!("Plugin '{plugin_id}': service_name cannot be empty"),
        });
    }

    if !(100..=60_000).contains(&cfg.flush_interval_ms) {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': flush_interval_ms ({}) must be between 100 and 60,000 ms",
                cfg.flush_interval_ms
            ),
        });
    }

    if !(1..=10_000).contains(&cfg.max_batch_size) {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': max_batch_size ({}) must be between 1 and 10,000",
                cfg.max_batch_size
            ),
        });
    }

    if !(100..=60_000).contains(&cfg.connect_timeout_ms) {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': connect_timeout_ms ({}) must be between 100 and 60,000 ms",
                cfg.connect_timeout_ms
            ),
        });
    }

    if !(100..=60_000).contains(&cfg.request_timeout_ms) {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': request_timeout_ms ({}) must be between 100 and 60,000 ms",
                cfg.request_timeout_ms
            ),
        });
    }

    Ok(cfg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_valid_otlp_log_shipper_config() {
        let raw = json!({
            "protocol": "http",
            "endpoint": "http://127.0.0.1:4318/v1/logs",
            "service_name": "velda-edge",
            "flush_interval_ms": 500,
            "max_batch_size": 256,
            "connect_timeout_ms": 2000,
            "request_timeout_ms": 5000,
            "headers": {
                "x-api-key": "secret"
            }
        });
        let cfg = validate_otlp_log_shipper_config("otlp_sink", &raw).unwrap();
        assert_eq!(cfg.protocol, "http");
        assert_eq!(cfg.service_name, "velda-edge");
    }
}
