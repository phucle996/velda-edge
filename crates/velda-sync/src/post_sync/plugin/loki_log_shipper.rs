//! Grafana Loki Log Shipper Schema & Semantic Validation (`phase: "background"`).
//!
//! Validates configuration for the background Loki exporter (`loki_log_shipper`), which drains
//! reference-counted 64-byte [`velda_observability::AccessLogRecord`] batches and ships them to
//! `/loki/api/v1/push` partitioned by protocol family stream labels.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::SyncError;

/// Declarative configuration for the Grafana Loki Log Shipper background plugin (`loki_log_shipper`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LokiLogShipperConfig {
    /// Loki transport protocol (`"http"` or `"grpc"`).
    pub protocol: String,
    /// Target Loki push URL (e.g. `http://127.0.0.1:3100/loki/api/v1/push` or `grpc://127.0.0.1:9096`).
    pub endpoint: String,
    /// Multi-tenant Loki identifier sent via `X-Scope-OrgID` header (use `""` for single-tenant).
    pub tenant_id: String,
    /// Logical service identifier attached to the `service` stream label.
    pub service_name: String,
    /// Periodic batch flush interval in milliseconds (100 to 60,000 ms).
    pub flush_interval_ms: u64,
    /// Maximum records grouped into a single Loki HTTP/gRPC push payload (1 to 10,000).
    pub max_batch_size: usize,
    /// TCP connection establishment timeout in milliseconds (100 to 60,000 ms).
    pub connect_timeout_ms: u64,
    /// HTTP/gRPC push request completion timeout in milliseconds (100 to 60,000 ms).
    pub request_timeout_ms: u64,
    /// Additional static stream labels merged into every Loki stream descriptor.
    pub labels: HashMap<String, String>,
}

/// Validates [`LokiLogShipperConfig`] at configuration compilation time so invalid endpoints,
/// out-of-range timeouts, or malformed Loki label keys fail fast before reaching the Data Plane.
pub fn validate_loki_log_shipper_config(
    plugin_id: &str,
    config_val: &serde_json::Value,
) -> Result<LokiLogShipperConfig, SyncError> {
    let cfg: LokiLogShipperConfig =
        serde_json::from_value(config_val.clone()).map_err(|e| SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}' has malformed or incomplete loki_log_shipper config: {e}"
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
        && !endpoint.starts_with("grpcs://")
    {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': endpoint '{endpoint}' must start with 'http://', 'https://', 'grpc://', or 'grpcs://'"
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

    for (k, v) in &cfg.labels {
        if k.trim().is_empty() || v.trim().is_empty() {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!("Plugin '{plugin_id}': Loki label keys and values cannot be empty"),
            });
        }
    }

    Ok(cfg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_valid_loki_log_shipper_config() {
        let raw = json!({
            "protocol": "http",
            "endpoint": "http://127.0.0.1:3100/loki/api/v1/push",
            "tenant_id": "velda-prod",
            "service_name": "velda-edge",
            "flush_interval_ms": 500,
            "max_batch_size": 512,
            "connect_timeout_ms": 2000,
            "request_timeout_ms": 5000,
            "labels": {
                "env": "production"
            }
        });
        let cfg = validate_loki_log_shipper_config("loki_shipper", &raw).unwrap();
        assert_eq!(cfg.protocol, "http");
        assert_eq!(cfg.service_name, "velda-edge");
        assert_eq!(cfg.max_batch_size, 512);

        let grpc_raw = json!({
            "protocol": "grpc",
            "endpoint": "grpc://127.0.0.1:9096",
            "tenant_id": "",
            "service_name": "velda-edge",
            "flush_interval_ms": 500,
            "max_batch_size": 512,
            "connect_timeout_ms": 2000,
            "request_timeout_ms": 5000,
            "labels": {}
        });
        let grpc_cfg = validate_loki_log_shipper_config("loki_shipper_grpc", &grpc_raw).unwrap();
        assert_eq!(grpc_cfg.protocol, "grpc");
    }

    #[test]
    fn test_invalid_loki_log_shipper_protocol() {
        let raw = json!({
            "protocol": "websocket",
            "endpoint": "http://127.0.0.1:3100/loki/api/v1/push",
            "tenant_id": "",
            "service_name": "velda-edge",
            "flush_interval_ms": 500,
            "max_batch_size": 512,
            "connect_timeout_ms": 2000,
            "request_timeout_ms": 5000,
            "labels": {}
        });
        assert!(validate_loki_log_shipper_config("loki_shipper", &raw).is_err());
    }

    #[test]
    fn test_invalid_loki_log_shipper_endpoint() {
        let raw = json!({
            "protocol": "http",
            "endpoint": "ftp://127.0.0.1:3100",
            "tenant_id": "",
            "service_name": "velda-edge",
            "flush_interval_ms": 500,
            "max_batch_size": 512,
            "connect_timeout_ms": 2000,
            "request_timeout_ms": 5000,
            "labels": {}
        });
        assert!(validate_loki_log_shipper_config("loki_shipper", &raw).is_err());
    }
}
