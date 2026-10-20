//! Access Log Configuration Schema & Semantic Validation (`phase: "post_response"`).
//!
//! Enforces a strict declarative contract for request logging and observability.
//!
//! # Strict Declarative Contract (Zero Silent Fallbacks)
//! - **Valid Log Format**: `format` must be `"json"`, `"text"`, or `"clf"`.
//! - **Bounded Buffer**: `buffer_capacity` must be between 128 and 65,536 entries.
//! - **Explicit Toggles**: `include_request_id`, `include_route_id`, `include_upstream`, `include_latency` are all explicit.

use serde::{Deserialize, Serialize};

use crate::SyncError;

/// Declarative configuration for the Access Log plugin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccessLogConfig {
    /// Serialization format: "json", "text", or "clf" (Common Log Format).
    pub format: String,
    /// Whether to log unique request UUID.
    pub include_request_id: bool,
    /// Whether to log matched route identifier.
    pub include_route_id: bool,
    /// Whether to log target upstream cluster and endpoint address.
    pub include_upstream: bool,
    /// Whether to log upstream and downstream processing latency breakdown.
    pub include_latency: bool,
    /// Asynchronous non-blocking ring buffer capacity for log batching.
    pub buffer_capacity: usize,
}

/// Validates Access Log configuration strictly according to production standards.
pub fn validate_access_log_config(
    plugin_id: &str,
    config_val: &serde_json::Value,
) -> Result<AccessLogConfig, SyncError> {
    let cfg: AccessLogConfig =
        serde_json::from_value(config_val.clone()).map_err(|e| SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}' has malformed or incomplete access_log config: {e}"
            ),
        })?;

    // 1. Validate format
    let fmt_lower = cfg.format.trim().to_ascii_lowercase();
    match fmt_lower.as_str() {
        "json" | "text" | "clf" => {}
        other => {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': invalid log format '{other}' (expected 'json', 'text', or 'clf')"
                ),
            });
        }
    }

    // 2. Validate buffer_capacity
    if !(128..=65536).contains(&cfg.buffer_capacity) {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': buffer_capacity ({}) must be between 128 and 65,536",
                cfg.buffer_capacity
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
    fn test_valid_access_log() {
        let val = json!({
            "format": "json",
            "include_request_id": true,
            "include_route_id": true,
            "include_upstream": true,
            "include_latency": true,
            "buffer_capacity": 4096
        });
        let res = validate_access_log_config("log_1", &val);
        assert!(res.is_ok());
    }

    #[test]
    fn test_invalid_format_rejected() {
        let val = json!({
            "format": "xml_unsupported",
            "include_request_id": true,
            "include_route_id": true,
            "include_upstream": true,
            "include_latency": true,
            "buffer_capacity": 1024
        });
        let res = validate_access_log_config("log_bad_fmt", &val);
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("invalid log format"));
    }

    #[test]
    fn test_invalid_buffer_capacity_rejected() {
        let val = json!({
            "format": "json",
            "include_request_id": true,
            "include_route_id": true,
            "include_upstream": true,
            "include_latency": true,
            "buffer_capacity": 10 // Too small (< 128)
        });
        let res = validate_access_log_config("log_tiny_buf", &val);
        assert!(res.is_err());
        assert!(
            res.unwrap_err()
                .to_string()
                .contains("between 128 and 65,536")
        );
    }

    #[test]
    fn test_missing_required_fields_rejected() {
        let val = json!({
            "format": "json"
            // Missing toggles and buffer_capacity
        });
        let res = validate_access_log_config("log_missing", &val);
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("missing field"));
    }
}
