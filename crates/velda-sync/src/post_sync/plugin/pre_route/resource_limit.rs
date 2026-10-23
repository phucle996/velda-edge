//! Resource Limit Configuration Schema & Semantic Validation (`phase: "pre_route"`).
//!
//! Enforces strict declarative resource limit thresholds for incoming downstream requests
//! prior to URI routing or body streaming.
//!
//! # Strict Declarative Contract (Zero Silent Fallbacks)
//! - **Bounded Ranges**: All thresholds must lie within safe, production-calibrated operational boundaries.
//! - **Structural Invariant**: `max_single_header_bytes` cannot exceed `max_header_bytes`.

use serde::{Deserialize, Serialize};

use crate::SyncError;

/// Declarative configuration for the Resource Limit plugin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceLimitConfig {
    /// Maximum allowed total URI length in bytes (Path + Query string).
    pub max_uri_length: usize,
    /// Maximum allowed number of distinct header entries.
    pub max_headers_count: usize,
    /// Maximum allowed cumulative size of all headers in bytes.
    pub max_header_bytes: usize,
    /// Maximum allowed size for any single header value in bytes.
    pub max_single_header_bytes: usize,
}

/// Validates Resource Limit configuration strictly according to production standards.
pub fn validate_resource_limit_config(
    plugin_id: &str,
    config_val: &serde_json::Value,
) -> Result<ResourceLimitConfig, SyncError> {
    let cfg: ResourceLimitConfig =
        serde_json::from_value(config_val.clone()).map_err(|e| SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}' has malformed or incomplete resource_limit config: {e}"
            ),
        })?;

    // 1. Validate max_uri_length (64 B ..= 64 KB)
    if !(64..=65536).contains(&cfg.max_uri_length) {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': max_uri_length ({}) must be between 64 and 65,536 bytes",
                cfg.max_uri_length
            ),
        });
    }

    // 2. Validate max_headers_count (8 ..= 1024)
    if !(8..=1024).contains(&cfg.max_headers_count) {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': max_headers_count ({}) must be between 8 and 1024",
                cfg.max_headers_count
            ),
        });
    }

    // 3. Validate max_header_bytes (512 B ..= 1 MB)
    if !(512..=1_048_576).contains(&cfg.max_header_bytes) {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': max_header_bytes ({}) must be between 512 and 1,048,576 bytes (1MB)",
                cfg.max_header_bytes
            ),
        });
    }

    // 4. Validate max_single_header_bytes (64 B ..= max_header_bytes)
    if !(64..=cfg.max_header_bytes).contains(&cfg.max_single_header_bytes) {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': max_single_header_bytes ({}) must be between 64 and max_header_bytes ({})",
                cfg.max_single_header_bytes, cfg.max_header_bytes
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
    fn test_valid_resource_limits() {
        let val = json!({
            "max_uri_length": 8192,
            "max_headers_count": 128,
            "max_header_bytes": 32768,
            "max_single_header_bytes": 16384
        });
        let res = validate_resource_limit_config("res_limit_1", &val);
        assert!(res.is_ok());
    }

    #[test]
    fn test_single_header_larger_than_total_rejected() {
        let val = json!({
            "max_uri_length": 4096,
            "max_headers_count": 64,
            "max_header_bytes": 1024,
            "max_single_header_bytes": 2048
        });
        let res = validate_resource_limit_config("res_limit_bad", &val);
        assert!(res.is_err());
        assert!(
            res.unwrap_err()
                .to_string()
                .contains("max_single_header_bytes")
        );
    }

    #[test]
    fn test_out_of_bounds_uri_length_rejected() {
        let val = json!({
            "max_uri_length": 10,
            "max_headers_count": 64,
            "max_header_bytes": 16384,
            "max_single_header_bytes": 8192
        });
        let res = validate_resource_limit_config("res_limit_tiny_uri", &val);
        assert!(res.is_err());
        assert!(
            res.unwrap_err()
                .to_string()
                .contains("between 64 and 65,536")
        );
    }

    #[test]
    fn test_missing_required_fields_rejected() {
        let val = json!({
            "max_uri_length": 4096
            // Missing max_headers_count, max_header_bytes, max_single_header_bytes
        });
        let res = validate_resource_limit_config("res_limit_missing", &val);
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("missing field"));
    }
}
