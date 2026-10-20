//! Request Mirror Configuration Schema & Semantic Validation (`phase: "pre_upstream"`).
//!
//! Enforces a strict declarative contract for shadow traffic mirroring.
//!
//! # Strict Declarative Contract (Zero Silent Fallbacks)
//! - **Mandatory Upstream**: `mirror_upstream` must be non-empty.
//! - **Sample Percentage**: `sample_percent` must be between 0 and 100.
//! - **Queue Capacity**: `queue_capacity` must be within production-safe boundaries (16..=1,000,000).
//! - **Valid Methods**: Every entry in `ignore_methods` must be a valid RFC 9110 HTTP method token.

use serde::{Deserialize, Serialize};

use crate::SyncError;

/// Declarative configuration for the Request Mirror plugin.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RequestMirrorConfig {
    /// Target mirror upstream cluster name to receive shadowed traffic.
    pub mirror_upstream: String,
    /// Percentage of traffic sampled and mirrored (0.0 to 100.0).
    pub sample_percent: f64,
    /// Optional replacement Host header to send to shadow upstream.
    pub replace_host: Option<String>,
    /// HTTP methods excluded from traffic mirroring (e.g. mutating methods like ["POST", "DELETE"]).
    pub ignore_methods: Vec<String>,
    /// Bounded non-blocking queue capacity for mirrored request dispatch.
    pub queue_capacity: usize,
}

fn is_valid_http_method(m: &str) -> bool {
    let trimmed = m.trim();
    if trimmed.is_empty() {
        return false;
    }
    trimmed
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-')
}

/// Validates Request Mirror configuration strictly according to production standards.
pub fn validate_request_mirror_config(
    plugin_id: &str,
    config_val: &serde_json::Value,
) -> Result<RequestMirrorConfig, SyncError> {
    let cfg: RequestMirrorConfig =
        serde_json::from_value(config_val.clone()).map_err(|e| SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}' has malformed or incomplete request_mirror config: {e}"
            ),
        })?;

    // 1. Validate mirror_upstream
    let upstream_trimmed = cfg.mirror_upstream.trim();
    if upstream_trimmed.is_empty() {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!("Plugin '{plugin_id}': mirror_upstream cannot be empty"),
        });
    }

    // 2. Validate sample_percent (0.0 ..= 100.0)
    if !(0.0..=100.0).contains(&cfg.sample_percent) || cfg.sample_percent.is_nan() {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': sample_percent ({}) must be between 0.0 and 100.0",
                cfg.sample_percent
            ),
        });
    }

    // 3. Validate queue_capacity (16 ..= 1,000,000)
    if !(16..=1_000_000).contains(&cfg.queue_capacity) {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': queue_capacity ({}) must be between 16 and 1,000,000",
                cfg.queue_capacity
            ),
        });
    }

    // 4. Validate ignore_methods
    for method in &cfg.ignore_methods {
        if !is_valid_http_method(method) {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': invalid HTTP method token '{method}' in ignore_methods"
                ),
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
    fn test_valid_request_mirror_config() {
        let val = json!({
            "mirror_upstream": "shadow-cluster-1",
            "sample_percent": 10.5,
            "replace_host": "shadow.internal",
            "ignore_methods": ["POST", "DELETE"],
            "queue_capacity": 4096
        });
        let res = validate_request_mirror_config("mirror_1", &val);
        assert!(res.is_ok());
    }

    #[test]
    fn test_empty_mirror_upstream_rejected() {
        let val = json!({
            "mirror_upstream": "",
            "sample_percent": 10.0,
            "replace_host": null,
            "ignore_methods": [],
            "queue_capacity": 1024
        });
        let res = validate_request_mirror_config("mirror_bad_up", &val);
        assert!(res.is_err());
        assert!(
            res.unwrap_err()
                .to_string()
                .contains("mirror_upstream cannot be empty")
        );
    }

    #[test]
    fn test_invalid_sample_percent_rejected() {
        let val = json!({
            "mirror_upstream": "shadow-up",
            "sample_percent": 105.0,
            "replace_host": null,
            "ignore_methods": [],
            "queue_capacity": 1024
        });
        let res = validate_request_mirror_config("mirror_bad_sample", &val);
        assert!(res.is_err());
        assert!(
            res.unwrap_err()
                .to_string()
                .contains("between 0.0 and 100.0")
        );
    }

    #[test]
    fn test_missing_required_fields_rejected() {
        let val = json!({
            "mirror_upstream": "shadow-up"
            // Missing sample_percent, ignore_methods, queue_capacity
        });
        let res = validate_request_mirror_config("mirror_missing", &val);
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("missing field"));
    }
}
