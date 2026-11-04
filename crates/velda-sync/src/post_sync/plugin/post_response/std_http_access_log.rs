//! Standard Stream HTTP Access Log Configuration Schema & Semantic Validation (`phase: "post_response"`).
//!
//! Enforces a strict declarative contract for request logging and observability.
//!
//! # Strict Declarative Contract (Zero Silent Fallbacks)
//! - **Target Stream**: `target` must be strictly `"stdout"` or `"stderr"` (file logging is forbidden).
//! - **Valid Log Format**: `format` must be `"json"`, `"text"`, or `"clf"`.
//! - **Bounded Buffer**: `buffer_capacity` must be between 128 and 65,536 entries.
//! - **Explicit Toggles**: `include_request_id`, `include_route_id`, `include_upstream`, `include_latency`, `include_user_agent` are explicit.
//! - **Status-Aware Sampling**: Exact 3-digit HTTP status codes only (`100..=599`). Wildcard patterns (`"2xx"`, `"5xx"`) are strictly rejected. Unconfigured codes have a sampling rate of 0.0 (not logged).

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::SyncError;

/// Declarative configuration for the Standard HTTP Access Log plugin (`std_http_access_log`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StdHttpAccessLogConfig {
    /// Target standard stream: "stdout" (default) or "stderr". Direct file logging is forbidden.
    #[serde(default = "default_target")]
    pub target: String,
    /// Serialization format: "json", "text", or "clf" (Common Log Format).
    pub format: String,
    /// Whether to log the client User-Agent header (stored inline up to 192 bytes).
    #[serde(default = "default_true")]
    pub include_user_agent: bool,
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
    /// Status-code-specific sampling rates (`"200": 0.05`, `"429": 1.0`, `"502": 1.0`).
    /// Wildcard patterns are strictly forbidden. Unconfigured status codes have rate = 0.0.
    #[serde(default)]
    pub sampling: HashMap<String, f64>,
}

fn default_target() -> String {
    "stdout".to_string()
}

fn default_true() -> bool {
    true
}

/// Validates Standard HTTP Access Log configuration strictly according to production standards.
pub fn validate_std_http_access_log_config(
    plugin_id: &str,
    config_val: &serde_json::Value,
) -> Result<StdHttpAccessLogConfig, SyncError> {
    let cfg: StdHttpAccessLogConfig =
        serde_json::from_value(config_val.clone()).map_err(|e| SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}' has malformed or incomplete std_http_access_log config: {e}"
            ),
        })?;

    // 1. Validate target standard stream
    let target_lower = cfg.target.trim().to_ascii_lowercase();
    match target_lower.as_str() {
        "stdout" | "stderr" => {}
        other => {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': invalid target '{other}' (expected 'stdout' or 'stderr'; direct file logging is forbidden)"
                ),
            });
        }
    }

    // 2. Validate format
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

    // 3. Validate buffer_capacity
    if !(128..=65536).contains(&cfg.buffer_capacity) {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': buffer_capacity ({}) must be between 128 and 65,536",
                cfg.buffer_capacity
            ),
        });
    }

    // 4. Validate sampling rules: exact 3-digit numeric HTTP status codes ONLY
    for (key, &rate) in &cfg.sampling {
        let trimmed_key = key.trim();

        // Reject wildcard patterns explicitly
        if trimmed_key.contains('x') || trimmed_key.contains('X') {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': invalid status code '{trimmed_key}'. Wildcard patterns like '2xx' or '5xx' are strictly forbidden; specify exact 3-digit HTTP status codes (e.g. '200', '429', '502')"
                ),
            });
        }

        // Validate 3-digit numeric status code
        let code = trimmed_key.parse::<u16>().map_err(|_| SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': invalid HTTP status code '{trimmed_key}' (must be a 3-digit integer between 100 and 599)"
            ),
        })?;

        if !(100..=599).contains(&code) {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': HTTP status code '{trimmed_key}' is out of range (must be between 100 and 599)"
                ),
            });
        }

        // Validate sample rate range
        if !(0.0..=1.0).contains(&rate) || rate.is_nan() {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': sampling rate for status code '{trimmed_key}' ({rate}) must be between 0.0 and 1.0"
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
    fn test_valid_std_http_access_log() {
        let val = json!({
            "target": "stdout",
            "format": "json",
            "include_user_agent": true,
            "include_request_id": true,
            "include_route_id": true,
            "include_upstream": true,
            "include_latency": true,
            "buffer_capacity": 4096,
            "sampling": {
                "200": 0.05,
                "201": 0.1,
                "404": 0.5,
                "429": 1.0,
                "500": 1.0,
                "502": 1.0
            }
        });
        let res = validate_std_http_access_log_config("log_1", &val);
        assert!(res.is_ok());
        let cfg = res.unwrap();
        assert_eq!(cfg.target, "stdout");
        assert_eq!(cfg.sampling.len(), 6);
        assert_eq!(cfg.sampling.get("200"), Some(&0.05));
        assert_eq!(cfg.sampling.get("502"), Some(&1.0));
    }

    #[test]
    fn test_wildcard_status_code_rejected() {
        let val = json!({
            "format": "json",
            "include_request_id": true,
            "include_route_id": true,
            "include_upstream": true,
            "include_latency": true,
            "buffer_capacity": 1024,
            "sampling": {
                "2xx": 0.05
            }
        });
        let res = validate_std_http_access_log_config("log_wildcard", &val);
        assert!(res.is_err());
        let err = res.unwrap_err().to_string();
        assert!(err.contains("Wildcard patterns like '2xx' or '5xx' are strictly forbidden"));
    }

    #[test]
    fn test_file_target_rejected() {
        let val = json!({
            "target": "file",
            "format": "json",
            "include_request_id": true,
            "include_route_id": true,
            "include_upstream": true,
            "include_latency": true,
            "buffer_capacity": 1024
        });
        let res = validate_std_http_access_log_config("log_file", &val);
        assert!(res.is_err());
        assert!(
            res.unwrap_err()
                .to_string()
                .contains("direct file logging is forbidden")
        );
    }

    #[test]
    fn test_invalid_rate_rejected() {
        let val = json!({
            "format": "json",
            "include_request_id": true,
            "include_route_id": true,
            "include_upstream": true,
            "include_latency": true,
            "buffer_capacity": 1024,
            "sampling": {
                "200": 1.5 // > 1.0
            }
        });
        let res = validate_std_http_access_log_config("log_bad_rate", &val);
        assert!(res.is_err());
        assert!(
            res.unwrap_err()
                .to_string()
                .contains("must be between 0.0 and 1.0")
        );
    }
}
