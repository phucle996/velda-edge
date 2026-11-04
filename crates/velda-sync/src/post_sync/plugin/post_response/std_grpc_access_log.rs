//! Standard Stream gRPC Access Log Configuration Schema & Semantic Validation (`phase: "post_response"`).
//!
//! Enforces a strict declarative contract for gRPC request and stream logging.
//!
//! # Strict Declarative Contract (Zero Silent Fallbacks)
//! - **Target Stream**: `target` must be strictly `"stdout"` or `"stderr"` (file logging is forbidden).
//! - **Valid Log Format**: `format` must be `"json"` or `"text"`.
//! - **Bounded Buffer**: `buffer_capacity` must be between 128 and 65,536 entries.
//! - **Explicit Toggles**: `include_request_id`, `include_route_id`, `include_upstream`, `include_peer_addr`, `include_latency`.
//! - **Status-Aware Sampling**: Canonical gRPC status codes only (`0..=16` or standard names like `"OK"`, `"UNAVAILABLE"`).
//!   Wildcard patterns are strictly forbidden. Unconfigured status codes have a rate of 0.0 (not logged).

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::SyncError;

/// Declarative configuration for the Standard gRPC Access Log plugin (`std_grpc_access_log`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StdGrpcAccessLogConfig {
    /// Target standard stream: "stdout" (default) or "stderr". Direct file logging is forbidden.
    #[serde(default = "default_target")]
    pub target: String,
    /// Serialization format: "json" (default) or "text".
    #[serde(default = "default_format")]
    pub format: String,
    /// Whether to log unique request UUID.
    #[serde(default = "default_true")]
    pub include_request_id: bool,
    /// Whether to log matched route identifier.
    #[serde(default = "default_true")]
    pub include_route_id: bool,
    /// Whether to log target upstream cluster and endpoint address.
    #[serde(default = "default_true")]
    pub include_upstream: bool,
    /// Whether to log client peer address.
    #[serde(default = "default_true")]
    pub include_peer_addr: bool,
    /// Whether to log RPC call duration.
    #[serde(default = "default_true")]
    pub include_latency: bool,
    /// Asynchronous non-blocking ring buffer capacity for log batching.
    #[serde(default = "default_buffer_capacity")]
    pub buffer_capacity: usize,
    /// Status-code-specific sampling rates (`"0": 0.05`, `"UNAVAILABLE": 1.0`, `"14": 1.0`).
    /// Wildcard patterns are strictly forbidden. Unconfigured status codes have rate = 0.0.
    #[serde(default)]
    pub sampling: HashMap<String, f64>,
}

fn default_target() -> String {
    "stdout".to_string()
}

fn default_format() -> String {
    "json".to_string()
}

fn default_true() -> bool {
    true
}

fn default_buffer_capacity() -> usize {
    4096
}

/// Validates Standard gRPC Access Log configuration strictly according to production standards.
pub fn validate_std_grpc_access_log_config(
    plugin_id: &str,
    config_val: &serde_json::Value,
) -> Result<StdGrpcAccessLogConfig, SyncError> {
    let cfg: StdGrpcAccessLogConfig =
        serde_json::from_value(config_val.clone()).map_err(|e| SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}' has malformed or incomplete std_grpc_access_log config: {e}"
            ),
        })?;

    // 1. Validate target stream
    let target_lower = cfg.target.trim().to_ascii_lowercase();
    match target_lower.as_str() {
        "stdout" | "stderr" => {}
        other => {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}' specifies invalid target '{other}': expected 'stdout' or 'stderr' (direct file logging is forbidden)"
                ),
            });
        }
    }

    // 2. Validate format
    let format_lower = cfg.format.trim().to_ascii_lowercase();
    match format_lower.as_str() {
        "json" | "text" => {}
        other => {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}' specifies invalid format '{other}': expected 'json' or 'text'"
                ),
            });
        }
    }

    // 3. Validate buffer capacity
    if cfg.buffer_capacity < 128 || cfg.buffer_capacity > 65_536 {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}' buffer_capacity {} is outside the allowed range [128, 65536]",
                cfg.buffer_capacity
            ),
        });
    }

    // 4. Validate status-aware sampling keys and rates
    for (key, &rate) in &cfg.sampling {
        let trimmed = key.trim();

        // Reject wildcard patterns explicitly
        if trimmed.contains('*')
            || trimmed.ends_with("xx")
            || trimmed.ends_with("XX")
            || trimmed == "x"
            || trimmed == "X"
        {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}' sampling key '{key}' is invalid. Wildcard patterns like '*' or '2xx' are strictly forbidden. Specify exact canonical gRPC status codes (0..=16) or names (e.g. 'OK', 'UNAVAILABLE')"
                ),
            });
        }

        // Validate rate range [0.0, 1.0]
        if !(0.0..=1.0).contains(&rate) {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}' sampling rate {rate} for status '{key}' must be between 0.0 and 1.0"
                ),
            });
        }

        // Validate that key is either a number in 0..=16 or valid status name
        if let Ok(code) = trimmed.parse::<u16>() {
            if code > 16 {
                return Err(SyncError::Validation {
                    domain: "plugins".into(),
                    reason: format!(
                        "Plugin '{plugin_id}' sampling key '{key}' is out of range. Canonical gRPC status codes are 0..=16"
                    ),
                });
            }
        } else {
            match trimmed.to_ascii_uppercase().as_str() {
                "OK"
                | "CANCELLED"
                | "UNKNOWN"
                | "INVALID_ARGUMENT"
                | "DEADLINE_EXCEEDED"
                | "NOT_FOUND"
                | "ALREADY_EXISTS"
                | "PERMISSION_DENIED"
                | "RESOURCE_EXHAUSTED"
                | "FAILED_PRECONDITION"
                | "ABORTED"
                | "OUT_OF_RANGE"
                | "UNIMPLEMENTED"
                | "INTERNAL"
                | "UNAVAILABLE"
                | "DATA_LOSS"
                | "UNAUTHENTICATED" => {}
                other => {
                    return Err(SyncError::Validation {
                        domain: "plugins".into(),
                        reason: format!(
                            "Plugin '{plugin_id}' sampling key '{key}' is not a valid canonical gRPC status name '{other}'"
                        ),
                    });
                }
            }
        }
    }

    Ok(cfg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_valid_std_grpc_access_log() {
        let val = json!({
            "target": "stdout",
            "format": "json",
            "include_request_id": true,
            "include_route_id": true,
            "include_upstream": true,
            "include_peer_addr": true,
            "include_latency": true,
            "buffer_capacity": 2048,
            "sampling": {
                "0": 0.05,
                "OK": 0.05,
                "14": 1.0,
                "UNAVAILABLE": 1.0,
                "DEADLINE_EXCEEDED": 1.0
            }
        });
        let res = validate_std_grpc_access_log_config("grpc_log_1", &val);
        assert!(res.is_ok());
        let cfg = res.unwrap();
        assert_eq!(cfg.target, "stdout");
        assert_eq!(cfg.sampling.len(), 5);
        assert_eq!(cfg.sampling.get("0"), Some(&0.05));
        assert_eq!(cfg.sampling.get("UNAVAILABLE"), Some(&1.0));
    }

    #[test]
    fn test_grpc_wildcard_rejected() {
        let val = json!({
            "sampling": {
                "*": 0.5
            }
        });
        let res = validate_std_grpc_access_log_config("grpc_wildcard", &val);
        assert!(res.is_err());
        let err = res.unwrap_err().to_string();
        assert!(err.contains("Wildcard patterns like '*' or '2xx' are strictly forbidden"));
    }

    #[test]
    fn test_grpc_file_target_rejected() {
        let val = json!({
            "target": "file",
            "format": "json"
        });
        let res = validate_std_grpc_access_log_config("grpc_file", &val);
        assert!(res.is_err());
        assert!(
            res.unwrap_err()
                .to_string()
                .contains("direct file logging is forbidden")
        );
    }

    #[test]
    fn test_grpc_status_code_out_of_range() {
        let val = json!({
            "sampling": {
                "200": 1.0 // HTTP 200 is not a canonical gRPC status code (0..16)
            }
        });
        let res = validate_std_grpc_access_log_config("grpc_bad_code", &val);
        assert!(res.is_err());
        assert!(
            res.unwrap_err()
                .to_string()
                .contains("Canonical gRPC status codes are 0..=16")
        );
    }
}
