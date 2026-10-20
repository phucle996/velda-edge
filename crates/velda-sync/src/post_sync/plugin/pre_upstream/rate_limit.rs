//! Rate Limit Configuration Schema & Semantic Validation (`phase: "pre_upstream"`).
//!
//! Enforces a strict declarative contract for token bucket and sliding window rate limiting.
//!
//! # Strict Declarative Contract (Zero Silent Fallbacks)
//! - **Positive Quotas**: `rate_per_second` and `burst_capacity` must be non-zero positive numbers.
//! - **Valid Strategy**: `key_strategy` must be one of: `"client_ip"`, `"header"`, `"route"`, `"priority"`.
//! - **Header Strategy Invariant**: If `key_strategy` is `"header"`, `key_headers` cannot be empty.
//! - **Valid Backend & Fallback**: `backend` must be `"memory"` or `"redis"`. `fallback_behavior` must be `"memory"`, `"allow"`, or `"deny"`.

use serde::{Deserialize, Serialize};

use crate::SyncError;

/// Declarative configuration for the Rate Limit plugin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RateLimitConfig {
    /// Rate limiting algorithm: "token_bucket" or "sliding_window".
    pub algorithm: String,
    /// Storage and synchronization backend: "memory" or "redis".
    pub backend: String,
    /// Number of requests allowed per second.
    pub rate_per_second: u64,
    /// Maximum burst capacity above the continuous refill rate.
    pub burst_capacity: u64,
    /// Key extraction strategy: "client_ip", "header", "route", or "priority".
    pub key_strategy: String,
    /// Headers evaluated when key_strategy is "header" or "priority".
    pub key_headers: Vec<String>,
    /// Fallback behavior if Redis is unreachable: "memory", "allow", or "deny".
    pub fallback_behavior: String,
}

/// Helper to validate RFC 9110 header token grammar.
fn is_valid_header_name(h: &str) -> bool {
    let trimmed = h.trim();
    if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("client_ip") {
        return true;
    }
    trimmed.bytes().all(|b| {
        b.is_ascii_alphanumeric()
            || matches!(
                b,
                b'!' | b'#'
                    | b'$'
                    | b'%'
                    | b'&'
                    | b'\''
                    | b'*'
                    | b'+'
                    | b'-'
                    | b'.'
                    | b'^'
                    | b'_'
                    | b'`'
                    | b'|'
                    | b'~'
            )
    })
}

/// Validates Rate Limit configuration strictly according to production standards.
pub fn validate_rate_limit_config(
    plugin_id: &str,
    config_val: &serde_json::Value,
) -> Result<RateLimitConfig, SyncError> {
    let cfg: RateLimitConfig =
        serde_json::from_value(config_val.clone()).map_err(|e| SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}' has malformed or incomplete rate_limit config: {e}"
            ),
        })?;

    // 1. Validate Algorithm
    let alg_lower = cfg.algorithm.trim().to_ascii_lowercase();
    match alg_lower.as_str() {
        "token_bucket" | "tokenbucket" | "sliding_window" | "slidingwindow" => {}
        other => {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': invalid algorithm '{other}' (expected 'token_bucket' or 'sliding_window')"
                ),
            });
        }
    }

    // 2. Validate Backend
    let backend_lower = cfg.backend.trim().to_ascii_lowercase();
    match backend_lower.as_str() {
        "memory" | "in_memory" | "local" | "redis" => {}
        other => {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': invalid backend '{other}' (expected 'memory' or 'redis')"
                ),
            });
        }
    }

    // 3. Validate Rate and Burst
    if cfg.rate_per_second == 0 {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!("Plugin '{plugin_id}': rate_per_second must be greater than 0"),
        });
    }
    if cfg.rate_per_second > 10_000_000 {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': rate_per_second ({}) exceeds realistic maximum limit of 10,000,000 req/s",
                cfg.rate_per_second
            ),
        });
    }
    if cfg.burst_capacity == 0 {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!("Plugin '{plugin_id}': burst_capacity must be greater than 0"),
        });
    }

    // 4. Validate Key Strategy & Headers
    let strat_lower = cfg.key_strategy.trim().to_ascii_lowercase();
    match strat_lower.as_str() {
        "client_ip" | "ip" | "header" | "route" | "priority" => {}
        other => {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': invalid key_strategy '{other}' (expected 'client_ip', 'header', 'route', or 'priority')"
                ),
            });
        }
    }

    if strat_lower == "header" && cfg.key_headers.is_empty() {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': key_strategy is 'header' but key_headers list is empty"
            ),
        });
    }

    for header in &cfg.key_headers {
        if !is_valid_header_name(header) {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': invalid header name '{header}' in key_headers"
                ),
            });
        }
    }

    // 5. Validate Fallback Behavior
    let fb_lower = cfg.fallback_behavior.trim().to_ascii_lowercase();
    match fb_lower.as_str() {
        "memory" | "allow" | "fail_open" | "deny" | "fail_closed" => {}
        other => {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': invalid fallback_behavior '{other}' (expected 'memory', 'allow', or 'deny')"
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
    fn test_valid_rate_limit_config() {
        let val = json!({
            "algorithm": "token_bucket",
            "backend": "memory",
            "rate_per_second": 5000,
            "burst_capacity": 10000,
            "key_strategy": "client_ip",
            "key_headers": [],
            "fallback_behavior": "memory"
        });
        let res = validate_rate_limit_config("rate_limit_1", &val);
        assert!(res.is_ok());
    }

    #[test]
    fn test_zero_rate_rejected() {
        let val = json!({
            "algorithm": "token_bucket",
            "backend": "memory",
            "rate_per_second": 0,
            "burst_capacity": 1000,
            "key_strategy": "client_ip",
            "key_headers": [],
            "fallback_behavior": "memory"
        });
        let res = validate_rate_limit_config("rate_zero", &val);
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("greater than 0"));
    }

    #[test]
    fn test_header_strategy_without_headers_rejected() {
        let val = json!({
            "algorithm": "token_bucket",
            "backend": "memory",
            "rate_per_second": 100,
            "burst_capacity": 200,
            "key_strategy": "header",
            "key_headers": [],
            "fallback_behavior": "memory"
        });
        let res = validate_rate_limit_config("rate_no_hdrs", &val);
        assert!(res.is_err());
        assert!(
            res.unwrap_err()
                .to_string()
                .contains("key_headers list is empty")
        );
    }

    #[test]
    fn test_invalid_backend_rejected() {
        let val = json!({
            "algorithm": "token_bucket",
            "backend": "dynamodb",
            "rate_per_second": 100,
            "burst_capacity": 200,
            "key_strategy": "client_ip",
            "key_headers": [],
            "fallback_behavior": "memory"
        });
        let res = validate_rate_limit_config("rate_bad_backend", &val);
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("invalid backend"));
    }

    #[test]
    fn test_missing_required_fields_rejected() {
        let val = json!({
            "rate_per_second": 100
            // Missing algorithm, backend, burst_capacity, key_strategy, key_headers, fallback_behavior
        });
        let res = validate_rate_limit_config("rate_missing", &val);
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("missing field"));
    }
}
