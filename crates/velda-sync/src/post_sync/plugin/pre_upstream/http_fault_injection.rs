//! HTTP Fault Injection Configuration Schema & Semantic Validation (`phase: "pre_upstream"`).
//!
//! Enforces a strict declarative contract for HTTP chaos engineering and resilience testing.
//!
//! # Strict Declarative Contract (Zero Silent Fallbacks)
//! - **Strict Protocol Invariant**: Restricted exclusively to HTTP routes (prohibited on gRPC/Raw routes).
//! - **Bounded Percentage**: Chaos trigger percentages must be between 0 and 100.
//! - **Valid HTTP Status**: Abort status code must be an RFC 9110 HTTP status (200..=599).
//! - **Safe Delay Threshold**: Fixed latency delay cannot exceed 60,000ms (1 minute).

use serde::{Deserialize, Serialize};

use crate::SyncError;

/// Declarative synthetic delay configuration for HTTP requests.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HttpFaultDelayConfig {
    /// Whether synthetic delay injection is active.
    pub enabled: bool,
    /// Fixed delay added in milliseconds (1ms to 60,000ms).
    pub fixed_delay_ms: u64,
    /// Probability percentage of applying delay (0 to 100).
    pub percentage: u32,
}

/// Declarative synthetic abort configuration for HTTP requests.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HttpFaultAbortConfig {
    /// Whether synthetic abort injection is active.
    pub enabled: bool,
    /// Synthetic HTTP status code returned to client (200 to 599).
    pub http_status: u16,
    /// Probability percentage of applying abort (0 to 100).
    pub percentage: u32,
}

/// Declarative configuration for the HTTP Fault Injection plugin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HttpFaultInjectionConfig {
    /// Latency injection block.
    pub delay: HttpFaultDelayConfig,
    /// Immediate abort block.
    pub abort: HttpFaultAbortConfig,
}

/// Validates HTTP Fault Injection configuration strictly according to production standards.
pub fn validate_http_fault_injection_config(
    plugin_id: &str,
    config_val: &serde_json::Value,
) -> Result<HttpFaultInjectionConfig, SyncError> {
    let cfg: HttpFaultInjectionConfig =
        serde_json::from_value(config_val.clone()).map_err(|e| SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}' has malformed or incomplete http_fault_injection config: {e}"
            ),
        })?;

    // 1. Validate Delay Block
    if cfg.delay.percentage > 100 {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': delay.percentage ({}) must be between 0 and 100",
                cfg.delay.percentage
            ),
        });
    }

    if cfg.delay.enabled {
        if cfg.delay.fixed_delay_ms == 0 || cfg.delay.fixed_delay_ms > 60_000 {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': delay.fixed_delay_ms ({}) must be between 1 and 60,000ms",
                    cfg.delay.fixed_delay_ms
                ),
            });
        }
        if cfg.delay.percentage == 0 {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': delay.enabled is true but delay.percentage is 0"
                ),
            });
        }
    }

    // 2. Validate Abort Block
    if cfg.abort.percentage > 100 {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': abort.percentage ({}) must be between 0 and 100",
                cfg.abort.percentage
            ),
        });
    }

    if cfg.abort.enabled {
        if !(200..=599).contains(&cfg.abort.http_status) {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': abort.http_status ({}) must be between 200 and 599",
                    cfg.abort.http_status
                ),
            });
        }
        if cfg.abort.percentage == 0 {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': abort.enabled is true but abort.percentage is 0"
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
    fn test_valid_http_fault_injection() {
        let val = json!({
            "delay": {
                "enabled": true,
                "fixed_delay_ms": 250,
                "percentage": 5
            },
            "abort": {
                "enabled": true,
                "http_status": 503,
                "percentage": 2
            }
        });
        let res = validate_http_fault_injection_config("http_fault_1", &val);
        assert!(res.is_ok());
    }

    #[test]
    fn test_invalid_http_status_rejected() {
        let val = json!({
            "delay": {
                "enabled": false,
                "fixed_delay_ms": 0,
                "percentage": 0
            },
            "abort": {
                "enabled": true,
                "http_status": 100,
                "percentage": 10
            }
        });
        let res = validate_http_fault_injection_config("http_fault_bad_status", &val);
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("between 200 and 599"));
    }

    #[test]
    fn test_delay_exceeds_max_rejected() {
        let val = json!({
            "delay": {
                "enabled": true,
                "fixed_delay_ms": 120_000,
                "percentage": 10
            },
            "abort": {
                "enabled": false,
                "http_status": 500,
                "percentage": 0
            }
        });
        let res = validate_http_fault_injection_config("http_fault_delay_high", &val);
        assert!(res.is_err());
        assert!(
            res.unwrap_err()
                .to_string()
                .contains("between 1 and 60,000ms")
        );
    }

    #[test]
    fn test_missing_required_fields_rejected() {
        let val = json!({
            "delay": { "enabled": true }
            // Missing fixed_delay_ms, percentage, abort
        });
        let res = validate_http_fault_injection_config("http_fault_missing", &val);
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("missing field"));
    }
}
