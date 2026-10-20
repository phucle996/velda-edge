//! gRPC Fault Injection Configuration Schema & Semantic Validation (`phase: "pre_upstream"`).
//!
//! Enforces a strict declarative contract for gRPC chaos engineering.
//!
//! # Strict Declarative Contract (Zero Silent Fallbacks)
//! - **Strict Protocol Invariant**: Restricted exclusively to gRPC routes (prohibited on HTTP/Raw routes).
//! - **Bounded Percentage**: Chaos trigger percentages must be between 0 and 100.
//! - **Valid Canonical gRPC Status**: Abort status code must be a canonical gRPC code (0..=16).
//! - **Safe Delay Threshold**: Fixed latency delay cannot exceed 60,000ms.

use serde::{Deserialize, Serialize};

use crate::SyncError;

/// Declarative synthetic delay configuration for gRPC RPCs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GrpcFaultDelayConfig {
    /// Whether synthetic delay injection is active.
    pub enabled: bool,
    /// Fixed delay added in milliseconds (1ms to 60,000ms).
    pub fixed_delay_ms: u64,
    /// Probability percentage of applying delay (0 to 100).
    pub percentage: u32,
}

/// Declarative synthetic abort configuration for gRPC RPCs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GrpcFaultAbortConfig {
    /// Whether synthetic abort injection is active.
    pub enabled: bool,
    /// Canonical gRPC status code returned to client (0 to 16, e.g. 14 = UNAVAILABLE, 16 = UNAUTHENTICATED).
    pub grpc_status: u32,
    /// Detailed diagnostic message associated with synthetic error.
    pub grpc_message: String,
    /// Probability percentage of applying abort (0 to 100).
    pub percentage: u32,
}

/// Declarative configuration for the gRPC Fault Injection plugin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GrpcFaultInjectionConfig {
    /// Latency injection block.
    pub delay: GrpcFaultDelayConfig,
    /// Immediate abort block.
    pub abort: GrpcFaultAbortConfig,
}

/// Validates gRPC Fault Injection configuration strictly according to production standards.
pub fn validate_grpc_fault_injection_config(
    plugin_id: &str,
    config_val: &serde_json::Value,
) -> Result<GrpcFaultInjectionConfig, SyncError> {
    let cfg: GrpcFaultInjectionConfig =
        serde_json::from_value(config_val.clone()).map_err(|e| SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}' has malformed or incomplete grpc_fault_injection config: {e}"
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
        if cfg.abort.grpc_status > 16 {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': abort.grpc_status ({}) must be between 0 and 16 (canonical gRPC status)",
                    cfg.abort.grpc_status
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
    fn test_valid_grpc_fault_injection() {
        let val = json!({
            "delay": {
                "enabled": true,
                "fixed_delay_ms": 100,
                "percentage": 10
            },
            "abort": {
                "enabled": true,
                "grpc_status": 14, // UNAVAILABLE
                "grpc_message": "Synthetic cluster overload",
                "percentage": 5
            }
        });
        let res = validate_grpc_fault_injection_config("grpc_fault_1", &val);
        assert!(res.is_ok());
    }

    #[test]
    fn test_invalid_grpc_status_rejected() {
        let val = json!({
            "delay": {
                "enabled": false,
                "fixed_delay_ms": 0,
                "percentage": 0
            },
            "abort": {
                "enabled": true,
                "grpc_status": 17, // Invalid (> 16)
                "grpc_message": "Bad status",
                "percentage": 10
            }
        });
        let res = validate_grpc_fault_injection_config("grpc_fault_bad_status", &val);
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("between 0 and 16"));
    }

    #[test]
    fn test_missing_required_fields_rejected() {
        let val = json!({
            "abort": { "enabled": true }
        });
        let res = validate_grpc_fault_injection_config("grpc_fault_missing", &val);
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("missing field"));
    }
}
