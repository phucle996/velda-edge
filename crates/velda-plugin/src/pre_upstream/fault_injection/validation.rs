//! Unified Fault Injection Configuration Schema & Semantic Validation.
//!
//! ```text
//! JSON Config ─▶ serde(deny_unknown_fields) ─▶ Semantic Bounds
//!                                                   │
//!                                                   ├─▶ neither delay/abort/override? ─▶ Err
//!                                                   ├─▶ override without scope?       ─▶ Err
//!                                                   ├─▶ invalid/non-abort status?     ─▶ Err
//!                                                   └─▶ OK ─▶ FaultInjectionPlugin
//! ```
//!
//! # Strict Declarative Contract
//! - **L7 Application Protocols Only**: Compatible with HTTP/1.1, HTTP/2, HTTP/3, and gRPC (rejected on Raw L4).
//! - **Protocol-Blind Status**: `abort.internal_status` uses canonical [`velda_core::InternalStatus`]
//!   identifiers (e.g. `"service_unavailable"`, `"upstream_timeout"`, `"rate_limited"`).
//! - **Bounded Delay**: Fixed latency delay is constrained to `1..=60_000` ms when enabled.
//! - **Scoped Dynamic Overrides**: `scope.allow_header_override` requires `client_ips` or `header_match`.

use std::net::IpAddr;

use serde::{Deserialize, Serialize};
use velda_core::InternalStatus;

use crate::error::PluginError;

const DEFAULT_MAX_OVERRIDE_DELAY_MS: u64 = 5_000;

#[inline]
const fn default_max_override_delay_ms() -> u64 {
    DEFAULT_MAX_OVERRIDE_DELAY_MS
}

/// Declarative synthetic delay configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FaultInjectionDelayConfig {
    /// Whether synthetic delay injection is active.
    pub enabled: bool,
    /// Fixed delay added in milliseconds (1ms to 60,000ms).
    pub fixed_delay_ms: u64,
    /// Probability percentage of applying delay (0 to 100).
    pub percentage: u32,
}

/// Declarative synthetic abort configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FaultInjectionAbortConfig {
    /// Whether synthetic abort injection is active.
    pub enabled: bool,
    /// Canonical `InternalStatus` snake_case identifier returned on abort (e.g. `"service_unavailable"`).
    pub internal_status: String,
    /// Probability percentage of applying abort (0 to 100).
    pub percentage: u32,
}

/// Declarative matching rule for request headers in fault injection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FaultHeaderMatchConfig {
    /// Header name to inspect.
    pub name: String,
    /// If `Some`, matches exact UTF-8 string value. If `None`, matches header presence.
    pub exact_match: Option<String>,
}

/// Declarative targeting scope and dynamic override controls.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FaultScopeConfig {
    /// CIDR blocks or IP addresses targeted for fault injection.
    #[serde(default)]
    pub client_ips: Vec<String>,
    /// Header matching rule for targeted fault injection.
    #[serde(default)]
    pub header_match: Option<FaultHeaderMatchConfig>,
    /// Whether scoped clients can dynamically trigger delay or abort via `x-fault-*` headers.
    #[serde(default)]
    pub allow_header_override: bool,
    /// Maximum allowed delay override in milliseconds (1..=60,000ms).
    #[serde(default = "default_max_override_delay_ms")]
    pub max_override_delay_ms: u64,
}

impl Default for FaultScopeConfig {
    fn default() -> Self {
        Self {
            client_ips: Vec::new(),
            header_match: None,
            allow_header_override: false,
            max_override_delay_ms: DEFAULT_MAX_OVERRIDE_DELAY_MS,
        }
    }
}

/// Declarative configuration for the unified `fault_injection` plugin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FaultInjectionConfig {
    /// Targeting scope and dynamic override controls.
    #[serde(default)]
    pub scope: FaultScopeConfig,
    /// Latency injection block.
    pub delay: FaultInjectionDelayConfig,
    /// Immediate abort block.
    pub abort: FaultInjectionAbortConfig,
}

/// Validates and normalizes `fault_injection` configuration.
pub fn validate_fault_injection_config(
    plugin_id: &str,
    config_val: &serde_json::Value,
) -> Result<FaultInjectionConfig, PluginError> {
    let mut cfg: FaultInjectionConfig =
        serde_json::from_value(config_val.clone()).map_err(|e| {
            PluginError::Config(format!(
                "Plugin '{plugin_id}' has malformed or incomplete fault_injection config: {e}"
            ))
        })?;

    if !cfg.delay.enabled && !cfg.abort.enabled && !cfg.scope.allow_header_override {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': fault_injection configuration must enable at least delay, abort, or allow_header_override"
        )));
    }

    if cfg.scope.max_override_delay_ms == 0 || cfg.scope.max_override_delay_ms > 60_000 {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': scope.max_override_delay_ms ({}) must be between 1 and 60,000ms",
            cfg.scope.max_override_delay_ms
        )));
    }

    if cfg.scope.allow_header_override
        && cfg.scope.client_ips.is_empty()
        && cfg.scope.header_match.is_none()
    {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': allow_header_override cannot be enabled without scoping; declare client_ips or header_match to prevent unauthorized denial-of-service"
        )));
    }

    for ip_str in &cfg.scope.client_ips {
        let s = ip_str.trim();
        if s.is_empty() {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': scope.client_ips contains an empty entry"
            )));
        }
        if let Some((ip_part, prefix_part)) = s.split_once('/') {
            let ip: IpAddr = ip_part.trim().parse().map_err(|e| {
                PluginError::Config(format!(
                    "Plugin '{plugin_id}': invalid IP '{ip_part}' in scope.client_ips: {e}"
                ))
            })?;
            let prefix: u8 = prefix_part.trim().parse().map_err(|e| {
                PluginError::Config(format!(
                    "Plugin '{plugin_id}': invalid prefix '{prefix_part}' in scope.client_ips: {e}"
                ))
            })?;
            let max_prefix = if ip.is_ipv4() { 32 } else { 128 };
            if prefix > max_prefix {
                return Err(PluginError::Config(format!(
                    "Plugin '{plugin_id}': prefix {prefix} exceeds maximum of {max_prefix} for '{s}'"
                )));
            }
        } else {
            s.parse::<IpAddr>().map_err(|e| {
                PluginError::Config(format!(
                    "Plugin '{plugin_id}': invalid IP address '{s}' in scope.client_ips: {e}"
                ))
            })?;
        }
    }

    if let Some(hm) = &cfg.scope.header_match {
        if hm.name.is_empty() {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': scope.header_match.name cannot be empty"
            )));
        }
        if hm.name.starts_with(':') {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': scope.header_match.name cannot be a pseudo-header ('{}')",
                hm.name
            )));
        }
        let all_tchar = hm.name.bytes().all(|b| {
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
        });
        if !all_tchar {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': scope.header_match.name '{}' contains invalid characters",
                hm.name
            )));
        }
    }

    if cfg.delay.percentage > 100 {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': delay.percentage ({}) must be between 0 and 100",
            cfg.delay.percentage
        )));
    }
    if cfg.delay.enabled {
        if cfg.delay.fixed_delay_ms == 0 || cfg.delay.fixed_delay_ms > 60_000 {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': delay.fixed_delay_ms ({}) must be between 1 and 60,000ms",
                cfg.delay.fixed_delay_ms
            )));
        }
        if cfg.delay.percentage == 0 && !cfg.scope.allow_header_override {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': delay.enabled is true but delay.percentage is 0"
            )));
        }
    }

    if cfg.abort.percentage > 100 {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': abort.percentage ({}) must be between 0 and 100",
            cfg.abort.percentage
        )));
    }

    let trimmed_status = cfg.abort.internal_status.trim();
    if cfg.abort.enabled || !trimmed_status.is_empty() {
        let parsed = InternalStatus::from_name(trimmed_status).ok_or_else(|| {
            PluginError::Config(format!(
                "Plugin '{plugin_id}': abort.internal_status '{}' is not a valid InternalStatus",
                cfg.abort.internal_status
            ))
        })?;
        if !parsed.is_aborted() {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': abort.internal_status '{}' is not an aborted fault status",
                cfg.abort.internal_status
            )));
        }
        cfg.abort.internal_status = parsed.as_str().to_string();
    } else if cfg.scope.allow_header_override {
        cfg.abort.internal_status = InternalStatus::ServiceUnavailable.as_str().to_string();
    }

    if cfg.abort.enabled && cfg.abort.percentage == 0 && !cfg.scope.allow_header_override {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': abort.enabled is true but abort.percentage is 0"
        )));
    }

    Ok(cfg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_valid_fault_injection_with_default_scope() {
        let val = json!({
            "delay": {
                "enabled": true,
                "fixed_delay_ms": 250,
                "percentage": 5
            },
            "abort": {
                "enabled": true,
                "internal_status": "service_unavailable",
                "percentage": 2
            }
        });
        let cfg = validate_fault_injection_config("fault_1", &val).unwrap();
        assert_eq!(cfg.scope.max_override_delay_ms, 5000);
        assert_eq!(cfg.abort.internal_status, "service_unavailable");
    }

    #[test]
    fn test_valid_fault_injection_with_scope() {
        let val = json!({
            "scope": {
                "client_ips": ["10.0.0.0/8", "192.168.1.1"],
                "header_match": {
                    "name": "x-chaos-test",
                    "exact_match": "enabled"
                },
                "allow_header_override": true,
                "max_override_delay_ms": 3000
            },
            "delay": {
                "enabled": true,
                "fixed_delay_ms": 100,
                "percentage": 10
            },
            "abort": {
                "enabled": false,
                "internal_status": "upstream_timeout",
                "percentage": 0
            }
        });
        assert!(validate_fault_injection_config("fault_scoped", &val).is_ok());
    }

    #[test]
    fn test_unscoped_override_rejected() {
        let val = json!({
            "scope": {
                "client_ips": [],
                "header_match": null,
                "allow_header_override": true,
                "max_override_delay_ms": 5000
            },
            "delay": {
                "enabled": true,
                "fixed_delay_ms": 100,
                "percentage": 10
            },
            "abort": {
                "enabled": false,
                "internal_status": "service_unavailable",
                "percentage": 0
            }
        });
        let res = validate_fault_injection_config("fault_unscoped", &val);
        assert!(
            res.unwrap_err()
                .to_string()
                .contains("cannot be enabled without scoping")
        );
    }

    #[test]
    fn test_invalid_or_completed_internal_status_rejected() {
        let completed = json!({
            "delay": { "enabled": false, "fixed_delay_ms": 0, "percentage": 0 },
            "abort": { "enabled": true, "internal_status": "completed", "percentage": 10 }
        });
        assert!(
            validate_fault_injection_config("fault_completed", &completed)
                .unwrap_err()
                .to_string()
                .contains("not an aborted fault status")
        );

        let unknown = json!({
            "delay": { "enabled": false, "fixed_delay_ms": 0, "percentage": 0 },
            "abort": { "enabled": true, "internal_status": "unknown_status", "percentage": 10 }
        });
        assert!(
            validate_fault_injection_config("fault_unknown_status", &unknown)
                .unwrap_err()
                .to_string()
                .contains("not a valid InternalStatus")
        );
    }

    #[test]
    fn test_delay_exceeds_max_rejected() {
        let val = json!({
            "delay": { "enabled": true, "fixed_delay_ms": 120_000, "percentage": 10 },
            "abort": { "enabled": false, "internal_status": "service_unavailable", "percentage": 0 }
        });
        assert!(
            validate_fault_injection_config("fault_delay_high", &val)
                .unwrap_err()
                .to_string()
                .contains("between 1 and 60,000ms")
        );
    }

    #[test]
    fn test_noop_and_unknown_fields_rejected() {
        let noop = json!({
            "delay": { "enabled": false, "fixed_delay_ms": 0, "percentage": 0 },
            "abort": { "enabled": false, "internal_status": "service_unavailable", "percentage": 0 }
        });
        assert!(
            validate_fault_injection_config("fault_noop", &noop)
                .unwrap_err()
                .to_string()
                .contains("must enable at least delay, abort")
        );

        let unknown_field = json!({
            "delay": { "enabled": true, "fixed_delay_ms": 100, "percentage": 10, "extra": true },
            "abort": { "enabled": false, "internal_status": "service_unavailable", "percentage": 0 }
        });
        assert!(
            validate_fault_injection_config("fault_unknown_field", &unknown_field)
                .unwrap_err()
                .to_string()
                .contains("unknown field")
        );
    }
}
