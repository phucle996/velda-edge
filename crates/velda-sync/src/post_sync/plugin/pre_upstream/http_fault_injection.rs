//! HTTP Fault Injection Configuration Schema & Semantic Validation (`phase: "pre_upstream"`).
//!
//! Enforces a strict declarative contract for HTTP chaos engineering and resilience testing.
//!
//! # Strict Declarative Contract (Zero Silent Fallbacks)
//! - **Strict Protocol Invariant**: Restricted exclusively to HTTP routes (prohibited on gRPC/Raw routes).
//! - **Bounded Percentage**: Chaos trigger percentages must be between 0 and 100.
//! - **Valid HTTP Status**: Abort status code must be an RFC 9110 client/server error status (400..=599).
//! - **Safe Delay Threshold**: Fixed latency delay cannot exceed 60,000ms (1 minute).
//! - **Targeting Scope Guard**: Dynamic overrides require scoping (header match or client IPs) to prevent Slowloris attacks.

use std::net::IpAddr;

use serde::{Deserialize, Serialize};

use crate::SyncError;

/// Declarative synthetic delay configuration for HTTP requests.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
pub struct HttpFaultAbortConfig {
    /// Whether synthetic abort injection is active.
    pub enabled: bool,
    /// Synthetic HTTP status code returned to client (400 to 599).
    pub http_status: u16,
    /// Probability percentage of applying abort (0 to 100).
    pub percentage: u32,
}

/// Declarative matching rule for request headers in fault injection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HttpFaultHeaderMatchConfig {
    /// Header name to inspect.
    pub name: String,
    /// If Some, matches exact string value. If None, matches presence of header.
    #[serde(default)]
    pub exact_match: Option<String>,
}

/// Declarative targeting scope and dynamic override settings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HttpFaultScopeConfig {
    /// CIDR blocks or IP addresses targeted for fault injection.
    #[serde(default)]
    pub client_ips: Vec<String>,
    /// Header matching rule for targeted fault injection.
    #[serde(default)]
    pub header_match: Option<HttpFaultHeaderMatchConfig>,
    /// Whether downstream clients can dynamically trigger delay or abort via headers.
    #[serde(default)]
    pub allow_header_override: bool,
    /// Maximum allowed delay override in milliseconds (clamped to prevent Slowloris attacks).
    #[serde(default = "default_max_override_delay_ms")]
    pub max_override_delay_ms: u64,
}

impl Default for HttpFaultScopeConfig {
    fn default() -> Self {
        Self {
            client_ips: Vec::new(),
            header_match: None,
            allow_header_override: false,
            max_override_delay_ms: default_max_override_delay_ms(),
        }
    }
}

fn default_max_override_delay_ms() -> u64 {
    5_000 // 5 seconds default safe cap
}

/// Declarative configuration for the HTTP Fault Injection plugin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HttpFaultInjectionConfig {
    /// Targeting scope and dynamic override controls.
    #[serde(default)]
    pub scope: HttpFaultScopeConfig,
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

    // Invariant: Non-empty injection policy (at least one mechanism must be enabled)
    if !cfg.delay.enabled && !cfg.abort.enabled && !cfg.scope.allow_header_override {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': http_fault_injection configuration must enable at least delay, abort, or allow_header_override"
            ),
        });
    }

    // 1. Validate Scope Block
    if cfg.scope.max_override_delay_ms == 0 || cfg.scope.max_override_delay_ms > 60_000 {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': scope.max_override_delay_ms ({}) must be between 1 and 60,000ms",
                cfg.scope.max_override_delay_ms
            ),
        });
    }

    // Security Invariant: Dynamic overrides must be scoped by IP or QA header to prevent open Slowloris DOS
    if cfg.scope.allow_header_override
        && cfg.scope.client_ips.is_empty()
        && cfg.scope.header_match.is_none()
    {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': allow_header_override cannot be enabled without scoping; declare client_ips or header_match to prevent unauthorized Slowloris denial-of-service"
            ),
        });
    }

    // Validate CIDRs / IP addresses in client_ips
    for ip_str in &cfg.scope.client_ips {
        let s = ip_str.trim();
        if s.is_empty() {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!("Plugin '{plugin_id}': scope.client_ips contains an empty entry"),
            });
        }
        if let Some((ip_part, prefix_part)) = s.split_once('/') {
            let ip: IpAddr = ip_part.trim().parse().map_err(|e| SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': invalid IP '{ip_part}' in scope.client_ips: {e}"
                ),
            })?;
            let prefix: u8 = prefix_part
                .trim()
                .parse()
                .map_err(|e| SyncError::Validation {
                    domain: "plugins".into(),
                    reason: format!(
                        "Plugin '{plugin_id}': invalid prefix '{prefix_part}' in scope.client_ips: {e}"
                    ),
                })?;
            let max_prefix = if ip.is_ipv4() { 32 } else { 128 };
            if prefix > max_prefix {
                return Err(SyncError::Validation {
                    domain: "plugins".into(),
                    reason: format!(
                        "Plugin '{plugin_id}': prefix {prefix} exceeds maximum of {max_prefix} for '{s}'"
                    ),
                });
            }
        } else {
            s.parse::<IpAddr>().map_err(|e| SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': invalid IP address '{s}' in scope.client_ips: {e}"
                ),
            })?;
        }
    }

    // Validate header_match
    if let Some(hm) = &cfg.scope.header_match {
        if hm.name.is_empty() {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!("Plugin '{plugin_id}': scope.header_match.name cannot be empty"),
            });
        }
        if hm.name.starts_with(':') {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': scope.header_match.name cannot be a pseudo-header ('{}')",
                    hm.name
                ),
            });
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
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': scope.header_match.name '{}' contains invalid characters",
                    hm.name
                ),
            });
        }
    }

    // 2. Validate Delay Block
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
        if cfg.delay.percentage == 0 && !cfg.scope.allow_header_override {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': delay.enabled is true but delay.percentage is 0"
                ),
            });
        }
    }

    // 3. Validate Abort Block
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
        if !(400..=599).contains(&cfg.abort.http_status) {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': abort.http_status ({}) must be an error status between 400 and 599",
                    cfg.abort.http_status
                ),
            });
        }
        if cfg.abort.percentage == 0 && !cfg.scope.allow_header_override {
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
    fn test_valid_http_fault_with_scope() {
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
                "http_status": 500,
                "percentage": 0
            }
        });
        let res = validate_http_fault_injection_config("http_fault_scoped", &val);
        assert!(res.is_ok());
    }

    #[test]
    fn test_unscoped_override_rejected() {
        let val = json!({
            "scope": {
                "allow_header_override": true
                // Missing both client_ips and header_match!
            },
            "delay": {
                "enabled": true,
                "fixed_delay_ms": 100,
                "percentage": 10
            },
            "abort": {
                "enabled": false,
                "http_status": 500,
                "percentage": 0
            }
        });
        let res = validate_http_fault_injection_config("http_fault_unscoped", &val);
        assert!(res.is_err());
        assert!(
            res.unwrap_err()
                .to_string()
                .contains("cannot be enabled without scoping")
        );
    }

    #[test]
    fn test_invalid_http_status_rejected() {
        // Status 200 is not an error/fault abort!
        let val = json!({
            "delay": {
                "enabled": false,
                "fixed_delay_ms": 0,
                "percentage": 0
            },
            "abort": {
                "enabled": true,
                "http_status": 200,
                "percentage": 10
            }
        });
        let res = validate_http_fault_injection_config("http_fault_bad_status", &val);
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("between 400 and 599"));
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
    fn test_noop_config_rejected() {
        let val = json!({
            "delay": {
                "enabled": false,
                "fixed_delay_ms": 0,
                "percentage": 0
            },
            "abort": {
                "enabled": false,
                "http_status": 500,
                "percentage": 0
            }
        });
        let res = validate_http_fault_injection_config("http_fault_noop", &val);
        assert!(res.is_err());
        assert!(
            res.unwrap_err()
                .to_string()
                .contains("must enable at least delay, abort")
        );
    }

    #[test]
    fn test_unknown_fields_rejected() {
        let val = json!({
            "delay": {
                "enabled": true,
                "fixed_delay_ms": 100,
                "percentage": 10,
                "unknown_field": true
            },
            "abort": {
                "enabled": false,
                "http_status": 500,
                "percentage": 0
            }
        });
        let res = validate_http_fault_injection_config("http_fault_unknown", &val);
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("unknown field"));
    }
}
