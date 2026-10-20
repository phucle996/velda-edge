//! IP Filter Configuration Schema & Semantic Validation (`phase: "pre_route"`).
//!
//! Enforces a strict declarative contract for the IP / CIDR Subnet Access Filter plugin.
//!
//! # Strict Declarative Contract (Zero Silent Fallbacks)
//! - **Explicit Mode**: `mode` must be explicitly declared as `"allow"` or `"deny"` (aliases: `"whitelist"`, `"blacklist"`).
//! - **Valid CIDR / IP Format**: Every CIDR entry in `allow_cidrs` and `deny_cidrs` must parse into a valid IPv4 or IPv6 address or subnet prefix.
//! - **Source Verification**: `client_ip_source` must be a supported ingress IP header extraction strategy.
//! - **Non-Empty Whitelist**: If `mode` is `"allow"`, `allow_cidrs` cannot be empty.

use std::net::IpAddr;

use serde::{Deserialize, Serialize};

use crate::SyncError;

/// Declarative configuration for the IP Filter plugin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IpFilterConfig {
    /// Operational filter mode: "allow" (whitelist) or "deny" (blacklist).
    pub mode: String,
    /// CIDR networks permitted when mode is allow, or exceptions when mode is deny.
    pub allow_cidrs: Vec<String>,
    /// CIDR networks blocked when mode is deny, or exceptions when mode is allow.
    pub deny_cidrs: Vec<String>,
    /// Client IP resolution strategy: "x_forwarded_for" or "x_real_ip".
    pub client_ip_source: String,
}

/// Helper to parse and validate IPv4/IPv6 CIDR notation or bare IP address.
pub fn validate_cidr(cidr_str: &str) -> Result<(), String> {
    let trimmed = cidr_str.trim();
    if trimmed.is_empty() {
        return Err("CIDR entry cannot be empty".to_string());
    }

    if let Some((ip_part, prefix_part)) = trimmed.split_once('/') {
        let ip = ip_part
            .parse::<IpAddr>()
            .map_err(|e| format!("Invalid IP address '{ip_part}': {e}"))?;
        let prefix = prefix_part
            .parse::<u8>()
            .map_err(|e| format!("Invalid CIDR prefix length '{prefix_part}': {e}"))?;

        match ip {
            IpAddr::V4(_) if prefix > 32 => {
                Err(format!("IPv4 prefix length must be 0..=32, got {prefix}"))
            }
            IpAddr::V6(_) if prefix > 128 => {
                Err(format!("IPv6 prefix length must be 0..=128, got {prefix}"))
            }
            _ => Ok(()),
        }
    } else {
        // Bare IP without slash
        trimmed
            .parse::<IpAddr>()
            .map(|_| ())
            .map_err(|e| format!("Invalid bare IP address '{trimmed}': {e}"))
    }
}

/// Validates IP Filter configuration strictly according to production standards.
pub fn validate_ip_filter_config(
    plugin_id: &str,
    config_val: &serde_json::Value,
) -> Result<IpFilterConfig, SyncError> {
    let cfg: IpFilterConfig =
        serde_json::from_value(config_val.clone()).map_err(|e| SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}' has malformed or incomplete ip_filter config: {e}"
            ),
        })?;

    // 1. Validate Mode
    let mode_lower = cfg.mode.trim().to_ascii_lowercase();
    match mode_lower.as_str() {
        "allow" | "whitelist" | "deny" | "blacklist" => {}
        other => {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': invalid mode '{other}' (expected 'allow' or 'deny')"
                ),
            });
        }
    }

    // 2. Validate Client IP Source
    let source_lower = cfg.client_ip_source.trim().to_ascii_lowercase();
    match source_lower.as_str() {
        "x_forwarded_for" | "x-forwarded-for" | "x_real_ip" | "x-real-ip" | "real_ip" => {}
        other => {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': invalid client_ip_source '{other}' (expected 'x_forwarded_for' or 'x_real_ip')"
                ),
            });
        }
    }

    // 3. Validate CIDR formats in allow_cidrs
    for cidr in &cfg.allow_cidrs {
        if cidr.trim().eq_ignore_ascii_case("none") {
            continue;
        }
        if let Err(e) = validate_cidr(cidr) {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!("Plugin '{plugin_id}': allow_cidrs contains invalid CIDR: {e}"),
            });
        }
    }

    // 4. Validate CIDR formats in deny_cidrs
    for cidr in &cfg.deny_cidrs {
        if cidr.trim().eq_ignore_ascii_case("none") {
            continue;
        }
        if let Err(e) = validate_cidr(cidr) {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!("Plugin '{plugin_id}': deny_cidrs contains invalid CIDR: {e}"),
            });
        }
    }

    // 5. Fail-Safe: In allowlist mode, allow_cidrs must have at least one valid entry
    let is_allow_mode = matches!(mode_lower.as_str(), "allow" | "whitelist");
    let has_allow_cidrs = cfg
        .allow_cidrs
        .iter()
        .any(|c| !c.trim().is_empty() && !c.trim().eq_ignore_ascii_case("none"));

    if is_allow_mode && !has_allow_cidrs {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': mode is 'allow' but allow_cidrs is empty (would reject 100% of traffic)"
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
    fn test_valid_allow_list() {
        let val = json!({
            "mode": "allow",
            "allow_cidrs": ["192.168.1.0/24", "10.0.0.1", "::1/128"],
            "deny_cidrs": [],
            "client_ip_source": "x_forwarded_for"
        });
        let res = validate_ip_filter_config("ip_filter_1", &val);
        assert!(res.is_ok());
    }

    #[test]
    fn test_valid_deny_list() {
        let val = json!({
            "mode": "deny",
            "allow_cidrs": [],
            "deny_cidrs": ["198.51.100.0/24"],
            "client_ip_source": "x_real_ip"
        });
        let res = validate_ip_filter_config("ip_filter_2", &val);
        assert!(res.is_ok());
    }

    #[test]
    fn test_invalid_mode() {
        let val = json!({
            "mode": "bypass",
            "allow_cidrs": ["10.0.0.0/8"],
            "deny_cidrs": [],
            "client_ip_source": "x_forwarded_for"
        });
        let res = validate_ip_filter_config("ip_filter_bad_mode", &val);
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("invalid mode"));
    }

    #[test]
    fn test_invalid_cidr_format() {
        let val = json!({
            "mode": "deny",
            "allow_cidrs": [],
            "deny_cidrs": ["10.0.0.1/33"],
            "client_ip_source": "x_real_ip"
        });
        let res = validate_ip_filter_config("ip_filter_bad_cidr", &val);
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("IPv4 prefix length"));
    }

    #[test]
    fn test_empty_allowlist_rejected() {
        let val = json!({
            "mode": "allow",
            "allow_cidrs": [],
            "deny_cidrs": [],
            "client_ip_source": "x_forwarded_for"
        });
        let res = validate_ip_filter_config("ip_filter_empty_allow", &val);
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("would reject 100%"));
    }

    #[test]
    fn test_missing_required_fields_rejected() {
        let val = json!({
            "mode": "allow",
            "allow_cidrs": ["10.0.0.0/8"]
            // Missing deny_cidrs and client_ip_source
        });
        let res = validate_ip_filter_config("ip_filter_missing", &val);
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("missing field"));
    }
}
