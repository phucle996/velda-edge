//! IP Filter Configuration Schema & Semantic Validation (`phase: "pre_route"`).
//!
//! Enforces a strict declarative contract for the IP / CIDR Subnet Access Filter plugin.
//!
//! # Strict Declarative Contract (Zero Silent Fallbacks)
//! - **Explicit Mode**: `mode` must be explicitly declared as `"allow"` or `"deny"`.
//! - **Valid CIDR / IP Format**: Every CIDR entry in `allow_cidrs` and `deny_cidrs` must parse into a valid IPv4 or IPv6 address or subnet prefix.
//! - **Canonical Normalization**: Host bits are masked, bare IPs are expanded to `/32` or `/128`, and IPv4-mapped IPv6 is canonicalized to IPv4.
//! - **Deduplication & Sanitization**: Duplicates and invalid placeholders are removed.
//! - **Contradictory Rule Defense**: Detects identical rules declared in both allow and deny sets.
//! - **Source Verification**: `client_ip_source` must be a supported ingress IP header extraction strategy.
//! - **Non-Empty Whitelist**: If `mode` is `"allow"`, `allow_cidrs` cannot be empty.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use serde::{Deserialize, Serialize};

use crate::SyncError;

/// Declarative configuration for the IP Filter plugin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IpFilterConfig {
    /// Operational filter mode: "allow" or "deny".
    pub mode: String,
    /// CIDR networks permitted when mode is allow, or exceptions when mode is deny.
    pub allow_cidrs: Vec<String>,
    /// CIDR networks blocked when mode is deny, or exceptions when mode is allow.
    pub deny_cidrs: Vec<String>,
    /// Client IP resolution strategy: "x_forwarded_for" or "x_real_ip".
    pub client_ip_source: String,
}

/// Internal bitmask representation for semantic overlap and shadowing analysis.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ParsedCidr {
    V4 { net: u32, mask: u32, prefix: u8 },
    V6 { net: u128, mask: u128, prefix: u8 },
}

impl ParsedCidr {
    /// Returns true if `self` (broader or equal prefix) completely covers `other` (narrower or equal prefix).
    #[inline]
    fn covers(self, other: Self) -> bool {
        match (self, other) {
            (
                Self::V4 {
                    net: n1,
                    mask: m1,
                    prefix: p1,
                },
                Self::V4 {
                    net: n2,
                    mask: _,
                    prefix: p2,
                },
            ) => p1 <= p2 && (n2 & m1) == n1,
            (
                Self::V6 {
                    net: n1,
                    mask: m1,
                    prefix: p1,
                },
                Self::V6 {
                    net: n2,
                    mask: _,
                    prefix: p2,
                },
            ) => p1 <= p2 && (n2 & m1) == n1,
            _ => false,
        }
    }
}

/// Extracts an embedded IPv4 address from IPv6 transition/tunneling mechanisms:
/// - RFC 4291 IPv4-mapped (`::ffff:a.b.c.d`)
/// - RFC 2765 SIIT IPv4-translated (`::ffff:0:a.b.c.d`)
/// - RFC 6052 Well-Known NAT64 (`64:ff9b::a.b.c.d`)
/// - RFC 4291 §2.5.5.1 IPv4-compatible (`::a.b.c.d`, excluding `::` and `::1`)
#[inline]
fn extract_embedded_ipv4(v6: Ipv6Addr) -> Option<Ipv4Addr> {
    let bits = v6.to_bits();
    let upper_96 = bits >> 32;
    let lower_32 = (bits & 0xFFFF_FFFF) as u32;
    if upper_96 == 0x0000_FFFF
        || upper_96 == 0xFFFF_0000
        || upper_96 == 0x0064_FF9B_0000_0000_0000_0000
    {
        return Some(Ipv4Addr::from_bits(lower_32));
    }
    if upper_96 == 0 && lower_32 > 1 {
        return Some(Ipv4Addr::from_bits(lower_32));
    }
    None
}

fn parse_and_canonicalize_cidr(cidr_str: &str) -> Result<(String, ParsedCidr), String> {
    let mut trimmed = cidr_str.trim();
    if trimmed.starts_with('"') && trimmed.ends_with('"') && trimmed.len() >= 2 {
        trimmed = trimmed[1..trimmed.len() - 1].trim();
    }
    if trimmed.is_empty() {
        return Err("CIDR entry cannot be empty".to_string());
    }

    if let Some((ip_part, prefix_part)) = trimmed.split_once('/') {
        let ip_part = ip_part.trim();
        let prefix_part = prefix_part.trim();
        let ip: IpAddr = ip_part
            .parse()
            .map_err(|e| format!("Invalid IP address '{ip_part}': {e}"))?;
        let prefix: u8 = prefix_part
            .parse()
            .map_err(|e| format!("Invalid CIDR prefix length '{prefix_part}': {e}"))?;

        match ip {
            IpAddr::V4(v4) => {
                if prefix > 32 {
                    return Err(format!("IPv4 prefix length must be 0..=32, got {prefix}"));
                }
                let mask = if prefix == 0 {
                    0u32
                } else {
                    !0u32 << (32 - prefix)
                };
                let net_bits = v4.to_bits() & mask;
                let net_ip = Ipv4Addr::from_bits(net_bits);
                Ok((
                    format!("{net_ip}/{prefix}"),
                    ParsedCidr::V4 {
                        net: net_bits,
                        mask,
                        prefix,
                    },
                ))
            }
            IpAddr::V6(v6) => {
                if let Some(v4) = extract_embedded_ipv4(v6) {
                    if !(96..=128).contains(&prefix) {
                        return Err(format!(
                            "IPv4-embedded IPv6 prefix must be 96..=128, got {prefix}"
                        ));
                    }
                    let v4_prefix = prefix - 96;
                    let mask = if v4_prefix == 0 {
                        0u32
                    } else {
                        !0u32 << (32 - v4_prefix)
                    };
                    let net_bits = v4.to_bits() & mask;
                    let net_ip = Ipv4Addr::from_bits(net_bits);
                    Ok((
                        format!("{net_ip}/{v4_prefix}"),
                        ParsedCidr::V4 {
                            net: net_bits,
                            mask,
                            prefix: v4_prefix,
                        },
                    ))
                } else {
                    if prefix > 128 {
                        return Err(format!("IPv6 prefix length must be 0..=128, got {prefix}"));
                    }
                    let mask = if prefix == 0 {
                        0u128
                    } else {
                        !0u128 << (128 - prefix)
                    };
                    let net_bits = v6.to_bits() & mask;
                    let net_ip = Ipv6Addr::from_bits(net_bits);
                    Ok((
                        format!("{net_ip}/{prefix}"),
                        ParsedCidr::V6 {
                            net: net_bits,
                            mask,
                            prefix,
                        },
                    ))
                }
            }
        }
    } else {
        // Bare IP
        let ip: IpAddr = trimmed
            .parse()
            .map_err(|e| format!("Invalid bare IP address '{trimmed}': {e}"))?;
        match ip {
            IpAddr::V4(v4) => Ok((
                format!("{v4}/32"),
                ParsedCidr::V4 {
                    net: v4.to_bits(),
                    mask: !0u32,
                    prefix: 32,
                },
            )),
            IpAddr::V6(v6) => {
                if let Some(v4) = extract_embedded_ipv4(v6) {
                    Ok((
                        format!("{v4}/32"),
                        ParsedCidr::V4 {
                            net: v4.to_bits(),
                            mask: !0u32,
                            prefix: 32,
                        },
                    ))
                } else {
                    Ok((
                        format!("{v6}/128"),
                        ParsedCidr::V6 {
                            net: v6.to_bits(),
                            mask: !0u128,
                            prefix: 128,
                        },
                    ))
                }
            }
        }
    }
}

/// Helper to parse, validate, and canonicalize IPv4/IPv6 CIDR notation or bare IP address.
pub fn canonicalize_cidr(cidr_str: &str) -> Result<String, String> {
    parse_and_canonicalize_cidr(cidr_str).map(|(s, _)| s)
}

/// Helper to parse and validate IPv4/IPv6 CIDR notation or bare IP address.
pub fn validate_cidr(cidr_str: &str) -> Result<(), String> {
    canonicalize_cidr(cidr_str).map(|_| ())
}

/// Validates and normalizes IP Filter configuration strictly according to production standards.
pub fn validate_ip_filter_config(
    plugin_id: &str,
    config_val: &serde_json::Value,
) -> Result<IpFilterConfig, SyncError> {
    let mut cfg: IpFilterConfig =
        serde_json::from_value(config_val.clone()).map_err(|e| SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}' has malformed or incomplete ip_filter config: {e}"
            ),
        })?;

    // 1. Validate and normalize Mode
    let mode_lower = cfg.mode.trim().to_ascii_lowercase();
    match mode_lower.as_str() {
        "allow" | "deny" => {
            cfg.mode = mode_lower.clone();
        }
        other => {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': invalid mode '{other}' (expected 'allow' or 'deny')"
                ),
            });
        }
    }

    // 2. Validate and normalize Client IP Source
    let source_lower = cfg.client_ip_source.trim().to_ascii_lowercase();
    let canonical_source = match source_lower.as_str() {
        "x_forwarded_for" | "x-forwarded-for" => "x_forwarded_for",
        "x_real_ip" | "x-real-ip" | "real_ip" => "x_real_ip",
        other => {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': invalid client_ip_source '{other}' (expected 'x_forwarded_for' or 'x_real_ip')"
                ),
            });
        }
    };
    cfg.client_ip_source = canonical_source.to_string();

    // 3. Validate, canonicalize, and deduplicate CIDR formats in allow_cidrs
    let mut raw_allow: Vec<(String, ParsedCidr)> = Vec::with_capacity(cfg.allow_cidrs.len());
    for cidr in &cfg.allow_cidrs {
        if cidr.trim().eq_ignore_ascii_case("none") {
            continue;
        }
        let (canonical, parsed) =
            parse_and_canonicalize_cidr(cidr).map_err(|e| SyncError::Validation {
                domain: "plugins".into(),
                reason: format!("Plugin '{plugin_id}': allow_cidrs contains invalid CIDR: {e}"),
            })?;
        if !raw_allow.iter().any(|(c, _)| c == &canonical) {
            raw_allow.push((canonical, parsed));
        }
    }

    // 4. Validate, canonicalize, and deduplicate CIDR formats in deny_cidrs
    let mut raw_deny: Vec<(String, ParsedCidr)> = Vec::with_capacity(cfg.deny_cidrs.len());
    for cidr in &cfg.deny_cidrs {
        if cidr.trim().eq_ignore_ascii_case("none") {
            continue;
        }
        let (canonical, parsed) =
            parse_and_canonicalize_cidr(cidr).map_err(|e| SyncError::Validation {
                domain: "plugins".into(),
                reason: format!("Plugin '{plugin_id}': deny_cidrs contains invalid CIDR: {e}"),
            })?;
        if !raw_deny.iter().any(|(c, _)| c == &canonical) {
            raw_deny.push((canonical, parsed));
        }
    }

    // 5. Fail-Safe: In allow mode, allow_cidrs must have at least one valid entry
    let is_allow_mode = cfg.mode == "allow";
    if is_allow_mode && raw_allow.is_empty() {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': mode is 'allow' but allow_cidrs is empty (would reject 100% of traffic)"
            ),
        });
    }

    // 6. Contradictory & Cross-List Shadowing Detection
    // - Exact match in both allow_cidrs and deny_cidrs is always contradictory.
    // - In 'allow' mode, deny_cidrs are narrow exceptions inside broader allow_cidrs.
    //   If a deny rule covers (is broader than or equal to) an allow rule, that allow rule is 100% dead/shadowed.
    // - In 'deny' mode, allow_cidrs are narrow exceptions inside broader deny_cidrs.
    //   If an allow rule covers (is broader than or equal to) a deny rule, that deny rule is 100% dead/shadowed.
    for (allow_str, allow_net) in &raw_allow {
        for (deny_str, deny_net) in &raw_deny {
            if allow_net == deny_net {
                return Err(SyncError::Validation {
                    domain: "plugins".into(),
                    reason: format!(
                        "Plugin '{plugin_id}': contradictory configuration: rule '{allow_str}' is declared in both allow_cidrs and deny_cidrs"
                    ),
                });
            }
            if is_allow_mode && deny_net.covers(*allow_net) {
                return Err(SyncError::Validation {
                    domain: "plugins".into(),
                    reason: format!(
                        "Plugin '{plugin_id}': shadowed rule in 'allow' mode: allow_cidr '{allow_str}' is completely shadowed by broader deny_cidr '{deny_str}'"
                    ),
                });
            }
            if !is_allow_mode && allow_net.covers(*deny_net) {
                return Err(SyncError::Validation {
                    domain: "plugins".into(),
                    reason: format!(
                        "Plugin '{plugin_id}': shadowed rule in 'deny' mode: deny_cidr '{deny_str}' is completely shadowed by broader allow_cidr '{allow_str}'"
                    ),
                });
            }
        }
    }

    // 7. Intra-list Subsumption Compaction (remove narrower rules already covered by a broader rule in the same list)
    let mut compacted_allow = Vec::with_capacity(raw_allow.len());
    for (idx, (cidr_str, net)) in raw_allow.iter().enumerate() {
        let is_subsumed = raw_allow
            .iter()
            .enumerate()
            .any(|(other_idx, (_, other_net))| other_idx != idx && other_net.covers(*net));
        if !is_subsumed {
            compacted_allow.push(cidr_str.clone());
        }
    }
    cfg.allow_cidrs = compacted_allow;

    let mut compacted_deny = Vec::with_capacity(raw_deny.len());
    for (idx, (cidr_str, net)) in raw_deny.iter().enumerate() {
        let is_subsumed = raw_deny
            .iter()
            .enumerate()
            .any(|(other_idx, (_, other_net))| other_idx != idx && other_net.covers(*net));
        if !is_subsumed {
            compacted_deny.push(cidr_str.clone());
        }
    }
    cfg.deny_cidrs = compacted_deny;

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
        let cfg = res.unwrap();
        assert_eq!(cfg.allow_cidrs[0], "192.168.1.0/24");
        assert_eq!(cfg.allow_cidrs[1], "10.0.0.1/32");
        assert_eq!(cfg.allow_cidrs[2], "::1/128");
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
        });
        let res = validate_ip_filter_config("ip_filter_missing", &val);
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("missing field"));
    }

    #[test]
    fn test_contradictory_cidr_detection() {
        let val = json!({
            "mode": "allow",
            "allow_cidrs": ["10.0.0.0/8", "192.168.1.1"],
            "deny_cidrs": ["192.168.1.1/32"],
            "client_ip_source": "x_forwarded_for"
        });
        let res = validate_ip_filter_config("ip_filter_contradict", &val);
        assert!(res.is_err());
        assert!(
            res.unwrap_err()
                .to_string()
                .contains("contradictory configuration")
        );
    }

    #[test]
    fn test_cross_list_shadowing_detection() {
        // In allow mode: allow_cidr '10.99.0.0/16' is completely shadowed by broader deny_cidr '10.0.0.0/8'
        let shadowed_allow = json!({
            "mode": "allow",
            "allow_cidrs": ["10.99.0.0/16"],
            "deny_cidrs": ["10.0.0.0/8"],
            "client_ip_source": "x_forwarded_for"
        });
        let res1 = validate_ip_filter_config("ip_filter_shadow_allow", &shadowed_allow);
        assert!(res1.is_err());
        assert!(res1.unwrap_err().to_string().contains("shadowed rule"));

        // In deny mode: deny_cidr '203.0.113.5/32' is completely shadowed by broader allow_cidr '203.0.113.0/24'
        let shadowed_deny = json!({
            "mode": "deny",
            "allow_cidrs": ["203.0.113.0/24"],
            "deny_cidrs": ["203.0.113.5/32"],
            "client_ip_source": "x_real_ip"
        });
        let res2 = validate_ip_filter_config("ip_filter_shadow_deny", &shadowed_deny);
        assert!(res2.is_err());
        assert!(res2.unwrap_err().to_string().contains("shadowed rule"));
    }

    #[test]
    fn test_intra_list_subsumption_and_ipv6_tunnel_canonicalization() {
        let val = json!({
            "mode": "allow",
            "allow_cidrs": ["10.0.0.0/8", "10.1.2.0/24", "10.0.0.5"],
            "deny_cidrs": ["64:ff9b::10.99.0.0/112", "::ffff:0:10.99.1.5"],
            "client_ip_source": "x_forwarded_for"
        });
        let cfg = validate_ip_filter_config("ip_filter_compact", &val).expect("valid config");
        // 10.1.2.0/24 and 10.0.0.5/32 are subsumed by 10.0.0.0/8
        assert_eq!(cfg.allow_cidrs, vec!["10.0.0.0/8"]);
        // 64:ff9b::10.99.0.0/112 canonicalizes to 10.99.0.0/16, which subsumes ::ffff:0:10.99.1.5 (10.99.1.5/32)
        assert_eq!(cfg.deny_cidrs, vec!["10.99.0.0/16"]);
    }
}
