//! Canary Traffic Splitting Configuration Schema & Semantic Validation (`phase: "pre_upstream"`).
//!
//! Enforces a strict declarative contract for canary routing, percentage rollouts,
//! multi-source hash keys, and rule-based traffic steering.
//!
//! # Strict Declarative Contract (Zero Silent Fallbacks)
//! - **Mandatory Upstream**: `canary_upstream` must be non-empty.
//! - **Valid Weight**: `weight` must be in range `0..=100`.
//! - **Zero Hardcoded Headers**: All header, cookie, and query parameter names must be
//!   explicitly defined by the user (no hardcoded defaults like `x-user-id` or `session_id`).
//! - **Valid Rules**: Header names must be valid RFC 9110 tokens; operators must be one of
//!   `equals`, `prefix`, `suffix`, `contains`, `exists`.
//! - **Flexible Hash Strategies**: `priority`, `composite`, or `random`.
//! - **Flexible Hash Sources**: `header:<name>`, `cookie:<name>`, `query:<name>`, `client_ip`, `uri_path`.
//! - **Canary Marker Header**: Valid RFC 9110 token (or `"none"` to disable marker injection).

use serde::{Deserialize, Serialize};

use crate::SyncError;

/// Declarative rule matching on request headers to override traffic to canary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanaryHeaderRuleConfig {
    /// Header field name to evaluate.
    pub name: String,
    /// Matching operator: "equals", "prefix", "suffix", "contains", or "exists".
    pub operator: String,
    /// Value to compare against (empty string when operator is "exists").
    pub value: String,
}

/// Declarative configuration for the Canary plugin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanaryConfig {
    /// Target canary upstream cluster name.
    pub canary_upstream: String,
    /// Traffic percentage directed to canary (0 to 100).
    pub weight: u32,
    /// Header rules triggering immediate canary routing.
    #[serde(default)]
    pub rules: Vec<CanaryHeaderRuleConfig>,
    /// Hashing strategy: "priority", "composite", or "random".
    #[serde(default = "default_hash_strategy")]
    pub hash_strategy: String,
    /// Ordered list of sources used for consistent session hashing:
    /// e.g. ["header:x-user-id", "cookie:session_token", "client_ip"].
    #[serde(default)]
    pub hash_sources: Vec<String>,
    /// Backwards-compatible legacy attribute: "client_ip", "random", or specific source.
    #[serde(default)]
    pub hash_by: Option<String>,
    /// Header name injected into upstream request to mark canary traffic (or "none" to disable).
    pub canary_header: String,
    /// Injected header value (or "none" if canary_header is "none").
    pub canary_header_value: String,
}

fn default_hash_strategy() -> String {
    "priority".into()
}

/// Helper to validate RFC 9110 header token grammar.
pub fn is_valid_header_name(h: &str) -> bool {
    let trimmed = h.trim();
    if trimmed.is_empty() {
        return false;
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

/// Validates Canary configuration strictly according to production standards.
pub fn validate_canary_config(
    plugin_id: &str,
    config_val: &serde_json::Value,
) -> Result<CanaryConfig, SyncError> {
    let mut cfg: CanaryConfig =
        serde_json::from_value(config_val.clone()).map_err(|e| SyncError::Validation {
            domain: "plugins".into(),
            reason: format!("Plugin '{plugin_id}' has malformed or incomplete canary config: {e}"),
        })?;

    // 1. Validate canary_upstream
    let upstream_trimmed = cfg.canary_upstream.trim();
    if upstream_trimmed.is_empty() {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!("Plugin '{plugin_id}': canary_upstream cannot be empty"),
        });
    }

    // 2. Validate weight (0..=100)
    if cfg.weight > 100 {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': weight ({}) must be between 0 and 100",
                cfg.weight
            ),
        });
    }

    // 3. Validate Header Rules
    for (idx, rule) in cfg.rules.iter().enumerate() {
        if !is_valid_header_name(&rule.name) {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': rule at index {idx} has invalid header name '{}'",
                    rule.name
                ),
            });
        }

        let op_lower = rule.operator.trim().to_ascii_lowercase();
        match op_lower.as_str() {
            "equals" | "prefix" | "suffix" | "contains" => {
                if rule.value.is_empty() {
                    return Err(SyncError::Validation {
                        domain: "plugins".into(),
                        reason: format!(
                            "Plugin '{plugin_id}': rule at index {idx} with operator '{}' requires non-empty value",
                            rule.operator
                        ),
                    });
                }
            }
            "exists" => {}
            other => {
                return Err(SyncError::Validation {
                    domain: "plugins".into(),
                    reason: format!(
                        "Plugin '{plugin_id}': rule at index {idx} has invalid operator '{other}' (expected 'equals', 'prefix', 'suffix', 'contains', or 'exists')"
                    ),
                });
            }
        }
    }

    // 4. Validate and Normalize Hashing Strategy & Sources
    // Backwards compatibility normalization from legacy `hash_by`
    if cfg.hash_sources.is_empty()
        && let Some(ref legacy) = cfg.hash_by
    {
        let legacy_clean = legacy.trim().to_ascii_lowercase();
        match legacy_clean.as_str() {
            "client_ip" | "ip" => {
                cfg.hash_strategy = "priority".into();
                cfg.hash_sources = vec!["client_ip".into()];
            }
            "random" => {
                cfg.hash_strategy = "random".into();
            }
            "uri_path" | "path" => {
                cfg.hash_strategy = "priority".into();
                cfg.hash_sources = vec!["uri_path".into()];
            }
            s if s.starts_with("header:")
                || s.starts_with("cookie:")
                || s.starts_with("query:") =>
            {
                cfg.hash_strategy = "priority".into();
                cfg.hash_sources = vec![s.to_string()];
            }
            "header" | "cookie" | "query" => {
                return Err(SyncError::Validation {
                    domain: "plugins".into(),
                    reason: format!(
                        "Plugin '{plugin_id}': '{legacy_clean}' requires an explicit target name (e.g. 'header:x-user-id', 'cookie:session_id', 'query:tenant_id'). Bare '{legacy_clean}' without a name is forbidden to prevent unconfigured defaults."
                    ),
                });
            }
            other => {
                return Err(SyncError::Validation {
                    domain: "plugins".into(),
                    reason: format!(
                        "Plugin '{plugin_id}': invalid hash_by '{other}'. Supported values: 'random', 'client_ip', 'uri_path', 'header:<name>', 'cookie:<name>', 'query:<name>'."
                    ),
                });
            }
        }
    }

    let strat_lower = cfg.hash_strategy.trim().to_ascii_lowercase();
    match strat_lower.as_str() {
        "priority" | "composite" => {
            if cfg.hash_sources.is_empty() {
                return Err(SyncError::Validation {
                    domain: "plugins".into(),
                    reason: format!(
                        "Plugin '{plugin_id}': hash_strategy '{strat_lower}' requires at least one entry in 'hash_sources' (e.g. ['header:x-user-id', 'client_ip'])"
                    ),
                });
            }

            for (idx, src) in cfg.hash_sources.iter().enumerate() {
                let src_trim = src.trim();
                let src_lower = src_trim.to_ascii_lowercase();
                if src_lower == "client_ip" || src_lower == "ip" {
                    continue;
                }
                if src_lower == "uri_path" || src_lower == "path" {
                    continue;
                }
                if let Some(hdr) = src_trim.strip_prefix("header:") {
                    let hdr_name = hdr.trim();
                    if !is_valid_header_name(hdr_name) {
                        return Err(SyncError::Validation {
                            domain: "plugins".into(),
                            reason: format!(
                                "Plugin '{plugin_id}': hash_source at index {idx} has invalid header name '{hdr_name}'"
                            ),
                        });
                    }
                } else if let Some(cookie) = src_trim.strip_prefix("cookie:") {
                    let cookie_name = cookie.trim();
                    if cookie_name.is_empty()
                        || !cookie_name.bytes().all(|b| {
                            b.is_ascii_graphic() && !matches!(b, b';' | b',' | b'=' | b' ' | b'\t')
                        })
                    {
                        return Err(SyncError::Validation {
                            domain: "plugins".into(),
                            reason: format!(
                                "Plugin '{plugin_id}': hash_source at index {idx} has invalid cookie name '{cookie_name}'"
                            ),
                        });
                    }
                } else if let Some(query) = src_trim.strip_prefix("query:") {
                    let query_name = query.trim();
                    if query_name.is_empty()
                        || !query_name.bytes().all(|b| {
                            b.is_ascii_graphic()
                                && !matches!(b, b'&' | b'=' | b'?' | b'#' | b' ' | b'\t')
                        })
                    {
                        return Err(SyncError::Validation {
                            domain: "plugins".into(),
                            reason: format!(
                                "Plugin '{plugin_id}': hash_source at index {idx} has invalid query parameter name '{query_name}'"
                            ),
                        });
                    }
                } else if src_lower == "header" || src_lower == "cookie" || src_lower == "query" {
                    return Err(SyncError::Validation {
                        domain: "plugins".into(),
                        reason: format!(
                            "Plugin '{plugin_id}': hash_source at index {idx} ('{src_trim}') must specify a target name (e.g. '{src_lower}:<name>'). Bare '{src_lower}' is forbidden to avoid hardcoded defaults."
                        ),
                    });
                } else {
                    return Err(SyncError::Validation {
                        domain: "plugins".into(),
                        reason: format!(
                            "Plugin '{plugin_id}': unsupported hash_source '{src_trim}' at index {idx}. Expected 'header:<name>', 'cookie:<name>', 'query:<name>', 'client_ip', or 'uri_path'."
                        ),
                    });
                }
            }
        }
        "random" => {
            // Random does not require hash_sources
        }
        other => {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': invalid hash_strategy '{other}'. Allowed values: 'priority', 'composite', 'random'."
                ),
            });
        }
    }

    // 5. Validate canary_header and canary_header_value
    let header_trim = cfg.canary_header.trim();
    if header_trim.eq_ignore_ascii_case("none") {
        // Disabled header injection sentinel
    } else {
        if !is_valid_header_name(header_trim) {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': invalid canary_header name '{header_trim}' (must be valid RFC 9110 token or 'none')"
                ),
            });
        }

        let val_trim = cfg.canary_header_value.trim();
        if val_trim.is_empty() {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': canary_header_value cannot be empty when canary_header is active"
                ),
            });
        }
        if val_trim.bytes().any(|b| !(0x20..=0x7E).contains(&b)) {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': canary_header_value contains invalid non-printable or control characters"
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
    fn test_valid_canary_config_multi_sources() {
        let val = json!({
            "canary_upstream": "orders-v2-canary",
            "weight": 25,
            "rules": [
                { "name": "X-Beta-Tester", "operator": "equals", "value": "true" },
                { "name": "User-Agent", "operator": "prefix", "value": "InternalSmoke" },
                { "name": "X-User-Email", "operator": "suffix", "value": "@velda.io" },
                { "name": "X-Tags", "operator": "contains", "value": "staging" },
                { "name": "X-Debug-Session", "operator": "exists", "value": "" }
            ],
            "hash_strategy": "priority",
            "hash_sources": [
                "header:x-user-id",
                "cookie:session_token",
                "query:tenant_id",
                "client_ip"
            ],
            "canary_header": "x-canary-route",
            "canary_header_value": "true"
        });
        let res = validate_canary_config("canary_1", &val);
        assert!(res.is_ok());
    }

    #[test]
    fn test_composite_hash_strategy() {
        let val = json!({
            "canary_upstream": "orders-v2-canary",
            "weight": 50,
            "hash_strategy": "composite",
            "hash_sources": [
                "header:x-tenant-id",
                "client_ip"
            ],
            "canary_header": "none",
            "canary_header_value": "none"
        });
        let res = validate_canary_config("canary_composite", &val);
        assert!(res.is_ok());
    }

    #[test]
    fn test_bare_header_rejected_to_prevent_hardcoded_defaults() {
        let val = json!({
            "canary_upstream": "upstream-b",
            "weight": 20,
            "hash_sources": ["header"],
            "canary_header": "x-canary",
            "canary_header_value": "true"
        });
        let res = validate_canary_config("canary_bare", &val);
        assert!(res.is_err());
        assert!(
            res.unwrap_err()
                .to_string()
                .contains("specify a target name")
        );
    }

    #[test]
    fn test_empty_canary_upstream_rejected() {
        let val = json!({
            "canary_upstream": "",
            "weight": 25,
            "hash_strategy": "random",
            "canary_header": "x-canary-route",
            "canary_header_value": "true"
        });
        let res = validate_canary_config("canary_empty_up", &val);
        assert!(res.is_err());
        assert!(
            res.unwrap_err()
                .to_string()
                .contains("canary_upstream cannot be empty")
        );
    }

    #[test]
    fn test_invalid_weight_rejected() {
        let val = json!({
            "canary_upstream": "upstream-b",
            "weight": 105,
            "hash_strategy": "random",
            "canary_header": "x-canary-route",
            "canary_header_value": "true"
        });
        let res = validate_canary_config("canary_bad_weight", &val);
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("between 0 and 100"));
    }

    #[test]
    fn test_control_character_in_header_value_rejected() {
        let val = json!({
            "canary_upstream": "upstream-b",
            "weight": 50,
            "hash_strategy": "random",
            "canary_header": "x-canary",
            "canary_header_value": "line1\r\ninjected: evil"
        });
        let res = validate_canary_config("canary_crlf", &val);
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("control characters"));
    }
}
