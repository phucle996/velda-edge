//! WAF Lite Configuration Schema & Semantic Validation (`phase: "pre_route"`).
//!
//! Enforces strict declarative contract for the Web Application Firewall Lite plugin.
//!
//! # Strict Declarative Contract (Zero Silent Fallbacks)
//! - **Explicit Mode**: `mode` must be explicitly declared as `"block"` or `"detect"`.
//! - **Valid Scope**: At least one inspection vector must be active (`check_path`, `check_query`, `check_user_agent`, or `check_headers`).
//! - **Valid Header Names**: Every header in `check_headers` must conform to RFC 9110 token grammar.
//! - **Non-Empty Custom Patterns**: Custom attack and user-agent patterns cannot be empty or whitespace.

use serde::{Deserialize, Serialize};

use crate::SyncError;

/// Declarative configuration for the WAF Lite plugin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WafLiteConfig {
    /// Operational mode: "block" (rejects with 403) or "detect" (audit header injection only).
    pub mode: String,
    /// Whether to inspect request URI path for attack signatures (SQLi, path traversal, XSS).
    pub check_path: bool,
    /// Whether to inspect request query string parameters for attack signatures.
    pub check_query: bool,
    /// Whether to inspect User-Agent header against known scanners/malicious scrapers.
    pub check_user_agent: bool,
    /// List of specific request header names to inspect for malicious injection payloads.
    pub check_headers: Vec<String>,
    /// Additional custom attack pattern substrings to match and block.
    pub custom_blocked_patterns: Vec<String>,
    /// Additional custom user-agent pattern substrings to match and block.
    pub custom_blocked_user_agents: Vec<String>,
}

/// Helper to validate RFC 9110 header token grammar.
fn is_valid_header_name(h: &str) -> bool {
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

/// Validates WAF Lite configuration strictly according to production standards.
pub fn validate_waf_lite_config(
    plugin_id: &str,
    config_val: &serde_json::Value,
) -> Result<WafLiteConfig, SyncError> {
    let cfg: WafLiteConfig =
        serde_json::from_value(config_val.clone()).map_err(|e| SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}' has malformed or incomplete waf_lite config: {e}"
            ),
        })?;

    // 1. Validate Mode
    let mode_lower = cfg.mode.trim().to_ascii_lowercase();
    match mode_lower.as_str() {
        "block" | "detect" => {}
        other => {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': invalid mode '{other}' (expected 'block' or 'detect')"
                ),
            });
        }
    }

    // 2. Validate Active Scope (at least one check must be enabled)
    let has_any_check =
        cfg.check_path || cfg.check_query || cfg.check_user_agent || !cfg.check_headers.is_empty();

    if !has_any_check {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': all inspection vectors are disabled (must enable at least one of check_path, check_query, check_user_agent, or check_headers)"
            ),
        });
    }

    // 3. Validate Header Names in check_headers
    for header in &cfg.check_headers {
        if !is_valid_header_name(header) {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': invalid HTTP header name '{header}' in check_headers"
                ),
            });
        }
    }

    // 4. Validate Custom Blocked Patterns
    for pattern in &cfg.custom_blocked_patterns {
        let trimmed = pattern.trim();
        if trimmed.is_empty() {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': custom_blocked_patterns cannot contain empty strings"
                ),
            });
        }
        if trimmed.len() > 256 {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': custom pattern '{trimmed}' exceeds maximum length of 256 characters"
                ),
            });
        }
    }

    // 5. Validate Custom Blocked User Agents
    for ua in &cfg.custom_blocked_user_agents {
        let trimmed = ua.trim();
        if trimmed.is_empty() {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': custom_blocked_user_agents cannot contain empty strings"
                ),
            });
        }
        if trimmed.len() > 256 {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': custom user agent '{trimmed}' exceeds maximum length of 256 characters"
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
    fn test_valid_waf_lite_config() {
        let val = json!({
            "mode": "block",
            "check_path": true,
            "check_query": true,
            "check_user_agent": true,
            "check_headers": ["X-Forwarded-Host", "Referer"],
            "custom_blocked_patterns": ["etc/shadow", "eval("],
            "custom_blocked_user_agents": ["evil-bot", "sqlmap-probe"]
        });
        let res = validate_waf_lite_config("waf_1", &val);
        assert!(res.is_ok());
    }

    #[test]
    fn test_invalid_mode_rejected() {
        let val = json!({
            "mode": "quarantine",
            "check_path": true,
            "check_query": false,
            "check_user_agent": false,
            "check_headers": [],
            "custom_blocked_patterns": [],
            "custom_blocked_user_agents": []
        });
        let res = validate_waf_lite_config("waf_bad_mode", &val);
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("invalid mode"));
    }

    #[test]
    fn test_all_checks_disabled_rejected() {
        let val = json!({
            "mode": "block",
            "check_path": false,
            "check_query": false,
            "check_user_agent": false,
            "check_headers": [],
            "custom_blocked_patterns": [],
            "custom_blocked_user_agents": []
        });
        let res = validate_waf_lite_config("waf_all_disabled", &val);
        assert!(res.is_err());
        assert!(
            res.unwrap_err()
                .to_string()
                .contains("all inspection vectors are disabled")
        );
    }

    #[test]
    fn test_invalid_header_in_check_headers_rejected() {
        let val = json!({
            "mode": "block",
            "check_path": true,
            "check_query": false,
            "check_user_agent": false,
            "check_headers": ["Bad Header: Name"],
            "custom_blocked_patterns": [],
            "custom_blocked_user_agents": []
        });
        let res = validate_waf_lite_config("waf_bad_header", &val);
        assert!(res.is_err());
        assert!(
            res.unwrap_err()
                .to_string()
                .contains("invalid HTTP header name")
        );
    }

    #[test]
    fn test_missing_required_fields_rejected() {
        let val = json!({
            "mode": "block"
            // Missing check_path, check_query, check_user_agent, check_headers, etc.
        });
        let res = validate_waf_lite_config("waf_missing", &val);
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("missing field"));
    }
}
