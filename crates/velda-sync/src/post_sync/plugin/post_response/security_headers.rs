//! Security Headers Configuration Schema & Semantic Validation (`phase: "post_response"`).
//!
//! Enforces a strict declarative contract for browser defensive response headers.
//!
//! # Strict Declarative Contract (Zero Silent Fallbacks)
//! - **Explicit Features**: All sub-blocks and headers must be explicitly declared.
//! - **Valid Frame Options**: `frame_options` must be `"DENY"`, `"SAMEORIGIN"`, `"ALLOW-FROM"`, or `"none"`.
//! - **Valid Referrer Policy**: `referrer_policy` must be a valid standard referrer policy or `"none"`.

use serde::{Deserialize, Serialize};

use crate::SyncError;

/// Declarative HSTS (HTTP Strict Transport Security) configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HstsConfig {
    /// Whether HSTS is enabled.
    pub enable: bool,
    /// Cache max-age duration in seconds (`Strict-Transport-Security: max-age=...`).
    pub max_age_secs: u64,
    /// Whether to apply HSTS to all subdomains (`includeSubDomains`).
    pub include_subdomains: bool,
    /// Whether to permit inclusion in browser preload lists (`preload`).
    pub preload: bool,
}

/// Declarative configuration for the Security Headers plugin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecurityHeadersConfig {
    /// HSTS configuration block.
    pub hsts: HstsConfig,
    /// X-Frame-Options value: "DENY", "SAMEORIGIN", "ALLOW-FROM", or "none".
    pub frame_options: String,
    /// Whether to emit `X-Content-Type-Options: nosniff`.
    pub content_type_nosniff: bool,
    /// Referrer-Policy header value (e.g. "strict-origin-when-cross-origin", "no-referrer", or "none").
    pub referrer_policy: String,
    /// Optional Content-Security-Policy header value.
    pub content_security_policy: Option<String>,
    /// Optional Permissions-Policy header value.
    pub permissions_policy: Option<String>,
}

/// Validates Security Headers configuration strictly according to production standards.
pub fn validate_security_headers_config(
    plugin_id: &str,
    config_val: &serde_json::Value,
) -> Result<SecurityHeadersConfig, SyncError> {
    let cfg: SecurityHeadersConfig =
        serde_json::from_value(config_val.clone()).map_err(|e| SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}' has malformed or incomplete security_headers config: {e}"
            ),
        })?;

    // 1. Validate HSTS
    if cfg.hsts.enable && cfg.hsts.max_age_secs == 0 {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!("Plugin '{plugin_id}': hsts is enabled but max_age_secs is 0"),
        });
    }

    // 2. Validate frame_options
    let fo_upper = cfg.frame_options.trim().to_ascii_uppercase();
    match fo_upper.as_str() {
        "DENY" | "SAMEORIGIN" | "ALLOW-FROM" | "NONE" => {}
        other => {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': invalid frame_options '{other}' (expected 'DENY', 'SAMEORIGIN', 'ALLOW-FROM', or 'none')"
                ),
            });
        }
    }

    // 3. Validate referrer_policy
    let rp_lower = cfg.referrer_policy.trim().to_ascii_lowercase();
    match rp_lower.as_str() {
        "no-referrer"
        | "no-referrer-when-downgrade"
        | "origin"
        | "origin-when-cross-origin"
        | "same-origin"
        | "strict-origin"
        | "strict-origin-when-cross-origin"
        | "unsafe-url"
        | "none" => {}
        other => {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!("Plugin '{plugin_id}': invalid referrer_policy '{other}'"),
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
    fn test_valid_security_headers() {
        let val = json!({
            "hsts": {
                "enable": true,
                "max_age_secs": 31536000,
                "include_subdomains": true,
                "preload": false
            },
            "frame_options": "DENY",
            "content_type_nosniff": true,
            "referrer_policy": "strict-origin-when-cross-origin",
            "content_security_policy": null,
            "permissions_policy": null
        });
        let res = validate_security_headers_config("sec_1", &val);
        assert!(res.is_ok());
    }

    #[test]
    fn test_hsts_enabled_with_zero_max_age_rejected() {
        let val = json!({
            "hsts": {
                "enable": true,
                "max_age_secs": 0,
                "include_subdomains": false,
                "preload": false
            },
            "frame_options": "DENY",
            "content_type_nosniff": true,
            "referrer_policy": "no-referrer",
            "content_security_policy": null,
            "permissions_policy": null
        });
        let res = validate_security_headers_config("sec_bad_hsts", &val);
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("max_age_secs is 0"));
    }

    #[test]
    fn test_invalid_frame_options_rejected() {
        let val = json!({
            "hsts": {
                "enable": false,
                "max_age_secs": 0,
                "include_subdomains": false,
                "preload": false
            },
            "frame_options": "INVALID_FRAME_VALUE",
            "content_type_nosniff": true,
            "referrer_policy": "no-referrer",
            "content_security_policy": null,
            "permissions_policy": null
        });
        let res = validate_security_headers_config("sec_bad_frame", &val);
        assert!(res.is_err());
        assert!(
            res.unwrap_err()
                .to_string()
                .contains("invalid frame_options")
        );
    }

    #[test]
    fn test_missing_required_fields_rejected() {
        let val = json!({
            "frame_options": "DENY"
            // Missing hsts, content_type_nosniff, referrer_policy
        });
        let res = validate_security_headers_config("sec_missing", &val);
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("missing field"));
    }
}
