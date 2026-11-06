//! Security Headers Configuration Schema & Semantic Validation (`phase: "post_response"`).
//!
//! Enforces a strict declarative contract for browser defensive response headers,
//! incorporating modern OWASP and W3C origin isolation security policies.
//!
//! # Strict Declarative Contract (Zero Silent Fallbacks)
//! - **100% Explicit Declaration**: All 11 security header directives must be explicitly declared
//!   in the configuration JSON. Omissions of any key trigger a validation error.
//! - **Explicit 'none' / null Sentinel**: To intentionally omit or disable a header, declare `"none"`
//!   or `null` explicitly.
//! - **Valid Frame Options**: `frame_options` must be `"DENY"`, `"SAMEORIGIN"`, `"ALLOW-FROM"`, or `"none"`.
//! - **Valid Referrer Policy**: `referrer_policy` must be a valid standard referrer policy or `"none"`.
//! - **Modern Isolation Policies**: Explicit W3C COOP, COEP, CORP, cross-domain, and XSS directives.

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

impl Default for HstsConfig {
    fn default() -> Self {
        Self {
            enable: true,
            max_age_secs: 31_536_000, // 1 year standard
            include_subdomains: true,
            preload: false,
        }
    }
}

/// Declarative configuration for the Security Headers plugin.
///
/// Every single field must be explicitly provided in the manifest JSON.
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
    /// Explicit Content-Security-Policy header value (or "none" / null to omit).
    pub content_security_policy: Option<String>,
    /// Explicit Permissions-Policy header value (or "none" / null to omit).
    pub permissions_policy: Option<String>,
    /// Explicit Cross-Origin-Opener-Policy (COOP): "same-origin", "same-origin-allow-popups", "unsafe-none", or "none" / null.
    pub cross_origin_opener_policy: Option<String>,
    /// Explicit Cross-Origin-Embedder-Policy (COEP): "require-corp", "credentialless", "unsafe-none", or "none" / null.
    pub cross_origin_embedder_policy: Option<String>,
    /// Explicit Cross-Origin-Resource-Policy (CORP): "same-origin", "same-site", "cross-origin", or "none" / null.
    pub cross_origin_resource_policy: Option<String>,
    /// Explicit X-Permitted-Cross-Domain-Policies: "none", "master-only", "by-content-type", "all", or null.
    pub cross_domain_policies: Option<String>,
    /// Explicit X-XSS-Protection header value (e.g. "0", "1", "1; mode=block", or "none" / null).
    pub xss_protection: Option<String>,
}

/// Required top-level keys that must be explicitly present in `plugins.json` for security_headers.
const REQUIRED_HEADER_KEYS: &[&str] = &[
    "hsts",
    "frame_options",
    "content_type_nosniff",
    "referrer_policy",
    "content_security_policy",
    "permissions_policy",
    "cross_origin_opener_policy",
    "cross_origin_embedder_policy",
    "cross_origin_resource_policy",
    "cross_domain_policies",
    "xss_protection",
];

/// Validates Security Headers configuration strictly according to production standards.
///
/// Enforces 100% explicit configuration: all 11 security header keys must exist in `config_val`.
pub fn validate_security_headers_config(
    plugin_id: &str,
    config_val: &serde_json::Value,
) -> Result<SecurityHeadersConfig, SyncError> {
    let obj = config_val
        .as_object()
        .ok_or_else(|| SyncError::Validation {
            domain: "plugins".into(),
            reason: format!("Plugin '{plugin_id}': security_headers config must be a JSON object"),
        })?;

    // 0. Enforce explicit presence of all 11 header keys
    for &key in REQUIRED_HEADER_KEYS {
        if !obj.contains_key(key) {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': missing required explicit configuration key '{key}'. All 11 security headers must be explicitly declared (declare 'none' or null to omit)"
                ),
            });
        }
    }

    let cfg: SecurityHeadersConfig =
        serde_json::from_value(config_val.clone()).map_err(|e| SyncError::Validation {
            domain: "plugins".into(),
            reason: format!("Plugin '{plugin_id}' has malformed security_headers config: {e}"),
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

    // 4. Validate Cross-Origin-Opener-Policy (COOP) if specified
    if let Some(ref coop) = cfg.cross_origin_opener_policy {
        let coop_lower = coop.trim().to_ascii_lowercase();
        match coop_lower.as_str() {
            "same-origin" | "same-origin-allow-popups" | "unsafe-none" | "none" => {}
            other => {
                return Err(SyncError::Validation {
                    domain: "plugins".into(),
                    reason: format!(
                        "Plugin '{plugin_id}': invalid cross_origin_opener_policy '{other}' (expected 'same-origin', 'same-origin-allow-popups', 'unsafe-none', or 'none')"
                    ),
                });
            }
        }
    }

    // 5. Validate Cross-Origin-Embedder-Policy (COEP) if specified
    if let Some(ref coep) = cfg.cross_origin_embedder_policy {
        let coep_lower = coep.trim().to_ascii_lowercase();
        match coep_lower.as_str() {
            "require-corp" | "credentialless" | "unsafe-none" | "none" => {}
            other => {
                return Err(SyncError::Validation {
                    domain: "plugins".into(),
                    reason: format!(
                        "Plugin '{plugin_id}': invalid cross_origin_embedder_policy '{other}' (expected 'require-corp', 'credentialless', 'unsafe-none', or 'none')"
                    ),
                });
            }
        }
    }

    // 6. Validate Cross-Origin-Resource-Policy (CORP) if specified
    if let Some(ref corp) = cfg.cross_origin_resource_policy {
        let corp_lower = corp.trim().to_ascii_lowercase();
        match corp_lower.as_str() {
            "same-origin" | "same-site" | "cross-origin" | "none" => {}
            other => {
                return Err(SyncError::Validation {
                    domain: "plugins".into(),
                    reason: format!(
                        "Plugin '{plugin_id}': invalid cross_origin_resource_policy '{other}' (expected 'same-origin', 'same-site', 'cross-origin', or 'none')"
                    ),
                });
            }
        }
    }

    // 7. Validate X-Permitted-Cross-Domain-Policies if specified
    if let Some(ref xpd) = cfg.cross_domain_policies {
        let xpd_lower = xpd.trim().to_ascii_lowercase();
        match xpd_lower.as_str() {
            "none" | "master-only" | "by-content-type" | "by-ftp-filename" | "all" => {}
            other => {
                return Err(SyncError::Validation {
                    domain: "plugins".into(),
                    reason: format!(
                        "Plugin '{plugin_id}': invalid cross_domain_policies '{other}' (expected 'none', 'master-only', 'by-content-type', 'by-ftp-filename', or 'all')"
                    ),
                });
            }
        }
    }

    // 8. Validate X-XSS-Protection if specified
    if let Some(ref xxss) = cfg.xss_protection {
        let xxss_trimmed = xxss.trim();
        match xxss_trimmed {
            "0" | "1" | "1; mode=block" | "none" | "NONE" => {}
            other => {
                return Err(SyncError::Validation {
                    domain: "plugins".into(),
                    reason: format!(
                        "Plugin '{plugin_id}': invalid xss_protection '{other}' (expected '0', '1', '1; mode=block', or 'none')"
                    ),
                });
            }
        }
    }

    Ok(cfg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_valid_explicit_security_headers() {
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
            "permissions_policy": null,
            "cross_origin_opener_policy": "same-origin",
            "cross_origin_embedder_policy": "require-corp",
            "cross_origin_resource_policy": "same-origin",
            "cross_domain_policies": "none",
            "xss_protection": "0"
        });
        let res = validate_security_headers_config("sec_1", &val);
        assert!(res.is_ok());
    }

    #[test]
    fn test_missing_modern_header_key_rejected() {
        // Missing cross_origin_opener_policy
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
            "permissions_policy": null,
            // cross_origin_opener_policy missing!
            "cross_origin_embedder_policy": "require-corp",
            "cross_origin_resource_policy": "same-origin",
            "cross_domain_policies": "none",
            "xss_protection": "0"
        });
        let res = validate_security_headers_config("sec_missing_coop", &val);
        assert!(res.is_err());
        assert!(
            res.unwrap_err().to_string().contains(
                "missing required explicit configuration key 'cross_origin_opener_policy'"
            )
        );
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
            "permissions_policy": null,
            "cross_origin_opener_policy": null,
            "cross_origin_embedder_policy": null,
            "cross_origin_resource_policy": null,
            "cross_domain_policies": null,
            "xss_protection": null
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
            "permissions_policy": null,
            "cross_origin_opener_policy": null,
            "cross_origin_embedder_policy": null,
            "cross_origin_resource_policy": null,
            "cross_domain_policies": null,
            "xss_protection": null
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
    fn test_invalid_coop_rejected() {
        let val = json!({
            "hsts": {
                "enable": false,
                "max_age_secs": 0,
                "include_subdomains": false,
                "preload": false
            },
            "frame_options": "DENY",
            "content_type_nosniff": true,
            "referrer_policy": "no-referrer",
            "content_security_policy": null,
            "permissions_policy": null,
            "cross_origin_opener_policy": "invalid-coop",
            "cross_origin_embedder_policy": null,
            "cross_origin_resource_policy": null,
            "cross_domain_policies": null,
            "xss_protection": null
        });
        let res = validate_security_headers_config("sec_bad_coop", &val);
        assert!(res.is_err());
        assert!(
            res.unwrap_err()
                .to_string()
                .contains("invalid cross_origin_opener_policy")
        );
    }

    #[test]
    fn test_missing_required_fields_rejected() {
        let val = json!({
            "frame_options": "DENY"
        });
        let res = validate_security_headers_config("sec_missing", &val);
        assert!(res.is_err());
        assert!(
            res.unwrap_err()
                .to_string()
                .contains("missing required explicit configuration key")
        );
    }
}
