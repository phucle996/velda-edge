//! CORS Response Headers Configuration Schema & Semantic Validation (`phase: "post_response"`).
//!
//! Enforces a strict declarative contract for Cross-Origin Resource Sharing response headers.
//!
//! # Strict Declarative Contract (Zero Silent Fallbacks)
//! - **Explicit Origins**: `allow_origins` must be non-empty.
//! - **Security Invariant (RFC 6454 / W3C Fetch Spec)**: If `allow_credentials` is `true`, wildcard origin `"*"` and `"null"` are strictly forbidden.
//! - **Origin Grammar**: Absolute origins must start with `http://` or `https://` and contain no path (`/`), query (`?`), fragment (`#`), or userinfo (`@`).
//! - **Expose Headers Restriction**: If `allow_credentials` is `true`, wildcard `"*"` in `expose_headers` is forbidden by W3C Fetch specification.
//! - **Preflight Cache Bounds**: `max_age_secs`, if specified, must not exceed 86,400 seconds (24 hours).

use serde::{Deserialize, Serialize};

use crate::SyncError;

/// Declarative configuration for the CORS Response plugin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CorsResponseConfig {
    /// Permitted origins (e.g. `["https://app.velda.io"]` or `["*"]`).
    pub allow_origins: Vec<String>,
    /// Headers exposed to browser JavaScript (`Access-Control-Expose-Headers`).
    pub expose_headers: Vec<String>,
    /// Whether downstream requests can include credentials.
    pub allow_credentials: bool,
    /// Preflight cache duration in seconds (`Access-Control-Max-Age`), or null.
    pub max_age_secs: Option<u64>,
}

/// Validates that an origin pattern conforms to RFC 6454 and security standards.
fn validate_origin_pattern(
    pattern: &str,
    allow_credentials: bool,
    plugin_id: &str,
) -> Result<(), SyncError> {
    let trimmed = pattern.trim();
    if trimmed.is_empty() {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!("Plugin '{plugin_id}': origin pattern cannot be empty"),
        });
    }

    // 1. Wildcard origin '*'
    if trimmed == "*" {
        if allow_credentials {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': allow_credentials cannot be true when allow_origins contains '*' (RFC 6454 / Fetch Spec violation)"
                ),
            });
        }
        return Ok(());
    }

    // 2. Serialized 'null' origin
    if trimmed.eq_ignore_ascii_case("null") {
        if allow_credentials {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': allow_credentials cannot be true when allow_origins contains 'null' (security risk: credential exposure to sandboxed contexts)"
                ),
            });
        }
        return Ok(());
    }

    // 3. Absolute URI scheme: must be http:// or https://
    let after_scheme = if let Some(rest) = trimmed.strip_prefix("https://") {
        rest
    } else if let Some(rest) = trimmed.strip_prefix("http://") {
        rest
    } else {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': origin '{trimmed}' must start with 'http://' or 'https://'"
            ),
        });
    };

    if after_scheme.is_empty() {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!("Plugin '{plugin_id}': origin '{trimmed}' has empty authority"),
        });
    }

    // 4. Must not contain path '/', userinfo '@', query '?', or fragment '#'
    if after_scheme.contains('/')
        || after_scheme.contains('@')
        || after_scheme.contains('?')
        || after_scheme.contains('#')
    {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': origin '{trimmed}' must not contain path, userinfo, query, or fragment"
            ),
        });
    }

    // 5. Wildcard validation
    if after_scheme.contains('*') {
        if !after_scheme.starts_with("*.") {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': wildcard origin '{trimmed}' must use '*.domain' syntax immediately after scheme (e.g. 'https://*.example.com')"
                ),
            });
        }
        let host_rest = &after_scheme[2..];
        if host_rest.is_empty() || host_rest.contains('*') || host_rest.starts_with('.') {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': invalid wildcard origin pattern '{trimmed}'"
                ),
            });
        }
    }

    Ok(())
}

/// Helper to validate RFC 9110 header field name grammar: 1*tchar.
fn is_valid_header_name(h: &str) -> bool {
    let trimmed = h.trim();
    if trimmed == "*" {
        return true;
    }
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

/// Validates CORS Response configuration strictly according to production standards.
pub fn validate_cors_response_config(
    plugin_id: &str,
    config_val: &serde_json::Value,
) -> Result<CorsResponseConfig, SyncError> {
    let cfg: CorsResponseConfig =
        serde_json::from_value(config_val.clone()).map_err(|e| SyncError::Validation {
            domain: "plugins".into(),
            reason: format!("Plugin '{plugin_id}' has malformed or incomplete cors config: {e}"),
        })?;

    // 1. Validate Origins
    if cfg.allow_origins.is_empty() {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!("Plugin '{plugin_id}': allow_origins cannot be empty"),
        });
    }

    for origin in &cfg.allow_origins {
        validate_origin_pattern(origin, cfg.allow_credentials, plugin_id)?;
    }

    // 2. Validate Expose Headers
    for h in &cfg.expose_headers {
        if !is_valid_header_name(h) {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': invalid custom header token '{h}' in expose_headers"
                ),
            });
        }
        if cfg.allow_credentials && h.trim() == "*" {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': expose_headers cannot contain '*' when allow_credentials is true (W3C Fetch Spec violation)"
                ),
            });
        }
    }

    // 3. Validate Max Age
    if let Some(secs) = cfg.max_age_secs
        && secs > 86400
    {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': max_age_secs ({secs}) exceeds 86,400 seconds (24 hours)"
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
    fn test_valid_cors_response() {
        let val = json!({
            "allow_origins": ["https://app.velda.io", "https://*.velda.io"],
            "allow_credentials": true,
            "expose_headers": ["x-request-id", "x-total-count"],
            "max_age_secs": 3600
        });
        let res = validate_cors_response_config("cors_resp_1", &val);
        assert!(res.is_ok());
    }

    #[test]
    fn test_wildcard_with_credentials_rejected() {
        let val = json!({
            "allow_origins": ["*"],
            "allow_credentials": true,
            "expose_headers": [],
            "max_age_secs": null
        });
        let res = validate_cors_response_config("cors_bad_sec", &val);
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("RFC 6454"));
    }

    #[test]
    fn test_expose_wildcard_with_credentials_rejected() {
        let val = json!({
            "allow_origins": ["https://app.velda.io"],
            "allow_credentials": true,
            "expose_headers": ["*"],
            "max_age_secs": null
        });
        let res = validate_cors_response_config("cors_bad_expose", &val);
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("expose_headers"));
    }

    #[test]
    fn test_origin_with_userinfo_rejected() {
        let val = json!({
            "allow_origins": ["https://admin@app.velda.io"],
            "allow_credentials": false,
            "expose_headers": [],
            "max_age_secs": null
        });
        let res = validate_cors_response_config("cors_bad_userinfo", &val);
        assert!(res.is_err());
        assert!(
            res.unwrap_err()
                .to_string()
                .contains("must not contain path, userinfo")
        );
    }

    #[test]
    fn test_missing_required_fields_rejected() {
        let val = json!({
            "allow_origins": ["https://app.velda.io"]
        });
        let res = validate_cors_response_config("cors_missing", &val);
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("missing field"));
    }
}
