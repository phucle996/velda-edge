//! CORS Preflight Configuration Schema & Semantic Validation (`phase: "pre_route"`).
//!
//! Enforces a strict declarative contract for CORS Preflight OPTIONS interception.
//!
//! # Strict Declarative Contract (Zero Silent Fallbacks)
//! - **Explicit Origins**: `allowed_origins` must be non-empty (can include `"*"`, `"null"`, specific origins, or wildcard subdomains `https://*.example.com`).
//! - **Security Invariant (RFC 6454 / W3C Fetch Spec)**: If `allow_credentials` is `true`, wildcard origin `"*"` and `"null"` are strictly forbidden to prevent credential exposure to arbitrary or sandboxed contexts.
//! - **Origin Grammar**: Absolute origins must start with `http://` or `https://` and contain no path (`/`), query (`?`), fragment (`#`), or userinfo (`@`).
//! - **Wildcard Subdomains**: Wildcards are restricted to subdomain prefixes (e.g. `https://*.example.com`). Arbitrary wildcard placements (e.g. `https://*.com` or `https://prefix*`) are rejected.
//! - **Valid HTTP Methods**: Every entry in `allowed_methods` must be a valid RFC 9110 HTTP method token.
//! - **Valid HTTP Header Names**: Every entry in `allowed_headers` must conform to RFC 9110 header token grammar.
//! - **Max Age Bounds**: `max_age_secs` must not exceed 86,400 seconds (24 hours).

use serde::{Deserialize, Serialize};

use crate::SyncError;

/// Production-grade declarative configuration for the CORS Preflight plugin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CorsPreflightConfig {
    /// Allowed origin patterns (e.g. `["https://app.example.com", "https://*.corp.internal"]`).
    pub allowed_origins: Vec<String>,
    /// Allowed HTTP request methods (e.g. `["GET", "POST", "PUT", "DELETE", "OPTIONS"]`).
    pub allowed_methods: Vec<String>,
    /// Allowed request headers (e.g. `["Content-Type", "Authorization", "X-Custom-Header"]` or `["*"]`).
    pub allowed_headers: Vec<String>,
    /// Whether downstream requests can include user credentials (cookies, HTTP auth).
    pub allow_credentials: bool,
    /// Preflight cache duration in seconds (`Access-Control-Max-Age`).
    pub max_age_secs: u64,
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
                    "Plugin '{plugin_id}': allow_credentials cannot be true when allowed_origins contains '*' (RFC 6454 / Fetch Spec violation)"
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
                    "Plugin '{plugin_id}': allow_credentials cannot be true when allowed_origins contains 'null' (security risk: credential exposure to sandboxed contexts)"
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

/// Helper to check if a method token is a standard/valid HTTP method.
fn is_valid_http_method(m: &str) -> bool {
    let trimmed = m.trim();
    if trimmed.is_empty() {
        return false;
    }
    trimmed
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-')
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

/// Validates CORS Preflight configuration strictly according to production standards.
pub fn validate_cors_preflight_config(
    plugin_id: &str,
    config_val: &serde_json::Value,
) -> Result<CorsPreflightConfig, SyncError> {
    let cfg: CorsPreflightConfig =
        serde_json::from_value(config_val.clone()).map_err(|e| SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}' has malformed or incomplete cors_preflight config: {e}"
            ),
        })?;

    // 1. Validate Origins
    if cfg.allowed_origins.is_empty() {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!("Plugin '{plugin_id}': allowed_origins cannot be empty"),
        });
    }

    for origin in &cfg.allowed_origins {
        validate_origin_pattern(origin, cfg.allow_credentials, plugin_id)?;
    }

    // 2. Validate Methods
    if cfg.allowed_methods.is_empty() {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!("Plugin '{plugin_id}': allowed_methods cannot be empty"),
        });
    }
    for method in &cfg.allowed_methods {
        if !is_valid_http_method(method) {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': invalid HTTP method token '{method}' in allowed_methods"
                ),
            });
        }
    }

    // 3. Validate Headers
    for header in &cfg.allowed_headers {
        if !is_valid_header_name(header) {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': invalid HTTP header name '{header}' in allowed_headers"
                ),
            });
        }
    }

    // 4. Validate Max-Age (Bounded to 24h as modern browsers clamp preflight cache to 2-24h)
    if cfg.max_age_secs > 86400 {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': max_age_secs ({}) exceeds maximum limit of 86,400s (24 hours)",
                cfg.max_age_secs
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
    fn test_valid_cors_preflight_config() {
        let val = json!({
            "allowed_origins": ["https://app.velda.io", "https://*.internal"],
            "allowed_methods": ["GET", "POST", "OPTIONS"],
            "allowed_headers": ["Content-Type", "Authorization"],
            "allow_credentials": true,
            "max_age_secs": 3600
        });
        let res = validate_cors_preflight_config("cors_preflight_1", &val);
        assert!(res.is_ok());
    }

    #[test]
    fn test_credentials_with_wildcard_rejected() {
        let val = json!({
            "allowed_origins": ["*"],
            "allowed_methods": ["GET", "POST", "OPTIONS"],
            "allowed_headers": ["Content-Type"],
            "allow_credentials": true,
            "max_age_secs": 3600
        });
        let res = validate_cors_preflight_config("cors_preflight_bad", &val);
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("RFC 6454"));
    }

    #[test]
    fn test_credentials_with_null_rejected() {
        let val = json!({
            "allowed_origins": ["null"],
            "allowed_methods": ["GET", "POST", "OPTIONS"],
            "allowed_headers": ["Content-Type"],
            "allow_credentials": true,
            "max_age_secs": 3600
        });
        let res = validate_cors_preflight_config("cors_preflight_null", &val);
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("null"));
    }

    #[test]
    fn test_origin_with_path_rejected() {
        let val = json!({
            "allowed_origins": ["https://example.com/api"],
            "allowed_methods": ["GET", "OPTIONS"],
            "allowed_headers": ["Content-Type"],
            "allow_credentials": false,
            "max_age_secs": 3600
        });
        let res = validate_cors_preflight_config("cors_bad_path", &val);
        assert!(res.is_err());
        assert!(
            res.unwrap_err()
                .to_string()
                .contains("must not contain path")
        );
    }

    #[test]
    fn test_origin_malformed_wildcard_rejected() {
        let val = json!({
            "allowed_origins": ["https://example.*"],
            "allowed_methods": ["GET", "OPTIONS"],
            "allowed_headers": ["Content-Type"],
            "allow_credentials": false,
            "max_age_secs": 3600
        });
        let res = validate_cors_preflight_config("cors_bad_wildcard", &val);
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("wildcard origin"));
    }

    #[test]
    fn test_invalid_method_rejected() {
        let val = json!({
            "allowed_origins": ["https://example.com"],
            "allowed_methods": ["GET", "INVALID METHOD WITH SPACES"],
            "allowed_headers": ["Content-Type"],
            "allow_credentials": false,
            "max_age_secs": 3600
        });
        let res = validate_cors_preflight_config("cors_bad_method", &val);
        assert!(res.is_err());
        assert!(
            res.unwrap_err()
                .to_string()
                .contains("invalid HTTP method token")
        );
    }

    #[test]
    fn test_invalid_header_name_rejected() {
        let val = json!({
            "allowed_origins": ["https://example.com"],
            "allowed_methods": ["GET"],
            "allowed_headers": ["Content Type Bad: Header"],
            "allow_credentials": false,
            "max_age_secs": 3600
        });
        let res = validate_cors_preflight_config("cors_bad_header", &val);
        assert!(res.is_err());
        assert!(
            res.unwrap_err()
                .to_string()
                .contains("invalid HTTP header name")
        );
    }

    #[test]
    fn test_max_age_exceeded_rejected() {
        let val = json!({
            "allowed_origins": ["https://example.com"],
            "allowed_methods": ["GET"],
            "allowed_headers": ["Content-Type"],
            "allow_credentials": false,
            "max_age_secs": 100_000
        });
        let res = validate_cors_preflight_config("cors_max_age", &val);
        assert!(res.is_err());
        assert!(
            res.unwrap_err()
                .to_string()
                .contains("exceeds maximum limit")
        );
    }

    #[test]
    fn test_missing_required_fields_rejected() {
        let val = json!({
            "allowed_origins": ["https://example.com"]
        });
        let res = validate_cors_preflight_config("cors_missing", &val);
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("missing field"));
    }
}
