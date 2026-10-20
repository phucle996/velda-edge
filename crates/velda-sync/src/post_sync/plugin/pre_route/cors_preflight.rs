//! CORS Preflight Configuration Schema & Semantic Validation (`phase: "pre_route"`).
//!
//! Enforces strict declarative contract for CORS Preflight OPTIONS interception.
//!
//! # Strict Declarative Contract (Zero Silent Fallbacks)
//! - **Explicit Origins**: `allowed_origins` must be non-empty (can include `"*"`, specific origins, or wildcard subdomains `*.example.com`).
//! - **Security Invariant**: If `allow_credentials` is `true`, wildcard origin `"*"`, is strictly forbidden (RFC 6454 / Fetch Spec violation).
//! - **Valid HTTP Methods**: Every entry in `allowed_methods` must be a valid RFC 9110 HTTP method token.
//! - **Valid HTTP Header Names**: Every entry in `allowed_headers` must conform to RFC 9110 header token grammar.

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

    let has_wildcard_origin = cfg.allowed_origins.iter().any(|o| o.trim() == "*");

    // 2. Security invariant: credentials + wildcard origin
    if cfg.allow_credentials && has_wildcard_origin {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': allow_credentials cannot be true when allowed_origins contains '*' (RFC 6454 security violation)"
            ),
        });
    }

    // 3. Validate Methods
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

    // 4. Validate Headers
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

    // 5. Validate Max-Age
    if cfg.max_age_secs > 86400 * 365 {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': max_age_secs ({}) exceeds maximum limit of 31,536,000s (1 year)",
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
    fn test_missing_required_fields_rejected() {
        let val = json!({
            "allowed_origins": ["https://example.com"]
            // Missing allowed_methods, allowed_headers, allow_credentials, max_age_secs
        });
        let res = validate_cors_preflight_config("cors_missing", &val);
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("missing field"));
    }
}
