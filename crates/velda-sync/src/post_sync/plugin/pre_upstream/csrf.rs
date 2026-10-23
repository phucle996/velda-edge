//! CSRF Protection Schema & Semantic Validation (`phase: "pre_upstream"`).

use serde::{Deserialize, Serialize};

use crate::SyncError;

/// Declarative configuration for the CSRF protection plugin.
///
/// # Strict Invariant: 100% Explicit Declaration
/// Every parameter must be explicitly declared in the configuration JSON.
/// Zero defaults are permitted. Missing fields fail fast at configuration sync time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CsrfConfig {
    /// Name of the cookie containing the CSRF token (e.g. `__Host-velda_csrf` or `aurora_csrf`).
    pub cookie_name: String,
    /// Name of the custom request header containing the matching token (e.g. `X-CSRF-Token`).
    pub header_name: String,
    /// Optional shared secret key (minimum 16 bytes) for HMAC-SHA256 signed stateless tokens.
    /// When `None`, operates in pure Double-Submit Cookie mode.
    pub secret_key: Option<String>,
    /// Lifetime of signed tokens in seconds before expiration.
    pub token_ttl_secs: u64,
    /// HTTP methods exempt from CSRF validation (idempotent/safe methods, e.g. `["GET", "HEAD", "OPTIONS"]`).
    pub safe_methods: Vec<String>,
    /// Whitelist of external origins permitted for cross-origin state changes (e.g. `["https://app.example.com"]`).
    pub trusted_origins: Vec<String>,
    /// Whether to inspect and enforce W3C Fetch Metadata (`Sec-Fetch-Site`).
    pub enforce_fetch_metadata: bool,
    /// Whether to enforce strict Origin / Referer validation against Host or trusted_origins.
    pub enforce_origin: bool,
}

/// Helper to validate RFC 9110 header token grammar without external crate dependency.
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

/// Validates CSRF configuration strictly according to production security standards.
pub fn validate_csrf_config(
    plugin_id: &str,
    config_val: &serde_json::Value,
) -> Result<CsrfConfig, SyncError> {
    let cfg: CsrfConfig =
        serde_json::from_value(config_val.clone()).map_err(|e| SyncError::Validation {
            domain: "plugins".into(),
            reason: format!("Plugin '{plugin_id}' has malformed or incomplete csrf config: {e}"),
        })?;

    if cfg.cookie_name.trim().is_empty() {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!("Plugin '{plugin_id}': 'cookie_name' cannot be empty"),
        });
    }

    if !is_valid_header_name(&cfg.header_name) {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': 'header_name' '{}' is not a valid RFC 9110 HTTP header token",
                cfg.header_name
            ),
        });
    }

    if cfg.safe_methods.is_empty() {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!("Plugin '{plugin_id}': 'safe_methods' cannot be empty"),
        });
    }

    for method in &cfg.safe_methods {
        match method.to_ascii_uppercase().as_str() {
            "GET" | "HEAD" | "OPTIONS" | "TRACE" => {}
            other => {
                return Err(SyncError::Validation {
                    domain: "plugins".into(),
                    reason: format!(
                        "Plugin '{plugin_id}': non-idempotent method '{other}' cannot be declared in 'safe_methods'"
                    ),
                });
            }
        }
    }

    if cfg.token_ttl_secs == 0 {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!("Plugin '{plugin_id}': 'token_ttl_secs' must be greater than 0"),
        });
    }

    if cfg
        .secret_key
        .as_ref()
        .is_some_and(|secret| secret.trim().len() < 16)
    {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': 'secret_key' must be at least 16 characters for cryptographic safety"
            ),
        });
    }

    Ok(cfg)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_csrf_config() {
        let val = serde_json::json!({
            "cookie_name": "__Host-velda_csrf",
            "header_name": "X-CSRF-Token",
            "secret_key": "sec_k89f2a019482bca881726c0981273412",
            "token_ttl_secs": 7200,
            "safe_methods": ["GET", "HEAD", "OPTIONS"],
            "trusted_origins": ["https://app.example.com"],
            "enforce_fetch_metadata": true,
            "enforce_origin": true
        });
        let res = validate_csrf_config("csrf_test", &val);
        assert!(res.is_ok());
    }

    #[test]
    fn test_rejects_unsafe_methods() {
        let val = serde_json::json!({
            "cookie_name": "__Host-velda_csrf",
            "header_name": "X-CSRF-Token",
            "secret_key": null,
            "token_ttl_secs": 7200,
            "safe_methods": ["GET", "POST"], // POST is unsafe!
            "trusted_origins": [],
            "enforce_fetch_metadata": true,
            "enforce_origin": true
        });
        let res = validate_csrf_config("csrf_test", &val);
        assert!(res.is_err(), "Must reject POST in safe_methods");
    }

    #[test]
    fn test_short_secret_key_rejected() {
        let val = serde_json::json!({
            "cookie_name": "__Host-velda_csrf",
            "header_name": "X-CSRF-Token",
            "secret_key": "too_short",
            "token_ttl_secs": 7200,
            "safe_methods": ["GET", "HEAD", "OPTIONS"],
            "trusted_origins": [],
            "enforce_fetch_metadata": true,
            "enforce_origin": true
        });
        let res = validate_csrf_config("csrf_test", &val);
        assert!(res.is_err(), "Must reject secret keys < 16 chars");
    }
}
