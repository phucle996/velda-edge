//! CSRF Protection Schema & Strict Validation (`csrf::validation`).
//!
//! ```text
//! JSON Config ──► CsrfConfig ──► validate_csrf_config()
//!   ├─ safe_methods    : non-empty idempotent HTTP methods (`GET|HEAD|OPTIONS|TRACE`)
//!   ├─ trusted_origins : explicit `http(s)://host[:port]` or `http(s)://*.domain.tld`
//!   ├─ allow_same_site : permit `Sec-Fetch-Site: same-site` sibling subdomains
//!   ├─ enforce_origin  : verify `Origin`/`Referer` and block cookie requests missing provenance
//!   └─ double_submit   : Option<{ cookie_name, header_name }> for legacy SPA/form cookies
//! ```

use http::HeaderName;
use serde::{Deserialize, Serialize};

use crate::error::PluginError;

/// Optional Double-Submit Cookie verification for legacy apps (`cookie_name == header_name`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CsrfDoubleSubmitConfig {
    /// Cookie carrying the anti-CSRF token (e.g. `XSRF-TOKEN` or `__Host-velda_csrf`).
    pub cookie_name: String,
    /// Request header carrying the matching token (e.g. `X-XSRF-TOKEN` or `X-CSRF-Token`).
    pub header_name: String,
}

/// Declarative configuration for modern Cross-Origin & optional Double-Submit CSRF protection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CsrfConfig {
    /// Idempotent HTTP methods exempt from CSRF checks (e.g. `["GET", "HEAD", "OPTIONS"]`).
    pub safe_methods: Vec<String>,
    /// Explicitly permitted cross-origin callers (exact `scheme://host[:port]` or `scheme://*.domain.tld`).
    pub trusted_origins: Vec<String>,
    /// Allow sibling subdomains (`Sec-Fetch-Site: same-site`) without listing them in `trusted_origins`.
    pub allow_same_site: bool,
    /// Enforce `Origin` / `Referer` checks when `Sec-Fetch-Site` is absent (for older browsers/proxies).
    pub enforce_origin: bool,
    /// Optional Double-Submit Cookie comparison (`cookie_name == header_name`) for legacy backends.
    pub double_submit: Option<CsrfDoubleSubmitConfig>,
}

#[inline]
fn is_valid_cookie_name(name: &str) -> bool {
    !name.is_empty()
        && name.bytes().all(|b| {
            b.is_ascii_graphic()
                && !matches!(
                    b,
                    b'(' | b')'
                        | b'<'
                        | b'>'
                        | b'@'
                        | b','
                        | b';'
                        | b':'
                        | b'\\'
                        | b'"'
                        | b'/'
                        | b'['
                        | b']'
                        | b'?'
                        | b'='
                        | b'{'
                        | b'}'
                )
        })
}

fn validate_trusted_origin(plugin_id: &str, origin: &str) -> Result<(), PluginError> {
    let trimmed = origin.trim();
    let rest = if let Some(h) = trimmed.strip_prefix("https://") {
        h
    } else if let Some(h) = trimmed.strip_prefix("http://") {
        h
    } else {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': trusted origin '{origin}' must specify 'http://' or 'https://'"
        )));
    };

    if rest.is_empty()
        || rest
            .bytes()
            .any(|b| b.is_ascii_whitespace() || matches!(b, b'/' | b'?' | b'#' | b'@' | b'\\'))
    {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': trusted origin '{origin}' contains empty or invalid authority"
        )));
    }

    if let Some(after_star) = rest.strip_prefix("*.") {
        if after_star.is_empty()
            || after_star.contains('*')
            || !after_star
                .split(':')
                .next()
                .is_some_and(|h| h.contains('.'))
        {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': wildcard trusted origin '{origin}' must scope to a domain like 'https://*.example.com'"
            )));
        }
    } else if rest.contains('*') {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': invalid wildcard placement in trusted origin '{origin}'"
        )));
    }

    Ok(())
}

/// Validates CSRF configuration strictly against modern Cross-Origin security invariants.
pub fn validate_csrf_config(
    plugin_id: &str,
    config_val: &serde_json::Value,
) -> Result<CsrfConfig, PluginError> {
    let cfg: CsrfConfig = serde_json::from_value(config_val.clone()).map_err(|e| {
        PluginError::Config(format!(
            "Plugin '{plugin_id}' has malformed or incomplete csrf config: {e}"
        ))
    })?;

    if cfg.safe_methods.is_empty() {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': 'safe_methods' cannot be empty"
        )));
    }

    for method in &cfg.safe_methods {
        match method.trim().to_ascii_uppercase().as_str() {
            "GET" | "HEAD" | "OPTIONS" | "TRACE" => {}
            other => {
                return Err(PluginError::Config(format!(
                    "Plugin '{plugin_id}': non-idempotent method '{other}' cannot be declared in 'safe_methods'"
                )));
            }
        }
    }

    for origin in &cfg.trusted_origins {
        validate_trusted_origin(plugin_id, origin)?;
    }

    if let Some(ds) = &cfg.double_submit {
        let cookie = ds.cookie_name.trim();
        if !is_valid_cookie_name(cookie) {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': invalid 'double_submit.cookie_name' '{}'",
                ds.cookie_name
            )));
        }
        if HeaderName::from_bytes(ds.header_name.trim().as_bytes()).is_err() {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': 'double_submit.header_name' '{}' is not a valid RFC 9110 header name",
                ds.header_name
            )));
        }
    }

    Ok(cfg)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_modern_and_legacy_csrf_configs() {
        let modern = serde_json::json!({
            "safe_methods": ["GET", "HEAD", "OPTIONS"],
            "trusted_origins": ["https://app.example.com", "https://*.partner.example.com"],
            "allow_same_site": false,
            "enforce_origin": true,
            "double_submit": null
        });
        assert!(validate_csrf_config("csrf_modern", &modern).is_ok());

        let legacy = serde_json::json!({
            "safe_methods": ["GET", "HEAD"],
            "trusted_origins": [],
            "allow_same_site": false,
            "enforce_origin": true,
            "double_submit": {
                "cookie_name": "XSRF-TOKEN",
                "header_name": "X-XSRF-TOKEN"
            }
        });
        assert!(validate_csrf_config("csrf_legacy", &legacy).is_ok());
    }

    #[test]
    fn test_rejects_unsafe_methods_and_invalid_origins_and_unknown_fields() {
        assert!(
            validate_csrf_config(
                "csrf_bad_method",
                &serde_json::json!({
                    "safe_methods": ["GET", "POST"],
                    "trusted_origins": [],
                    "allow_same_site": false,
                    "enforce_origin": true,
                    "double_submit": null
                })
            )
            .is_err()
        );

        for bad_origin in ["*", "https://*", "https://*.com", "https://", "example.com"] {
            assert!(
                validate_csrf_config(
                    "csrf_bad_origin",
                    &serde_json::json!({
                        "safe_methods": ["GET"],
                        "trusted_origins": [bad_origin],
                        "allow_same_site": false,
                        "enforce_origin": true,
                        "double_submit": null
                    })
                )
                .is_err(),
                "must reject invalid trusted_origin '{bad_origin}'"
            );
        }

        // Legacy fields from removed 114-byte HMAC schema must be rejected strictly
        assert!(
            validate_csrf_config(
                "csrf_legacy_hmac_fields",
                &serde_json::json!({
                    "safe_methods": ["GET"],
                    "trusted_origins": [],
                    "allow_same_site": false,
                    "enforce_origin": true,
                    "double_submit": null,
                    "secret_key": "0123456789abcdef0123456789abcdef"
                })
            )
            .is_err()
        );
    }
}
