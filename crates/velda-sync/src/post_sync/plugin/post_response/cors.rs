//! CORS Response Headers Configuration Schema & Semantic Validation (`phase: "post_response"`).
//!
//! Enforces a strict declarative contract for Cross-Origin Resource Sharing response headers.
//!
//! # Strict Declarative Contract (Zero Silent Fallbacks)
//! - **RFC 6454 Security Invariant**: If `allow_credentials` is true, wildcard `*` origin is strictly forbidden.
//! - **Explicit Parameters**: `allow_origins`, `allow_credentials`, `expose_headers`, `max_age_secs` are mandatory.

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

    let has_wildcard = cfg.allow_origins.iter().any(|o| o.trim() == "*");
    if cfg.allow_credentials && has_wildcard {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': allow_credentials cannot be true when allow_origins contains '*' (RFC 6454 violation)"
            ),
        });
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
    }

    // 3. Validate Max Age
    if let Some(secs) = cfg.max_age_secs
        && secs > 86400
    {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': max_age_secs ({secs}) exceeds 86400 seconds (24 hours)"
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
            "allow_origins": ["https://app.velda.io", "https://admin.velda.io"],
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
        assert!(res.unwrap_err().to_string().contains("RFC 6454 violation"));
    }

    #[test]
    fn test_missing_required_fields_rejected() {
        let val = json!({
            "allow_origins": ["https://app.velda.io"]
            // Missing allow_credentials, expose_headers, max_age_secs
        });
        let res = validate_cors_response_config("cors_missing", &val);
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("missing field"));
    }
}
