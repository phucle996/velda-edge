//! CORS Response Headers Configuration Schema & Validation (`post_response::cors::validation`).
//!
//! ```text
//!   Plugin JSON ─► serde_json::from_value::<CorsResponseConfig>
//!                     ├── allow_origins empty? ──────────────────────► Error
//!                     ├── origin grammar + Fetch credentials guard ──► Error if '*'/'null' + creds
//!                     └── expose_headers tokens + '*' + creds guard ─► Error if '*' + creds
//! ```
//!
//! # Strict Declarative Contract (Zero Silent Fallbacks)
//! - All fields (`allow_origins`, `expose_headers`, `allow_credentials`) are mandatory.
//! - `Access-Control-Max-Age` belongs exclusively to preflight `OPTIONS` (`cors_preflight`),
//!   not post-response normal traffic.

use serde::{Deserialize, Serialize};

use crate::error::PluginError;
use crate::post_response::cors::origin::{is_valid_header_name, validate_origin_pattern};

/// Declarative configuration for the CORS Post-Response plugin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CorsResponseConfig {
    /// Permitted origins (`["https://app.velda.io"]`, `["https://*.velda.io"]`, or `["*"]`).
    pub allow_origins: Vec<String>,
    /// Headers exposed to browser scripts (`Access-Control-Expose-Headers`).
    pub expose_headers: Vec<String>,
    /// Whether cross-origin requests may include credentials (`Access-Control-Allow-Credentials: true`).
    pub allow_credentials: bool,
}

/// Validates CORS Post-Response configuration strictly according to RFC 6454 & W3C Fetch.
pub fn validate_cors_response_config(
    plugin_id: &str,
    config_val: &serde_json::Value,
) -> Result<CorsResponseConfig, PluginError> {
    let cfg: CorsResponseConfig = serde_json::from_value(config_val.clone()).map_err(|e| {
        PluginError::Config(format!(
            "Plugin '{plugin_id}' has malformed or incomplete cors config: {e}"
        ))
    })?;

    if cfg.allow_origins.is_empty() {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': allow_origins cannot be empty"
        )));
    }

    for origin in &cfg.allow_origins {
        validate_origin_pattern(origin, cfg.allow_credentials, plugin_id)?;
    }

    for h in &cfg.expose_headers {
        if !is_valid_header_name(h) {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': invalid header token '{h}' in expose_headers"
            )));
        }
        if cfg.allow_credentials && h.trim() == "*" {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': expose_headers cannot contain '*' when allow_credentials is true (W3C Fetch Spec violation)"
            )));
        }
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
            "expose_headers": ["x-request-id", "x-total-count"]
        });
        assert!(validate_cors_response_config("cors_1", &val).is_ok());
    }

    #[test]
    fn test_wildcard_with_credentials_rejected() {
        let val = json!({
            "allow_origins": ["*"],
            "allow_credentials": true,
            "expose_headers": []
        });
        let err = validate_cors_response_config("cors_bad_sec", &val).unwrap_err();
        assert!(err.to_string().contains("RFC 6454"));
    }

    #[test]
    fn test_null_with_credentials_rejected() {
        let val = json!({
            "allow_origins": ["null"],
            "allow_credentials": true,
            "expose_headers": []
        });
        let err = validate_cors_response_config("cors_bad_null", &val).unwrap_err();
        assert!(err.to_string().contains("null"));
    }

    #[test]
    fn test_expose_wildcard_with_credentials_rejected() {
        let val = json!({
            "allow_origins": ["https://app.velda.io"],
            "allow_credentials": true,
            "expose_headers": ["*"]
        });
        let err = validate_cors_response_config("cors_bad_expose", &val).unwrap_err();
        assert!(err.to_string().contains("expose_headers"));
    }

    #[test]
    fn test_origin_with_userinfo_rejected() {
        let val = json!({
            "allow_origins": ["https://admin@app.velda.io"],
            "allow_credentials": false,
            "expose_headers": []
        });
        let err = validate_cors_response_config("cors_bad_userinfo", &val).unwrap_err();
        assert!(err.to_string().contains("must not contain path, userinfo"));
    }

    #[test]
    fn test_missing_required_fields_rejected() {
        let val = json!({
            "allow_origins": ["https://app.velda.io"]
        });
        let err = validate_cors_response_config("cors_missing", &val).unwrap_err();
        assert!(err.to_string().contains("missing field"));
    }
}
