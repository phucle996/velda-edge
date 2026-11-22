//! HTTP Method Filter Configuration Schema & Semantic Validation.
//!
//! ```text
//!  +─────────────────────────────────────────────────────────────────────────+
//!  |                  METHOD_FILTER DECLARATIVE VALIDATION                   |
//!  +─────────────────────────────────────────────────────────────────────────+
//!  |                                                                         |
//!  |  JSON Config ─► Deserialize MethodFilterConfig { allowed_methods }      |
//!  |                      │                                                  |
//!  |                      ├─ 1. Verify allowed_methods is non-empty          |
//!  |                      ├─ 2. Verify RFC 9110 token grammar per entry      |
//!  |                      └─ 3. Uppercase, sort_unstable & deduplicate       |
//!  +─────────────────────────────────────────────────────────────────────────+
//! ```

use super::config::MethodFilterConfig;
use crate::error::PluginError;

/// Returns `true` if `s` is a valid non-empty RFC 9110 HTTP token.
fn is_valid_method_token(s: &str) -> bool {
    !s.is_empty()
        && s.bytes().all(|b| {
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

/// Validates Method Filter configuration strictly and deduplicates allowed HTTP methods.
pub fn validate_method_filter_config(
    plugin_id: &str,
    config_val: &serde_json::Value,
) -> Result<MethodFilterConfig, PluginError> {
    let mut cfg: MethodFilterConfig = serde_json::from_value(config_val.clone()).map_err(|e| {
        PluginError::Config(format!(
            "Plugin '{plugin_id}' has malformed or incomplete method_filter config: {e}"
        ))
    })?;

    if cfg.allowed_methods.is_empty() {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': allowed_methods cannot be empty"
        )));
    }

    let mut normalized = Vec::with_capacity(cfg.allowed_methods.len());
    for method in &cfg.allowed_methods {
        let upper = method.trim().to_ascii_uppercase();
        if !is_valid_method_token(&upper) {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': invalid HTTP method token '{method}'"
            )));
        }
        normalized.push(upper);
    }

    normalized.sort_unstable();
    normalized.dedup();
    cfg.allowed_methods = normalized;

    Ok(cfg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_valid_config_normalizes_and_deduplicates() {
        let raw = json!({
            "allowed_methods": ["get", " POST ", "GET", "options"]
        });

        let cfg = validate_method_filter_config("method_filter", &raw).unwrap();
        assert_eq!(cfg.allowed_methods, vec!["GET", "OPTIONS", "POST"]);
    }

    #[test]
    fn test_rejects_empty_allowed_methods() {
        let raw = json!({
            "allowed_methods": []
        });
        assert!(validate_method_filter_config("method_filter", &raw).is_err());
    }

    #[test]
    fn test_rejects_invalid_method_token() {
        let raw = json!({
            "allowed_methods": ["GET", "BAD METHOD"]
        });
        assert!(validate_method_filter_config("method_filter", &raw).is_err());
    }
}
