//! Request Mirror Configuration Schema & Semantic Validation.
//!
//! ```text
//!   serde_json::Value
//!     └──► validate_request_mirror_config()
//!            ├─ mirror_upstream.trim() non-empty
//!            ├─ sample_percent in 0.0..=100.0 (non-NaN)
//!            └─ ignore_methods[] valid RFC 9110 HTTP method tokens
//! ```
//!
//! Enforces a strict declarative contract for traffic mirroring decisions.

pub use super::config::RequestMirrorConfig;
use crate::error::PluginError;

fn is_valid_http_method(m: &str) -> bool {
    let trimmed = m.trim();
    if trimmed.is_empty() {
        return false;
    }
    trimmed
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-')
}

/// Validates Request Mirror configuration strictly according to declarative traffic-control rules.
pub fn validate_request_mirror_config(
    plugin_id: &str,
    config_val: &serde_json::Value,
) -> Result<RequestMirrorConfig, PluginError> {
    let cfg: RequestMirrorConfig = serde_json::from_value(config_val.clone()).map_err(|e| {
        PluginError::Config(format!(
            "Plugin '{plugin_id}' has malformed or incomplete request_mirror config: {e}"
        ))
    })?;

    if cfg.mirror_upstream.trim().is_empty() {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': mirror_upstream cannot be empty"
        )));
    }

    if !(0.0..=100.0).contains(&cfg.sample_percent) || cfg.sample_percent.is_nan() {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': sample_percent ({}) must be between 0.0 and 100.0",
            cfg.sample_percent
        )));
    }

    for method in &cfg.ignore_methods {
        if !is_valid_http_method(method) {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': invalid HTTP method token '{method}' in ignore_methods"
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
    fn test_valid_request_mirror_config() {
        let val = json!({
            "mirror_upstream": "shadow-cluster-1",
            "sample_percent": 10.5,
            "ignore_methods": ["POST", "DELETE"]
        });
        let res = validate_request_mirror_config("mirror_1", &val);
        assert!(res.is_ok());
    }

    #[test]
    fn test_empty_mirror_upstream_rejected() {
        let val = json!({
            "mirror_upstream": "",
            "sample_percent": 10.0,
            "ignore_methods": []
        });
        let res = validate_request_mirror_config("mirror_bad_up", &val);
        assert!(res.is_err());
        assert!(
            res.unwrap_err()
                .to_string()
                .contains("mirror_upstream cannot be empty")
        );
    }

    #[test]
    fn test_invalid_sample_percent_rejected() {
        let val = json!({
            "mirror_upstream": "shadow-up",
            "sample_percent": 105.0,
            "ignore_methods": []
        });
        let res = validate_request_mirror_config("mirror_bad_sample", &val);
        assert!(res.is_err());
        assert!(
            res.unwrap_err()
                .to_string()
                .contains("between 0.0 and 100.0")
        );
    }

    #[test]
    fn test_missing_required_fields_rejected() {
        let val = json!({
            "mirror_upstream": "shadow-up"
        });
        let res = validate_request_mirror_config("mirror_missing", &val);
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("missing field"));
    }
}
