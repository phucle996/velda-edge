//! Schema & semantic validation for `https_redirect` (`pre_route`).
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────────────────────┐
//! │                     HTTPS REDIRECT VALIDATION FLOW                                      │
//! ├─────────────────────────────────────────────────────────────────────────────────────────┤
//! │  Raw JSON Value                                                                         │
//! │    ├── Deserialize into HttpsRedirectConfig (#[serde(deny_unknown_fields)])             │
//! │    ├── Validate status_code ∈ { 301, 308 }                                              │
//! │    ├── Validate https_port >= 1                                                         │
//! │    └── Return normalized HttpsRedirectConfig                                            │
//! └─────────────────────────────────────────────────────────────────────────────────────────┘
//! ```

use serde_json::Value;

use crate::error::PluginError;
use crate::pre_route::https_redirect::config::HttpsRedirectConfig;

/// Validates and normalizes `https_redirect` plugin configuration.
pub fn validate_https_redirect_config(
    plugin_id: &str,
    raw_config: &Value,
) -> Result<HttpsRedirectConfig, PluginError> {
    let cfg: HttpsRedirectConfig = serde_json::from_value(raw_config.clone()).map_err(|e| {
        PluginError::Config(format!(
            "Invalid configuration for plugin '{plugin_id}' (https_redirect): {e}"
        ))
    })?;

    if cfg.status_code != 301 && cfg.status_code != 308 {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}' (https_redirect) invalid status_code {}: expected 301 or 308",
            cfg.status_code
        )));
    }

    if cfg.https_port == 0 {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}' (https_redirect) invalid https_port 0: expected 1..=65535"
        )));
    }

    Ok(cfg)
}
