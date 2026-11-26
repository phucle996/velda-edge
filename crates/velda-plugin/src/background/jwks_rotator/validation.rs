//! JWKS Key Rotator Schema & Semantic Validation (`jwks_rotator::validation`).
//!
//! ```text
//!                  JWKS ROTATOR SEMANTIC VALIDATION PIPELINE
//!
//!     PluginConfig (JSON, deny_unknown_fields)
//!            │
//!            ▼
//!     validate_jwks_rotator_config() -> JwksRotatorConfig::validate_and_sanitize()
//!            ├──► Target JWT Auth ID non-empty check
//!            ├──► JWKS URI HTTP / HTTPS scheme check
//!            ├──► Coarse interval clamping (>= 10s)
//!            └──► Request timeout default (> 0s)
//! ```

pub use super::config::JwksRotatorConfig;
use crate::error::PluginError;

/// Validates JWKS Key Rotator configuration schema and value boundaries.
pub fn validate_jwks_rotator_config(
    plugin_id: &str,
    config_val: &serde_json::Value,
) -> Result<JwksRotatorConfig, PluginError> {
    let cfg: JwksRotatorConfig = serde_json::from_value(config_val.clone()).map_err(|e| {
        PluginError::Config(format!(
            "Plugin '{plugin_id}' has malformed or incomplete jwks_rotator config: {e}"
        ))
    })?;

    cfg.validate_and_sanitize()
        .map_err(|e| PluginError::Config(format!("Plugin '{plugin_id}': {e}")))
}
