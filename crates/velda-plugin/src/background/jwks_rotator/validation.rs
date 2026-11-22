//! JWKS Key Rotator Schema & Semantic Validation.
//!
//! ```text
//!                  JWKS ROTATOR SEMANTIC VALIDATION PIPELINE
//!
//!     PluginConfig (JSON)
//!            |
//!            v
//!     validate_jwks_rotator_config()
//!            |
//!            +---> Target JWT Auth ID non-empty check
//!            +---> JWKS URI HTTP / HTTPS scheme check
//!            +---> Coarse interval clamping (>= 10s)
//!            +---> Request timeout validation (> 0s)
//!            |
//!            v
//!     Normalized JwksRotatorConfig Struct
//! ```
//!
//! # Architectural Invariants
//! - **Strict Background Phase**: Runs exclusively as a compile-time `BackgroundPlugin`.
//! - **Inter-Plugin Dependency**: Rotator operates as a companion writer to `jwt_auth`
//!   and is strictly rejected if `jwt_auth` is not present in the configuration.

use serde::{Deserialize, Serialize};

use crate::error::PluginError;

const DEFAULT_REFRESH_INTERVAL_SECS: u64 = 300;
const MIN_REFRESH_INTERVAL_SECS: u64 = 10;
const DEFAULT_REQUEST_TIMEOUT_SECS: u64 = 10;

fn default_refresh_interval_secs() -> u64 {
    DEFAULT_REFRESH_INTERVAL_SECS
}

fn default_request_timeout_secs() -> u64 {
    DEFAULT_REQUEST_TIMEOUT_SECS
}

const fn default_true() -> bool {
    true
}

/// Declarative configuration for the JWKS background rotator plugin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JwksRotatorConfig {
    /// Identifier of the target `jwt_auth` plugin holding the pre-allocated key table.
    pub target_jwt_auth_id: String,

    /// HTTP or HTTPS URL of the JWKS endpoint.
    pub jwks_uri: String,

    /// Interval in seconds between periodic key rotation fetches.
    #[serde(default = "default_refresh_interval_secs")]
    pub refresh_interval_secs: u64,

    /// Outbound HTTP request timeout in seconds.
    #[serde(default = "default_request_timeout_secs")]
    pub request_timeout_secs: u64,

    /// Whether to preserve the static key configured in `jwt_auth` at Slot 0.
    #[serde(default = "default_true")]
    pub preserve_static_key: bool,

    /// Outbound TLS settings for connecting to HTTPS JWKS endpoints.
    #[serde(default)]
    pub tls: crate::config::OutboundTlsConfig,
}

/// Validates JWKS Key Rotator configuration schema and value boundaries.
pub fn validate_jwks_rotator_config(
    plugin_id: &str,
    config_val: &serde_json::Value,
) -> Result<JwksRotatorConfig, PluginError> {
    let mut cfg: JwksRotatorConfig = serde_json::from_value(config_val.clone()).map_err(|e| {
        PluginError::Config(format!(
            "Plugin '{plugin_id}' has malformed or incomplete jwks_rotator config: {e}"
        ))
    })?;

    let fail = |reason: String| PluginError::Config(format!("Plugin '{plugin_id}': {reason}"));

    let target_id = cfg.target_jwt_auth_id.trim();
    if target_id.is_empty() {
        return Err(fail("'target_jwt_auth_id' must not be empty".into()));
    }
    cfg.target_jwt_auth_id = target_id.to_string();

    let uri = cfg.jwks_uri.trim();
    if !uri.starts_with("http://") && !uri.starts_with("https://") {
        return Err(fail(format!(
            "'jwks_uri' must be an HTTP or HTTPS URL (got '{uri}')"
        )));
    }
    cfg.jwks_uri = uri.to_string();

    if cfg.refresh_interval_secs < MIN_REFRESH_INTERVAL_SECS {
        cfg.refresh_interval_secs = MIN_REFRESH_INTERVAL_SECS;
    }

    if cfg.request_timeout_secs == 0 {
        cfg.request_timeout_secs = DEFAULT_REQUEST_TIMEOUT_SECS;
    }

    Ok(cfg)
}
