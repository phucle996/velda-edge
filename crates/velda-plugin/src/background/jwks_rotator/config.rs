//! Configuration schema for JWKS Background Key Rotator (`JwksRotatorConfig`).
//!
//! ```text
//!                      JWKS ROTATOR CONFIGURATION FLOW
//!
//!     +-----------------------------------------------------------+
//!     | JwksRotatorConfig (JSON Configuration)                    |
//!     | - target_jwt_auth_id: "jwt_auth"                          |
//!     | - jwks_uri: "https://auth.example.com/.well-known/jwks.json"
//!     | - refresh_interval_secs: 300                              |
//!     | - request_timeout_secs: 10                                |
//!     | - preserve_static_key: true                               |
//!     +-----------------------------------------------------------+
//!                                   |
//!                                   v
//!     +-----------------------------------------------------------+
//!     | Clamped & Validated Runtime Configuration                 |
//!     | - Minimum refresh interval: 10s                           |
//!     | - HTTP / HTTPS URL scheme validation                      |
//!     +-----------------------------------------------------------+
//! ```
//!
//! # Architectural Invariants
//! - **Non-blocking Logistics**: Runs on background companion threads isolated from frontline workers.
//! - **Integer Coarse Clock**: Refresh intervals are evaluated using coarse integer time
//!   (`velda_core::clock::coarse_sec`), eliminating clock syscall overhead.

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

/// Declarative configuration for the [`super::JwksRotatorPlugin`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JwksRotatorConfig {
    /// Unique identifier of the target `jwt_auth` pre_upstream plugin holding the [`super::super::super::pre_upstream::jwt_auth::SharedJwksStore`].
    pub target_jwt_auth_id: String,

    /// HTTP/HTTPS URL of the JWKS endpoint (e.g. `https://auth.example.com/.well-known/jwks.json`).
    pub jwks_uri: String,

    /// Refresh interval in seconds between periodic background rotations.
    #[serde(default = "default_refresh_interval_secs")]
    pub refresh_interval_secs: u64,

    /// Request timeout in seconds for outbound HTTP requests to the JWKS endpoint.
    #[serde(default = "default_request_timeout_secs")]
    pub request_timeout_secs: u64,

    /// Whether to preserve the static key configured in `jwt_auth` at Slot 0.
    #[serde(default = "default_true")]
    pub preserve_static_key: bool,

    /// Outbound TLS settings for connecting to HTTPS JWKS endpoints.
    #[serde(default)]
    pub tls: crate::config::OutboundTlsConfig,
}

impl JwksRotatorConfig {
    /// Validates and sanitizes the configuration, clamping intervals to safe boundaries.
    pub fn validate_and_sanitize(mut self) -> Result<Self, PluginError> {
        if self.target_jwt_auth_id.trim().is_empty() {
            return Err(PluginError::Config(
                "jwks_rotator: 'target_jwt_auth_id' must not be empty".into(),
            ));
        }

        if !self.jwks_uri.starts_with("http://") && !self.jwks_uri.starts_with("https://") {
            return Err(PluginError::Config(format!(
                "jwks_rotator: 'jwks_uri' must be an HTTP or HTTPS URL (got '{}')",
                self.jwks_uri
            )));
        }

        if self.refresh_interval_secs < MIN_REFRESH_INTERVAL_SECS {
            self.refresh_interval_secs = MIN_REFRESH_INTERVAL_SECS;
        }

        if self.request_timeout_secs == 0 {
            self.request_timeout_secs = DEFAULT_REQUEST_TIMEOUT_SECS;
        }

        Ok(self)
    }
}
