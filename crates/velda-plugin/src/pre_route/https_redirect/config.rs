//! Configuration schema for `https_redirect` (`pre_route`).
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────────────────────┐
//! │                     HTTPS REDIRECT CONFIGURATION                                        │
//! ├─────────────────────────────────────────────────────────────────────────────────────────┤
//! │  JSON: { "status_code": 308, "https_port": 443 }                                        │
//! │    ├── status_code : 301 (Moved Permanently) | 308 (Permanent Redirect, default)        │
//! │    └── https_port  : 1..=65535 (default: 443, omitted from Location URI when 443)       │
//! └─────────────────────────────────────────────────────────────────────────────────────────┘
//! ```

use serde::{Deserialize, Serialize};

#[inline(always)]
const fn default_status_code() -> u16 {
    308
}

#[inline(always)]
const fn default_https_port() -> u16 {
    443
}

/// Configuration for the `https_redirect` pre-route plugin.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct HttpsRedirectConfig {
    /// Redirect HTTP status code (`301` or `308`). Defaults to `308`.
    #[serde(default = "default_status_code")]
    pub status_code: u16,

    /// Target HTTPS port appended to `Location` when non-standard (`!= 443`). Defaults to `443`.
    #[serde(default = "default_https_port")]
    pub https_port: u16,
}

impl Default for HttpsRedirectConfig {
    fn default() -> Self {
        Self {
            status_code: default_status_code(),
            https_port: default_https_port(),
        }
    }
}
