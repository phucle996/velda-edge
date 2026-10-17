//! Security Headers Post-Response Plugin Module (`phase: "post_response"`).
//!
//! Provides client-side browser hardening and OWASP compliance on outgoing HTTP responses.

pub mod plugin;

pub use plugin::{HstsConfig, SecurityHeadersConfig, SecurityHeadersPlugin};
