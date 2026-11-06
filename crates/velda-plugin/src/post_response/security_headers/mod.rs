//! Security Headers Post-Response Plugin Module (`phase: "post_response"`).
//!
//! Provides client-side browser hardening, modern origin isolation (COOP, COEP, CORP),
//! and OWASP compliance on outgoing HTTP responses with zero heap allocation on the hot path.

pub mod config;
pub mod plugin;

pub use config::{
    CompiledHeader, FLAG_COEP, FLAG_COOP, FLAG_CORP, FLAG_CROSS_DOMAIN, FLAG_CSP,
    FLAG_FRAME_OPTIONS, FLAG_HSTS, FLAG_NOSNIFF, FLAG_PERMISSIONS_POLICY, FLAG_REFERRER_POLICY,
    FLAG_XSS_PROTECTION, HstsConfig, MAX_SECURITY_HEADERS, SecurityHeadersConfig,
    compile_header_entry,
};
pub use plugin::SecurityHeadersPlugin;
