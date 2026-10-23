//! Pre-Upstream HMAC Authentication Plugin Subsystem (`hmac_auth`).
//!
//! Provides cryptographically secure, constant-time Hash-based Message Authentication Code
//! (HMAC-SHA256 and HMAC-SHA512) validation conforming to RFC 2104.
//!
//! Enforces zero-IO execution, anti-tampering verification, clock-skew replay window protection,
//! and sharded in-memory nonce tracking.

pub mod canonical;
pub mod nonce;
pub mod plugin;

pub use plugin::HmacAuthPlugin;
pub use velda_sync::{HmacAuthConfig, HmacCredentialConfig};
