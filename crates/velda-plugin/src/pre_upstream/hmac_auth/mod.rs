//! Pre-Upstream HMAC Authentication Plugin Subsystem (`hmac_auth`).
//!
//! Provides constant-time HMAC-SHA256 / HMAC-SHA512 request verification conforming to RFC 2104,
//! using zero-hash AGU bit-shift credential dispatch, pre-hashed [`velda_core::HeaderKey`] lookups,
//! streaming [`ring::hmac::Context`] canonical verification, and post-auth sharded nonce tracking.

pub mod canonical;
pub mod credential;
pub mod nonce;
pub mod plugin;
pub mod validation;

pub use credential::CredentialTable;
pub use plugin::HmacAuthPlugin;
pub use validation::*;
