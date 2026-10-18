//! JWT Authentication and Claim Injection Pre-Upstream Subsystem.
//!
//! Provides route-scoped Bearer token validation, flexible claim extraction (nested objects,
//! lists), RBAC claim assertions, and lock-striped dual (positive/negative) caching.

pub mod cache;
pub mod crypto;
pub mod plugin;

pub use cache::{InvalidTokenEntry, JwtCache, ValidTokenEntry};
pub use crypto::{JwtAlgorithm, VerifiedToken, VerifierKey, resolve_claim_path, verify_jwt};
pub use plugin::{JwtAuthPlugin, TokenSource};
