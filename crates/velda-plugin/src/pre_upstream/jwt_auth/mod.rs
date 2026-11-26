//! JWT Authentication and Claim Projection Pre-Upstream Subsystem (`jwt_auth`).
//!
//! ```text
//!   L2 Request Headers ──► 64B L1D Control (`boot_salt` + shifts) ──► L3 64-Way SIMD Bucket
//!                                                                          │
//!                          ┌──────────────────────(Hit)────────────────────┼─────────────────(Miss)─┐
//!                          ▼                                                                        ▼
//!                  `insert_prehashed`                                                      `JwksKeyTable` + Ring
//!                  claims & Continue                                                       (HMAC/RSA/ECDSA/EdDSA)
//! ```

pub mod cache;
pub mod crypto;
pub mod plugin;
pub mod table;
pub mod validation;

pub use cache::{CacheLookup, CachedDecision, GROUP_WIDTH, JwtCache, is_plausible_jwt_shape};
pub use crypto::{
    ClaimPath, JwtAlgorithm, JwtValidationPolicy, JwtVerifyError, VerifiedToken, VerifierKey,
    resolve_claim_path, verify_jwt,
};
pub use plugin::{JwtAuthPlugin, TokenSource};
pub use table::{
    JwksKeySlot, JwksKeyTable, MAX_JWKS_SLOTS, MAX_KID_BYTES, SharedJwksStore, hash_kid,
};
pub use validation::*;
