//! Unified JWT Authentication and Background Key Rotation Pre-Upstream Subsystem (`jwt_auth`).
//!
//! ```text
//!   [External Mode (`mode_bit == 1`, `pbww() > 0`)]
//!     Shadow Worker (`coarse_sec()`) ──► providers/<etp> (self-contained borrow + fetch)
//!                                            └─► swap into isolated `JwksStore`
//!
//!   [Hot Path (`velda-worker`)]
//!     L2 Headers ──► 64B L1D JwtCache ──► L3 Token Bucket (tagged by kid `key_tag`)
//!                                            ├─ Hit  ──► Inject Headers & Continue
//!                                            └─ Miss ──► L1D Bit-Map + u4 alg + u64 kid_int
//!                                                          ──► Key Byte Arena -> Verify
//! ```

pub mod cache;
pub mod claims;
pub mod crypto;
pub mod pem;
pub mod plugin;
pub mod providers;
pub mod simd;
pub mod store;
pub mod validation;

pub use cache::{CacheHitKind, CachedDecision, GROUP_WIDTH, JwtCache, is_plausible_jwt_shape};
pub use claims::{ClaimPath, resolve_claim_path};
pub use crypto::{
    ALG_INVALID, JwtAlgorithm, JwtValidationPolicy, JwtVerifyError, VerifiedToken, VerifierKey,
    verify_jwt,
};
pub use plugin::{JwtAuthPlugin, TokenSource};
pub use store::{
    DIR_FLAG_EXTERNAL_MODE, DIR_FLAG_WILDCARD_SINGLE, DirectoryLine, JwksKeyTable, JwksStore,
    KeyEntrySpec, KeySpan, MAX_JWKS_SLOTS, hash_kid_salted,
};
pub use validation::*;
