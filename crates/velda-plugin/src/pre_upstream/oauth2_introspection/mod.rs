//! Pre-Upstream OAuth 2.0 Token Introspection Subsystem (`oauth2_introspection`).
//!
//! ```text
//!   Warm L3 Hit (SIMD 64B Scan) ──► Synchronous `Action` (0 .await, 0 Future alloc)
//!   Cold Cache Miss             ──► Async `.await` Single-Flight (`velda-discovery` + TLS/HTTP Keep-Alive)
//!   Stale Window (10% in 3..10s)──► Serve Warm + Offload Async Refresh to `velda-shadow-{id}` (`coarse_sec`)
//! ```

pub mod cache;
pub mod client;
pub mod plugin;
pub mod scope;
pub mod validation;

pub use cache::{
    CacheLookup, ColdOutcome, IntrospectionCache, OAUTH2_CACHE_MEMORY_CEILING_PERCENT,
    compute_stale_refresh_secs, max_oauth2_cache_bytes_for,
};
pub use client::{IdpKeepAliveClient, IntrospectionResponse};
pub use plugin::Oauth2IntrospectionPlugin;
pub use validation::*;
