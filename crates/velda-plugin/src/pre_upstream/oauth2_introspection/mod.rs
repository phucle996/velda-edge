//! Pre-Upstream OAuth 2.0 Token Introspection Subsystem (`oauth2_introspection`).
//!
//! ```text
//!   Warm L3 Hit / Cold DRAM (Per-Core) ──► Sync `Action` (0 .await, 0 Future alloc)
//!   Cold Cache Miss                    ──► `inflight` Single-Flight -> `providers/<etp_kind>`
//!   Stale Window (10% in 3..=10s)      ──► Serve Warm + Offload Refresh to `velda-shadow-{id}`
//! ```

pub mod cache;
pub mod inflight;
pub mod plugin;
pub mod providers;
pub mod scope;
pub mod validation;

pub use cache::{
    COLD_DRAM_MEMORY_PERCENT, CacheLookupKind, ColdOutcome, GROUP_WIDTH, IntrospectionCache,
    MAX_COLD_DRAM_TOTAL_BYTES, MAX_HOT_L3_TOTAL_BYTES, MAX_OAUTH2_TOKEN_BYTES,
    compute_stale_refresh_secs, max_oauth2_cold_dram_bytes_for,
};
pub use inflight::InFlightCoordinator;
pub use plugin::Oauth2IntrospectionPlugin;
pub use providers::{AuthMethod, IntrospectionCall, IntrospectionCallParams};
pub use validation::*;
