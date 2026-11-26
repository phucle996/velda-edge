//! Memcached State-of-Truth (`velda_etp::memcached::MemcachedEtp`) adapter for `rate_limit`.
//!
//! ```text
//!   providers/memcached/
//!     ├─► mod.rs    (pure module root export)
//!     ├─► client.rs (ASCII incr / add over MemcachedEtp StreamLease)
//!     └─► sync.rs   (Shadow Worker drain -> push/reconcile -> health flag)
//! ```

pub mod client;
pub mod sync;
