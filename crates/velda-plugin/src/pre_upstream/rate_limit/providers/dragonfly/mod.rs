//! DragonflyDB State-of-Truth (`velda_etp::dragonfly::DragonflyEtp`) adapter for `rate_limit`.
//!
//! ```text
//!   providers/dragonfly/
//!     ├─► mod.rs    (pure module root export)
//!     ├─► client.rs (single-endpoint pipelined RESP2 SET NX + INCRBY over DragonflyLease)
//!     └─► sync.rs   (Shadow Worker drain -> push/reconcile -> health flag)
//! ```

pub mod client;
pub mod sync;
