//! DragonflyDB State-of-Truth (`velda_etp::dragonfly::DragonflyEtp`) adapter for `api_metering`.
//!
//! ```text
//!   providers/dragonfly/
//!     ├─► mod.rs    (pure module root export)
//!     ├─► client.rs (single-endpoint pipelined RESP2 HINCRBY over DragonflyLease)
//!     └─► sync.rs   (Shadow Worker delta collect -> HINCRBY -> reconcile_balances)
//! ```

pub mod client;
pub mod sync;
