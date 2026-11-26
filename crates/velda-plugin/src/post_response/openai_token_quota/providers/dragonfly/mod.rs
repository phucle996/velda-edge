//! DragonflyDB State-of-Truth (`velda_etp::dragonfly::DragonflyEtp`) adapter
//! for `openai_token_quota`.
//!
//! ```text
//!   providers/dragonfly/
//!     ├─► mod.rs    (module root export)
//!     ├─► codec.rs  (RESP2 DECRBY/INCRBY framing, reply parser & conservation math)
//!     ├─► client.rs (single-endpoint pipelined lease orchestration)
//!     └─► sync.rs   (Shadow Worker refill/ticker -> flush_batch)
//! ```

pub mod client;
pub mod codec;
pub mod sync;
