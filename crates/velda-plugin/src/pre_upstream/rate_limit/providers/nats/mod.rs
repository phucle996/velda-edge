//! NATS JetStream KV State-of-Truth (`velda_etp::nats::NatsEtp`) adapter for `rate_limit`.
//!
//! ```text
//!   providers/nats/
//!     ├─► mod.rs    (pure module root export)
//!     ├─► client.rs (JetStream KV GET + $KV.<bucket>.<key> PUT with reply-inbox sync)
//!     └─► sync.rs   (Shadow Worker drain -> push/reconcile -> health flag)
//! ```

pub mod client;
pub mod sync;
