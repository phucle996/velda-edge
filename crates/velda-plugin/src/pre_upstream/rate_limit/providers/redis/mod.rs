//! Redis State-of-Truth (`velda_etp::redis::RedisEtp`) adapter for `rate_limit`.
//!
//! ```text
//!   providers/redis/
//!     ├─► mod.rs        (module root export)
//!     ├─► codec.rs      (RESP2 SET NX + INCRBY framing & redirect/int reply parser)
//!     ├─► standalone.rs (single-endpoint 1-RTT pipelined upsert)
//!     ├─► cluster.rs    (CRC16 shard-grouped parallel RTT + -MOVED/-ASK redirects)
//!     ├─► client.rs     (topology dispatch push_and_read_totals)
//!     └─► sync.rs       (Shadow Worker drain -> push/reconcile -> health flag)
//! ```

pub mod client;
pub mod cluster;
pub mod codec;
pub mod standalone;
pub mod sync;
