//! AWS ElastiCache State-of-Truth (`velda_etp::aws_elasticache::AwsElastiCacheEtp`) adapter
//! for `openai_token_quota`.
//!
//! ```text
//!   providers/aws_elasticache/
//!     ├─► mod.rs        (module root export)
//!     ├─► codec.rs      (RESP2 DECRBY/INCRBY framing, conservation math & group executor)
//!     ├─► standalone.rs (single-endpoint pipelined lease branch)
//!     ├─► cluster.rs    (CRC16 shard-grouped parallel RTT + -MOVED/-ASK branch)
//!     ├─► client.rs     (topology dispatch + UsageSlot atomic reconciliation)
//!     └─► sync.rs       (Shadow Worker refill/ticker -> flush_batch)
//! ```

pub mod client;
pub mod cluster;
pub mod codec;
pub mod standalone;
pub mod sync;
