//! Redis State-of-Truth (`velda_etp::redis::RedisEtp`) adapter for `api_metering`.
//!
//! ```text
//!   providers/redis/
//!     ├─► mod.rs    (module root export)
//!     ├─► codec.rs  (RESP2 HINCRBY encoder, reply parser & lease round-trip)
//!     ├─► client.rs (Standalone & Cluster CRC16/-MOVED/-ASK batch orchestration)
//!     └─► sync.rs   (Shadow Worker delta collect -> HINCRBY -> reconcile_balances)
//! ```

pub mod client;
pub mod codec;
pub mod sync;
