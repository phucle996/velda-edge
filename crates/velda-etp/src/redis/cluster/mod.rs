//! Cluster-mode Redis pool branch (`velda-etp::redis::cluster`).
//!
//! ```text
//!   RedisEtpConfig (mode == Cluster) ──► RedisClusterPool
//!     ├─► slots.rs : CRC16-CCITT + CLUSTER SLOTS parser + Hybrid SlotMap (Ranges / #[cold] Dense)
//!     └─► pool.rs  : Wait-free ArcSwap<ClusterTopology> + per-Master RedisStandalonePool
//! ```

pub mod pool;
pub mod slots;

pub use pool::RedisClusterPool;
pub use slots::{SlotMap, crc16_slot};
