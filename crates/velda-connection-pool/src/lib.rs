//! # velda-connection-pool
//!
//! Two-tier, `4096`-byte page-aligned connection pooling for the Velda Edge Data Plane.
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────────────────┐
//! │                     TWO-TIER HOT / COLD CONNECTION POOL ARCHITECTURE                │
//! ├─────────────────────────────────────────────────────────────────────────────────────┤
//! │                                                                                     │
//! │   velda-worker-{i} (current_thread, epoll_i)                                        │
//! │        │                                                                            │
//! │        ▼  (1) acquire(): 0 Mutex, 0 HashMap, 0 epoll_ctl                            │
//! │   ┌─────────────────────────────────────────────┐                                   │
//! │   │  TIER 1: HotPool -> HotLane[i] (align=64B)  │                                   │
//! │   │  SoA PageAlignedSlab (align=4096, k*4096B)  │                                   │
//! │   │  • keys:  [CompactPoolKey (24B); N]         │  spill oldest (on_cold_store)     │
//! │   │  • pools: [SubPool<R> (LIFO); N]            │ ─────────────────────────────┐    │
//! │   └─────────────────────────────────────────────┘                              │    │
//! │        │ miss (HotLane[i] empty)                                               ▼    │
//! │        │                                         ┌──────────────────────────────┐   │
//! │        └──────────► (2) pull + on_hot_restore ─► │ TIER 2: ColdPool (Lock-Free) │   │
//! │                                                  │ FixedPageSlab (k * 4096B)    │   │
//! │                                                  │ Shared across all CPU cores  │   │
//! │   velda-shadow-{i} (SCHED_IDLE) ── pre-warm ───► │ Unregistered std::net FDs    │   │
//! │                                                  └──────────────────────────────┘   │
//! └─────────────────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Module Layout
//! - [`slab`]: `4096`-byte page-aligned contiguous memory slabs (`PageAlignedSlab`, `FixedPageSlab`).
//! - [`key`]: `24-byte` `Copy` [`CompactPoolKey`] (`0xFF`/`0xFFFF_FFFF` sentinels) + [`TlsStringArena`].
//! - [`hot`]: Tier 1 core-local `HotPool` / `HotLane` SoA tables (`0 Mutex`, `0 HashMap`).
//! - [`cold`]: Tier 2 shared lock-free `ColdPool` bank (`EMPTY -> WRITING -> READY -> READING`).
//! - [`subpool`]: LIFO idle connection queue with spill-oldest support.
//! - [`multiplexed`]: Lane-sharded contiguous container for H2 / H3 / gRPC stream multiplexing.
//! - [`pool`]: Unified [`PoolManager`] facade coordinating Tier 1 Hot, Tier 2 Cold, and Multiplexed.

pub mod cold;
pub mod config;
pub mod hot;
pub mod key;
pub mod lease;
pub mod multiplexed;
pub mod pool;
pub mod profile;
pub mod resource;
pub mod slab;
pub mod subpool;

pub use cold::ColdPool;
pub use config::{
    DEFAULT_IDLE_TIMEOUT, DEFAULT_MAX_LIFETIME, MAX_IDLE_PER_KEY, MAX_SHARDS, MIN_IDLE_PER_KEY,
    MIN_SHARDS, PoolConfig, PoolStats, optimal_max_concurrent_streams, optimal_max_idle_per_key,
    optimal_shard_count, probed_max_concurrent_streams, probed_max_idle_per_key,
    probed_shard_count,
};
pub use hot::{
    HotLane, HotPool, HotTable, LaneMetrics, current_thread_lane, set_current_thread_lane,
};
pub use key::{
    ALPN_NONE, CompactPoolKey, PROTO_GRPC, PROTO_HTTP1, PROTO_HTTP2, PROTO_HTTP3, PROTO_TCP,
    SNI_NONE, TlsStringArena,
};
pub use lease::{
    ConnectionLease, ExclusiveLease, ExclusiveReturnTarget, SequentialLease, StreamLease,
};
pub use multiplexed::{MultiplexedConnection, MultiplexedPool, MuxShard};
pub use pool::PoolManager;
pub use profile::{ConnectionProfile, ReuseMode};
pub use resource::PoolableResource;
pub use slab::{FixedPageSlab, PAGE_SIZE, PageAlignedSlab};
pub use subpool::SubPool;
