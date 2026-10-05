//! # velda-connection-pool
//!
//! Generic, protocol-agnostic, sharded connection pooling capability for the Velda Edge Data Plane.
//!
//! # Strict Monorepo Invariants (AGENTS.md)
//! - **Container-First**: Pools are containers indexed by `ConnectionKey`; connections are resources within them.
//! - **Workflow-independent Provider**: Does NOT own request business flow, routing, load-balancing, or DNS.
//! - **Zero connection establishment logic**: Pool NEVER performs `connect()` or DNS lookups (Stage 4 invariant).
//! - **In-memory hot paths**: Zero JSON, zero disk I/O, zero network calls during acquire/release.
//! - **Sharded concurrency**: Cache-aligned (`#[repr(align(64))]`) partitioned subpools eliminate lock contention across CPU cores.
//! - **No Arbitrary Function Splitting**: Workflows are contiguous, linear, and readable top-to-bottom.

pub mod config;
pub mod container;
pub mod key;
pub mod lease;
pub mod pool;
pub mod profile;
pub mod resource;

/// Backward-compatibility shims for pre-refactor module paths.
pub mod connection {
    pub use crate::container::multiplexed::{MultiplexedConnection, MultiplexedPool};
    pub use crate::lease::{ConnectionLease, ExclusiveLease, SequentialLease, StreamLease};
    pub use crate::profile::{ConnectionProfile, ReuseMode};
}

/// Backward-compatibility shims for pre-refactor module paths.
pub mod manager {
    pub use crate::config::{PoolConfig, PoolStats};
    pub use crate::pool::PoolManager;
}

// Flat canonical exports at crate root
pub use config::{DEFAULT_IDLE_TIMEOUT, DEFAULT_MAX_LIFETIME, PoolConfig, PoolStats};
pub use container::{
    FastBuildHasher, FastHasher, MAX_IDLE_PER_KEY, MAX_SHARDS, MIN_IDLE_PER_KEY, MIN_SHARDS,
    MultiplexedConnection, MultiplexedPool, MuxShard, PoolShard, ShardTable, SubPool,
    concurrency_shards_for_cpu_tier, max_concurrent_streams_for_mem_tier,
    max_idle_per_key_for_mem_tier, optimal_max_idle_per_key, optimal_shard_count,
    probed_max_idle_per_key, probed_shard_count,
};
pub use key::ConnectionKey;
pub use lease::{
    ConnectionLease, ExclusiveLease, ExclusiveReturnTarget, PoolLease, SequentialLease, StreamLease,
};
pub use pool::PoolManager;
pub use profile::{ConnectionProfile, ReuseMode};
pub use resource::PoolableResource;
