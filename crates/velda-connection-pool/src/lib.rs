//! # velda-connection-pool
//!
//! Generic, protocol-agnostic, sharded connection pooling capability for the Velda Edge Data Plane.
//!
//! Strict Invariants:
//! - **Container-First**: Pools are containers indexed by `ConnectionKey`; connections are resources within them.
//! - **Workflow-independent Provider**: Does NOT own request business flow, routing, load-balancing, or DNS.
//! - **Zero connection establishment logic**: Pool NEVER performs `connect()` or DNS lookups.
//! - **In-memory hot paths**: Zero JSON, zero disk I/O, zero network calls during acquire/release.
//! - **Sharded concurrency**: Cache-aligned (`align(64)`) partitioned subpools eliminate lock contention across CPU cores.

pub mod connection;
pub mod container;
pub mod key;
pub mod manager;
pub mod resource;

pub use connection::{
    ConnectionLease, ConnectionProfile, ExclusiveLease, MultiplexedConnection, MultiplexedPool,
    ReuseMode, SequentialLease, StreamLease,
};
pub use container::{
    PoolShard, ShardTable, SubPool, optimal_max_idle_per_key, optimal_shard_count,
    probed_max_idle_per_key, probed_shard_count,
};
pub use key::ConnectionKey;
pub use manager::{PoolConfig, PoolManager, PoolStats};
pub use resource::{PoolLease, PoolableResource};
