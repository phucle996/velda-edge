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
//!
//! # Per-Core Lane Sharding & L1/L2 Hardware Cache Affinity Invariant
//!
//! Velda Edge pairs two worker threads pinned to each physical CPU core:
//! - **Frontline Traffic Worker (`velda-worker-{id}`)**: Exclusively serves downstream traffic on the request hot path.
//! - **Shadow Logistics Worker (`velda-shadow-{id}`)**: Autonomous background companion pinned to the same physical
//!   CPU core under Linux `SCHED_IDLE`. Proactively executes handshakes (TLS, QUIC, RoCEv2 Queue Pairs) and deposits
//!   warmed connection handles into the core's dedicated lane shard (`shards[worker_id]`).
//!
//! ### Hardware Cache Locality Benefits:
//! 1. **L1/L2 Private Cache Warmth**: Because the frontline worker and shadow logistics worker run on the same
//!    physical CPU core, connection handle pointers, states, and file descriptors deposited by the shadow worker
//!    already reside hot in the core's private L1 Data Cache (32KB/48KB) and L2 Cache (512KB–1MB).
//! 2. **Elimination of Cross-Core Cache Invalidation**: Global pool architectures suffer from cache-line bouncing,
//!    where one core's allocation forces MESI/MOESI cache invalidation signals across other cores. Lane sharding
//!    confines allocations to the core's local shard, eliminating NUMA interconnect bus snooping (UPI/QPI) and
//!    inter-processor interrupts (IPIs) on the connection hot path.
//! 3. **Immutable Connection Identity Pre-Resolution**: Because connection handles are pre-warmed in the local
//!    cache, the frontline worker retrieves a fully established handle with known transport identity (Bit 1 RDMA
//!    QP vs Bit 0 Standard Socket) before reading client data, enabling zero-copy buffer selection without
//!    speculative allocation or intermediate user-space `memcpy`.

pub mod config;
pub mod container;
pub mod key;
pub mod lease;
pub mod pool;
pub mod profile;
pub mod resource;

// Flat canonical exports at crate root
pub use config::{DEFAULT_IDLE_TIMEOUT, DEFAULT_MAX_LIFETIME, PoolConfig, PoolStats};
pub use container::{
    FastBuildHasher, FastHasher, MAX_IDLE_PER_KEY, MAX_SHARDS, MIN_IDLE_PER_KEY, MIN_SHARDS,
    MultiplexedConnection, MultiplexedPool, MuxShard, PoolShard, ShardTable, SubPool,
    current_thread_lane, optimal_max_concurrent_streams, optimal_max_idle_per_key,
    optimal_shard_count, probed_max_concurrent_streams, probed_max_idle_per_key,
    probed_shard_count, set_current_thread_lane,
};
pub use key::ConnectionKey;
pub use lease::{
    ConnectionLease, ExclusiveLease, ExclusiveReturnTarget, SequentialLease, StreamLease,
};
pub use pool::PoolManager;
pub use profile::{ConnectionProfile, ReuseMode};
pub use resource::PoolableResource;
