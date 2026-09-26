//! Generic high-performance connection pool manager.
//!
//! Invariants:
//! - **Container-First**: Pool is a container lazy-created or pre-registered for a `ConnectionKey`.
//! - **Resource Lifecycle**: Connections are resources created after and registered into the pool.
//! - **Zero connection establishment logic**: Pool NEVER calls `connect()` or DNS lookups.
//! - **Zero disk I/O, zero JSON parsing, zero network I/O**.
//! - **Sharded concurrency**: Keys partitioned across independent shards to eliminate multicore lock contention.

pub mod acquire;
pub mod config;
pub mod pool;
pub mod profile;
pub mod sweep;

pub use config::{PoolConfig, PoolStats};
pub use pool::PoolManager;
