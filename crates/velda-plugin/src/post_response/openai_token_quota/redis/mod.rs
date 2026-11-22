//! Distributed Tier (`redis/`): Dual-Connection Redis Client & Background Sync Loop.

pub mod client;

pub use client::{RedisQuotaClient, RedisSyncParams, parse_redis_addr, run_redis_sync_loop};
