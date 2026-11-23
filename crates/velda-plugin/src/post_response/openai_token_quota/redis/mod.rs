//! Distributed Tier (`redis/`): Pooled `RedisEtp` RESP2 Lua `EVAL` Sync Loop.

pub mod client;

pub use client::{RedisSyncParams, run_redis_sync_loop};
