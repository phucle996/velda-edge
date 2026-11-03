//! Distributed Multi-Gateway Synchronization Module.

pub mod client;
pub mod config;

pub use client::{RedisClient, RedisError};
pub use config::DistributedSyncConfig;
