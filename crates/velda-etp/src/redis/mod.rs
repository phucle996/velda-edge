//! Redis External Third-Party (`velda-etp::redis`) explicitly branched into independent
//! `standalone` (single endpoint + PageArena) and `cluster` (seed discovery + CRC16 SlotMap)
//! connection engines.
//!
//! ```text
//!   EtpEntryConfig (kind == EtpKind::Redis) ──► RedisEtpConfig::from_entry()
//!         │
//!         ├─► mode == "standalone" ──► RedisBranch::Standalone(RedisStandalonePool)
//!         │                              └─► 1 Blueprint + 1 EtpPageArena (zero routing overhead)
//!         │
//!         └─► mode == "cluster"    ──► RedisBranch::Cluster(RedisClusterPool)
//!                                        └─► CLUSTER SLOTS -> SlotMap (Ranges | #[cold] Dense)
//!                                            + per-Master RedisStandalonePool map
//! ```

pub mod cluster;
pub mod config;
pub mod lease;
pub mod standalone;

pub use cluster::{RedisClusterPool, SlotMap, crc16_slot};
pub use config::{RedisEtpConfig, RedisMode};
pub use lease::RedisLease;
pub use standalone::RedisStandalonePool;

use std::fmt;
use std::sync::Arc;

use crate::config::EtpEntryConfig;
use crate::error::EtpError;

/// Isolated Redis topology connection branch.
#[derive(Debug, Clone)]
pub enum RedisBranch {
    Standalone(Arc<RedisStandalonePool>),
    Cluster(Arc<RedisClusterPool>),
}

/// Top-level Redis ETP handle dispatched by [`RedisMode`] into explicit `standalone` or
/// `cluster` connection engines.
pub struct RedisEtp {
    pub config: RedisEtpConfig,
    pub branch: RedisBranch,
}

impl fmt::Debug for RedisEtp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RedisEtp")
            .field("id", &self.config.id)
            .field("mode", &self.config.mode)
            .field("branch", &self.branch)
            .finish()
    }
}

impl RedisEtp {
    /// Creates a [`RedisEtp`] handle from a generic [`EtpEntryConfig`].
    pub fn from_entry(entry: &EtpEntryConfig) -> Result<Self, EtpError> {
        let config = RedisEtpConfig::from_entry(entry)?;
        Self::new(config)
    }

    /// Creates a [`RedisEtp`] dispatching cleanly into [`RedisBranch::Standalone`] or
    /// [`RedisBranch::Cluster`].
    pub fn new(config: RedisEtpConfig) -> Result<Self, EtpError> {
        let branch = match config.mode {
            RedisMode::Standalone => {
                RedisBranch::Standalone(Arc::new(RedisStandalonePool::from_config(&config)?))
            }
            RedisMode::Cluster => {
                RedisBranch::Cluster(Arc::new(RedisClusterPool::new(config.clone())?))
            }
        };
        Ok(Self { config, branch })
    }

    /// Returns `true` when configured with `mode == "cluster"`.
    #[inline(always)]
    pub const fn is_cluster(&self) -> bool {
        matches!(self.branch, RedisBranch::Cluster(_))
    }

    /// Returns the underlying standalone pool when in `standalone` mode.
    #[inline]
    pub fn standalone(&self) -> Option<&Arc<RedisStandalonePool>> {
        match &self.branch {
            RedisBranch::Standalone(pool) => Some(pool),
            RedisBranch::Cluster(_) => None,
        }
    }

    /// Returns the underlying cluster pool when in `cluster` mode.
    #[inline]
    pub fn cluster(&self) -> Option<&Arc<RedisClusterPool>> {
        match &self.branch {
            RedisBranch::Cluster(pool) => Some(pool),
            RedisBranch::Standalone(_) => None,
        }
    }

    /// Pre-warms baseline connections on the active topology branch.
    pub async fn warm(&self) -> Result<(), EtpError> {
        match &self.branch {
            RedisBranch::Standalone(pool) => pool.warm().await,
            RedisBranch::Cluster(pool) => pool.warm().await,
        }
    }

    /// Non-blocking lease attempt against the active branch.
    #[inline]
    pub fn try_acquire(&self) -> Option<RedisLease> {
        match &self.branch {
            RedisBranch::Standalone(pool) => pool.try_acquire(),
            RedisBranch::Cluster(pool) => pool.try_acquire_any(),
        }
    }

    /// Acquires an authenticated Redis session lease (`Standalone` fast path, or `Cluster`
    /// shard fallback).
    #[inline]
    pub async fn acquire(&self) -> Result<RedisLease, EtpError> {
        match &self.branch {
            RedisBranch::Standalone(pool) => pool.acquire().await,
            RedisBranch::Cluster(pool) => pool.acquire_any().await,
        }
    }

    /// Resolves Master `node_idx` owning `slot` in Cluster mode (`None` in Standalone mode).
    #[inline]
    pub fn cluster_node_for_slot(&self, slot: u16) -> Option<usize> {
        match &self.branch {
            RedisBranch::Standalone(_) => None,
            RedisBranch::Cluster(pool) => pool.node_for_slot(slot),
        }
    }

    /// Acquires a [`RedisLease`] for a specific Cluster Master `node_idx` (or standalone pool).
    #[inline]
    pub async fn acquire_for_node(&self, node_idx: usize) -> Result<RedisLease, EtpError> {
        match &self.branch {
            RedisBranch::Standalone(pool) => pool.acquire().await,
            RedisBranch::Cluster(pool) => pool.acquire_for_node(node_idx).await,
        }
    }

    /// Acquires a [`RedisLease`] directly for the Master node owning `slot`.
    #[inline]
    pub async fn acquire_for_slot(&self, slot: u16) -> Result<RedisLease, EtpError> {
        match &self.branch {
            RedisBranch::Standalone(pool) => pool.acquire().await,
            RedisBranch::Cluster(pool) => pool.acquire_for_slot(slot).await,
        }
    }

    /// Acquires a [`RedisLease`] targeting an explicit `host:port` (for Cluster `-ASK` redirects).
    #[inline]
    pub async fn acquire_for_addr(&self, host_port: &str) -> Result<RedisLease, EtpError> {
        match &self.branch {
            RedisBranch::Standalone(pool) => pool.acquire().await,
            RedisBranch::Cluster(pool) => pool.acquire_for_addr(host_port).await,
        }
    }

    /// Updates Cluster topology after a plugin receives `-MOVED <slot> <target_host_port>`.
    #[inline]
    pub fn report_moved(&self, slot: u16, target_host_port: &str) -> Result<usize, EtpError> {
        match &self.branch {
            RedisBranch::Standalone(_) => Err(EtpError::Config(format!(
                "Redis ETP '{}' received cluster -MOVED redirect while configured with mode='standalone'; set mode='cluster' in etp.json",
                self.config.id
            ))),
            RedisBranch::Cluster(pool) => pool.report_moved(slot, target_host_port),
        }
    }
}
