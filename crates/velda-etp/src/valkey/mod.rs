//! Valkey External Third-Party (`velda-etp::valkey`) connection pool branched explicitly into
//! `standalone` and `cluster` topologies.
//!
//! ```text
//!   ValkeyEtpConfig (mode: Standalone | Cluster) ──► ValkeyEtp
//!     ├─► ValkeyBranch::Standalone(Arc<ValkeyNodePool>)
//!     └─► ValkeyBranch::Cluster(Arc<ValkeyClusterPool>) ──► SlotMap + CLUSTER SLOTS
//! ```

pub mod config;
pub mod lease;
pub mod pool;
pub mod slots;

pub use config::{ValkeyEtpConfig, ValkeyMode};
pub use lease::ValkeyLease;
pub use pool::{ValkeyClusterPool, ValkeyNodePool};
pub use slots::{SlotMap, crc16_slot};

use std::fmt;
use std::sync::Arc;

use crate::config::EtpEntryConfig;
use crate::error::EtpError;

#[derive(Debug, Clone)]
pub enum ValkeyBranch {
    Standalone(Arc<ValkeyNodePool>),
    Cluster(Arc<ValkeyClusterPool>),
}

pub struct ValkeyEtp {
    pub config: ValkeyEtpConfig,
    pub branch: ValkeyBranch,
}

impl fmt::Debug for ValkeyEtp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ValkeyEtp")
            .field("id", &self.config.id)
            .field("mode", &self.config.mode)
            .field("branch", &self.branch)
            .finish()
    }
}

impl ValkeyEtp {
    pub fn from_entry(entry: &EtpEntryConfig) -> Result<Self, EtpError> {
        let config = ValkeyEtpConfig::from_entry(entry)?;
        Self::new(config)
    }

    pub fn new(config: ValkeyEtpConfig) -> Result<Self, EtpError> {
        let branch = match config.mode {
            ValkeyMode::Standalone => {
                ValkeyBranch::Standalone(Arc::new(ValkeyNodePool::from_config(&config)?))
            }
            ValkeyMode::Cluster => {
                ValkeyBranch::Cluster(Arc::new(ValkeyClusterPool::new(config.clone())))
            }
        };
        Ok(Self { config, branch })
    }

    #[inline(always)]
    pub const fn is_cluster(&self) -> bool {
        matches!(self.branch, ValkeyBranch::Cluster(_))
    }

    #[inline]
    pub fn cluster(&self) -> Option<&Arc<ValkeyClusterPool>> {
        match &self.branch {
            ValkeyBranch::Cluster(c) => Some(c),
            ValkeyBranch::Standalone(_) => None,
        }
    }

    pub async fn warm(&self) -> Result<(), EtpError> {
        match &self.branch {
            ValkeyBranch::Standalone(p) => p.warm().await,
            ValkeyBranch::Cluster(p) => p.warm().await,
        }
    }

    #[inline]
    pub fn try_acquire(&self) -> Option<ValkeyLease> {
        match &self.branch {
            ValkeyBranch::Standalone(p) => p.try_acquire(),
            ValkeyBranch::Cluster(p) => p.try_acquire_any(),
        }
    }

    #[inline]
    pub async fn acquire(&self) -> Result<ValkeyLease, EtpError> {
        match &self.branch {
            ValkeyBranch::Standalone(p) => p.acquire().await,
            ValkeyBranch::Cluster(p) => p.acquire_any().await,
        }
    }

    #[inline]
    pub fn cluster_node_for_slot(&self, slot: u16) -> Option<usize> {
        match &self.branch {
            ValkeyBranch::Standalone(_) => None,
            ValkeyBranch::Cluster(c) => c.node_for_slot(slot),
        }
    }

    #[inline]
    pub async fn acquire_for_node(&self, node_idx: usize) -> Result<ValkeyLease, EtpError> {
        match &self.branch {
            ValkeyBranch::Standalone(p) => p.acquire().await,
            ValkeyBranch::Cluster(c) => c.acquire_for_node(node_idx).await,
        }
    }

    #[inline]
    pub async fn acquire_for_slot(&self, slot: u16) -> Result<ValkeyLease, EtpError> {
        match &self.branch {
            ValkeyBranch::Standalone(p) => p.acquire().await,
            ValkeyBranch::Cluster(c) => c.acquire_for_slot(slot).await,
        }
    }

    #[inline]
    pub async fn acquire_for_addr(&self, host_port: &str) -> Result<ValkeyLease, EtpError> {
        match &self.branch {
            ValkeyBranch::Standalone(p) => p.acquire().await,
            ValkeyBranch::Cluster(c) => c.acquire_for_addr(host_port).await,
        }
    }

    #[inline]
    pub fn report_moved(&self, slot: u16, target_host_port: &str) -> Result<usize, EtpError> {
        match &self.branch {
            ValkeyBranch::Standalone(_) => Err(EtpError::Config(format!(
                "Valkey ETP '{}' received cluster -MOVED redirect while configured with mode='standalone'; set mode='cluster' in etp.json",
                self.config.id
            ))),
            ValkeyBranch::Cluster(c) => c.report_moved(slot, target_host_port),
        }
    }
}
