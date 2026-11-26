//! AWS ElastiCache (`velda-etp::aws_elasticache`) IAM-authenticated ETP supporting
//! `standalone` and `cluster` topologies.
//!
//! ```text
//!   AwsElastiCacheEtpConfig (AWS IAM Auth only; static password rejected)
//!     ├─► Standalone(Arc<AwsElastiCacheNodePool>)
//!     └─► Cluster(Arc<AwsElastiCacheClusterPool>)
//! ```

pub mod config;
pub mod lease;
pub mod pool;
pub mod slots;

pub use config::{AwsElastiCacheEtpConfig, AwsElastiCacheMode};
pub use lease::AwsElastiCacheLease;
pub use pool::{AwsElastiCacheClusterPool, AwsElastiCacheNodePool, AwsIamSigner};
pub use slots::{SlotMap, crc16_slot};

use std::fmt;
use std::sync::Arc;

use crate::config::EtpEntryConfig;
use crate::error::EtpError;

#[derive(Debug, Clone)]
pub enum AwsElastiCacheBranch {
    Standalone(Arc<AwsElastiCacheNodePool>),
    Cluster(Arc<AwsElastiCacheClusterPool>),
}

pub struct AwsElastiCacheEtp {
    pub config: AwsElastiCacheEtpConfig,
    pub branch: AwsElastiCacheBranch,
}

impl fmt::Debug for AwsElastiCacheEtp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AwsElastiCacheEtp")
            .field("id", &self.config.id)
            .field("mode", &self.config.mode)
            .field("region", &self.config.region)
            .finish()
    }
}

impl AwsElastiCacheEtp {
    pub fn from_entry(entry: &EtpEntryConfig) -> Result<Self, EtpError> {
        let config = AwsElastiCacheEtpConfig::from_entry(entry)?;
        Self::new(config)
    }

    pub fn new(config: AwsElastiCacheEtpConfig) -> Result<Self, EtpError> {
        let branch = match config.mode {
            AwsElastiCacheMode::Standalone => AwsElastiCacheBranch::Standalone(Arc::new(
                AwsElastiCacheNodePool::from_config(&config)?,
            )),
            AwsElastiCacheMode::Cluster => AwsElastiCacheBranch::Cluster(Arc::new(
                AwsElastiCacheClusterPool::new(config.clone()),
            )),
        };
        Ok(Self { config, branch })
    }

    #[inline(always)]
    pub const fn is_cluster(&self) -> bool {
        matches!(self.branch, AwsElastiCacheBranch::Cluster(_))
    }

    #[inline]
    pub fn cluster(&self) -> Option<&Arc<AwsElastiCacheClusterPool>> {
        match &self.branch {
            AwsElastiCacheBranch::Cluster(c) => Some(c),
            AwsElastiCacheBranch::Standalone(_) => None,
        }
    }

    pub async fn warm(&self) -> Result<(), EtpError> {
        match &self.branch {
            AwsElastiCacheBranch::Standalone(p) => p.warm().await,
            AwsElastiCacheBranch::Cluster(p) => p.warm().await,
        }
    }

    #[inline]
    pub fn try_acquire(&self) -> Option<AwsElastiCacheLease> {
        match &self.branch {
            AwsElastiCacheBranch::Standalone(p) => p.try_acquire(),
            AwsElastiCacheBranch::Cluster(p) => p.try_acquire_any(),
        }
    }

    #[inline]
    pub async fn acquire(&self) -> Result<AwsElastiCacheLease, EtpError> {
        match &self.branch {
            AwsElastiCacheBranch::Standalone(p) => p.acquire().await,
            AwsElastiCacheBranch::Cluster(p) => p.acquire_any().await,
        }
    }

    #[inline]
    pub fn cluster_node_for_slot(&self, slot: u16) -> Option<usize> {
        match &self.branch {
            AwsElastiCacheBranch::Standalone(_) => None,
            AwsElastiCacheBranch::Cluster(c) => c.node_for_slot(slot),
        }
    }

    #[inline]
    pub async fn acquire_for_node(&self, node_idx: usize) -> Result<AwsElastiCacheLease, EtpError> {
        match &self.branch {
            AwsElastiCacheBranch::Standalone(p) => p.acquire().await,
            AwsElastiCacheBranch::Cluster(c) => c.acquire_for_node(node_idx).await,
        }
    }

    #[inline]
    pub async fn acquire_for_slot(&self, slot: u16) -> Result<AwsElastiCacheLease, EtpError> {
        match &self.branch {
            AwsElastiCacheBranch::Standalone(p) => p.acquire().await,
            AwsElastiCacheBranch::Cluster(c) => c.acquire_for_slot(slot).await,
        }
    }

    #[inline]
    pub async fn acquire_for_addr(&self, host_port: &str) -> Result<AwsElastiCacheLease, EtpError> {
        match &self.branch {
            AwsElastiCacheBranch::Standalone(p) => p.acquire().await,
            AwsElastiCacheBranch::Cluster(c) => c.acquire_for_addr(host_port).await,
        }
    }

    #[inline]
    pub fn report_moved(&self, slot: u16, target_host_port: &str) -> Result<usize, EtpError> {
        match &self.branch {
            AwsElastiCacheBranch::Standalone(_) => Err(EtpError::Config(format!(
                "AwsElastiCache ETP '{}' received cluster -MOVED redirect while configured with mode='standalone'",
                self.config.id
            ))),
            AwsElastiCacheBranch::Cluster(c) => c.report_moved(slot, target_host_port),
        }
    }
}
