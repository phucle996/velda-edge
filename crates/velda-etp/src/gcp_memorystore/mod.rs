//! GCP Memorystore (`velda-etp::gcp_memorystore`) IAM-authenticated ETP supporting
//! `standalone` and `cluster` topologies.
//!
//! ```text
//!   GcpMemorystoreEtpConfig (GCP IAM Auth only; static password rejected)
//!     ├──► GcpTokenProvider (shared cached OAuth2 / Metadata / ServiceAccount JWT token)
//!     ├─► Standalone(Arc<GcpMemorystoreNodePool>)
//!     └─► Cluster(Arc<GcpMemorystoreClusterPool>)
//! ```

pub mod config;
pub mod lease;
pub mod pool;
pub mod slots;

pub use config::{GcpIamAuthSource, GcpMemorystoreEtpConfig, GcpMemorystoreMode};
pub use lease::GcpMemorystoreLease;
pub use pool::{GcpMemorystoreClusterPool, GcpMemorystoreNodePool, GcpTokenProvider};
pub use slots::{SlotMap, crc16_slot};

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use crate::config::EtpEntryConfig;
use crate::error::EtpError;

#[derive(Debug, Clone)]
pub enum GcpMemorystoreBranch {
    Standalone(Arc<GcpMemorystoreNodePool>),
    Cluster(Arc<GcpMemorystoreClusterPool>),
}

/// Top-level GCP Memorystore ETP handle dispatches cleanly to Standalone or Cluster.
pub struct GcpMemorystoreEtp {
    pub config: GcpMemorystoreEtpConfig,
    pub branch: GcpMemorystoreBranch,
}

impl fmt::Debug for GcpMemorystoreEtp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GcpMemorystoreEtp")
            .field("id", &self.config.id)
            .field("mode", &self.config.mode)
            .finish()
    }
}

impl GcpMemorystoreEtp {
    pub fn from_entry(entry: &EtpEntryConfig) -> Result<Self, EtpError> {
        let config = GcpMemorystoreEtpConfig::from_entry(entry)?;
        Self::new(config)
    }

    pub fn new(config: GcpMemorystoreEtpConfig) -> Result<Self, EtpError> {
        let timeout = Duration::from_millis(config.pool.connect_timeout_ms.max(1));
        let token_provider = Arc::new(GcpTokenProvider::new(
            &config.id,
            config.username.clone(),
            &config.auth_source,
            config.tls.as_ref(),
            timeout,
        )?);
        let branch = match config.mode {
            GcpMemorystoreMode::Standalone => GcpMemorystoreBranch::Standalone(Arc::new(
                GcpMemorystoreNodePool::from_config(&config, token_provider)?,
            )),
            GcpMemorystoreMode::Cluster => GcpMemorystoreBranch::Cluster(Arc::new(
                GcpMemorystoreClusterPool::new(config.clone(), token_provider),
            )),
        };
        Ok(Self { config, branch })
    }

    #[inline(always)]
    pub const fn is_cluster(&self) -> bool {
        matches!(self.branch, GcpMemorystoreBranch::Cluster(_))
    }

    #[inline]
    pub fn cluster(&self) -> Option<&Arc<GcpMemorystoreClusterPool>> {
        match &self.branch {
            GcpMemorystoreBranch::Cluster(c) => Some(c),
            GcpMemorystoreBranch::Standalone(_) => None,
        }
    }

    pub async fn warm(&self) -> Result<(), EtpError> {
        match &self.branch {
            GcpMemorystoreBranch::Standalone(p) => p.warm().await,
            GcpMemorystoreBranch::Cluster(p) => p.warm().await,
        }
    }

    #[inline]
    pub fn try_acquire(&self) -> Option<GcpMemorystoreLease> {
        match &self.branch {
            GcpMemorystoreBranch::Standalone(p) => p.try_acquire(),
            GcpMemorystoreBranch::Cluster(p) => p.try_acquire_any(),
        }
    }

    pub async fn acquire(&self) -> Result<GcpMemorystoreLease, EtpError> {
        match &self.branch {
            GcpMemorystoreBranch::Standalone(p) => p.acquire().await,
            GcpMemorystoreBranch::Cluster(p) => p.acquire_any().await,
        }
    }

    #[inline]
    pub fn cluster_node_for_slot(&self, slot: u16) -> Option<usize> {
        match &self.branch {
            GcpMemorystoreBranch::Standalone(_) => None,
            GcpMemorystoreBranch::Cluster(c) => c.node_for_slot(slot),
        }
    }

    #[inline]
    pub async fn acquire_for_node(&self, node_idx: usize) -> Result<GcpMemorystoreLease, EtpError> {
        match &self.branch {
            GcpMemorystoreBranch::Standalone(p) => p.acquire().await,
            GcpMemorystoreBranch::Cluster(c) => c.acquire_for_node(node_idx).await,
        }
    }

    #[inline]
    pub async fn acquire_for_slot(&self, slot: u16) -> Result<GcpMemorystoreLease, EtpError> {
        match &self.branch {
            GcpMemorystoreBranch::Standalone(p) => p.acquire().await,
            GcpMemorystoreBranch::Cluster(c) => c.acquire_for_slot(slot).await,
        }
    }

    #[inline]
    pub async fn acquire_for_addr(&self, host_port: &str) -> Result<GcpMemorystoreLease, EtpError> {
        match &self.branch {
            GcpMemorystoreBranch::Standalone(p) => p.acquire().await,
            GcpMemorystoreBranch::Cluster(c) => c.acquire_for_addr(host_port).await,
        }
    }

    #[inline]
    pub fn report_moved(&self, slot: u16, target_host_port: &str) -> Result<usize, EtpError> {
        match &self.branch {
            GcpMemorystoreBranch::Standalone(_) => Err(EtpError::Config(format!(
                "GcpMemorystore ETP '{}' received cluster -MOVED redirect while configured with mode='standalone'",
                self.config.id
            ))),
            GcpMemorystoreBranch::Cluster(c) => c.report_moved(slot, target_host_port),
        }
    }
}
