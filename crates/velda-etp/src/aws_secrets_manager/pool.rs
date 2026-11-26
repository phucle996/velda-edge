//! Lock-free AWS Secrets Manager ETP connection pool (`AwsSecretsManagerEtp`).
//!
//! ```text
//!   AwsSecretsManagerEtp::warm() / acquire() ──► StreamPool ──► StreamLease
//! ```

use std::fmt;
use std::sync::Arc;

use crate::aws_secrets_manager::config::AwsSecretsManagerEtpConfig;
use crate::config::EtpEntryConfig;
use crate::error::EtpError;
use crate::stream_pool::{StreamLease, StreamPool};

/// Bounded pool of TLS connections to an AWS Secrets Manager endpoint.
pub struct AwsSecretsManagerEtp {
    pub config: AwsSecretsManagerEtpConfig,
    pub pool: Arc<StreamPool>,
}

impl fmt::Debug for AwsSecretsManagerEtp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AwsSecretsManagerEtp")
            .field("id", &self.config.id)
            .field("endpoint", &self.config.endpoint)
            .field("region", &self.config.region)
            .field("baseline_slots", &self.pool.arena.baseline_slots)
            .finish()
    }
}

impl AwsSecretsManagerEtp {
    pub fn from_entry(entry: &EtpEntryConfig) -> Result<Self, EtpError> {
        let cfg = AwsSecretsManagerEtpConfig::from_entry(entry)?;
        let pool = Arc::new(StreamPool::new(
            &cfg.id,
            &cfg.host,
            cfg.port,
            cfg.tls.as_ref(),
            &cfg.pool,
        )?);
        Ok(Self { config: cfg, pool })
    }

    pub async fn warm(&self) -> Result<(), EtpError> {
        self.pool.warm().await
    }

    #[inline]
    pub async fn acquire(&self) -> Result<StreamLease, EtpError> {
        self.pool.acquire().await
    }
}
