//! Memcached ETP Connection Pool (`velda-etp::memcached`).
//!
//! ```text
//!   EtpEntryConfig (kind == EtpKind::Memcached)
//!         └──► MemcachedEtpConfig (host, port, key_prefix, pool, tls)
//!                └──► MemcachedEtp (StreamPool) -> acquire() -> StreamLease
//! ```

use std::fmt;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::config::{EtpEntryConfig, EtpKind, EtpPoolSettings};
use crate::connect::{EtpTlsConfig, EtpTransportSecurity};
use crate::error::EtpError;
use crate::stream_pool::{StreamLease, StreamPool};

/// Validated configuration for a Memcached / ElastiCache Memcached ETP pool.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemcachedEtpConfig {
    pub id: String,
    pub host: String,
    pub port: u16,
    pub key_prefix: Option<String>,
    pub pool: EtpPoolSettings,
    pub tls: Option<EtpTlsConfig>,
}

impl MemcachedEtpConfig {
    /// Parses a [`MemcachedEtpConfig`] from a validated [`EtpEntryConfig`].
    pub fn from_entry(entry: &EtpEntryConfig) -> Result<Self, EtpError> {
        if entry.kind != EtpKind::Memcached {
            return Err(EtpError::KindMismatch {
                id: entry.id.clone(),
                expected: EtpKind::Memcached.as_str(),
                actual: entry.kind.as_str(),
            });
        }
        entry.validate()?;
        entry.require_security(&[
            EtpTransportSecurity::Plain,
            EtpTransportSecurity::Tls,
            EtpTransportSecurity::Mtls,
        ])?;
        let (host, port) = entry.resolve_endpoint()?;
        let obj = entry.settings.as_object().ok_or_else(|| {
            EtpError::Config(format!(
                "Memcached ETP '{}' settings must be an object",
                entry.id
            ))
        })?;

        let key_prefix = obj
            .get("key_prefix")
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(ToString::to_string);

        Ok(Self {
            id: entry.id.clone(),
            host,
            port,
            key_prefix,
            pool: entry.pool.clone(),
            tls: entry.effective_tls(),
        })
    }
}

/// Lock-free pooled connection handle manager for Memcached SoT storage.
pub struct MemcachedEtp {
    pub config: MemcachedEtpConfig,
    pub pool: Arc<StreamPool>,
}

impl fmt::Debug for MemcachedEtp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemcachedEtp")
            .field("id", &self.config.id)
            .field("host", &self.config.host)
            .field("port", &self.config.port)
            .finish()
    }
}

impl MemcachedEtp {
    /// Constructs a [`MemcachedEtp`] pool from a declarative [`EtpEntryConfig`].
    pub fn from_entry(entry: &EtpEntryConfig) -> Result<Self, EtpError> {
        let config = MemcachedEtpConfig::from_entry(entry)?;
        let pool = Arc::new(StreamPool::new(
            &config.id,
            &config.host,
            config.port,
            config.tls.as_ref(),
            &config.pool,
        )?);
        Ok(Self { config, pool })
    }

    #[inline]
    pub async fn warm(&self) -> Result<(), EtpError> {
        self.pool.warm().await
    }

    #[inline]
    pub async fn acquire(&self) -> Result<StreamLease, EtpError> {
        self.pool.acquire().await
    }

    #[inline]
    pub fn try_acquire(&self) -> Option<StreamLease> {
        self.pool.try_acquire()
    }
}
