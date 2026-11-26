//! Upstash Redis HTTPS REST API ETP (`velda-etp::upstash`).
//!
//! ```text
//!   EtpEntryConfig (kind == EtpKind::Upstash)
//!         └──► UpstashEtpConfig (endpoint, host_header, base_path, token)
//!                └──► UpstashEtp (StreamPool HTTPS/HTTP keep-alive) -> StreamLease
//! ```

use std::fmt;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::config::{EtpEntryConfig, EtpKind, EtpPoolSettings, format_host_header, read_base_path};
use crate::connect::{EtpTlsConfig, EtpTransportSecurity};
use crate::error::EtpError;
use crate::stream_pool::{StreamLease, StreamPool};

/// Configuration for Upstash serverless Redis REST API (`/pipeline`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpstashEtpConfig {
    pub id: String,
    pub host: String,
    pub port: u16,
    pub host_header: String,
    pub base_path: String,
    pub token: String,
    pub key_prefix: Option<String>,
    pub pool: EtpPoolSettings,
    pub tls: Option<EtpTlsConfig>,
}

impl UpstashEtpConfig {
    /// Parses [`UpstashEtpConfig`] requiring host/port and REST `token`.
    pub fn from_entry(entry: &EtpEntryConfig) -> Result<Self, EtpError> {
        if entry.kind != EtpKind::Upstash {
            return Err(EtpError::KindMismatch {
                id: entry.id.clone(),
                expected: EtpKind::Upstash.as_str(),
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
                "Upstash ETP '{}' settings must be an object",
                entry.id
            ))
        })?;

        let is_tls = entry.is_tls_transport();
        let host_header = format_host_header(&host, port, is_tls);
        let base_path = read_base_path(obj, &entry.id)?;

        let token = obj
            .get("token")
            .or_else(|| obj.get("bearer_token"))
            .or_else(|| obj.get("password"))
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                EtpError::Config(format!(
                    "Upstash ETP '{}' requires a non-empty REST 'token'",
                    entry.id
                ))
            })?
            .to_string();

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
            host_header,
            base_path,
            token,
            key_prefix,
            pool: entry.pool.clone(),
            tls: entry.effective_tls(),
        })
    }
}

/// Pooled HTTPS/HTTP keep-alive transport manager for Upstash REST `/pipeline`.
pub struct UpstashEtp {
    pub config: UpstashEtpConfig,
    pub pool: Arc<StreamPool>,
}

impl fmt::Debug for UpstashEtp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UpstashEtp")
            .field("id", &self.config.id)
            .field("host", &self.config.host)
            .field("port", &self.config.port)
            .finish()
    }
}

impl UpstashEtp {
    pub fn from_entry(entry: &EtpEntryConfig) -> Result<Self, EtpError> {
        let config = UpstashEtpConfig::from_entry(entry)?;
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
