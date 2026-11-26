//! Microsoft Entra ID (Azure AD) ETP connection pool (`velda-etp::entra_id`).
//!
//! ```text
//!   EtpEntryConfig (kind == EtpKind::EntraId)
//!         └──► EntraIdEtpConfig (tenant_id/endpoint, host, port, tls, client creds)
//!                └──► EntraIdEtp (StreamPool) -> acquire() -> StreamLease
//! ```

use std::fmt;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::config::{EtpEntryConfig, EtpKind, EtpPoolSettings, extract_http_endpoint_fields};
use crate::connect::EtpTlsConfig;
use crate::error::EtpError;
use crate::stream_pool::{StreamLease, StreamPool};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EntraIdEtpConfig {
    pub id: String,
    pub endpoint: String,
    pub host: String,
    pub port: u16,
    pub host_header: String,
    pub bearer_token: Option<String>,
    pub client_id: Option<String>,
    pub client_secret: Option<String>,
    pub custom_headers: Vec<(String, String)>,
    pub pool: EtpPoolSettings,
    pub tls: Option<EtpTlsConfig>,
}

impl EntraIdEtpConfig {
    pub fn from_entry(entry: &EtpEntryConfig) -> Result<Self, EtpError> {
        let f = extract_http_endpoint_fields(entry, EtpKind::EntraId)?;
        Ok(Self {
            id: entry.id.clone(),
            endpoint: f.endpoint,
            host: f.host,
            port: f.port,
            host_header: f.host_header,
            bearer_token: f.bearer_token,
            client_id: f.client_id,
            client_secret: f.client_secret,
            custom_headers: f.custom_headers,
            pool: entry.pool.clone(),
            tls: f.tls,
        })
    }
}

pub struct EntraIdEtp {
    pub config: EntraIdEtpConfig,
    pub pool: Arc<StreamPool>,
}

impl fmt::Debug for EntraIdEtp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EntraIdEtp")
            .field("id", &self.config.id)
            .field("endpoint", &self.config.endpoint)
            .finish()
    }
}

impl EntraIdEtp {
    pub fn from_entry(entry: &EtpEntryConfig) -> Result<Self, EtpError> {
        let cfg = EntraIdEtpConfig::from_entry(entry)?;
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
    pub fn try_acquire(&self) -> Option<StreamLease> {
        self.pool.try_acquire()
    }

    #[inline]
    pub async fn acquire(&self) -> Result<StreamLease, EtpError> {
        self.pool.acquire().await
    }
}
