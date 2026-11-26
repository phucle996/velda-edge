//! Auth0 ETP connection pool (`velda-etp::auth0`).
//!
//! ```text
//!   EtpEntryConfig (kind == EtpKind::Auth0)
//!         └──► Auth0EtpConfig (domain/endpoint, port, host_header, tls, client creds)
//!                └──► Auth0Etp (StreamPool) -> acquire() -> StreamLease
//! ```

use std::fmt;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::config::{EtpEntryConfig, EtpKind, EtpPoolSettings, extract_http_endpoint_fields};
use crate::connect::EtpTlsConfig;
use crate::error::EtpError;
use crate::stream_pool::{StreamLease, StreamPool};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Auth0EtpConfig {
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

impl Auth0EtpConfig {
    pub fn from_entry(entry: &EtpEntryConfig) -> Result<Self, EtpError> {
        let f = extract_http_endpoint_fields(entry, EtpKind::Auth0)?;
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

pub struct Auth0Etp {
    pub config: Auth0EtpConfig,
    pub pool: Arc<StreamPool>,
}

impl fmt::Debug for Auth0Etp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Auth0Etp")
            .field("id", &self.config.id)
            .field("endpoint", &self.config.endpoint)
            .finish()
    }
}

impl Auth0Etp {
    pub fn from_entry(entry: &EtpEntryConfig) -> Result<Self, EtpError> {
        let cfg = Auth0EtpConfig::from_entry(entry)?;
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
