//! AWS DynamoDB ETP Connection Pool (`velda-etp::dynamodb`).
//!
//! ```text
//!   EtpEntryConfig (kind == EtpKind::DynamoDb)
//!         └──► DynamoDbEtpConfig (endpoint/region, SigV4 keys, host_header)
//!                └──► DynamoDbEtp (StreamPool) -> acquire() -> StreamLease
//! ```

use std::fmt;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::config::{
    EtpEntryConfig, EtpKind, EtpPoolSettings, format_host_header, synthesize_endpoint_uri,
};
use crate::connect::{EtpTlsConfig, EtpTransportSecurity};
use crate::error::EtpError;
use crate::stream_pool::{StreamLease, StreamPool};

/// Validated configuration for an AWS DynamoDB (or DynamoDB Local) ETP pool.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DynamoDbEtpConfig {
    pub id: String,
    pub endpoint: String,
    pub host: String,
    pub port: u16,
    pub host_header: String,
    pub region: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
    pub pool: EtpPoolSettings,
    pub tls: Option<EtpTlsConfig>,
}

impl DynamoDbEtpConfig {
    /// Parses a [`DynamoDbEtpConfig`] from a validated [`EtpEntryConfig`].
    pub fn from_entry(entry: &EtpEntryConfig) -> Result<Self, EtpError> {
        if entry.kind != EtpKind::DynamoDb {
            return Err(EtpError::KindMismatch {
                id: entry.id.clone(),
                expected: EtpKind::DynamoDb.as_str(),
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
                "DynamoDB ETP '{}' settings must be an object",
                entry.id
            ))
        })?;

        let region = obj
            .get("region")
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or("us-east-1")
            .to_string();

        let is_tls = entry.is_tls_transport();
        let scheme = if is_tls { "https" } else { "http" };
        let endpoint = synthesize_endpoint_uri(scheme, &host, port, "");
        let host_header = format_host_header(&host, port, is_tls);

        let opt_str = |k: &str| {
            obj.get(k)
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(ToString::to_string)
        };

        let access_key_id = opt_str("access_key_id").unwrap_or_default();
        let secret_access_key = opt_str("secret_access_key").unwrap_or_default();
        let session_token = opt_str("session_token");

        if !access_key_id.is_empty() && secret_access_key.is_empty() {
            return Err(EtpError::Config(format!(
                "DynamoDB ETP '{}' specifies access_key_id without secret_access_key",
                entry.id
            )));
        }

        Ok(Self {
            id: entry.id.clone(),
            endpoint,
            host,
            port,
            host_header,
            region,
            access_key_id,
            secret_access_key,
            session_token,
            pool: entry.pool.clone(),
            tls: entry.effective_tls(),
        })
    }
}

/// Lock-free pooled connection handle manager for AWS DynamoDB SoT storage.
pub struct DynamoDbEtp {
    pub config: DynamoDbEtpConfig,
    pub pool: Arc<StreamPool>,
}

impl fmt::Debug for DynamoDbEtp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DynamoDbEtp")
            .field("id", &self.config.id)
            .field("endpoint", &self.config.endpoint)
            .field("region", &self.config.region)
            .finish()
    }
}

impl DynamoDbEtp {
    /// Constructs a [`DynamoDbEtp`] pool from a declarative [`EtpEntryConfig`].
    pub fn from_entry(entry: &EtpEntryConfig) -> Result<Self, EtpError> {
        let config = DynamoDbEtpConfig::from_entry(entry)?;
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
