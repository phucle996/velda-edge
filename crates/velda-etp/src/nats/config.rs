//! NATS ETP configuration parser (`NatsEtpConfig`).
//!
//! ```text
//!   EtpEntryConfig (kind == EtpKind::Nats)
//!         │
//!         ├─► entry.validate() + resolve_endpoint() ──► (host, port) [default 4222]
//!         ├─► extract username, password, token, bucket (default "velda_rate_limits")
//!         └─► Ok(NatsEtpConfig)
//!
//!   Parsed Configuration Layout:
//!   ┌────────────────────────────────────────────────────────────────────────────┐
//!   │ id | host | port | username | password | token | bucket                    │
//!   │ pool: EtpPoolSettings (pool_size, connect_timeout_ms, idle_timeout_ms)     │
//!   │ tls: Option<EtpTlsConfig>                                                  │
//!   └────────────────────────────────────────────────────────────────────────────┘
//! ```

use serde::{Deserialize, Serialize};

use crate::config::{EtpEntryConfig, EtpKind, EtpPoolSettings};
use crate::connect::{EtpTlsConfig, EtpTransportSecurity};
use crate::error::EtpError;

/// Tailored configuration for a NATS / JetStream external third-party pool.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NatsEtpConfig {
    pub id: String,
    pub host: String,
    pub port: u16,
    pub username: Option<String>,
    pub password: Option<String>,
    pub token: Option<String>,
    pub bucket: String,
    pub security: EtpTransportSecurity,
    pub pool: EtpPoolSettings,
    pub tls: Option<EtpTlsConfig>,
}

impl NatsEtpConfig {
    /// Parses a [`NatsEtpConfig`] from a validated [`EtpEntryConfig`].
    pub fn from_entry(entry: &EtpEntryConfig) -> Result<Self, EtpError> {
        if entry.kind != EtpKind::Nats {
            return Err(EtpError::KindMismatch {
                id: entry.id.clone(),
                expected: EtpKind::Nats.as_str(),
                actual: entry.kind.as_str(),
            });
        }
        entry.validate()?;
        entry.require_security(&[EtpTransportSecurity::Plain, EtpTransportSecurity::StartTls])?;
        let (host, port) = entry.resolve_endpoint()?;
        let obj = entry.settings.as_object().ok_or_else(|| {
            EtpError::Config(format!(
                "NATS ETP '{}' settings must be an object",
                entry.id
            ))
        })?;

        let username = obj
            .get("username")
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(ToString::to_string);

        let password = obj
            .get("password")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(ToString::to_string);

        let token = obj
            .get("token")
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(ToString::to_string);

        let bucket = obj
            .get("bucket")
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or("velda_rate_limits")
            .to_string();

        Ok(Self {
            id: entry.id.clone(),
            host,
            port,
            username,
            password,
            token,
            bucket,
            security: entry.security,
            pool: entry.pool.clone(),
            tls: entry.effective_tls(),
        })
    }
}
