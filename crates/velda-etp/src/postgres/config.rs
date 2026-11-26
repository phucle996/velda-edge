//! PostgreSQL ETP configuration parser (`PostgresEtpConfig`).
//!
//! ```text
//!   EtpEntryConfig (kind == EtpKind::Postgres)
//!         │
//!         ├─► entry.validate() + resolve_endpoint() ──► (host, port) [default 5432]
//!         ├─► extract username, password, database, application_name
//!         └─► Ok(PostgresEtpConfig)
//!
//!   Parsed Configuration Layout:
//!   ┌────────────────────────────────────────────────────────────────────────────┐
//!   │ id | host | port | username | password | database | application_name       │
//!   │ pool: EtpPoolSettings (pool_size, connect_timeout_ms, idle_timeout_ms)     │
//!   │ tls: Option<EtpTlsConfig>                                                  │
//!   └────────────────────────────────────────────────────────────────────────────┘
//! ```

use serde::{Deserialize, Serialize};

use crate::config::{EtpEntryConfig, EtpKind, EtpPoolSettings};
use crate::connect::{EtpTlsConfig, EtpTransportSecurity};
use crate::error::EtpError;

/// Tailored configuration for a PostgreSQL external third-party pool.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PostgresEtpConfig {
    pub id: String,
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: Option<String>,
    pub database: String,
    pub application_name: Option<String>,
    pub security: EtpTransportSecurity,
    pub pool: EtpPoolSettings,
    pub tls: Option<EtpTlsConfig>,
}

impl PostgresEtpConfig {
    /// Parses a [`PostgresEtpConfig`] from a validated [`EtpEntryConfig`].
    pub fn from_entry(entry: &EtpEntryConfig) -> Result<Self, EtpError> {
        if entry.kind != EtpKind::Postgres {
            return Err(EtpError::KindMismatch {
                id: entry.id.clone(),
                expected: EtpKind::Postgres.as_str(),
                actual: entry.kind.as_str(),
            });
        }
        entry.validate()?;
        entry.require_security(&[EtpTransportSecurity::Plain, EtpTransportSecurity::StartTls])?;
        let (host, port) = entry.resolve_endpoint()?;
        let obj = entry.settings.as_object().ok_or_else(|| {
            EtpError::Config(format!(
                "Postgres ETP '{}' settings must be an object",
                entry.id
            ))
        })?;

        let username = obj
            .get("username")
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or("postgres")
            .to_string();

        let password = obj
            .get("password")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(ToString::to_string);

        let database = obj
            .get("database")
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(ToString::to_string)
            .unwrap_or_else(|| username.clone());

        let application_name = obj
            .get("application_name")
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(ToString::to_string);

        Ok(Self {
            id: entry.id.clone(),
            host,
            port,
            username,
            password,
            database,
            application_name,
            security: entry.security,
            pool: entry.pool.clone(),
            tls: entry.effective_tls(),
        })
    }
}
