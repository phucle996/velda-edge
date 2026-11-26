//! MySQL ETP configuration parser (`MysqlEtpConfig`).
//!
//! ```text
//!   EtpEntryConfig (kind == EtpKind::MySql)
//!         │
//!         ├─► entry.validate() + resolve_endpoint() ──► (host, port) [default 3306]
//!         ├─► extract username, password, database
//!         └─► Ok(MysqlEtpConfig)
//!
//!   Parsed Configuration Layout:
//!   ┌────────────────────────────────────────────────────────────────────────────┐
//!   │ id | host | port | username | password | database                          │
//!   │ pool: EtpPoolSettings (pool_size, connect_timeout_ms, idle_timeout_ms)     │
//!   │ tls: Option<EtpTlsConfig>                                                  │
//!   └────────────────────────────────────────────────────────────────────────────┘
//! ```

use serde::{Deserialize, Serialize};

use crate::config::{EtpEntryConfig, EtpKind, EtpPoolSettings};
use crate::connect::{EtpTlsConfig, EtpTransportSecurity};
use crate::error::EtpError;

/// Tailored configuration for a MySQL / MariaDB external third-party pool.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MysqlEtpConfig {
    pub id: String,
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: Option<String>,
    pub database: Option<String>,
    pub security: EtpTransportSecurity,
    pub pool: EtpPoolSettings,
    pub tls: Option<EtpTlsConfig>,
}

impl MysqlEtpConfig {
    /// Parses a [`MysqlEtpConfig`] from a validated [`EtpEntryConfig`].
    pub fn from_entry(entry: &EtpEntryConfig) -> Result<Self, EtpError> {
        if entry.kind != EtpKind::MySql {
            return Err(EtpError::KindMismatch {
                id: entry.id.clone(),
                expected: EtpKind::MySql.as_str(),
                actual: entry.kind.as_str(),
            });
        }
        entry.validate()?;
        entry.require_security(&[EtpTransportSecurity::Plain, EtpTransportSecurity::StartTls])?;
        let (host, port) = entry.resolve_endpoint()?;
        let obj = entry.settings.as_object().ok_or_else(|| {
            EtpError::Config(format!(
                "MySQL ETP '{}' settings must be an object",
                entry.id
            ))
        })?;

        let username = obj
            .get("username")
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or("root")
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
            .map(ToString::to_string);

        Ok(Self {
            id: entry.id.clone(),
            host,
            port,
            username,
            password,
            database,
            security: entry.security,
            pool: entry.pool.clone(),
            tls: entry.effective_tls(),
        })
    }
}
