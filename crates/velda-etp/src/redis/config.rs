//! Redis ETP configuration parser (`RedisEtpConfig`).
//!
//! ```text
//!   EtpEntryConfig (kind == EtpKind::Redis)
//!         │
//!         ├─► entry.validate() + resolve_endpoint() ──► (host, port) [default 6379/6380]
//!         ├─► extract username, password, db (default 0), key_prefix
//!         └─► Ok(RedisEtpConfig)
//!
//!   Parsed Configuration Layout:
//!   ┌────────────────────────────────────────────────────────────────────────────┐
//!   │ id | host | port | username | password | db: u16 | key_prefix              │
//!   │ pool: EtpPoolSettings (pool_size, connect_timeout_ms, idle_timeout_ms)     │
//!   │ tls: Option<EtpTlsConfig>                                                  │
//!   └────────────────────────────────────────────────────────────────────────────┘
//! ```

use serde::{Deserialize, Serialize};

use crate::config::{EtpEntryConfig, EtpKind, EtpPoolSettings};
use crate::connect::EtpTlsConfig;
use crate::error::EtpError;

/// Tailored configuration for a Redis external third-party pool.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RedisEtpConfig {
    pub id: String,
    pub host: String,
    pub port: u16,
    pub username: Option<String>,
    pub password: Option<String>,
    pub db: u16,
    pub key_prefix: Option<String>,
    pub pool: EtpPoolSettings,
    pub tls: Option<EtpTlsConfig>,
}

impl RedisEtpConfig {
    /// Parses a [`RedisEtpConfig`] from a validated [`EtpEntryConfig`].
    pub fn from_entry(entry: &EtpEntryConfig) -> Result<Self, EtpError> {
        if entry.kind != EtpKind::Redis {
            return Err(EtpError::KindMismatch {
                id: entry.id.clone(),
                expected: EtpKind::Redis.as_str(),
                actual: entry.kind.as_str(),
            });
        }
        entry.validate()?;
        let (host, port) = entry.resolve_endpoint()?;
        let obj = entry.settings.as_object().ok_or_else(|| {
            EtpError::Config(format!(
                "Redis ETP '{}' settings must be an object",
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

        let db = match obj.get("db") {
            Some(val) => {
                let raw = val.as_u64().ok_or_else(|| {
                    EtpError::Config(format!(
                        "Redis ETP '{}' setting 'db' must be a non-negative integer",
                        entry.id
                    ))
                })?;
                u16::try_from(raw).map_err(|_| {
                    EtpError::Config(format!(
                        "Redis ETP '{}' setting 'db' ({raw}) exceeds u16 range",
                        entry.id
                    ))
                })?
            }
            None => 0,
        };

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
            username,
            password,
            db,
            key_prefix,
            pool: entry.pool.clone(),
            tls: entry.tls.clone(),
        })
    }
}
