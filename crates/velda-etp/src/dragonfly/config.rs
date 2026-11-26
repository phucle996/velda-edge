//! DragonflyDB ETP configuration parser (`DragonflyEtpConfig`).
//!
//! ```text
//!   EtpEntryConfig (kind == EtpKind::Dragonfly)
//!         └──► (host, port) [default 6379/6380] + username + password + db + key_prefix
//! ```

use serde::{Deserialize, Serialize};

use crate::config::{EtpEntryConfig, EtpKind, EtpPoolSettings};
use crate::connect::{EtpTlsConfig, EtpTransportSecurity};
use crate::error::EtpError;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DragonflyEtpConfig {
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

impl DragonflyEtpConfig {
    pub fn from_entry(entry: &EtpEntryConfig) -> Result<Self, EtpError> {
        if entry.kind != EtpKind::Dragonfly {
            return Err(EtpError::KindMismatch {
                id: entry.id.clone(),
                expected: EtpKind::Dragonfly.as_str(),
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
                "Dragonfly ETP '{}' settings must be an object",
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
                        "Dragonfly ETP '{}' setting 'db' must be a non-negative integer",
                        entry.id
                    ))
                })?;
                u16::try_from(raw).map_err(|_| {
                    EtpError::Config(format!(
                        "Dragonfly ETP '{}' setting 'db' ({raw}) exceeds u16 range",
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
            tls: entry.effective_tls(),
        })
    }
}
