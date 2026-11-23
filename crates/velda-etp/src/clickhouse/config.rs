//! ClickHouse ETP configuration parser (`ClickHouseEtpConfig`).
//!
//! ```text
//!   EtpEntryConfig (kind == EtpKind::ClickHouse)
//!         │
//!         ├─► resolve_endpoint() ──► (host, port) [default 8123 plain / 8443 tls]
//!         ├─► format_host_header() + extract_endpoint_path() ──► host_header, base_path
//!         ├─► extract database (default "default"), user, password/key
//!         └─► Ok(ClickHouseEtpConfig)
//!
//!   Parsed Configuration Layout:
//!   ┌────────────────────────────────────────────────────────────────────────────┐
//!   │ id | host | port | host_header | base_path | database | user | password    │
//!   │ pool: EtpPoolSettings | tls: Option<EtpTlsConfig>                          │
//!   └────────────────────────────────────────────────────────────────────────────┘
//! ```

use serde::{Deserialize, Serialize};

use crate::config::{
    EtpEntryConfig, EtpKind, EtpPoolSettings, extract_endpoint_path, format_host_header,
};
use crate::connect::EtpTlsConfig;
use crate::error::EtpError;

/// Tailored configuration for a ClickHouse HTTP/HTTPS external third-party pool.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClickHouseEtpConfig {
    pub id: String,
    pub host: String,
    pub port: u16,
    pub host_header: String,
    pub base_path: String,
    pub database: String,
    pub user: Option<String>,
    pub password: Option<String>,
    pub pool: EtpPoolSettings,
    pub tls: Option<EtpTlsConfig>,
}

impl ClickHouseEtpConfig {
    /// Parses a [`ClickHouseEtpConfig`] from a validated [`EtpEntryConfig`].
    pub fn from_entry(entry: &EtpEntryConfig) -> Result<Self, EtpError> {
        if entry.kind != EtpKind::ClickHouse {
            return Err(EtpError::KindMismatch {
                id: entry.id.clone(),
                expected: EtpKind::ClickHouse.as_str(),
                actual: entry.kind.as_str(),
            });
        }
        entry.validate()?;
        let (host, port) = entry.resolve_endpoint()?;
        let obj = entry.settings.as_object().ok_or_else(|| {
            EtpError::Config(format!(
                "ClickHouse ETP '{}' settings must be an object",
                entry.id
            ))
        })?;

        let raw_endpoint = obj
            .get("endpoint")
            .or_else(|| obj.get("url"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let is_tls = entry.tls.is_some()
            || raw_endpoint
                .trim()
                .to_ascii_lowercase()
                .starts_with("https://");
        let host_header = format_host_header(&host, port, is_tls);
        let base_path = extract_endpoint_path(raw_endpoint);

        let database = obj
            .get("database")
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or("default")
            .to_string();

        let user = obj
            .get("user")
            .or_else(|| obj.get("username"))
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(ToString::to_string);

        let password = obj
            .get("password")
            .or_else(|| obj.get("key"))
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(ToString::to_string);

        Ok(Self {
            id: entry.id.clone(),
            host,
            port,
            host_header,
            base_path,
            database,
            user,
            password,
            pool: entry.pool.clone(),
            tls: entry.tls.clone(),
        })
    }
}
