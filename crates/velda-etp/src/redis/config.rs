//! Redis ETP configuration parser (`RedisEtpConfig`) supporting explicit `standalone` and
//! `cluster` topologies.
//!
//! ```text
//!   EtpEntryConfig (kind == EtpKind::Redis)
//!         │
//!         ├─► entry.validate() + resolve_endpoint() ──► primary (host, port)
//!         ├─► parse mode: "standalone" (default) | "cluster"
//!         │     ├─ Standalone ──► accepts db >= 0, uses host:port
//!         │     └─ Cluster    ──► rejects db > 0, collects seed_nodes (defaults to host:port)
//!         └─► Ok(RedisEtpConfig)
//! ```

use serde::{Deserialize, Serialize};

use crate::config::{
    EtpEntryConfig, EtpKind, EtpPoolSettings, format_host_port, parse_host_port_target,
};
use crate::connect::{EtpTlsConfig, EtpTransportSecurity};
use crate::error::EtpError;

/// Explicit Redis topology branch selected by `settings.mode` in `etp.json`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RedisMode {
    #[default]
    Standalone,
    Cluster,
}

/// Tailored configuration for a Redis external third-party pool (`standalone` or `cluster`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RedisEtpConfig {
    pub id: String,
    pub mode: RedisMode,
    pub host: String,
    pub port: u16,
    pub seed_nodes: Vec<String>,
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
        entry.require_security(&[
            EtpTransportSecurity::Plain,
            EtpTransportSecurity::Tls,
            EtpTransportSecurity::Mtls,
        ])?;
        let (host, port) = entry.resolve_endpoint()?;
        let is_tls = entry.is_tls_transport();
        let obj = entry.settings.as_object().ok_or_else(|| {
            EtpError::Config(format!(
                "Redis ETP '{}' settings must be an object",
                entry.id
            ))
        })?;

        let mode = match obj.get("mode").and_then(|v| v.as_str()) {
            None => RedisMode::Standalone,
            Some(raw) => match raw.trim().to_ascii_lowercase().as_str() {
                "standalone" | "" => RedisMode::Standalone,
                "cluster" => RedisMode::Cluster,
                other => {
                    return Err(EtpError::Config(format!(
                        "Redis ETP '{}' invalid mode '{other}' (expected 'standalone' or 'cluster')",
                        entry.id
                    )));
                }
            },
        };

        let mut seed_nodes = Vec::new();
        if let Some(arr) = obj.get("seed_nodes").and_then(|v| v.as_array()) {
            for item in arr {
                if let Some(s) = item.as_str().map(str::trim).filter(|s| !s.is_empty()) {
                    let (sh, sp) = parse_host_port_target(s, EtpKind::Redis, is_tls, &entry.id)?;
                    seed_nodes.push(format_host_port(&sh, sp));
                }
            }
        }
        if seed_nodes.is_empty() {
            seed_nodes.push(format_host_port(&host, port));
        }

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

        if mode == RedisMode::Cluster && db > 0 {
            return Err(EtpError::Config(format!(
                "Redis ETP '{}' cannot configure db={db} when mode='cluster' (Redis Cluster supports db 0 only)",
                entry.id
            )));
        }

        let key_prefix = obj
            .get("key_prefix")
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(ToString::to_string);

        Ok(Self {
            id: entry.id.clone(),
            mode,
            host,
            port,
            seed_nodes,
            username,
            password,
            db,
            key_prefix,
            pool: entry.pool.clone(),
            tls: entry.effective_tls(),
        })
    }
}
