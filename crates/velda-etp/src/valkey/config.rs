//! Valkey ETP configuration parser (`ValkeyEtpConfig`) with explicit `standalone` / `cluster`.
//!
//! ```text
//!   EtpEntryConfig (kind == EtpKind::Valkey)
//!         └─► ValkeyEtpConfig { mode: Standalone | Cluster, host, port, seed_nodes, auth, db }
//! ```

use serde::{Deserialize, Serialize};

use crate::config::{EtpEntryConfig, EtpKind, EtpPoolSettings, parse_host_port_target};
use crate::connect::{EtpTlsConfig, EtpTransportSecurity};
use crate::error::EtpError;
use crate::valkey::slots::format_host_port;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ValkeyMode {
    #[default]
    Standalone,
    Cluster,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValkeyEtpConfig {
    pub id: String,
    pub mode: ValkeyMode,
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

impl ValkeyEtpConfig {
    pub fn from_entry(entry: &EtpEntryConfig) -> Result<Self, EtpError> {
        if entry.kind != EtpKind::Valkey {
            return Err(EtpError::KindMismatch {
                id: entry.id.clone(),
                expected: EtpKind::Valkey.as_str(),
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
                "Valkey ETP '{}' settings must be an object",
                entry.id
            ))
        })?;

        let mode = match obj.get("mode").and_then(|v| v.as_str()) {
            None => ValkeyMode::Standalone,
            Some(raw) => match raw.trim().to_ascii_lowercase().as_str() {
                "standalone" | "" => ValkeyMode::Standalone,
                "cluster" => ValkeyMode::Cluster,
                other => {
                    return Err(EtpError::Config(format!(
                        "Valkey ETP '{}' invalid mode '{other}' (expected 'standalone' or 'cluster')",
                        entry.id
                    )));
                }
            },
        };

        let mut seed_nodes = Vec::new();
        if let Some(arr) = obj.get("seed_nodes").and_then(|v| v.as_array()) {
            for item in arr {
                if let Some(s) = item.as_str().map(str::trim).filter(|s| !s.is_empty()) {
                    let (sh, sp) = parse_host_port_target(s, EtpKind::Valkey, is_tls, &entry.id)?;
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
                        "Valkey ETP '{}' setting 'db' must be a non-negative integer",
                        entry.id
                    ))
                })?;
                u16::try_from(raw).map_err(|_| {
                    EtpError::Config(format!(
                        "Valkey ETP '{}' setting 'db' ({raw}) exceeds u16 range",
                        entry.id
                    ))
                })?
            }
            None => 0,
        };

        if mode == ValkeyMode::Cluster && db > 0 {
            return Err(EtpError::Config(format!(
                "Valkey ETP '{}' cannot configure db={db} when mode='cluster' (Cluster supports db 0 only)",
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
