//! AWS ElastiCache (`velda-etp::aws_elasticache`) IAM-authenticated ETP configuration parser.
//! Static passwords are rejected cleanly (`use 'redis' or 'valkey' for static password`).
//!
//! ```text
//!   EtpEntryConfig (kind == EtpKind::AwsElastiCache)
//!         └─► Validates region, username, replication_group_id, access_key_id, secret_access_key
//!               └─► AwsElastiCacheEtpConfig { mode: Standalone | Cluster, seed_nodes, db }
//! ```

use serde::{Deserialize, Serialize};

use crate::aws_elasticache::slots::format_host_port;
use crate::config::{EtpEntryConfig, EtpKind, EtpPoolSettings, parse_host_port_target};
use crate::connect::{EtpTlsConfig, EtpTransportSecurity};
use crate::error::EtpError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AwsElastiCacheMode {
    #[default]
    Standalone,
    Cluster,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AwsElastiCacheEtpConfig {
    pub id: String,
    pub mode: AwsElastiCacheMode,
    pub host: String,
    pub port: u16,
    pub seed_nodes: Vec<String>,
    pub region: String,
    pub replication_group_id: String,
    pub username: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
    pub db: u16,
    pub key_prefix: Option<String>,
    pub pool: EtpPoolSettings,
    pub tls: Option<EtpTlsConfig>,
}

impl AwsElastiCacheEtpConfig {
    pub fn from_entry(entry: &EtpEntryConfig) -> Result<Self, EtpError> {
        if entry.kind != EtpKind::AwsElastiCache {
            return Err(EtpError::KindMismatch {
                id: entry.id.clone(),
                expected: EtpKind::AwsElastiCache.as_str(),
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
                "AwsElastiCache ETP '{}' settings must be an object",
                entry.id
            ))
        })?;

        if obj
            .get("password")
            .and_then(|v| v.as_str())
            .is_some_and(|s| !s.trim().is_empty())
        {
            return Err(EtpError::Config(format!(
                "AwsElastiCache ETP '{}' enforces AWS IAM Auth only; static 'password' is rejected (use 'redis' or 'valkey' for static auth)",
                entry.id
            )));
        }

        let mode = match obj.get("mode").and_then(|v| v.as_str()) {
            None => AwsElastiCacheMode::Standalone,
            Some(raw) => match raw.trim().to_ascii_lowercase().as_str() {
                "standalone" | "" => AwsElastiCacheMode::Standalone,
                "cluster" => AwsElastiCacheMode::Cluster,
                other => {
                    return Err(EtpError::Config(format!(
                        "AwsElastiCache ETP '{}' invalid mode '{other}'",
                        entry.id
                    )));
                }
            },
        };

        let mut seed_nodes = Vec::new();
        if let Some(arr) = obj.get("seed_nodes").and_then(|v| v.as_array()) {
            for item in arr {
                if let Some(s) = item.as_str().map(str::trim).filter(|s| !s.is_empty()) {
                    let (sh, sp) =
                        parse_host_port_target(s, EtpKind::AwsElastiCache, is_tls, &entry.id)?;
                    seed_nodes.push(format_host_port(&sh, sp));
                }
            }
        }
        if seed_nodes.is_empty() {
            seed_nodes.push(format_host_port(&host, port));
        }

        let req_str = |k: &str| -> Result<String, EtpError> {
            obj.get(k)
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(ToString::to_string)
                .ok_or_else(|| {
                    EtpError::Config(format!(
                        "AwsElastiCache ETP '{}' requires AWS IAM setting '{k}'",
                        entry.id
                    ))
                })
        };

        let region = req_str("region")?;
        let username = req_str("username")?;
        let access_key_id = req_str("access_key_id")?;
        let secret_access_key = req_str("secret_access_key")?;
        let replication_group_id = obj
            .get("replication_group_id")
            .or_else(|| obj.get("serverless_cache_name"))
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(ToString::to_string)
            .ok_or_else(|| {
                EtpError::Config(format!(
                    "AwsElastiCache ETP '{}' requires 'replication_group_id' or 'serverless_cache_name'",
                    entry.id
                ))
            })?;

        let session_token = obj
            .get("session_token")
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(ToString::to_string);

        let db = match obj.get("db") {
            Some(val) => {
                let raw = val.as_u64().ok_or_else(|| {
                    EtpError::Config(format!(
                        "AwsElastiCache ETP '{}' setting 'db' must be an integer",
                        entry.id
                    ))
                })?;
                u16::try_from(raw).map_err(|_| {
                    EtpError::Config(format!(
                        "AwsElastiCache ETP '{}' setting 'db' exceeds u16 range",
                        entry.id
                    ))
                })?
            }
            None => 0,
        };
        if mode == AwsElastiCacheMode::Cluster && db > 0 {
            return Err(EtpError::Config(format!(
                "AwsElastiCache ETP '{}' cannot set db={db} when mode='cluster'",
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
            region,
            replication_group_id,
            username,
            access_key_id,
            secret_access_key,
            session_token,
            db,
            key_prefix,
            pool: entry.pool.clone(),
            tls: entry.effective_tls(),
        })
    }
}
