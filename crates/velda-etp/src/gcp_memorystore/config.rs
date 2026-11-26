//! GCP Memorystore (`velda-etp::gcp_memorystore`) IAM-authenticated ETP configuration parser.
//! Static passwords are rejected cleanly (`use 'redis' or 'valkey' for static auth`).
//!
//! ```text
//!   EtpEntryConfig (kind == EtpKind::GcpMemorystore)
//!         ├──► Reject non-empty settings.password
//!         ├──► mode: "standalone" (default) | "cluster" (rejects db > 0)
//!         └──► AuthSource:
//!                ├─ MetadataEndpoint { url }         (iam_token_endpoint GET)
//!                └─ ServiceAccount { client_email, private_key_pem, token_uri }
//! ```

use serde::{Deserialize, Serialize};

use crate::config::{EtpEntryConfig, EtpKind, EtpPoolSettings, parse_host_port_target};
use crate::connect::{EtpTlsConfig, EtpTransportSecurity};
use crate::error::EtpError;
use crate::gcp_memorystore::slots::format_host_port;

pub const DEFAULT_GCP_OAUTH_TOKEN_URI: &str = "https://oauth2.googleapis.com/token";
pub const DEFAULT_GCP_SCOPE: &str = "https://www.googleapis.com/auth/cloud-platform";

/// Explicit GCP Memorystore topology branch selected by `settings.mode`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GcpMemorystoreMode {
    #[default]
    Standalone,
    Cluster,
}

/// IAM OAuth2 token acquisition mechanism for GCP Memorystore.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum GcpIamAuthSource {
    /// Fetches short-lived OAuth2 access tokens from GCE metadata or local IAM proxy.
    MetadataEndpoint { endpoint: String },
    /// Mints an RS256 JWT assertion locally and exchanges it at Google OAuth2 `token_uri`.
    ServiceAccount {
        client_email: String,
        private_key_pem: String,
        token_uri: String,
    },
}

/// Validated GCP Memorystore ETP configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GcpMemorystoreEtpConfig {
    pub id: String,
    pub mode: GcpMemorystoreMode,
    pub host: String,
    pub port: u16,
    pub seed_nodes: Vec<String>,
    pub username: String,
    pub auth_source: GcpIamAuthSource,
    pub db: u16,
    pub key_prefix: Option<String>,
    pub pool: EtpPoolSettings,
    pub tls: Option<EtpTlsConfig>,
}

impl GcpMemorystoreEtpConfig {
    /// Parses a [`GcpMemorystoreEtpConfig`] and enforces GCP IAM authentication.
    pub fn from_entry(entry: &EtpEntryConfig) -> Result<Self, EtpError> {
        if entry.kind != EtpKind::GcpMemorystore {
            return Err(EtpError::KindMismatch {
                id: entry.id.clone(),
                expected: EtpKind::GcpMemorystore.as_str(),
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
                "GcpMemorystore ETP '{}' settings must be an object",
                entry.id
            ))
        })?;

        if obj
            .get("password")
            .and_then(|v| v.as_str())
            .is_some_and(|s| !s.trim().is_empty())
        {
            return Err(EtpError::Config(format!(
                "GcpMemorystore ETP '{}' enforces GCP IAM Auth only; static 'password' is rejected (use 'redis' or 'valkey' for static auth)",
                entry.id
            )));
        }

        let mode = match obj.get("mode").and_then(|v| v.as_str()) {
            None => GcpMemorystoreMode::Standalone,
            Some(raw) => match raw.trim().to_ascii_lowercase().as_str() {
                "standalone" | "" => GcpMemorystoreMode::Standalone,
                "cluster" => GcpMemorystoreMode::Cluster,
                other => {
                    return Err(EtpError::Config(format!(
                        "GcpMemorystore ETP '{}' invalid mode '{other}' (expected 'standalone' or 'cluster')",
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
                        parse_host_port_target(s, EtpKind::GcpMemorystore, is_tls, &entry.id)?;
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
            .unwrap_or("default")
            .to_string();

        let non_empty = |k: &str| -> Option<String> {
            obj.get(k)
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(ToString::to_string)
        };

        let auth_source = if let Some(endpoint) = non_empty("iam_token_endpoint") {
            GcpIamAuthSource::MetadataEndpoint { endpoint }
        } else if let (Some(client_email), Some(private_key_pem)) =
            (non_empty("client_email"), non_empty("private_key_pem"))
        {
            let token_uri =
                non_empty("token_uri").unwrap_or_else(|| DEFAULT_GCP_OAUTH_TOKEN_URI.to_string());
            GcpIamAuthSource::ServiceAccount {
                client_email,
                private_key_pem,
                token_uri,
            }
        } else {
            return Err(EtpError::Config(format!(
                "GcpMemorystore ETP '{}' requires GCP IAM credentials ('iam_token_endpoint' or 'client_email' + 'private_key_pem')",
                entry.id
            )));
        };

        let db = match obj.get("db") {
            Some(val) => {
                let raw = val.as_u64().ok_or_else(|| {
                    EtpError::Config(format!(
                        "GcpMemorystore ETP '{}' setting 'db' must be a non-negative integer",
                        entry.id
                    ))
                })?;
                u16::try_from(raw).map_err(|_| {
                    EtpError::Config(format!(
                        "GcpMemorystore ETP '{}' setting 'db' ({raw}) exceeds u16 range",
                        entry.id
                    ))
                })?
            }
            None => 0,
        };
        if mode == GcpMemorystoreMode::Cluster && db > 0 {
            return Err(EtpError::Config(format!(
                "GcpMemorystore ETP '{}' cannot configure db={db} when mode='cluster' (Cluster supports db 0 only)",
                entry.id
            )));
        }

        let key_prefix = non_empty("key_prefix");

        Ok(Self {
            id: entry.id.clone(),
            mode,
            host,
            port,
            seed_nodes,
            username,
            auth_source,
            db,
            key_prefix,
            pool: entry.pool.clone(),
            tls: entry.effective_tls(),
        })
    }
}
