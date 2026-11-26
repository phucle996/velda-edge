//! HashiCorp Vault / OpenBao ETP configuration parser (`VaultEtpConfig`).
//!
//! ```text
//!   EtpEntryConfig (kind == EtpKind::Vault)
//!         ├──► endpoint / host / port / host_header
//!         ├──► namespace (X-Vault-Namespace)
//!         └──► auth: None | Token | AppRole { role_id, secret_id, auth_mount }
//! ```

use serde::{Deserialize, Serialize};

use crate::config::{
    EtpEntryConfig, EtpKind, EtpPoolSettings, format_host_header, synthesize_endpoint_uri,
};
use crate::connect::{EtpTlsConfig, EtpTransportSecurity};
use crate::error::EtpError;

fn default_approle_mount() -> String {
    "approle".to_string()
}

/// Connection-layer authentication configuration for Vault / OpenBao.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum VaultAuthConfig {
    #[default]
    None,
    Token {
        token: String,
    },
    AppRole {
        role_id: String,
        secret_id: String,
        #[serde(default = "default_approle_mount")]
        auth_mount: String,
    },
}

/// Tailored connection configuration for a HashiCorp Vault / OpenBao cluster.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VaultEtpConfig {
    pub id: String,
    pub endpoint: String,
    pub host: String,
    pub port: u16,
    pub host_header: String,
    pub namespace: Option<String>,
    pub auth: VaultAuthConfig,
    pub pool: EtpPoolSettings,
    pub tls: Option<EtpTlsConfig>,
}

impl VaultEtpConfig {
    /// Parses a [`VaultEtpConfig`] from a validated [`EtpEntryConfig`].
    pub fn from_entry(entry: &EtpEntryConfig) -> Result<Self, EtpError> {
        if entry.kind != EtpKind::Vault {
            return Err(EtpError::KindMismatch {
                id: entry.id.clone(),
                expected: EtpKind::Vault.as_str(),
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
                "Vault ETP '{}' settings must be an object",
                entry.id
            ))
        })?;

        let is_tls = entry.is_tls_transport();
        let scheme = if is_tls { "https" } else { "http" };
        let raw_endpoint = synthesize_endpoint_uri(scheme, &host, port, "");
        let host_header = format_host_header(&host, port, is_tls);

        let namespace = obj
            .get("namespace")
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(ToString::to_string);

        let auth = parse_vault_auth(obj, &entry.id)?;

        Ok(Self {
            id: entry.id.clone(),
            endpoint: raw_endpoint,
            host,
            port,
            host_header,
            namespace,
            auth,
            pool: entry.pool.clone(),
            tls: entry.effective_tls(),
        })
    }
}

fn parse_vault_auth(
    obj: &serde_json::Map<String, serde_json::Value>,
    id: &str,
) -> Result<VaultAuthConfig, EtpError> {
    if let Some(auth_val) = obj.get("auth") {
        let auth: VaultAuthConfig = serde_json::from_value(auth_val.clone())
            .map_err(|e| EtpError::Config(format!("Vault ETP '{id}' invalid auth: {e}")))?;
        match &auth {
            VaultAuthConfig::Token { token } if token.trim().is_empty() => {
                return Err(EtpError::Config(format!(
                    "Vault ETP '{id}' auth token cannot be empty"
                )));
            }
            VaultAuthConfig::AppRole {
                role_id,
                secret_id,
                auth_mount,
            } if role_id.trim().is_empty()
                || secret_id.trim().is_empty()
                || auth_mount.trim().is_empty() =>
            {
                return Err(EtpError::Config(format!(
                    "Vault ETP '{id}' AppRole auth requires non-empty role_id, secret_id, auth_mount"
                )));
            }
            _ => {}
        }
        return Ok(auth);
    }

    if let Some(tok) = obj.get("token").and_then(|v| v.as_str()) {
        let trimmed = tok.trim();
        if trimmed.is_empty() {
            return Err(EtpError::Config(format!(
                "Vault ETP '{id}' 'token' cannot be empty"
            )));
        }
        return Ok(VaultAuthConfig::Token {
            token: trimmed.to_string(),
        });
    }

    if let (Some(role_id), Some(secret_id)) = (
        obj.get("role_id").and_then(|v| v.as_str()),
        obj.get("secret_id").and_then(|v| v.as_str()),
    ) {
        let mount = obj
            .get("auth_mount")
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or("approle");
        if role_id.trim().is_empty() || secret_id.trim().is_empty() {
            return Err(EtpError::Config(format!(
                "Vault ETP '{id}' role_id and secret_id cannot be empty"
            )));
        }
        return Ok(VaultAuthConfig::AppRole {
            role_id: role_id.trim().to_string(),
            secret_id: secret_id.trim().to_string(),
            auth_mount: mount.to_string(),
        });
    }

    Ok(VaultAuthConfig::None)
}
