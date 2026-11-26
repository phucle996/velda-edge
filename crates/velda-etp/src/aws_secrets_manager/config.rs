//! AWS Secrets Manager ETP configuration parser (`AwsSecretsManagerEtpConfig`).
//!
//! ```text
//!   EtpEntryConfig (kind == EtpKind::AwsSecretsManager)
//!         └──► region, host, port, host_header, access_key_id, secret_access_key, session_token
//! ```

use serde::{Deserialize, Serialize};

use crate::config::{
    EtpEntryConfig, EtpKind, EtpPoolSettings, format_host_header, synthesize_endpoint_uri,
};
use crate::connect::{EtpTlsConfig, EtpTransportSecurity};
use crate::error::EtpError;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AwsSecretsManagerEtpConfig {
    pub id: String,
    pub endpoint: String,
    pub host: String,
    pub port: u16,
    pub host_header: String,
    pub region: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
    pub pool: EtpPoolSettings,
    pub tls: Option<EtpTlsConfig>,
}

impl AwsSecretsManagerEtpConfig {
    pub fn from_entry(entry: &EtpEntryConfig) -> Result<Self, EtpError> {
        if entry.kind != EtpKind::AwsSecretsManager {
            return Err(EtpError::KindMismatch {
                id: entry.id.clone(),
                expected: EtpKind::AwsSecretsManager.as_str(),
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
                "AWS Secrets Manager ETP '{}' settings must be an object",
                entry.id
            ))
        })?;

        let region = obj
            .get("region")
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or("us-east-1")
            .to_string();

        let is_tls = entry.is_tls_transport();
        let scheme = if is_tls { "https" } else { "http" };
        let raw_endpoint = synthesize_endpoint_uri(scheme, &host, port, "");
        let host_header = format_host_header(&host, port, is_tls);

        let req_str = |key: &str| -> Result<String, EtpError> {
            obj.get(key)
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(ToString::to_string)
                .ok_or_else(|| {
                    EtpError::Config(format!(
                        "AWS Secrets Manager ETP '{}' requires non-empty '{key}'",
                        entry.id
                    ))
                })
        };

        Ok(Self {
            id: entry.id.clone(),
            endpoint: raw_endpoint,
            host,
            port,
            host_header,
            region,
            access_key_id: req_str("access_key_id")?,
            secret_access_key: req_str("secret_access_key")?,
            session_token: obj
                .get("session_token")
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(ToString::to_string),
            pool: entry.pool.clone(),
            tls: entry.effective_tls(),
        })
    }
}
