//! S3 ETP configuration parser (`S3EtpConfig`).
//!
//! ```text
//!   EtpEntryConfig (kind == EtpKind::S3)
//!         │
//!         ├─► resolve_endpoint() ──► (host, port) [default 80 plain / 443 tls]
//!         ├─► format_host_header() ──► host_header
//!         ├─► extract region (default "us-east-1"), bucket, keys, force_path_style
//!         └─► Ok(S3EtpConfig)
//!
//!   Parsed Configuration Layout:
//!   ┌────────────────────────────────────────────────────────────────────────────┐
//!   │ id | endpoint | host | port | host_header | region | bucket                │
//!   │ access_key | secret_key | force_path_style | pool | tls                    │
//!   └────────────────────────────────────────────────────────────────────────────┘
//! ```

use serde::{Deserialize, Serialize};

use crate::config::{EtpEntryConfig, EtpKind, EtpPoolSettings, format_host_header};
use crate::connect::EtpTlsConfig;
use crate::error::EtpError;

/// Tailored configuration for an S3-compatible object storage ETP pool.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct S3EtpConfig {
    pub id: String,
    pub endpoint: String,
    pub host: String,
    pub port: u16,
    pub host_header: String,
    pub region: String,
    pub bucket: String,
    pub access_key: String,
    pub secret_key: String,
    pub force_path_style: bool,
    pub pool: EtpPoolSettings,
    pub tls: Option<EtpTlsConfig>,
}

impl S3EtpConfig {
    /// Parses an [`S3EtpConfig`] from a validated [`EtpEntryConfig`].
    pub fn from_entry(entry: &EtpEntryConfig) -> Result<Self, EtpError> {
        if entry.kind != EtpKind::S3 {
            return Err(EtpError::KindMismatch {
                id: entry.id.clone(),
                expected: EtpKind::S3.as_str(),
                actual: entry.kind.as_str(),
            });
        }
        entry.validate()?;
        let (host, port) = entry.resolve_endpoint()?;
        let obj = entry.settings.as_object().ok_or_else(|| {
            EtpError::Config(format!("S3 ETP '{}' settings must be an object", entry.id))
        })?;

        let raw_endpoint = obj
            .get("endpoint")
            .or_else(|| obj.get("url"))
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(ToString::to_string)
            .unwrap_or_else(|| {
                let scheme = if entry.tls.is_some() { "https" } else { "http" };
                format!("{scheme}://{host}:{port}")
            });

        let is_tls =
            entry.tls.is_some() || raw_endpoint.to_ascii_lowercase().starts_with("https://");
        let host_header = format_host_header(&host, port, is_tls);

        let region = obj
            .get("region")
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or("us-east-1")
            .to_string();

        let bucket = obj
            .get("bucket")
            .and_then(|v| v.as_str())
            .map(str::trim)
            .unwrap_or("")
            .to_string();

        let access_key = obj
            .get("access_key")
            .or_else(|| obj.get("access_key_id"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let secret_key = obj
            .get("secret_key")
            .or_else(|| obj.get("secret_access_key"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let force_path_style = obj
            .get("force_path_style")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        Ok(Self {
            id: entry.id.clone(),
            endpoint: raw_endpoint,
            host,
            port,
            host_header,
            region,
            bucket,
            access_key,
            secret_key,
            force_path_style,
            pool: entry.pool.clone(),
            tls: entry.tls.clone(),
        })
    }
}
