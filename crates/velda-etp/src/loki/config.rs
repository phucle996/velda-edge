//! Loki ETP configuration parser (`LokiEtpConfig`).
//!
//! ```text
//!   EtpEntryConfig (kind == EtpKind::Loki)
//!         │
//!         ├─► resolve_endpoint() ──► (host, port) [default 3100 plain / 443 tls]
//!         ├─► format_host_header() + extract_endpoint_path() ──► host_header, base_path
//!         ├─► extract tenant_id (X-Scope-OrgID), bearer_token, basic_auth
//!         └─► Ok(LokiEtpConfig)
//!
//!   Parsed Configuration Layout:
//!   ┌────────────────────────────────────────────────────────────────────────────┐
//!   │ id | endpoint | host | port | host_header | base_path                      │
//!   │ tenant_id | bearer_token | basic_auth | pool | tls                         │
//!   └────────────────────────────────────────────────────────────────────────────┘
//! ```

use serde::{Deserialize, Serialize};

use crate::config::{
    EtpEntryConfig, EtpKind, EtpPoolSettings, extract_endpoint_path, format_host_header,
};
use crate::connect::EtpTlsConfig;
use crate::error::EtpError;

/// Tailored configuration for a Grafana Loki log push endpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LokiEtpConfig {
    pub id: String,
    pub endpoint: String,
    pub host: String,
    pub port: u16,
    pub host_header: String,
    pub base_path: String,
    pub tenant_id: Option<String>,
    pub bearer_token: Option<String>,
    pub basic_auth: Option<String>,
    pub pool: EtpPoolSettings,
    pub tls: Option<EtpTlsConfig>,
}

impl LokiEtpConfig {
    /// Parses a [`LokiEtpConfig`] from a validated [`EtpEntryConfig`].
    pub fn from_entry(entry: &EtpEntryConfig) -> Result<Self, EtpError> {
        if entry.kind != EtpKind::Loki {
            return Err(EtpError::KindMismatch {
                id: entry.id.clone(),
                expected: EtpKind::Loki.as_str(),
                actual: entry.kind.as_str(),
            });
        }
        entry.validate()?;
        let (host, port) = entry.resolve_endpoint()?;
        let obj = entry.settings.as_object().ok_or_else(|| {
            EtpError::Config(format!(
                "Loki ETP '{}' settings must be an object",
                entry.id
            ))
        })?;

        let endpoint = obj
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

        let is_tls = entry.tls.is_some() || endpoint.to_ascii_lowercase().starts_with("https://");
        let host_header = format_host_header(&host, port, is_tls);
        let base_path = extract_endpoint_path(&endpoint);

        let tenant_id = obj
            .get("tenant_id")
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(ToString::to_string);

        let bearer_token = obj
            .get("bearer_token")
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(ToString::to_string);

        let basic_auth = obj
            .get("basic_auth")
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(ToString::to_string);

        Ok(Self {
            id: entry.id.clone(),
            endpoint,
            host,
            port,
            host_header,
            base_path,
            tenant_id,
            bearer_token,
            basic_auth,
            pool: entry.pool.clone(),
            tls: entry.tls.clone(),
        })
    }
}
