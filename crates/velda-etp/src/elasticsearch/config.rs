//! Elasticsearch ETP configuration parser (`ElasticsearchEtpConfig`).
//!
//! ```text
//!   EtpEntryConfig (kind == EtpKind::Elasticsearch)
//!         │
//!         ├─► resolve_endpoint() ──► (host, port) [default 9200]
//!         ├─► format_host_header() + extract_endpoint_path() ──► host_header, base_path
//!         ├─► extract index, api_key, basic_auth
//!         └─► Ok(ElasticsearchEtpConfig)
//!
//!   Parsed Configuration Layout:
//!   ┌────────────────────────────────────────────────────────────────────────────┐
//!   │ id | endpoint | host | port | host_header | base_path                      │
//!   │ index | api_key | basic_auth | pool | tls                                  │
//!   └────────────────────────────────────────────────────────────────────────────┘
//! ```

use serde::{Deserialize, Serialize};

use crate::config::{
    EtpEntryConfig, EtpKind, EtpPoolSettings, extract_endpoint_path, format_host_header,
};
use crate::connect::EtpTlsConfig;
use crate::error::EtpError;

/// Tailored configuration for an Elasticsearch / OpenSearch HTTP/HTTPS ETP pool.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ElasticsearchEtpConfig {
    pub id: String,
    pub endpoint: String,
    pub host: String,
    pub port: u16,
    pub host_header: String,
    pub base_path: String,
    pub index: Option<String>,
    pub api_key: Option<String>,
    pub basic_auth: Option<String>,
    pub pool: EtpPoolSettings,
    pub tls: Option<EtpTlsConfig>,
}

impl ElasticsearchEtpConfig {
    /// Parses an [`ElasticsearchEtpConfig`] from a validated [`EtpEntryConfig`].
    pub fn from_entry(entry: &EtpEntryConfig) -> Result<Self, EtpError> {
        if entry.kind != EtpKind::Elasticsearch {
            return Err(EtpError::KindMismatch {
                id: entry.id.clone(),
                expected: EtpKind::Elasticsearch.as_str(),
                actual: entry.kind.as_str(),
            });
        }
        entry.validate()?;
        let (host, port) = entry.resolve_endpoint()?;
        let obj = entry.settings.as_object().ok_or_else(|| {
            EtpError::Config(format!(
                "Elasticsearch ETP '{}' settings must be an object",
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

        let index = obj
            .get("index")
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(ToString::to_string);

        let api_key = obj
            .get("api_key")
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
            index,
            api_key,
            basic_auth,
            pool: entry.pool.clone(),
            tls: entry.tls.clone(),
        })
    }
}
