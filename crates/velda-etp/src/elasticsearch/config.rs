//! Elasticsearch ETP configuration parser (`ElasticsearchEtpConfig`).
//!
//! ```text
//!   EtpEntryConfig (kind == EtpKind::Elasticsearch)
//!         │
//!         ├─► resolve_endpoint() ──► (host, port) [default 9200]
//!         ├─► format_host_header() + extract_endpoint_path() ──► host_header, base_path
//!         ├─► extract index, api_key, basic_auth, bearer_token
//!         └─► Ok(ElasticsearchEtpConfig)
//!
//!   Parsed Configuration Layout:
//!   ┌────────────────────────────────────────────────────────────────────────────┐
//!   │ id | endpoint | host | port | host_header | base_path                      │
//!   │ index | api_key | basic_auth | bearer_token | pool | tls                   │
//!   └────────────────────────────────────────────────────────────────────────────┘
//! ```

use serde::{Deserialize, Serialize};

use crate::config::{
    EtpEntryConfig, EtpKind, EtpPoolSettings, format_host_header, read_base_path,
    synthesize_endpoint_uri,
};
use crate::connect::{EtpTlsConfig, EtpTransportSecurity};
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
    pub bearer_token: Option<String>,
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
        entry.require_security(&[
            EtpTransportSecurity::Plain,
            EtpTransportSecurity::Tls,
            EtpTransportSecurity::Mtls,
        ])?;
        let (host, port) = entry.resolve_endpoint()?;
        let obj = entry.settings.as_object().ok_or_else(|| {
            EtpError::Config(format!(
                "Elasticsearch ETP '{}' settings must be an object",
                entry.id
            ))
        })?;

        let is_tls = entry.is_tls_transport();
        let base_path = read_base_path(obj, &entry.id)?;
        let scheme = if is_tls { "https" } else { "http" };
        let endpoint = synthesize_endpoint_uri(scheme, &host, port, &base_path);
        let host_header = format_host_header(&host, port, is_tls);

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
            .map(ToString::to_string)
            .or_else(|| {
                let user = obj
                    .get("username")
                    .or_else(|| obj.get("user"))
                    .and_then(|v| v.as_str())
                    .map(str::trim)
                    .filter(|s| !s.is_empty())?;
                let pass = obj.get("password").and_then(|v| v.as_str())?;
                Some(format!("{user}:{pass}"))
            });

        let bearer_token = obj
            .get("bearer_token")
            .or_else(|| obj.get("token"))
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
            bearer_token,
            pool: entry.pool.clone(),
            tls: entry.effective_tls(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::EtpPoolSettings;

    fn entry_with_settings(settings: serde_json::Value) -> EtpEntryConfig {
        EtpEntryConfig {
            id: "es-unit".into(),
            kind: EtpKind::Elasticsearch,
            security: EtpTransportSecurity::Plain,
            pool: EtpPoolSettings::default(),
            tls: None,
            settings,
        }
    }

    #[test]
    fn test_es_etp_parses_bearer_token_and_basic_auth_variants() {
        let cfg1 = ElasticsearchEtpConfig::from_entry(&entry_with_settings(serde_json::json!({
            "host": "127.0.0.1",
            "port": 9200,
            "base_path": "/proxy",
            "bearer_token": "tok-primary",
            "username": "elastic",
            "password": "changeme"
        })))
        .unwrap();
        assert_eq!(cfg1.base_path, "/proxy");
        assert_eq!(cfg1.bearer_token.as_deref(), Some("tok-primary"));
        assert_eq!(cfg1.basic_auth.as_deref(), Some("elastic:changeme"));

        let cfg2 = ElasticsearchEtpConfig::from_entry(&entry_with_settings(serde_json::json!({
            "host": "127.0.0.1",
            "port": 9200,
            "token": "tok-alias",
            "user": "reader",
            "password": ""
        })))
        .unwrap();
        assert_eq!(cfg2.bearer_token.as_deref(), Some("tok-alias"));
        assert_eq!(cfg2.basic_auth.as_deref(), Some("reader:"));

        let cfg3 = ElasticsearchEtpConfig::from_entry(&entry_with_settings(serde_json::json!({
            "host": "127.0.0.1",
            "port": 9200,
            "basic_auth": "dXNlcjpwYXNz"
        })))
        .unwrap();
        assert_eq!(cfg3.basic_auth.as_deref(), Some("dXNlcjpwYXNz"));
        assert_eq!(cfg3.bearer_token, None);
    }
}
