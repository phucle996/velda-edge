//! Loki ETP configuration parser (`LokiEtpConfig`, `LokiProtocol`).
//!
//! ```text
//!   EtpEntryConfig (kind == EtpKind::Loki, security: Plain | Tls | Mtls)
//!         │
//!         ├─► protocol: Http | Grpc (explicit "protocol", default Http)
//!         ├─► resolve_endpoint() ──► (host, port) [HTTP: 3100/443 | gRPC: 9096/443]
//!         ├─► format_host_header() + read_base_path() ──► host_header, base_path
//!         ├─► extract tenant_id (X-Scope-OrgID), bearer_token, basic_auth, custom_headers
//!         └─► Ok(LokiEtpConfig)
//!
//!   Parsed Configuration Layout:
//!   ┌────────────────────────────────────────────────────────────────────────────┐
//!   │ id | protocol | endpoint | host | port | host_header | base_path           │
//!   │ tenant_id | bearer_token | basic_auth | custom_headers | pool | tls        │
//!   └────────────────────────────────────────────────────────────────────────────┘
//! ```

use serde::{Deserialize, Serialize};

use crate::config::{
    EtpEntryConfig, EtpKind, EtpPoolSettings, format_host_header, read_base_path,
    synthesize_endpoint_uri,
};
use crate::connect::{EtpTlsConfig, EtpTransportSecurity};
use crate::error::EtpError;

/// Transport protocol for Grafana Loki log push ETP pools.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LokiProtocol {
    #[default]
    Http,
    Grpc,
}

/// Tailored configuration for a Grafana Loki log push endpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LokiEtpConfig {
    pub id: String,
    pub protocol: LokiProtocol,
    pub endpoint: String,
    pub http_scheme: String,
    pub host: String,
    pub port: u16,
    pub host_header: String,
    pub base_path: String,
    pub tenant_id: Option<String>,
    pub bearer_token: Option<String>,
    pub basic_auth: Option<String>,
    pub custom_headers: Vec<(String, String)>,
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
        entry.require_security(&[
            EtpTransportSecurity::Plain,
            EtpTransportSecurity::Tls,
            EtpTransportSecurity::Mtls,
        ])?;
        let (host, mut port) = entry.resolve_endpoint()?;
        let obj = entry.settings.as_object().ok_or_else(|| {
            EtpError::Config(format!(
                "Loki ETP '{}' settings must be an object",
                entry.id
            ))
        })?;

        let protocol = match obj.get("protocol").and_then(|v| v.as_str()) {
            Some(p) => match p.trim().to_ascii_lowercase().as_str() {
                "http" => LokiProtocol::Http,
                "grpc" => LokiProtocol::Grpc,
                other => {
                    return Err(EtpError::Config(format!(
                        "Loki ETP '{}' unsupported protocol '{other}'; expected 'http' or 'grpc'",
                        entry.id
                    )));
                }
            },
            None => LokiProtocol::Http,
        };

        let explicit_port = obj.get("port").is_some();
        let is_tls = entry.is_tls_transport();
        if protocol == LokiProtocol::Grpc && !explicit_port {
            port = if is_tls { 443 } else { 9096 };
        }

        let base_path = read_base_path(obj, &entry.id)?;
        let http_scheme = if is_tls { "https" } else { "http" }.to_string();
        let scheme = match (protocol, is_tls) {
            (LokiProtocol::Grpc, true) => "grpcs",
            (LokiProtocol::Grpc, false) => "grpc",
            (LokiProtocol::Http, true) => "https",
            (LokiProtocol::Http, false) => "http",
        };
        let endpoint = synthesize_endpoint_uri(scheme, &host, port, &base_path);
        let host_header = format_host_header(&host, port, is_tls);

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

        let mut custom_headers = Vec::new();
        if let Some(hdr_val) = obj.get("headers") {
            let map = hdr_val.as_object().ok_or_else(|| {
                EtpError::Config(format!(
                    "Loki ETP '{}' setting 'headers' must be a JSON object",
                    entry.id
                ))
            })?;
            for (k, v) in map {
                let name = k.trim();
                let val = v.as_str().map(str::trim).ok_or_else(|| {
                    EtpError::Config(format!(
                        "Loki ETP '{}' header '{k}' value must be a string",
                        entry.id
                    ))
                })?;
                if name.is_empty() || name.contains([':', '\r', '\n']) || val.contains(['\r', '\n'])
                {
                    return Err(EtpError::Config(format!(
                        "Loki ETP '{}' has invalid custom header '{k}'",
                        entry.id
                    )));
                }
                custom_headers.push((name.to_string(), val.to_string()));
            }
        }

        Ok(Self {
            id: entry.id.clone(),
            protocol,
            endpoint,
            http_scheme,
            host,
            port,
            host_header,
            base_path,
            tenant_id,
            bearer_token,
            basic_auth,
            custom_headers,
            pool: entry.pool.clone(),
            tls: entry.effective_tls(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::EtpPoolSettings;

    fn loki_entry(
        security: EtpTransportSecurity,
        settings: serde_json::Value,
        tls: Option<EtpTlsConfig>,
    ) -> EtpEntryConfig {
        EtpEntryConfig {
            id: "loki-cfg-test".into(),
            kind: EtpKind::Loki,
            security,
            pool: EtpPoolSettings::default(),
            tls,
            settings,
        }
    }

    #[test]
    fn test_loki_protocol_and_default_ports_and_headers() {
        let http_cfg = LokiEtpConfig::from_entry(&loki_entry(
            EtpTransportSecurity::Plain,
            serde_json::json!({
                "host": "loki.local",
                "headers": { "X-Custom": "abc" }
            }),
            None,
        ))
        .unwrap();
        assert_eq!(http_cfg.protocol, LokiProtocol::Http);
        assert_eq!(http_cfg.port, 3100);
        assert_eq!(
            http_cfg.custom_headers,
            vec![("X-Custom".to_string(), "abc".to_string())]
        );

        let grpc_plain = LokiEtpConfig::from_entry(&loki_entry(
            EtpTransportSecurity::Plain,
            serde_json::json!({ "host": "loki.local", "protocol": "grpc" }),
            None,
        ))
        .unwrap();
        assert_eq!(grpc_plain.protocol, LokiProtocol::Grpc);
        assert_eq!(grpc_plain.port, 9096);
        assert_eq!(grpc_plain.endpoint, "grpc://loki.local:9096");

        let grpcs_tls = LokiEtpConfig::from_entry(&loki_entry(
            EtpTransportSecurity::Tls,
            serde_json::json!({ "host": "loki.local", "protocol": "grpc" }),
            None,
        ))
        .unwrap();
        assert_eq!(grpcs_tls.protocol, LokiProtocol::Grpc);
        assert_eq!(grpcs_tls.port, 443);
        assert!(grpcs_tls.tls.is_some());
        assert_eq!(grpcs_tls.endpoint, "grpcs://loki.local:443");

        let host_grpc = LokiEtpConfig::from_entry(&loki_entry(
            EtpTransportSecurity::Tls,
            serde_json::json!({ "host": "loki.local", "protocol": "grpc" }),
            Some(EtpTlsConfig {
                insecure_skip_verify: true,
                ..Default::default()
            }),
        ))
        .unwrap();
        assert_eq!(host_grpc.protocol, LokiProtocol::Grpc);
        assert_eq!(host_grpc.port, 443);
        assert_eq!(host_grpc.endpoint, "grpcs://loki.local:443");
    }
}
