//! Loki ETP configuration parser (`LokiEtpConfig`, `LokiProtocol`).
//!
//! ```text
//!   EtpEntryConfig (kind == EtpKind::Loki)
//!         │
//!         ├─► protocol: Http | Grpc (explicit "protocol" or grpc:// / grpcs:// scheme)
//!         ├─► resolve_endpoint() ──► (host, port) [HTTP: 3100/443 | gRPC: 9096/443]
//!         ├─► format_host_header() + extract_endpoint_path() ──► host_header, base_path
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
    EtpEntryConfig, EtpKind, EtpPoolSettings, extract_endpoint_path, format_host_header,
};
use crate::connect::EtpTlsConfig;
use crate::error::EtpError;

/// Transport protocol for Grafana Loki log push ETP pools.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LokiProtocol {
    #[default]
    Http,
    Grpc,
}

fn endpoint_has_explicit_port(raw: &str) -> bool {
    let without_scheme = raw.split_once("://").map(|(_, r)| r).unwrap_or(raw);
    let authority = without_scheme.split('/').next().unwrap_or(without_scheme);
    let authority = authority
        .rsplit_once('@')
        .map(|(_, h)| h)
        .unwrap_or(authority);
    if let Some(after_open) = authority.strip_prefix('[') {
        after_open
            .split_once(']')
            .is_some_and(|(_, rest)| rest.starts_with(':'))
    } else if let Some((host, _)) = authority.rsplit_once(':') {
        !host.contains(':')
    } else {
        false
    }
}

/// Tailored configuration for a Grafana Loki log push endpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LokiEtpConfig {
    pub id: String,
    pub protocol: LokiProtocol,
    pub endpoint: String,
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
        let (host, mut port) = entry.resolve_endpoint()?;
        let obj = entry.settings.as_object().ok_or_else(|| {
            EtpError::Config(format!(
                "Loki ETP '{}' settings must be an object",
                entry.id
            ))
        })?;

        let raw_endpoint = obj
            .get("endpoint")
            .or_else(|| obj.get("url"))
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty());

        let protocol = match obj.get("protocol").and_then(|v| v.as_str()) {
            Some(p) => match p.trim().to_ascii_lowercase().as_str() {
                "http" | "https" => LokiProtocol::Http,
                "grpc" | "grpcs" => LokiProtocol::Grpc,
                other => {
                    return Err(EtpError::Config(format!(
                        "Loki ETP '{}' unsupported protocol '{other}'; expected 'http' or 'grpc'",
                        entry.id
                    )));
                }
            },
            None => match raw_endpoint {
                Some(ep)
                    if ep.to_ascii_lowercase().starts_with("grpc://")
                        || ep.to_ascii_lowercase().starts_with("grpcs://") =>
                {
                    LokiProtocol::Grpc
                }
                _ => LokiProtocol::Http,
            },
        };

        let explicit_port = if obj.get("host").is_some() {
            obj.get("port").is_some()
        } else if let Some(ep) = raw_endpoint {
            endpoint_has_explicit_port(ep)
        } else {
            false
        };

        let raw_lower = raw_endpoint.map(str::to_ascii_lowercase);
        let is_tls = entry.tls.is_some()
            || raw_lower
                .as_deref()
                .is_some_and(|s| s.starts_with("https://") || s.starts_with("grpcs://"));

        if protocol == LokiProtocol::Grpc && !explicit_port {
            port = if is_tls { 443 } else { 9096 };
        }

        let endpoint = raw_endpoint.map(ToString::to_string).unwrap_or_else(|| {
            let scheme = match (protocol, is_tls) {
                (LokiProtocol::Grpc, true) => "grpcs",
                (LokiProtocol::Grpc, false) => "grpc",
                (LokiProtocol::Http, true) => "https",
                (LokiProtocol::Http, false) => "http",
            };
            format!("{scheme}://{host}:{port}")
        });

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
            host,
            port,
            host_header,
            base_path,
            tenant_id,
            bearer_token,
            basic_auth,
            custom_headers,
            pool: entry.pool.clone(),
            tls: entry.tls.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::EtpPoolSettings;

    fn loki_entry(settings: serde_json::Value, tls: Option<EtpTlsConfig>) -> EtpEntryConfig {
        EtpEntryConfig {
            id: "loki-cfg-test".into(),
            kind: EtpKind::Loki,
            pool: EtpPoolSettings::default(),
            tls,
            settings,
        }
    }

    #[test]
    fn test_loki_protocol_and_default_ports_and_headers() {
        let http_cfg = LokiEtpConfig::from_entry(&loki_entry(
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
            serde_json::json!({ "endpoint": "grpc://loki.local" }),
            None,
        ))
        .unwrap();
        assert_eq!(grpc_plain.protocol, LokiProtocol::Grpc);
        assert_eq!(grpc_plain.port, 9096);

        let grpcs_tls = LokiEtpConfig::from_entry(&loki_entry(
            serde_json::json!({ "endpoint": "grpcs://loki.local" }),
            None,
        ))
        .unwrap();
        assert_eq!(grpcs_tls.protocol, LokiProtocol::Grpc);
        assert_eq!(grpcs_tls.port, 443);

        let host_grpc = LokiEtpConfig::from_entry(&loki_entry(
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
