//! OTLP ETP configuration parser (`OtlpEtpConfig`, `OtlpProtocol`).
//!
//! ```text
//!   EtpEntryConfig (kind == EtpKind::Otlp)
//!         │
//!         ├─► protocol: explicit "protocol" or inferred from grpc:// / grpcs:// scheme
//!         │     ("http" | "http/protobuf" -> HttpProtobuf, "json" | "http/json" -> HttpJson, "grpc" -> Grpc)
//!         ├─► resolve_endpoint() ──► (host, port) [gRPC: 4317/443 | HTTP: 4318/443]
//!         ├─► format_host_header() + extract_endpoint_path() ──► host_header, base_path
//!         ├─► extract custom headers Vec<(String, String)>
//!         └─► Ok(OtlpEtpConfig)
//!
//!   Parsed Configuration Layout:
//!   ┌────────────────────────────────────────────────────────────────────────────┐
//!   │ id | endpoint | host | port | host_header | base_path                      │
//!   │ protocol: OtlpProtocol | headers: Vec<(String, String)> | pool | tls       │
//!   └────────────────────────────────────────────────────────────────────────────┘
//! ```

use serde::{Deserialize, Serialize};

use crate::config::{
    EtpEntryConfig, EtpKind, EtpPoolSettings, extract_endpoint_path, format_host_header,
};
use crate::connect::EtpTlsConfig;
use crate::error::EtpError;

/// Wire transport/encoding protocol for OTLP telemetry export.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OtlpProtocol {
    #[default]
    HttpProtobuf,
    HttpJson,
    Grpc,
}

impl OtlpProtocol {
    pub fn parse(raw: &str) -> Result<Self, EtpError> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "http" | "https" | "http/protobuf" | "http_protobuf" | "http-protobuf" => {
                Ok(Self::HttpProtobuf)
            }
            "json" | "http/json" | "http_json" | "http-json" => Ok(Self::HttpJson),
            "grpc" | "grpcs" => Ok(Self::Grpc),
            other => Err(EtpError::Config(format!(
                "Unsupported OTLP protocol '{other}'; expected http, http/protobuf, http/json, json, or grpc"
            ))),
        }
    }
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

/// Tailored configuration for an OpenTelemetry Collector (OTLP) ETP pool.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OtlpEtpConfig {
    pub id: String,
    pub endpoint: String,
    pub host: String,
    pub port: u16,
    pub host_header: String,
    pub base_path: String,
    pub protocol: OtlpProtocol,
    pub headers: Vec<(String, String)>,
    pub pool: EtpPoolSettings,
    pub tls: Option<EtpTlsConfig>,
}

impl OtlpEtpConfig {
    /// Parses an [`OtlpEtpConfig`] from a validated [`EtpEntryConfig`].
    pub fn from_entry(entry: &EtpEntryConfig) -> Result<Self, EtpError> {
        if entry.kind != EtpKind::Otlp {
            return Err(EtpError::KindMismatch {
                id: entry.id.clone(),
                expected: EtpKind::Otlp.as_str(),
                actual: entry.kind.as_str(),
            });
        }
        entry.validate()?;
        let (host, mut port) = entry.resolve_endpoint()?;
        let obj = entry.settings.as_object().ok_or_else(|| {
            EtpError::Config(format!(
                "OTLP ETP '{}' settings must be an object",
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
            Some(p) => OtlpProtocol::parse(p)?,
            None => match raw_endpoint {
                Some(ep)
                    if ep.to_ascii_lowercase().starts_with("grpc://")
                        || ep.to_ascii_lowercase().starts_with("grpcs://") =>
                {
                    OtlpProtocol::Grpc
                }
                _ => OtlpProtocol::HttpProtobuf,
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

        if !explicit_port {
            port = match (protocol, is_tls) {
                (_, true) => 443,
                (OtlpProtocol::Grpc, false) => 4317,
                (OtlpProtocol::HttpProtobuf | OtlpProtocol::HttpJson, false) => 4318,
            };
        }

        let endpoint = raw_endpoint.map(ToString::to_string).unwrap_or_else(|| {
            let scheme = match (protocol, is_tls) {
                (OtlpProtocol::Grpc, true) => "grpcs",
                (OtlpProtocol::Grpc, false) => "grpc",
                (_, true) => "https",
                (_, false) => "http",
            };
            format!("{scheme}://{host}:{port}")
        });

        let host_header = format_host_header(&host, port, is_tls);
        let base_path = extract_endpoint_path(&endpoint);

        let mut headers = Vec::new();
        if let Some(hdr_val) = obj.get("headers") {
            if let Some(map) = hdr_val.as_object() {
                for (k, v) in map {
                    if let Some(vs) = v.as_str() {
                        headers.push((k.clone(), vs.to_string()));
                    }
                }
            } else if let Some(arr) = hdr_val.as_array() {
                for item in arr {
                    if let Some(pair) = item.as_array()
                        && pair.len() == 2
                        && let (Some(k), Some(v)) = (pair[0].as_str(), pair[1].as_str())
                    {
                        headers.push((k.to_string(), v.to_string()));
                    }
                }
            }
        }

        Ok(Self {
            id: entry.id.clone(),
            endpoint,
            host,
            port,
            host_header,
            base_path,
            protocol,
            headers,
            pool: entry.pool.clone(),
            tls: entry.tls.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::EtpPoolSettings;

    fn make_otlp_entry(settings: serde_json::Value) -> EtpEntryConfig {
        EtpEntryConfig {
            id: "otlp-test".into(),
            kind: EtpKind::Otlp,
            pool: EtpPoolSettings::default(),
            tls: None,
            settings,
        }
    }

    #[test]
    fn test_otlp_protocol_variants_scheme_inference_and_ports() {
        let http_proto = OtlpEtpConfig::from_entry(&make_otlp_entry(serde_json::json!({
            "endpoint": "http://otel.internal",
            "protocol": "http"
        })))
        .unwrap();
        assert_eq!(http_proto.protocol, OtlpProtocol::HttpProtobuf);
        assert_eq!(http_proto.port, 4318);

        let http_json = OtlpEtpConfig::from_entry(&make_otlp_entry(serde_json::json!({
            "endpoint": "http://otel.internal:4318/custom",
            "protocol": "http/json"
        })))
        .unwrap();
        assert_eq!(http_json.protocol, OtlpProtocol::HttpJson);
        assert_eq!(http_json.port, 4318);
        assert_eq!(http_json.base_path, "/custom");

        let json_alias = OtlpEtpConfig::from_entry(&make_otlp_entry(serde_json::json!({
            "endpoint": "https://otel.example.com",
            "protocol": "json"
        })))
        .unwrap();
        assert_eq!(json_alias.protocol, OtlpProtocol::HttpJson);
        assert_eq!(json_alias.port, 443);

        let grpc_inferred = OtlpEtpConfig::from_entry(&make_otlp_entry(serde_json::json!({
            "endpoint": "grpcs://otel-grpc.internal"
        })))
        .unwrap();
        assert_eq!(grpc_inferred.protocol, OtlpProtocol::Grpc);
        assert_eq!(grpc_inferred.port, 443);

        let grpc_plain = OtlpEtpConfig::from_entry(&make_otlp_entry(serde_json::json!({
            "endpoint": "grpc://otel-grpc.internal"
        })))
        .unwrap();
        assert_eq!(grpc_plain.protocol, OtlpProtocol::Grpc);
        assert_eq!(grpc_plain.port, 4317);
    }
}
