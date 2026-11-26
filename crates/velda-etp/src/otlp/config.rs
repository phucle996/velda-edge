//! OTLP ETP configuration parser (`OtlpEtpConfig`, `OtlpProtocol`).
//!
//! ```text
//!   EtpEntryConfig (kind == EtpKind::Otlp, security: Plain | Tls | Mtls)
//!         │
//!         ├─► protocol: explicit "protocol" ("http"|"http/protobuf"|"json"|"http/json"|"grpc")
//!         ├─► resolve_endpoint() ──► (host, port) [gRPC: 4317/443 | HTTP: 4318/443]
//!         ├─► format_host_header() + read_base_path() ──► host_header, base_path
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
    EtpEntryConfig, EtpKind, EtpPoolSettings, format_host_header, read_base_path,
    synthesize_endpoint_uri,
};
use crate::connect::{EtpTlsConfig, EtpTransportSecurity};
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
            "http" | "http/protobuf" | "http_protobuf" | "http-protobuf" => Ok(Self::HttpProtobuf),
            "json" | "http/json" | "http_json" | "http-json" => Ok(Self::HttpJson),
            "grpc" => Ok(Self::Grpc),
            other => Err(EtpError::Config(format!(
                "Unsupported OTLP protocol '{other}'; expected http, http/protobuf, http/json, json, or grpc"
            ))),
        }
    }
}

/// Tailored configuration for an OpenTelemetry Collector (OTLP) ETP pool.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OtlpEtpConfig {
    pub id: String,
    pub endpoint: String,
    pub http_scheme: String,
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
        entry.require_security(&[
            EtpTransportSecurity::Plain,
            EtpTransportSecurity::Tls,
            EtpTransportSecurity::Mtls,
        ])?;
        let (host, mut port) = entry.resolve_endpoint()?;
        let obj = entry.settings.as_object().ok_or_else(|| {
            EtpError::Config(format!(
                "OTLP ETP '{}' settings must be an object",
                entry.id
            ))
        })?;

        let protocol = match obj.get("protocol").and_then(|v| v.as_str()) {
            Some(p) => OtlpProtocol::parse(p)?,
            None => OtlpProtocol::HttpProtobuf,
        };

        let explicit_port = obj.get("port").is_some();
        let is_tls = entry.is_tls_transport();
        if !explicit_port {
            port = match (protocol, is_tls) {
                (_, true) => 443,
                (OtlpProtocol::Grpc, false) => 4317,
                (OtlpProtocol::HttpProtobuf | OtlpProtocol::HttpJson, false) => 4318,
            };
        }

        let base_path = read_base_path(obj, &entry.id)?;
        let http_scheme = if is_tls { "https" } else { "http" }.to_string();
        let scheme = match (protocol, is_tls) {
            (OtlpProtocol::Grpc, true) => "grpcs",
            (OtlpProtocol::Grpc, false) => "grpc",
            (_, true) => "https",
            (_, false) => "http",
        };
        let endpoint = synthesize_endpoint_uri(scheme, &host, port, &base_path);
        let host_header = format_host_header(&host, port, is_tls);

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
            http_scheme,
            host,
            port,
            host_header,
            base_path,
            protocol,
            headers,
            pool: entry.pool.clone(),
            tls: entry.effective_tls(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::EtpPoolSettings;

    fn make_otlp_entry(
        security: EtpTransportSecurity,
        settings: serde_json::Value,
    ) -> EtpEntryConfig {
        EtpEntryConfig {
            id: "otlp-test".into(),
            kind: EtpKind::Otlp,
            security,
            pool: EtpPoolSettings::default(),
            tls: None,
            settings,
        }
    }

    #[test]
    fn test_otlp_protocol_variants_scheme_inference_and_ports() {
        let http_proto = OtlpEtpConfig::from_entry(&make_otlp_entry(
            EtpTransportSecurity::Plain,
            serde_json::json!({
                "host": "otel.internal",
                "protocol": "http"
            }),
        ))
        .unwrap();
        assert_eq!(http_proto.protocol, OtlpProtocol::HttpProtobuf);
        assert_eq!(http_proto.port, 4318);
        assert_eq!(http_proto.endpoint, "http://otel.internal:4318");

        let http_json = OtlpEtpConfig::from_entry(&make_otlp_entry(
            EtpTransportSecurity::Plain,
            serde_json::json!({
                "host": "otel.internal",
                "port": 4318,
                "base_path": "/custom",
                "protocol": "http/json"
            }),
        ))
        .unwrap();
        assert_eq!(http_json.protocol, OtlpProtocol::HttpJson);
        assert_eq!(http_json.port, 4318);
        assert_eq!(http_json.base_path, "/custom");
        assert_eq!(http_json.endpoint, "http://otel.internal:4318/custom");

        let json_alias = OtlpEtpConfig::from_entry(&make_otlp_entry(
            EtpTransportSecurity::Tls,
            serde_json::json!({
                "host": "otel.example.com",
                "protocol": "json"
            }),
        ))
        .unwrap();
        assert_eq!(json_alias.protocol, OtlpProtocol::HttpJson);
        assert_eq!(json_alias.port, 443);
        assert!(json_alias.tls.is_some());
        assert_eq!(json_alias.endpoint, "https://otel.example.com:443");

        let grpc_tls = OtlpEtpConfig::from_entry(&make_otlp_entry(
            EtpTransportSecurity::Tls,
            serde_json::json!({
                "host": "otel-grpc.internal",
                "protocol": "grpc"
            }),
        ))
        .unwrap();
        assert_eq!(grpc_tls.protocol, OtlpProtocol::Grpc);
        assert_eq!(grpc_tls.port, 443);
        assert_eq!(grpc_tls.endpoint, "grpcs://otel-grpc.internal:443");

        let grpc_plain = OtlpEtpConfig::from_entry(&make_otlp_entry(
            EtpTransportSecurity::Plain,
            serde_json::json!({
                "host": "otel-grpc.internal",
                "protocol": "grpc"
            }),
        ))
        .unwrap();
        assert_eq!(grpc_plain.protocol, OtlpProtocol::Grpc);
        assert_eq!(grpc_plain.port, 4317);
        assert_eq!(grpc_plain.endpoint, "grpc://otel-grpc.internal:4317");
    }
}
