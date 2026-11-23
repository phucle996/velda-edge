//! OTLP ETP configuration parser (`OtlpEtpConfig`, `OtlpProtocol`).
//!
//! ```text
//!   EtpEntryConfig (kind == EtpKind::Otlp)
//!         │
//!         ├─► resolve_endpoint() ──► (host, port) [default 4317]
//!         ├─► OtlpProtocol::parse(protocol) ──► HttpProtobuf | HttpJson | Grpc
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
            "http/protobuf" | "http_protobuf" | "http-protobuf" => Ok(Self::HttpProtobuf),
            "http/json" | "http_json" | "http-json" => Ok(Self::HttpJson),
            "grpc" => Ok(Self::Grpc),
            other => Err(EtpError::Config(format!(
                "Unsupported OTLP protocol '{other}'; expected http/protobuf, http/json, or grpc"
            ))),
        }
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
        let (host, port) = entry.resolve_endpoint()?;
        let obj = entry.settings.as_object().ok_or_else(|| {
            EtpError::Config(format!(
                "OTLP ETP '{}' settings must be an object",
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

        let protocol = match obj.get("protocol").and_then(|v| v.as_str()) {
            Some(p) => OtlpProtocol::parse(p)?,
            None => OtlpProtocol::HttpProtobuf,
        };

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
