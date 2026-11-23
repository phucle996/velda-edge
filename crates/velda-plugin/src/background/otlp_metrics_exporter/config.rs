//! OpenTelemetry Metrics Exporter Configuration (`otlp_metrics_exporter::config`).
//!
//! ```text
//!   plugins.json ("otlp_metrics_exporter")
//!         │
//!         ├─► etp_id ─────────────► resolved from EtpRegistry (OtlpEtp)
//!         ├─► service_name ───────► OpenTelemetry Resource service.name attribute
//!         └─► cadence / timeout ──► interval_secs | request_timeout_ms
//!
//!   Plugin Config Layout:
//!   ┌────────────────────────────────────────────────────────────────────────────┐
//!   │ etp_id: String | service_name: String                                      │
//!   │ interval_secs: u64 | request_timeout_ms: u64                               │
//!   └────────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! Collector endpoints, protocol selection (`http/protobuf`, `http/json`, `grpc`), custom headers,
//! pool capacity, and TLS/mTLS settings belong strictly to the referenced `otlp` entry in
//! `etp.json` / [`velda_etp::EtpRegistry`].

use serde::{Deserialize, Serialize};

/// Declarative configuration for the OpenTelemetry Metrics Exporter plugin (`otlp_metrics_exporter`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OtlpMetricsExporterConfig {
    /// Identifier of the `otlp` ETP entry in [`velda_etp::EtpRegistry`].
    pub etp_id: String,
    /// Periodic push interval in seconds (1 to 3,600, default: 15).
    #[serde(default = "default_interval_secs")]
    pub interval_secs: u64,
    /// Request export completion timeout in milliseconds (100 to 60,000, default: 5000).
    #[serde(default = "default_request_timeout")]
    pub request_timeout_ms: u64,
    /// Service name reported in OpenTelemetry Resource attributes.
    #[serde(default = "default_service_name")]
    pub service_name: String,
}

fn default_interval_secs() -> u64 {
    15
}

fn default_request_timeout() -> u64 {
    5000
}

fn default_service_name() -> String {
    "velda-edge".to_string()
}

impl Default for OtlpMetricsExporterConfig {
    fn default() -> Self {
        Self {
            etp_id: String::new(),
            interval_secs: default_interval_secs(),
            request_timeout_ms: default_request_timeout(),
            service_name: default_service_name(),
        }
    }
}
