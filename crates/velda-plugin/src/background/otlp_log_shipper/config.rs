//! OpenTelemetry Log Shipper Configuration (`otlp_log_shipper::config`).
//!
//! ```text
//!   plugins.json ("otlp_log_shipper")
//!         │
//!         ├─► etp_id ─────────────► resolved from EtpRegistry (OtlpEtp)
//!         ├─► service_name ───────► OpenTelemetry Resource service.name attribute
//!         └─► batch / timeouts ───► flush_interval_ms | max_batch_size | request_timeout_ms
//!
//!   Plugin Config Layout:
//!   ┌────────────────────────────────────────────────────────────────────────────┐
//!   │ etp_id: String | service_name: String                                      │
//!   │ flush_interval_ms: u64 | max_batch_size: usize | request_timeout_ms: u64   │
//!   └────────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! Collector endpoints, protocol selection (`http/protobuf`, `http/json`, `grpc`), custom headers,
//! pool capacity, and TLS/mTLS settings belong strictly to the referenced `otlp` entry in
//! `etp.json` / [`velda_etp::EtpRegistry`].

use serde::{Deserialize, Serialize};

/// Declarative configuration for the OpenTelemetry Log Shipper background plugin (`otlp_log_shipper`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OtlpLogShipperConfig {
    /// Identifier of the `otlp` ETP entry in [`velda_etp::EtpRegistry`].
    pub etp_id: String,
    /// Logical service name reported in OpenTelemetry Resource attributes.
    #[serde(default = "default_service_name")]
    pub service_name: String,
    /// Periodic batch flush interval in milliseconds (100 to 60,000 ms, default: 1000).
    #[serde(default = "default_flush_interval")]
    pub flush_interval_ms: u64,
    /// Maximum records grouped in a single OTLP export payload (1 to 10,000, default: 1000).
    #[serde(default = "default_max_batch_size")]
    pub max_batch_size: usize,
    /// Request export completion timeout in milliseconds (100 to 60,000 ms, default: 5000).
    #[serde(default = "default_request_timeout")]
    pub request_timeout_ms: u64,
}

fn default_service_name() -> String {
    "velda-edge".to_string()
}

fn default_flush_interval() -> u64 {
    1000
}

fn default_max_batch_size() -> usize {
    1000
}

fn default_request_timeout() -> u64 {
    5000
}

impl Default for OtlpLogShipperConfig {
    fn default() -> Self {
        Self {
            etp_id: String::new(),
            service_name: default_service_name(),
            flush_interval_ms: default_flush_interval(),
            max_batch_size: default_max_batch_size(),
            request_timeout_ms: default_request_timeout(),
        }
    }
}
