//! Grafana Loki Log Shipper Configuration (`loki_log_shipper::config`).
//!
//! ```text
//!   plugins.json ("loki_log_shipper")
//!         │
//!         ├─► etp_id ─────────────► resolved from EtpRegistry (LokiEtp: Http | Grpc)
//!         ├─► service_name ───────► static stream label service="..."
//!         ├─► labels ─────────────► static Prometheus identifier labels {k="v",...}
//!         └─► batch / timeout ────► flush_interval_ms | max_batch_size | request_timeout_ms
//!
//!   Plugin Config Layout:
//!   ┌────────────────────────────────────────────────────────────────────────────┐
//!   │ etp_id: String | service_name: String | labels: HashMap<String, String>    │
//!   │ flush_interval_ms: u64 | max_batch_size: usize | request_timeout_ms: u64   │
//!   └────────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! Transport protocol (`http` vs `grpc`), endpoint, `tenant_id`, custom headers, pool sizing,
//! and TLS/mTLS settings belong strictly to the referenced `loki` entry in `etp.json` /
//! [`velda_etp::EtpRegistry`].

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Declarative configuration for the Grafana Loki Log Shipper background plugin (`loki_log_shipper`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LokiLogShipperConfig {
    /// Identifier of the `loki` ETP entry in [`velda_etp::EtpRegistry`].
    pub etp_id: String,
    /// Logical service identifier attached to the `service` stream label.
    #[serde(default = "default_service_name")]
    pub service_name: String,
    /// Periodic batch flush interval in milliseconds (100 to 60,000 ms, default: 1,000).
    #[serde(default = "default_flush_interval")]
    pub flush_interval_ms: u64,
    /// Maximum records grouped into a single Loki HTTP/gRPC push payload (1 to 10,000, default: 1,000).
    #[serde(default = "default_max_batch_size")]
    pub max_batch_size: usize,
    /// HTTP/gRPC push request completion timeout in milliseconds (100 to 60,000 ms, default: 5,000).
    #[serde(default = "default_request_timeout")]
    pub request_timeout_ms: u64,
    /// Additional static Prometheus stream labels merged into every Loki stream descriptor.
    #[serde(default)]
    pub labels: HashMap<String, String>,
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

impl Default for LokiLogShipperConfig {
    fn default() -> Self {
        Self {
            etp_id: String::new(),
            service_name: default_service_name(),
            flush_interval_ms: default_flush_interval(),
            max_batch_size: default_max_batch_size(),
            request_timeout_ms: default_request_timeout(),
            labels: HashMap::new(),
        }
    }
}
