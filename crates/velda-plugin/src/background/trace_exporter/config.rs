//! Consolidated Trace Exporter Configuration (`trace_exporter::config`).
//!
//! ```text
//!   plugins.json ("trace_exporter")
//!         │
//!         └─► targets: Vec<TraceTargetConfig>
//!               ├─► etp_id (mandatory, dispatched by EtpBackend at attach_etp time)
//!               └─► service_name, flush_interval_ms, max_batch_size, request_timeout_ms
//! ```

use serde::{Deserialize, Serialize};

/// Top-level configuration for the unified `trace_exporter` background plugin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TraceExporterConfig {
    /// Ordered list of trace export targets backed by `velda-etp`.
    pub targets: Vec<TraceTargetConfig>,
}

/// Declarative trace export target configuration backed by `velda-etp`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TraceTargetConfig {
    /// Identifier of the target entry in `etp.json` / [`velda_etp::EtpRegistry`].
    pub etp_id: String,
    /// Optional explicit ETP kind assertion checked during `velda-sync`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub etp_kind: Option<String>,
    /// Logical service name reported in OpenTelemetry Resource attributes.
    #[serde(default = "default_service_name")]
    pub service_name: String,
    /// Periodic batch flush interval in milliseconds (`10..=60_000`, default: `1000`).
    #[serde(default = "default_flush_interval")]
    pub flush_interval_ms: u64,
    /// Maximum records grouped per flush payload (`1..=10_000`, default: `1000`).
    #[serde(default = "default_max_batch_size")]
    pub max_batch_size: usize,
    /// Request completion timeout in milliseconds (`100..=60_000`, default: `5000`).
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

impl Default for TraceTargetConfig {
    fn default() -> Self {
        Self {
            etp_id: String::new(),
            etp_kind: None,
            service_name: default_service_name(),
            flush_interval_ms: default_flush_interval(),
            max_batch_size: default_max_batch_size(),
            request_timeout_ms: default_request_timeout(),
        }
    }
}
