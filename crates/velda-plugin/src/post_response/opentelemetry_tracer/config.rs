//! Configuration Contract for OpenTelemetry Tracer (`opentelemetry_tracer::config`).

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::sampling::SamplingTable;

/// Declarative configuration for [`OpentelemetryTracerPlugin`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OpentelemetryTracerConfig {
    /// OTLP transport protocol (`"grpc"` or `"http"`).
    pub protocol: String,
    /// OTLP collector endpoint URL (e.g. `grpc://127.0.0.1:4317` or `http://127.0.0.1:4318/v1/traces`).
    pub endpoint: String,
    /// Batch size threshold triggering an immediate flush (between 1 and 4096).
    pub batch_size: usize,
    /// Maximum duration in milliseconds before flushing pending spans (between 10 and 60000).
    pub flush_interval_ms: u64,
    /// Pre-allocated slot capacity per CPU core lane in physical RAM (between 128 and 262144).
    pub buffer_capacity: usize,
    /// TCP and TLS connection establishment timeout in milliseconds (100 to 60000).
    pub connect_timeout_ms: u64,
    /// Request export completion timeout in milliseconds (100 to 60000).
    pub request_timeout_ms: u64,
    /// Service name reported in OpenTelemetry Resource attributes.
    pub service_name: String,
    /// Custom HTTP or gRPC headers.
    pub headers: HashMap<String, String>,
    /// Status-code-specific sampling rates (`"200": 0.05`, `"500": 1.0`). If omitted, all status codes default to 1.0 (100%).
    #[serde(default = "default_full_sampling")]
    pub sampling: HashMap<String, f64>,
}

fn default_full_sampling() -> HashMap<String, f64> {
    let mut m = HashMap::new();
    for code in 100..=599 {
        m.insert(code.to_string(), 1.0);
    }
    m
}

impl OpentelemetryTracerConfig {
    pub fn build_sampling_table(&self) -> Result<SamplingTable, String> {
        SamplingTable::from_map(&self.sampling)
    }
}
