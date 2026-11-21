//! OpenTelemetry Trace Exporter Schema & Validation (`phase: "background"`).
//!
//! Validates configuration for [`OtlpTraceExporterPlugin`], which drains reference-counted
//! [`velda_observability::TelemetryRecord`] batches from [`velda_observability::TelemetryStore`]
//! and exports them to OpenTelemetry Collectors over persistent HTTP/1.1 or HTTP/2 gRPC connections.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::error::PluginError;

/// Declarative configuration for the OpenTelemetry Trace Exporter background plugin (`otlp_trace_exporter`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OtlpTraceExporterConfig {
    /// OTLP transport protocol (`"http"` or `"grpc"`).
    pub protocol: String,
    /// OTLP collector endpoint URL (e.g. `http://127.0.0.1:4318/v1/traces` or `grpc://127.0.0.1:4317`).
    pub endpoint: String,
    /// Logical service name reported in OpenTelemetry Resource attributes.
    pub service_name: String,
    /// Periodic batch flush interval in milliseconds (10 to 60,000 ms).
    pub flush_interval_ms: u64,
    /// Maximum records grouped in a single OTLP export payload (1 to 10,000).
    pub max_batch_size: usize,
    /// TCP and TLS connection establishment timeout in milliseconds (100 to 60,000 ms).
    pub connect_timeout_ms: u64,
    /// Request export completion timeout in milliseconds (100 to 60,000 ms).
    pub request_timeout_ms: u64,
    /// Optional HTTP/gRPC custom headers (use `{}` if no custom headers are required).
    pub headers: HashMap<String, String>,
    /// Outbound TLS settings for connecting to HTTPS/gRPCs OTLP trace endpoints.
    #[serde(default)]
    pub tls: crate::config::OutboundTlsConfig,
}

/// Validates [`OtlpTraceExporterConfig`] at configuration compilation time.
pub fn validate_otlp_trace_exporter_config(
    plugin_id: &str,
    config_val: &serde_json::Value,
) -> Result<OtlpTraceExporterConfig, PluginError> {
    let cfg: OtlpTraceExporterConfig = serde_json::from_value(config_val.clone()).map_err(|e| {
        PluginError::Config(format!(
            "Plugin '{plugin_id}' has malformed or incomplete otlp_trace_exporter config: {e}"
        ))
    })?;

    let protocol = cfg.protocol.trim().to_ascii_lowercase();
    if protocol != "http" && protocol != "grpc" {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': protocol must be explicitly set to 'http' or 'grpc', found '{protocol}'"
        )));
    }

    let endpoint = cfg.endpoint.trim();
    if !endpoint.starts_with("http://")
        && !endpoint.starts_with("https://")
        && !endpoint.starts_with("grpc://")
        && !endpoint.starts_with("grpcs://")
    {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': endpoint '{endpoint}' must start with 'http://', 'https://', 'grpc://', or 'grpcs://'"
        )));
    }

    if cfg.service_name.trim().is_empty() {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': service_name cannot be empty"
        )));
    }

    if cfg.flush_interval_ms < 10 || cfg.flush_interval_ms > 60_000 {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': flush_interval_ms must be between 10 and 60,000, found {}",
            cfg.flush_interval_ms
        )));
    }

    if cfg.max_batch_size < 1 || cfg.max_batch_size > 10_000 {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': max_batch_size must be between 1 and 10,000, found {}",
            cfg.max_batch_size
        )));
    }

    if cfg.connect_timeout_ms < 100 || cfg.connect_timeout_ms > 60_000 {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': connect_timeout_ms must be between 100 and 60,000, found {}",
            cfg.connect_timeout_ms
        )));
    }

    if cfg.request_timeout_ms < 100 || cfg.request_timeout_ms > 60_000 {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': request_timeout_ms must be between 100 and 60,000, found {}",
            cfg.request_timeout_ms
        )));
    }

    Ok(cfg)
}
