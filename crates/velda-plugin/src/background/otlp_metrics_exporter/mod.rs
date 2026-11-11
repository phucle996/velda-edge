//! OpenTelemetry Metrics Exporter Background Plugin Module (`otlp_metrics_exporter`).

pub mod config;
pub mod json;
pub mod lut;
pub mod plugin;
pub mod proto;

pub use config::OtlpMetricsExporterConfig;
pub use plugin::OtlpMetricsExporterPlugin;
