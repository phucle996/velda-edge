//! OpenTelemetry Metrics JSON Subsystem (`otlp_metrics_exporter::json`).

pub mod client;
pub mod template;

pub use client::{OtlpEndpoint, OtlpMetricsClient};
pub use template::{OtlpJsonTemplate, format_otlp_metrics_into, format_otlp_metrics_json};
