//! OpenTelemetry Metrics JSON Serialization Subsystem (`otlp_metrics_exporter::json`).
//!
//! ```text
//!   &MetricsSnapshot ──► OtlpJsonTemplate::format_into() ──► JSON bytes buffer
//! ```

pub mod template;

pub use template::{OtlpJsonTemplate, format_otlp_metrics_into, format_otlp_metrics_json};
