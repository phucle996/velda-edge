//! OpenTelemetry Metrics Exporter Configuration (`otlp_metrics_exporter::config`).
//!
//! # Strict 100% Explicit Invariant
//! All configuration fields must be explicitly supplied in `plugins.json`. No implicit fallbacks,
//! hidden defaults, or magic values. Validation occurs strictly at config sync time.

pub use super::validation::OtlpMetricsExporterConfig;
