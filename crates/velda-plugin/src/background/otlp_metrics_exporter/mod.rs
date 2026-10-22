//! OpenTelemetry Metrics Exporter Background Plugin Module (`otlp_metrics_exporter`).

pub mod client;
pub mod client_grpc;
pub mod format;
pub mod plugin;
pub mod proto;

pub use client::{OtlpEndpoint, OtlpMetricsClient};
pub use client_grpc::{OtlpGrpcEndpoint, OtlpMetricsGrpcClient};
pub use format::format_otlp_metrics_json;
pub use plugin::{OtlpMetricsExporterConfig, OtlpMetricsExporterPlugin};
pub use proto::{ExportMetricsServiceRequest, format_otlp_metrics_proto};
