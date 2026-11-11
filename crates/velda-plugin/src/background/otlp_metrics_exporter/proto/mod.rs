//! OpenTelemetry Metrics Protobuf Subsystem (`otlp_metrics_exporter::proto`).

pub mod client;
pub mod model;
pub mod template;

pub use client::{GrpcIoStream, OtlpGrpcEndpoint, OtlpMetricsGrpcClient};
pub use model::*;
pub use template::{OtlpProtoTemplate, format_otlp_metrics_proto, format_otlp_metrics_proto_into};
