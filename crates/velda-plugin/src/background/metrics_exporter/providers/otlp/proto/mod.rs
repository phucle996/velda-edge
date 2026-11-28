//! OpenTelemetry Metrics Protobuf Serialization Subsystem (`otlp_metrics_exporter::proto`).
//!
//! ```text
//!   &MetricsSnapshot ──► OtlpProtoTemplate / Prost ExportMetricsServiceRequest ──► Protobuf bytes
//! ```

pub mod model;
pub mod template;

pub use model::*;
pub use template::{OtlpProtoTemplate, format_otlp_metrics_proto, format_otlp_metrics_proto_into};
