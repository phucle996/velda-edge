//! OpenTelemetry Span Reporter Post-Response Plugin Module (`opentelemetry_tracer`).

pub mod client;
pub mod client_grpc;
pub mod plugin;
pub mod proto;
pub mod span;

pub use client::{OtlpTraceClient, OtlpTraceEndpoint};
pub use client_grpc::{OtlpTracesGrpcClient, OtlpTracesGrpcEndpoint};
pub use plugin::{OpentelemetryTracerConfig, OpentelemetryTracerPlugin};
pub use proto::{ExportTraceServiceRequest, format_otlp_spans_proto};
pub use span::{OtlpSpanItem, format_otlp_spans_json};
