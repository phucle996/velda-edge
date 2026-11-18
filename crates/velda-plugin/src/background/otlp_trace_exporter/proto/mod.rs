//! OpenTelemetry Protobuf Trace Wire Encoding & gRPC Transport (`otlp_trace_exporter::proto`).

pub mod client;
pub mod template;

pub use client::OtlpTracesGrpcClient;
pub use template::OtlpTraceProtoEnvelope;
