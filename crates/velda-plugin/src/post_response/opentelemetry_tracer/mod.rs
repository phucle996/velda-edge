//! OpenTelemetry Span Reporter Post-Response Plugin Module (`opentelemetry_tracer`).

pub mod client;
pub mod client_grpc;
pub mod config;
pub mod drainer;
pub mod plugin;
pub mod proto;
pub mod ring_buffer;
pub mod sampling;
pub mod simd;
pub mod span;

pub use client::{OtlpTraceClient, OtlpTraceEndpoint};
pub use client_grpc::{OtlpTracesGrpcClient, OtlpTracesGrpcEndpoint};
pub use config::OpentelemetryTracerConfig;
pub use drainer::ShadowOtlpTracerDrainer;
pub use plugin::OpentelemetryTracerPlugin;
pub use proto::{ExportTraceServiceRequest, format_otlp_spans_proto, format_packed_spans_proto};
pub use ring_buffer::{ShardedRingBuffer, SpscRingBuffer};
pub use sampling::SamplingTable;
pub use span::{
    OtlpSpanItem, PackedOtlpSpan, decode_hex, format_otlp_spans_json, format_packed_spans_json,
};
