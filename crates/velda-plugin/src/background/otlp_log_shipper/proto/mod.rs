//! OTLP gRPC Protobuf Transport & Serialization Subsystem (`otlp_log_shipper::proto`).
//!
//! Owns persistent HTTP/2 gRPC stream delivery (`/opentelemetry.proto.collector.logs.v1.LogsService/Export`)
//! and direct Protobuf wire serialization with zero AST allocation and SIMD timestamp stamping.

pub mod client;
pub mod model;
pub mod template;

pub use client::{OtlpLogsGrpcClient, OtlpLogsGrpcEndpoint};
pub use model::{
    AnyValue, ExportLogsServiceRequest, InstrumentationScope, KeyValue, LogRecord, Resource,
    ResourceLogs, ScopeLogs, any_value, format_otlp_records_proto,
};
pub use template::{OTLP_SCHEMA_URL, OtlpProtoWireEnvelope, varint_len, write_varint};
