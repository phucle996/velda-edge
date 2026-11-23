//! OTLP Trace Protobuf Wire Serialization Subsystem (`otlp_trace_exporter::proto`).
//!
//! ```text
//!   &[TelemetryBatch] ──► OtlpTraceProtoEnvelope::format_spans_wire ──► Protobuf bytes buffer
//! ```

pub mod template;

pub use template::{OtlpTraceProtoEnvelope, varint_len, write_varint};
