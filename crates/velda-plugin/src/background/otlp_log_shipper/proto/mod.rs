//! OTLP Protobuf Wire Serialization Subsystem (`otlp_log_shipper::proto`).
//!
//! ```text
//!   &[TelemetryBatch] ──► OtlpProtoWireEnvelope / Prost ExportLogsServiceRequest ──► Protobuf bytes
//! ```

pub mod model;
pub mod template;

pub use model::{
    AnyValue, ExportLogsServiceRequest, InstrumentationScope, KeyValue, LogRecord, Resource,
    ResourceLogs, ScopeLogs, any_value, format_otlp_records_proto,
};
pub use template::{OTLP_SCHEMA_URL, OtlpProtoWireEnvelope, varint_len, write_varint};
