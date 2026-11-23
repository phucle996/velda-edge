//! OTLP JSON Serialization Subsystem (`otlp_log_shipper::json`).
//!
//! ```text
//!   &[TelemetryBatch] ──► OtlpJsonEnvelope + fast LUT stamping ──► JSON bytes buffer
//! ```

pub mod template;

pub use template::{OTLP_SCHEMA_URL, OtlpJsonEnvelope, format_otlp_records_json};
