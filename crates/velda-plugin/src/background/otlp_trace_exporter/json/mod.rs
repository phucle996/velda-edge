//! OTLP Trace JSON Serialization Subsystem (`otlp_trace_exporter::json`).
//!
//! ```text
//!   &[TelemetryBatch] ──► OtlpTraceJsonEnvelope::format_spans_json ──► JSON string buffer
//! ```

pub mod template;

pub use template::OtlpTraceJsonEnvelope;
