//! OpenTelemetry JSON Trace Formatting & HTTP Client (`otlp_trace_exporter::json`).

pub mod client;
pub mod template;

pub use client::OtlpTraceHttpClient;
pub use template::OtlpTraceJsonEnvelope;
