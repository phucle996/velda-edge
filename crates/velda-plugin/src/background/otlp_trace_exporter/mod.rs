//! OpenTelemetry Trace Exporter Background Plugin Module (`otlp_trace_exporter`).
//!
//! ```text
//!    TraceStore (Observability Core)
//!          │
//!          ▼
//!   subscribe(256) ──► TraceSubscriber
//!                            │
//!                            ▼
//!               OtlpTraceExporterPlugin (Shadow Worker, weight: 50)
//!               ├── gRPC (HTTP/2 Protobuf: ExportTraceServiceRequest)
//!               └── HTTP (HTTP/1.1 JSON: /v1/traces)
//! ```

mod config;
mod json;
mod plugin;
mod proto;
mod validation;

// Root exports only
pub use config::OtlpTraceExporterConfig;
pub use plugin::OtlpTraceExporterPlugin;
pub use validation::validate_otlp_trace_exporter_config;
