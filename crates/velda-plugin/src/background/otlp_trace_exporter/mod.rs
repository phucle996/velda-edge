//! OpenTelemetry Trace Exporter Background Plugin (`otlp_trace_exporter`).
//!
//! ```text
//!   TelemetryStore ──► JSON / Protobuf Wire Encode ──► wire::push_traces(&etp, ...)
//!                                                             │
//!                                                             ▼
//!                                               OtlpEtp::acquire() ──► OtlpLease
//!
//!   Module Boundaries:
//!   ┌────────────────────────────────────────────────────────────────────────────┐
//!   │ config.rs | validation.rs | json/ | proto/ | wire.rs | plugin.rs           │
//!   └────────────────────────────────────────────────────────────────────────────┘
//! ```

pub mod config;
pub mod json;
pub mod plugin;
pub mod proto;
pub mod validation;
pub mod wire;

pub use config::OtlpTraceExporterConfig;
pub use plugin::OtlpTraceExporterPlugin;
pub use validation::validate_otlp_trace_exporter_config;
