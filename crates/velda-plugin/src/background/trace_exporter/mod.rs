//! Consolidated Trace Exporter Background Plugin (`trace_exporter`).
//!
//! Subscribes to [`velda_observability::TelemetryStore`] and exports request span batches
//! via `velda-etp` backends (`OtlpEtp`).

pub mod config;
pub mod plugin;
pub mod providers;
pub mod validation;

pub use config::{TraceExporterConfig, TraceTargetConfig};
pub use plugin::TraceExporterPlugin;
pub use validation::validate_trace_exporter_config;
