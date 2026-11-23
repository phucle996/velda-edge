//! OpenTelemetry Metrics Exporter Background Plugin (`otlp_metrics_exporter`).
//!
//! ```text
//!   MetricsStore::snapshot() ──► JSON / Protobuf Encode ──► wire::push_metrics(&etp, ...)
//!                                                                  │
//!                                                                  ▼
//!                                                    OtlpEtp::acquire() ──► OtlpLease
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

pub use config::OtlpMetricsExporterConfig;
pub use plugin::OtlpMetricsExporterPlugin;
pub use validation::validate_otlp_metrics_exporter_config;
