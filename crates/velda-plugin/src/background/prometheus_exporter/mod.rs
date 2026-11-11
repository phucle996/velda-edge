//! Prometheus Exporter Plugin Module.
//!
//! Provides zero-allocation, lock-free OpenMetrics telemetry export on dedicated Shadow Worker threads.

pub mod config;
pub mod lut;
pub mod plugin;
pub mod server;
pub mod template;

pub use config::PrometheusExporterConfig;
pub use plugin::PrometheusExporterPlugin;
