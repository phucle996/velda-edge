//! Consolidated Metrics Exporter Background Plugin (`metrics_exporter`).
//!
//! Exports runtime metrics across one or more declared targets routed either locally via a
//! Prometheus pull scrape endpoint (`mode: "pull"`) or via `velda-etp` (`mode: "push"`: OTLP, StatsD),
//! using bit-scanned `InternalStatus` state masks and tick deltas.

pub mod config;
pub mod plugin;
pub mod providers;
pub mod validation;

pub use config::{
    AuthExporterConfig, CorsExporterConfig, MetricsExporterConfig, MetricsPushTargetConfig,
    MetricsTargetConfig, OtlpMetricsPushConfig, OtlpTemporality, PrometheusPullConfig,
    ScrapeExporterConfig, StatsdMetricsPushConfig, TlsExporterConfig,
};
pub use plugin::MetricsExporterPlugin;
pub use providers::statsd::dialect::StatsdDialect;
pub use validation::validate_metrics_exporter_config;
