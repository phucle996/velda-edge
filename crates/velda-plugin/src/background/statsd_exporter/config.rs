//! StatsD / DogStatsD Metrics Exporter Configuration (`statsd_exporter::config`).
//!
//! # Strict 100% Explicit Invariant
//! All configuration fields must be explicitly supplied in `plugins.json`. No implicit fallbacks,
//! hidden defaults, or magic values. Validation occurs strictly at config sync time.

pub use velda_sync::StatsdExporterConfig;
