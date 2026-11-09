//! StatsD & DogStatsD Metrics Exporter Plugin.

pub mod formatter;
pub mod plugin;
pub mod transport;

pub use formatter::{StatsdDialect, format_statsd_snapshot};
pub use plugin::StatsdExporterPlugin;
pub use transport::StatsdTransport;
pub use velda_sync::StatsdExporterConfig;
