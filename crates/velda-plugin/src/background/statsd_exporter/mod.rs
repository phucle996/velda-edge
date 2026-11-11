//! StatsD & DogStatsD Metrics Exporter Plugin.

pub mod config;
pub mod dialect;
pub mod lut;
pub mod plugin;
pub mod transport;

pub use config::StatsdExporterConfig;
pub use plugin::StatsdExporterPlugin;
