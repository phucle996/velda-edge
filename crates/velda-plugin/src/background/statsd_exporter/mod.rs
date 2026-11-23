//! StatsD & DogStatsD Metrics Exporter Background Plugin (`statsd_exporter`).
//!
//! ```text
//!   MetricsStore::snapshot() ─► delta_since(&prev) ─► format_statsd_snapshot()
//!                                                             │
//!                                                             ▼
//!                                               wire::flush_snapshot(&etp, ...)
//!                                                             │
//!                                                             ▼
//!                                            StatsdEtp::acquire() ──► StatsdLease
//!
//!   Module Boundaries:
//!   ┌────────────────────────────────────────────────────────────────────────────┐
//!   │ config.rs | validation.rs | dialect/ | wire.rs | plugin.rs                 │
//!   └────────────────────────────────────────────────────────────────────────────┘
//! ```

pub mod config;
pub mod dialect;
pub mod plugin;
pub mod validation;
pub mod wire;

pub use config::StatsdExporterConfig;
pub use plugin::StatsdExporterPlugin;
pub use validation::validate_statsd_exporter_config;
