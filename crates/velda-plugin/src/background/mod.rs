//! Background Execution Phase Plugins (`phase: "background"`).
//!
//! Background plugins run strictly on auxiliary supervisory runtimes isolated from
//! Data Plane traffic workers (e.g., Prometheus Exporter, Health Probes).
//!
//! # Strict Architectural Invariants
//! - **Auxiliary-Only**: Background plugins MUST NEVER bind or poll client traffic sockets.
//! - **Non-blocking Metrics Extraction**: Telemetry is extracted via lock-free atomic snapshots
//!   (`MetricsStore::snapshot`), guaranteeing zero write contention on Data Plane worker hot paths.

use std::future::Future;
use tokio::sync::watch;

use crate::error::PluginError;

pub mod health_probe;
pub mod prometheus;

pub use health_probe::HealthProbePlugin;
pub use prometheus::{PrometheusExporter, format_snapshot};

/// Contract for plugins operating as background auxiliary services.
pub trait BackgroundPlugin: Send + Sync {
    /// Unique identifier of this background plugin instance.
    fn id(&self) -> &str;

    /// Runs the background service loop until shutdown is signaled.
    fn run(
        self: Box<Self>,
        shutdown: watch::Receiver<bool>,
    ) -> impl Future<Output = Result<(), PluginError>> + Send;
}
