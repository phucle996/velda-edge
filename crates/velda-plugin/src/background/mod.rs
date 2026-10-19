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

use crate::capability::{Direction, PluginCapability, SupportedProtocols};

pub mod health_probe;
pub mod prometheus;

pub use health_probe::{HealthProbeConfig, HealthProbePlugin, HealthState};
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

    /// Declares the reverse-permission capabilities (always default None for background services).
    #[inline(always)]
    fn capability(&self) -> PluginCapability {
        PluginCapability::default()
    }

    /// Hardcoded network/application protocols supported by this plugin.
    #[inline(always)]
    fn supported_protocols(&self) -> SupportedProtocols {
        SupportedProtocols::empty()
    }

    /// Execution direction. Always [`Direction::Background`] for background auxiliary plugins.
    #[inline(always)]
    fn direction(&self) -> Direction {
        Direction::Background
    }
}

/// Sub-dispatcher: instantiates background auxiliary plugin descriptors.
///
/// # Open-Closed Principle (OCP)
/// Background auxiliary services are instantiated on dedicated supervisory threads.
pub fn instantiate(
    config: &velda_sync::post_sync::plugin::PluginConfig,
) -> Result<crate::factory::LoadedPlugin, PluginError> {
    match config.id.as_str() {
        "health_probe" => Ok(crate::factory::LoadedPlugin::Background {
            id: config.id.clone(),
        }),
        "prometheus_exporter" => Ok(crate::factory::LoadedPlugin::Background {
            id: config.id.clone(),
        }),
        other => Err(PluginError::Config(format!(
            "Unsupported background plugin '{other}' for plugin '{}'",
            config.id
        ))),
    }
}
