//! Background Execution Phase Plugins (`phase: "background"`).
//!
//! Background plugins run strictly on Shadow Logistics runtimes isolated from
//! Data Plane traffic workers (e.g., Prometheus Exporter, Health Probes).
//!
//! # Strict Architectural Invariants
//! - **Background-Only**: Background plugins MUST NEVER bind or poll client traffic sockets.
//! - **Non-blocking Metrics Extraction**: Telemetry is extracted via lock-free atomic snapshots
//!   (`MetricsStore::snapshot`), guaranteeing zero write contention on Data Plane worker hot paths.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::watch;

use crate::capability::{Direction, PluginCapability, SupportedProtocols};
use crate::error::PluginError;

pub mod health_probe;
pub mod otlp_metrics_exporter;
pub mod prometheus_exporter;
pub mod statsd_exporter;

pub use health_probe::{HealthProbeConfig, HealthProbePlugin, HealthState};
pub use otlp_metrics_exporter::{OtlpMetricsExporterConfig, OtlpMetricsExporterPlugin};
pub use prometheus_exporter::{PrometheusExporter, PrometheusExporterPlugin, format_snapshot};
pub use statsd_exporter::{StatsdExporterConfig, StatsdExporterPlugin};

/// Contract for plugins operating as background logistics services.
pub trait BackgroundPlugin: Send + Sync {
    /// Unique identifier of this background plugin instance.
    fn id(&self) -> &str;

    /// Runs the background service loop until shutdown is signaled.
    fn run(
        self: Arc<Self>,
        shutdown: watch::Receiver<bool>,
    ) -> Pin<Box<dyn Future<Output = Result<(), PluginError>> + Send>>;

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

    /// Execution direction. Always [`Direction::Background`] for background plugins.
    #[inline(always)]
    fn direction(&self) -> Direction {
        Direction::Background
    }
}

/// Spawns all declared background plugins onto isolated Tokio tasks.
///
/// Each task is configured with idle scheduling priority (`set_current_thread_idle_priority`)
/// to guarantee zero CPU contention with traffic worker threads.
pub fn spawn_background_plugins(
    plugins: &[Arc<dyn BackgroundPlugin>],
    shutdown: watch::Receiver<bool>,
) -> Vec<tokio::task::JoinHandle<()>> {
    let mut handles = Vec::with_capacity(plugins.len());
    for plugin in plugins {
        let p = Arc::clone(plugin);
        let shutdown_rx = shutdown.clone();
        let handle = tokio::spawn(async move {
            velda_core::hardware::set_current_thread_idle_priority();
            let id = p.id().to_string();
            tracing::info!(plugin_id = %id, "Background plugin starting");
            if let Err(e) = p.run(shutdown_rx).await {
                tracing::error!(plugin_id = %id, error = %e, "Background plugin exited with error");
            } else {
                tracing::info!(plugin_id = %id, "Background plugin stopped cleanly");
            }
        });
        handles.push(handle);
    }
    handles
}

/// Sub-dispatcher: instantiates background plugin descriptors.
///
/// # Open-Closed Principle (OCP)
/// Background services are instantiated on dedicated supervisory threads.
pub fn instantiate(
    config: &velda_sync::post_sync::plugin::PluginConfig,
) -> Result<Arc<dyn BackgroundPlugin>, PluginError> {
    match config.id.as_str() {
        "health_probe" => {
            let probe = HealthProbePlugin::from_config(config)?;
            probe.state.set_ready(true);
            Ok(Arc::new(probe))
        }
        "prometheus_exporter" => {
            let exporter = PrometheusExporterPlugin::from_config(config)?;
            Ok(Arc::new(exporter))
        }
        "otlp_metrics_exporter" => {
            let exporter = OtlpMetricsExporterPlugin::from_config(config)?;
            Ok(Arc::new(exporter))
        }
        "statsd_exporter" => {
            let exporter = StatsdExporterPlugin::from_config(config)?;
            Ok(Arc::new(exporter))
        }
        other => Err(PluginError::Config(format!(
            "Unsupported background plugin '{other}' for plugin '{}'",
            config.id
        ))),
    }
}
