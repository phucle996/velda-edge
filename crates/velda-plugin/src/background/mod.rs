//! Background Execution Stage Plugins (`BackgroundPlugin`).
//!
//! ```text
//!   Plugins (Shadow Workers, SCHED_IDLE):
//!   ├── health_probe        ──► local liveness / readiness HTTP endpoint
//!   ├── acme_cert_rotator   ──► ACME / TLS certificate lifecycle automation
//!   ├── log_exporter        ──► local streams + ETP log targets (ClickHouse/ES/Loki/S3/Kafka/OTLP)
//!   ├── metrics_exporter    ──► Prometheus pull scrape + ETP push targets (OTLP/StatsD)
//!   └── trace_exporter      ──► ETP distributed span targets (OTLP)
//! ```
//!
//! # Strict Architectural Invariants
//! - **Background-Only**: Background plugins MUST NEVER bind or poll client traffic sockets.
//! - **Non-blocking Telemetry**: Telemetry is extracted via lock-free atomic snapshots
//!   (`MetricsStore::snapshot`) and SPSC record rings (`TelemetryStore`), guaranteeing zero write
//!   contention on Data Plane worker hot paths.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::watch;

use crate::capability::{Direction, PluginCapability, SupportedProtocols};
use crate::error::PluginError;

pub mod acme_cert_rotator;
pub mod health_probe;
pub mod log_exporter;
pub mod metrics_exporter;
pub mod trace_exporter;

pub use crate::config::OutboundTlsConfig;
pub use acme_cert_rotator::{AcmeCertRotatorConfig, AcmeCertRotatorPlugin};
pub use health_probe::{HealthProbeConfig, HealthProbePlugin, HealthState};
pub use log_exporter::{LogExporterConfig, LogExporterPlugin};
pub use metrics_exporter::{MetricsExporterConfig, MetricsExporterPlugin};
pub use trace_exporter::{TraceExporterConfig, TraceExporterPlugin};

/// Boxed future returned by any plugin's background workload loop (`pbww() > 0`).
pub type BackgroundTaskFuture = Pin<Box<dyn Future<Output = Result<(), PluginError>> + Send>>;

/// Contract for plugins operating as background logistics services.
pub trait BackgroundPlugin: Send + Sync {
    /// Unique identifier of this background plugin instance.
    fn id(&self) -> &str;

    /// Runs the background service loop until shutdown is signaled.
    fn run(self: Arc<Self>, shutdown: watch::Receiver<bool>) -> BackgroundTaskFuture;

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

    /// Plugin Background Workload Weight (`0..=100`, where `0` = none, `1..=100` = shadow daemon weight)
    /// representing compute intensity, memory footprint, and cadence for static multi-core LPT bin-packing.
    fn pbww(&self) -> u32;

    /// Returns all External Third-Party (`velda-etp`) pool identifiers referenced by this
    /// background plugin (empty for local/pull-only background services).
    #[inline(always)]
    fn etp_ids(&self) -> Vec<&str> {
        Vec::new()
    }

    /// Attaches a resolved [`velda_etp::EtpInstance`] from [`velda_etp::EtpRegistry`] before
    /// background workers start.
    #[inline(always)]
    fn attach_etp(&self, _instance: &velda_etp::EtpInstance) -> Result<(), PluginError> {
        Ok(())
    }
}

/// Compile-time registry check for plugins owned by the `background` stage.
#[inline]
pub fn owns_id(id: &str) -> bool {
    matches!(
        id,
        "health_probe"
            | "acme_cert_rotator"
            | "log_exporter"
            | "metrics_exporter"
            | "trace_exporter"
    )
}

/// Sub-dispatcher: instantiates background plugin descriptors.
pub fn instantiate(
    config: &crate::config::PluginConfig,
) -> Result<Arc<dyn BackgroundPlugin>, PluginError> {
    match config.id.as_str() {
        "health_probe" => {
            let probe = HealthProbePlugin::from_config(config)?;
            probe.state.set_ready(true);
            Ok(Arc::new(probe))
        }
        "acme_cert_rotator" => {
            let rotator = AcmeCertRotatorPlugin::from_config(config)?;
            Ok(Arc::new(rotator))
        }
        "log_exporter" => {
            let exporter = LogExporterPlugin::from_config(config)?;
            Ok(Arc::new(exporter))
        }
        "metrics_exporter" => {
            let exporter = MetricsExporterPlugin::from_config(config)?;
            Ok(Arc::new(exporter))
        }
        "trace_exporter" => {
            let exporter = TraceExporterPlugin::from_config(config)?;
            Ok(Arc::new(exporter))
        }
        other => Err(PluginError::Config(format!(
            "Unsupported background plugin '{other}' for plugin '{}'",
            config.id
        ))),
    }
}
