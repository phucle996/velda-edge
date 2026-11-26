//! Background Execution Stage Plugins (`BackgroundPlugin`).
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

pub mod acme_cert_rotator;
pub mod clickhouse_log_shipper;
pub mod elasticsearch_log_shipper;
pub mod health_probe;
pub mod kafka_log_shipper;
pub mod loki_log_shipper;
pub mod otlp_log_shipper;
pub mod otlp_metrics_exporter;
pub mod otlp_trace_exporter;
pub mod prometheus_exporter;
pub mod s3_log_shipper;
pub mod statsd_exporter;
pub mod std_log_shipper;

pub use crate::config::OutboundTlsConfig;
pub use acme_cert_rotator::{AcmeCertRotatorConfig, AcmeCertRotatorPlugin};
pub use clickhouse_log_shipper::{ClickHouseLogShipperConfig, ClickHouseLogShipperPlugin};
pub use elasticsearch_log_shipper::{ElasticsearchLogShipperConfig, ElasticsearchLogShipperPlugin};
pub use health_probe::{HealthProbeConfig, HealthProbePlugin, HealthState};
pub use kafka_log_shipper::{KafkaLogShipperConfig, KafkaLogShipperPlugin};
pub use loki_log_shipper::{LokiLogShipperConfig, LokiLogShipperPlugin};
pub use otlp_log_shipper::{OtlpLogShipperConfig, OtlpLogShipperPlugin};
pub use otlp_metrics_exporter::{OtlpMetricsExporterConfig, OtlpMetricsExporterPlugin};
pub use otlp_trace_exporter::{OtlpTraceExporterConfig, OtlpTraceExporterPlugin};
pub use prometheus_exporter::{PrometheusExporterConfig, PrometheusExporterPlugin};
pub use s3_log_shipper::{S3LogShipperConfig, S3LogShipperPlugin};
pub use statsd_exporter::{StatsdExporterConfig, StatsdExporterPlugin};
pub use std_log_shipper::{StdLogShipperConfig, StdLogShipperPlugin};

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

    /// If this background plugin borrows connections from an External Third-Party (`velda-etp`)
    /// data source, returns the target `etp_id`.
    #[inline(always)]
    fn etp_id(&self) -> Option<&str> {
        None
    }

    /// Attaches the resolved [`velda_etp::EtpInstance`] from [`velda_etp::EtpRegistry`] before
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
            | "prometheus_exporter"
            | "otlp_metrics_exporter"
            | "statsd_exporter"
            | "acme_cert_rotator"
            | "loki_log_shipper"
            | "std_log_shipper"
            | "otlp_log_shipper"
            | "clickhouse_log_shipper"
            | "elasticsearch_log_shipper"
            | "s3_log_shipper"
            | "kafka_log_shipper"
            | "otlp_trace_exporter"
    )
}

/// Sub-dispatcher: instantiates background plugin descriptors.
///
/// # Open-Closed Principle (OCP)
/// Background services are instantiated on dedicated supervisory threads.
pub fn instantiate(
    config: &crate::config::PluginConfig,
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
        "acme_cert_rotator" => {
            let rotator = AcmeCertRotatorPlugin::from_config(config)?;
            Ok(Arc::new(rotator))
        }
        "loki_log_shipper" => {
            let shipper = LokiLogShipperPlugin::from_config(config)?;
            Ok(Arc::new(shipper))
        }
        "std_log_shipper" => {
            let shipper = StdLogShipperPlugin::from_config(config)?;
            Ok(Arc::new(shipper))
        }
        "otlp_log_shipper" => {
            let shipper = OtlpLogShipperPlugin::from_config(config)?;
            Ok(Arc::new(shipper))
        }
        "clickhouse_log_shipper" => {
            let shipper = ClickHouseLogShipperPlugin::from_config(config)?;
            Ok(Arc::new(shipper))
        }
        "elasticsearch_log_shipper" => {
            let shipper = ElasticsearchLogShipperPlugin::from_config(config)?;
            Ok(Arc::new(shipper))
        }
        "s3_log_shipper" => {
            let shipper = S3LogShipperPlugin::from_config(config)?;
            Ok(Arc::new(shipper))
        }
        "kafka_log_shipper" => {
            let shipper = KafkaLogShipperPlugin::from_config(config)?;
            Ok(Arc::new(shipper))
        }
        "otlp_trace_exporter" => {
            let exporter = OtlpTraceExporterPlugin::from_config(config)?;
            Ok(Arc::new(exporter))
        }
        other => Err(PluginError::Config(format!(
            "Unsupported background plugin '{other}' for plugin '{}'",
            config.id
        ))),
    }
}
