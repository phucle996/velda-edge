//! Static-Dispatch Metrics Providers (`metrics_exporter::providers`).
//!
//! ```text
//!   mode == "pull"                      ──► PrometheusExporter (HTTP/HTTPS GET /metrics + status_mask)
//!   mode == "push" + EtpInstance.backend
//!         ├─► EtpBackend::Otlp          ──► OtlpMetricsProvider (delta_since/cumulative + status_mask)
//!         └─► EtpBackend::Statsd        ──► StatsdMetricsProvider (delta_since + status_mask)
//! ```

pub mod otlp;
pub mod prometheus;
pub mod statsd;

use std::sync::Arc;

use velda_etp::otlp::OtlpEtp;
use velda_etp::statsd::StatsdEtp;
use velda_etp::{EtpBackend, EtpInstance};
use velda_observability::MetricsStore;

use crate::error::PluginError;

use super::config::{
    MetricsPushTargetConfig, OtlpMetricsPushConfig, PrometheusPullConfig, StatsdMetricsPushConfig,
};

use self::otlp::OtlpMetricsProvider;
use self::prometheus::PrometheusExporter;
use self::statsd::StatsdMetricsProvider;

/// Validated, pre-bound ETP handle paired with its resolved push metrics target configuration.
#[derive(Clone)]
pub enum EtpMetricsAttachment {
    Otlp {
        etp: Arc<OtlpEtp>,
        config: OtlpMetricsPushConfig,
    },
    Statsd {
        etp: Arc<StatsdEtp>,
        config: StatsdMetricsPushConfig,
    },
}

impl EtpMetricsAttachment {
    pub fn from_etp_instance(
        plugin_id: &str,
        cfg: &MetricsPushTargetConfig,
        instance: &EtpInstance,
    ) -> Result<Self, PluginError> {
        match &instance.backend {
            EtpBackend::Otlp(pool) => Ok(Self::Otlp {
                etp: Arc::clone(pool),
                config: cfg.resolve_otlp()?,
            }),
            EtpBackend::Statsd(pool) => Ok(Self::Statsd {
                etp: Arc::clone(pool),
                config: cfg.resolve_statsd()?,
            }),
            _ => Err(PluginError::Config(format!(
                "Metrics plugin '{plugin_id}' requires an 'otlp' or 'statsd' ETP for etp_id '{}', got '{}'",
                cfg.etp_id, instance.kind
            ))),
        }
    }
}

/// Owned metrics provider ready to execute its pull scrape listener or push interval loop.
pub enum MetricsProvider {
    Pull(Arc<PrometheusExporter>),
    Otlp(Box<OtlpMetricsProvider>),
    Statsd(Box<StatsdMetricsProvider>),
}

impl MetricsProvider {
    pub fn from_pull(
        cfg: PrometheusPullConfig,
        store: Arc<MetricsStore>,
    ) -> Result<Self, PluginError> {
        Ok(Self::Pull(Arc::new(PrometheusExporter::new(cfg, store)?)))
    }

    pub fn from_attachment(attachment: &EtpMetricsAttachment) -> Result<Self, PluginError> {
        match attachment {
            EtpMetricsAttachment::Otlp { etp, config } => Ok(Self::Otlp(Box::new(
                OtlpMetricsProvider::new(Arc::clone(etp), config)?,
            ))),
            EtpMetricsAttachment::Statsd { etp, config } => Ok(Self::Statsd(Box::new(
                StatsdMetricsProvider::new(Arc::clone(etp), config),
            ))),
        }
    }
}
