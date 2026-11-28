//! Unified Metrics Exporter Configuration (`metrics_exporter::config`).
//!
//! ```text
//!   plugins.json ("metrics_exporter")
//!         │
//!         └─► targets: Vec<MetricsTargetConfig>  (tagged by "mode": "pull" | "push")
//!               │
//!               ├─► mode == "pull" ──► PrometheusPullConfig
//!               │     └─► scrape + tls + cors + auth (local HTTP/HTTPS GET /metrics)
//!               │
//!               └─► mode == "push" ──► MetricsPushTargetConfig
//!                     ├─► etp_id (mandatory, dispatched by EtpBackend: Otlp | Statsd)
//!                     └─► interval_secs, temporality (delta|cumulative), prefix, dialect, ...
//! ```

use serde::{Deserialize, Serialize};

use crate::background::metrics_exporter::providers::statsd::dialect::StatsdDialect;
use crate::error::PluginError;

/// Top-level configuration for the unified `metrics_exporter` background plugin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetricsExporterConfig {
    /// Ordered list of metric export targets (`mode: "pull"` or `mode: "push"`).
    pub targets: Vec<MetricsTargetConfig>,
}

/// Discriminated metrics target routed either to a local Prometheus scrape server or an ETP sink.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "lowercase")]
pub enum MetricsTargetConfig {
    /// Local Prometheus OpenMetrics HTTP/HTTPS scrape endpoint (`GET /metrics`).
    Pull(PrometheusPullConfig),
    /// Outbound periodic push target backed by `velda-etp` (`OtlpEtp` or `StatsdEtp`).
    Push(MetricsPushTargetConfig),
}

// ============================================================================
// Pull Target Config (Prometheus Scrape Server)
// ============================================================================

/// Operational scrape limits for Prometheus Pull exporter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScrapeExporterConfig {
    /// Request timeout threshold in milliseconds (`100..=120_000`).
    pub timeout_ms: u64,
    /// Maximum concurrent scraper connections allowed simultaneously (`1..=1024`).
    pub max_concurrency: usize,
    /// Maximum scratch buffer capacity allocated for OpenMetrics text serialization (`8..=4096` KB).
    pub max_buffer_size_kb: usize,
}

/// Strict TLS/mTLS configuration for Prometheus Pull exporter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TlsExporterConfig {
    /// Operating mode: `"none"`, `"tls"`, or `"mtls"`.
    pub mode: String,
    pub cert_file: Option<String>,
    pub key_file: Option<String>,
    pub client_ca_file: Option<String>,
}

/// Strict CORS configuration for Prometheus Pull exporter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CorsExporterConfig {
    pub enabled: bool,
    pub allow_origins: Vec<String>,
    pub allow_methods: Vec<String>,
    pub allow_headers: Vec<String>,
}

/// Strict authentication configuration for Prometheus Pull exporter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthExporterConfig {
    /// Authentication mode: `"none"`, `"bearer"`, or `"basic"`.
    pub mode: String,
    pub token: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,
}

/// Declarative configuration for a Prometheus Pull (`mode: "pull"`) scrape server.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrometheusPullConfig {
    pub listen: String,
    pub path: String,
    pub protocol: String,
    pub scrape: ScrapeExporterConfig,
    pub tls: TlsExporterConfig,
    pub cors: CorsExporterConfig,
    pub auth: AuthExporterConfig,
}

// ============================================================================
// Push Target Config (OTLP / StatsD via velda-etp)
// ============================================================================

/// Aggregation temporality for OTLP Sum metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OtlpTemporality {
    /// Emits tick deltas via `curr.delta_since(&prev)` (OTLP value `1`).
    #[default]
    Delta,
    /// Emits cumulative snapshot totals since startup (OTLP value `2`).
    Cumulative,
}

impl OtlpTemporality {
    #[inline]
    pub const fn as_otlp_proto_int(self) -> i32 {
        match self {
            Self::Delta => 1,
            Self::Cumulative => 2,
        }
    }
}

/// Declarative configuration for `mode: "push"` metrics targets.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct MetricsPushTargetConfig {
    /// Identifier of the target entry in `etp.json` / [`velda_etp::EtpRegistry`].
    pub etp_id: String,
    /// Optional explicit ETP kind assertion checked during `velda-sync`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub etp_kind: Option<String>,
    /// Periodic push interval in seconds (`1..=3600`; OTLP default 15, StatsD default 2).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interval_secs: Option<u64>,

    // OTLP
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_timeout_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub temporality: Option<OtlpTemporality>,

    // StatsD
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dialect: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_packet_size: Option<usize>,
}

/// Resolved parameters for the OTLP Metrics Push provider.
#[derive(Debug, Clone)]
pub struct OtlpMetricsPushConfig {
    pub etp_id: String,
    pub interval_secs: u64,
    pub request_timeout_ms: u64,
    pub service_name: String,
    pub temporality: OtlpTemporality,
}

/// Resolved parameters for the StatsD Metrics Push provider.
#[derive(Debug, Clone)]
pub struct StatsdMetricsPushConfig {
    pub etp_id: String,
    pub interval_secs: u64,
    pub prefix: String,
    pub dialect: Option<StatsdDialect>,
    pub max_packet_size: usize,
}

impl MetricsPushTargetConfig {
    pub fn resolve_otlp(&self) -> Result<OtlpMetricsPushConfig, PluginError> {
        let interval_secs = self.interval_secs.unwrap_or(15);
        if !(1..=3600).contains(&interval_secs) {
            return Err(PluginError::Config(format!(
                "OTLP Metrics target '{}': interval_secs ({interval_secs}) must be in 1..=3600",
                self.etp_id
            )));
        }
        let request_timeout_ms = self.request_timeout_ms.unwrap_or(5000);
        if !(100..=60_000).contains(&request_timeout_ms) {
            return Err(PluginError::Config(format!(
                "OTLP Metrics target '{}': request_timeout_ms ({request_timeout_ms}) must be in 100..=60_000",
                self.etp_id
            )));
        }
        let service_name = self
            .service_name
            .clone()
            .unwrap_or_else(|| "velda-edge".to_string());
        if service_name.trim().is_empty() {
            return Err(PluginError::Config(format!(
                "OTLP Metrics target '{}': service_name cannot be empty",
                self.etp_id
            )));
        }
        Ok(OtlpMetricsPushConfig {
            etp_id: self.etp_id.clone(),
            interval_secs,
            request_timeout_ms,
            service_name,
            temporality: self.temporality.unwrap_or_default(),
        })
    }

    pub fn resolve_statsd(&self) -> Result<StatsdMetricsPushConfig, PluginError> {
        let interval_secs = self.interval_secs.unwrap_or(2);
        if !(1..=3600).contains(&interval_secs) {
            return Err(PluginError::Config(format!(
                "StatsD target '{}': interval_secs ({interval_secs}) must be in 1..=3600",
                self.etp_id
            )));
        }
        let max_packet_size = self.max_packet_size.unwrap_or(1432);
        if !(256..=65507).contains(&max_packet_size) {
            return Err(PluginError::Config(format!(
                "StatsD target '{}': max_packet_size ({max_packet_size}) must be in 256..=65507",
                self.etp_id
            )));
        }
        let dialect = match self
            .dialect
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            Some(raw) => Some(raw.parse::<StatsdDialect>().map_err(PluginError::Config)?),
            None => None,
        };
        Ok(StatsdMetricsPushConfig {
            etp_id: self.etp_id.clone(),
            interval_secs,
            prefix: self.prefix.clone().unwrap_or_default(),
            dialect,
            max_packet_size,
        })
    }
}
