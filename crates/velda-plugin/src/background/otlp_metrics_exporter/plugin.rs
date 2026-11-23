//! OpenTelemetry Metrics Exporter Background Coordinator (`otlp_metrics_exporter::plugin`).
//!
//! ```text
//!   Startup wiring (wire_plugin_etps / with_etp) attaches Arc<OtlpEtp> into OnceLock
//!         │
//!         ▼
//!   run(shutdown) ──► wire::bake_wire_template(&etp.config)
//!         │
//!         └─► Interval / shutdown select loop:
//!               ├─ shutdown signaled? ──► break immediately
//!               ├─ in 30s cooldown?   ──► skip transmission
//!               └─ metrics_store.snapshot() -> encode (OtlpJsonTemplate | OtlpProtoTemplate)
//!                     └─► wire::push_metrics(&etp, &template, &mut head_buf, &payload, ...)
//!                           ├─ Ok(())  ──► ++flushed_count, reset failures
//!                           └─ Err(e)  ──► ++failed_count, backoff (..2000ms) -> 30s cooldown (>10)
//!
//!   Coordinator Struct Layout:
//!   ┌────────────────────────────────────────────────────────────────────────────┐
//!   │ OtlpMetricsExporterPlugin                                                  │
//!   │   id: String | config: OtlpMetricsExporterConfig (etp_id, service_name)    │
//!   │   etp: OnceLock<Arc<OtlpEtp>> | metrics_store: Arc<MetricsStore>           │
//!   │   flushed_count: Arc<AtomicU64> | failed_count: Arc<AtomicU64>             │
//!   └────────────────────────────────────────────────────────────────────────────┘
//! ```

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use tokio::sync::watch;
use velda_etp::otlp::{OtlpEtp, OtlpProtocol};
use velda_etp::{EtpBackend, EtpInstance};
use velda_observability::{MetricsStore, global_metrics};

use super::config::OtlpMetricsExporterConfig;
use super::json::OtlpJsonTemplate;
use super::proto::{OtlpProtoTemplate, format_otlp_metrics_proto_into};
use super::wire;
use crate::background::BackgroundPlugin;
use crate::capability::{Direction, PluginCapability, SupportedProtocols};
use crate::config::PluginConfig;
use crate::error::PluginError;

/// Background OTLP Metrics Exporter Plugin.
pub struct OtlpMetricsExporterPlugin {
    pub id: String,
    pub config: OtlpMetricsExporterConfig,
    pub etp: OnceLock<Arc<OtlpEtp>>,
    pub metrics_store: Arc<MetricsStore>,
    pub flushed_count: Arc<AtomicU64>,
    pub failed_count: Arc<AtomicU64>,
}

impl OtlpMetricsExporterPlugin {
    /// Constructs a new exporter plugin bound to the global metrics store.
    pub fn new(
        id: impl Into<String>,
        config: OtlpMetricsExporterConfig,
    ) -> Result<Self, PluginError> {
        Self::with_store(id, config, global_metrics())
    }

    /// Constructs a new exporter plugin with an explicit [`MetricsStore`].
    pub fn with_store(
        id: impl Into<String>,
        config: OtlpMetricsExporterConfig,
        metrics_store: Arc<MetricsStore>,
    ) -> Result<Self, PluginError> {
        Ok(Self {
            id: id.into(),
            config,
            etp: OnceLock::new(),
            metrics_store,
            flushed_count: Arc::new(AtomicU64::new(0)),
            failed_count: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Constructs a new exporter plugin pre-attached to an explicit [`OtlpEtp`] and [`MetricsStore`].
    pub fn with_etp(
        id: impl Into<String>,
        config: OtlpMetricsExporterConfig,
        etp: Arc<OtlpEtp>,
        metrics_store: Arc<MetricsStore>,
    ) -> Result<Self, PluginError> {
        let plugin = Self::with_store(id, config, metrics_store)?;
        let _ = plugin.etp.set(etp);
        Ok(plugin)
    }

    /// Deserializes and validates this plugin from declarative [`PluginConfig`].
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let validated =
            super::validation::validate_otlp_metrics_exporter_config(&config.id, &config.config)?;
        Self::with_store(&config.id, validated, global_metrics())
    }
}

impl BackgroundPlugin for OtlpMetricsExporterPlugin {
    fn id(&self) -> &str {
        &self.id
    }

    #[inline(always)]
    fn pbww(&self) -> u32 {
        40
    }

    #[inline(always)]
    fn capability(&self) -> PluginCapability {
        PluginCapability::default()
    }

    #[inline(always)]
    fn supported_protocols(&self) -> SupportedProtocols {
        SupportedProtocols::empty()
    }

    #[inline(always)]
    fn direction(&self) -> Direction {
        Direction::Background
    }

    #[inline(always)]
    fn etp_id(&self) -> Option<&str> {
        Some(&self.config.etp_id)
    }

    fn attach_etp(&self, instance: &EtpInstance) -> Result<(), PluginError> {
        match &instance.backend {
            EtpBackend::Otlp(pool) => {
                let _ = self.etp.set(Arc::clone(pool));
                Ok(())
            }
            _ => Err(PluginError::Config(format!(
                "OTLP Metrics plugin '{}' requires an 'otlp' ETP for etp_id '{}', got '{}'",
                self.id, self.config.etp_id, instance.kind
            ))),
        }
    }

    fn run(
        self: Arc<Self>,
        mut shutdown: watch::Receiver<bool>,
    ) -> Pin<Box<dyn Future<Output = Result<(), PluginError>> + Send>> {
        Box::pin(async move {
            let etp = self.etp.get().ok_or_else(|| {
                PluginError::Config(format!(
                    "OTLP Metrics plugin '{}' has no attached OtlpEtp for etp_id '{}'",
                    self.id, self.config.etp_id
                ))
            })?;

            let wire_template = wire::bake_wire_template(&etp.config)?;
            let use_json = matches!(etp.config.protocol, OtlpProtocol::HttpJson);
            let json_template = use_json.then(|| OtlpJsonTemplate::new(&self.config.service_name));
            let mut proto_template =
                (!use_json).then(|| OtlpProtoTemplate::new(&self.config.service_name));

            let request_timeout = Duration::from_millis(self.config.request_timeout_ms);
            let mut ticker =
                tokio::time::interval(Duration::from_secs(self.config.interval_secs.max(1)));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            tracing::info!(
                plugin_id = %self.id,
                etp_id = %etp.config.id,
                endpoint = %etp.config.endpoint,
                protocol = ?etp.config.protocol,
                interval_secs = self.config.interval_secs,
                "Starting OTLP metrics exporter via OtlpEtp pool"
            );

            let mut head_buf = Vec::with_capacity(512);
            let mut payload_buf = Vec::with_capacity(64 * 1024);
            let mut resp_buf = Vec::with_capacity(1024);
            let mut consecutive_failures: u32 = 0;
            let mut cooldown_until: Option<tokio::time::Instant> = None;

            loop {
                let is_shutting_down = tokio::select! {
                    _ = ticker.tick() => false,
                    _ = shutdown.changed() => true,
                };

                if is_shutting_down {
                    tracing::info!(
                        plugin_id = %self.id,
                        "Shutting down OTLP metrics background exporter"
                    );
                    break;
                }

                if cooldown_until.is_some_and(|until| tokio::time::Instant::now() < until) {
                    tracing::debug!(
                        plugin_id = %self.id,
                        "OTLP metrics exporter in cool-down; skipping snapshot transmission"
                    );
                    continue;
                }

                if cooldown_until.is_some() {
                    cooldown_until = None;
                    consecutive_failures = 0;
                    tracing::info!(
                        plugin_id = %self.id,
                        "OTLP metrics exporter cool-down ended; resuming exports"
                    );
                }

                let snapshot = self.metrics_store.snapshot();
                payload_buf.clear();
                if let Some(ref tpl) = json_template {
                    tpl.format_into(&snapshot, &mut payload_buf);
                } else if let Some(ref mut tpl) = proto_template {
                    format_otlp_metrics_proto_into(tpl, &snapshot, &mut payload_buf);
                }

                match wire::push_metrics(
                    etp,
                    &wire_template,
                    &mut head_buf,
                    &payload_buf,
                    &mut resp_buf,
                    request_timeout,
                )
                .await
                {
                    Ok(()) => {
                        consecutive_failures = 0;
                        self.flushed_count.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => {
                        consecutive_failures += 1;
                        self.failed_count.fetch_add(1, Ordering::Relaxed);
                        tracing::error!(
                            plugin_id = %self.id,
                            etp_id = %etp.config.id,
                            error = %e,
                            attempt = consecutive_failures,
                            "Failed to push OTLP metrics payload"
                        );

                        if consecutive_failures <= 10 {
                            let backoff_ms = (100u64
                                .saturating_mul(1u64 << (consecutive_failures - 1).min(31)))
                            .min(2000);
                            tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                        } else {
                            tracing::warn!(
                                plugin_id = %self.id,
                                "OTLP metrics exporter failed 10 consecutive times; entering 30s cool-down"
                            );
                            cooldown_until =
                                Some(tokio::time::Instant::now() + Duration::from_secs(30));
                            consecutive_failures = 0;
                        }
                    }
                }
            }

            Ok(())
        })
    }
}
