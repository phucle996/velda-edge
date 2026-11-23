//! Elasticsearch / OpenSearch Log Shipper Background Coordinator (`elasticsearch_log_shipper::plugin`).
//!
//! ```text
//!   Startup wiring (wire_plugin_etps / with_etp) attaches Arc<ElasticsearchEtp> into OnceLock
//!         │
//!         ▼
//!   run(shutdown) ──► wire::bake_bulk_prefix + prebake_bulk_action_line
//!         │
//!         └─► Interval / shutdown select loop:
//!               ├─ in 30s cooldown? ──► drain & discard batches (RAM guard)
//!               └─ drain_batches(max) -> format_bulk_ndjson -> optional GzEncoder -> compress_buf
//!                     └─► wire::push_bulk_payload(&etp, &prefix, &mut head_buf, body, ...)
//!                           ├─ 2xx      ──► ++shipped_records (warn if has_errors)
//!                           └─ err/!2xx ──► ++failed_pushes (> 10 -> 30s cooldown)
//!
//!   Coordinator Struct Layout:
//!   ┌────────────────────────────────────────────────────────────────────────────┐
//!   │ ElasticsearchLogShipperPlugin                                              │
//!   │   id: String | config: ElasticsearchLogShipperConfig (etp_id)              │
//!   │   etp: OnceLock<Arc<ElasticsearchEtp>> | telemetry_store                   │
//!   │   shipped_records: Arc<AtomicU64> | failed_pushes: Arc<AtomicU64>          │
//!   └────────────────────────────────────────────────────────────────────────────┘
//! ```

use std::future::Future;
use std::io::Write;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use flate2::Compression;
use flate2::write::GzEncoder;
use tokio::sync::watch;
use velda_etp::elasticsearch::ElasticsearchEtp;
use velda_etp::{EtpBackend, EtpInstance};
use velda_observability::{TelemetryStore, global_telemetry_store};

use crate::background::BackgroundPlugin;
use crate::capability::{Direction, PluginCapability, SupportedProtocols};
use crate::config::PluginConfig;
use crate::error::PluginError;

use super::config::{ElasticsearchLogShipperConfig, EsCompression};
use super::format::{format_bulk_ndjson, prebake_bulk_action_line};
use super::wire;

/// Background Elasticsearch / OpenSearch `_bulk` log shipper.
pub struct ElasticsearchLogShipperPlugin {
    pub id: String,
    pub config: ElasticsearchLogShipperConfig,
    pub etp: OnceLock<Arc<ElasticsearchEtp>>,
    pub telemetry_store: Arc<TelemetryStore>,
    pub shipped_records: Arc<AtomicU64>,
    pub failed_pushes: Arc<AtomicU64>,
}

impl ElasticsearchLogShipperPlugin {
    pub fn new(
        id: impl Into<String>,
        config: ElasticsearchLogShipperConfig,
    ) -> Result<Self, PluginError> {
        Self::with_store(id, config, Arc::clone(global_telemetry_store()))
    }

    pub fn with_store(
        id: impl Into<String>,
        config: ElasticsearchLogShipperConfig,
        telemetry_store: Arc<TelemetryStore>,
    ) -> Result<Self, PluginError> {
        Ok(Self {
            id: id.into(),
            config,
            etp: OnceLock::new(),
            telemetry_store,
            shipped_records: Arc::new(AtomicU64::new(0)),
            failed_pushes: Arc::new(AtomicU64::new(0)),
        })
    }

    pub fn with_etp(
        id: impl Into<String>,
        config: ElasticsearchLogShipperConfig,
        etp: Arc<ElasticsearchEtp>,
        telemetry_store: Arc<TelemetryStore>,
    ) -> Result<Self, PluginError> {
        let plugin = Self::with_store(id, config, telemetry_store)?;
        let _ = plugin.etp.set(etp);
        Ok(plugin)
    }

    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let validated = super::validation::validate_elasticsearch_log_shipper_config(
            &config.id,
            &config.config,
        )?;
        Self::with_store(&config.id, validated, Arc::clone(global_telemetry_store()))
    }
}

impl BackgroundPlugin for ElasticsearchLogShipperPlugin {
    fn id(&self) -> &str {
        &self.id
    }
    #[inline(always)]
    fn pbww(&self) -> u32 {
        60
    }
    fn direction(&self) -> Direction {
        Direction::Background
    }
    fn capability(&self) -> PluginCapability {
        PluginCapability::default()
    }
    fn supported_protocols(&self) -> SupportedProtocols {
        SupportedProtocols::empty()
    }
    #[inline(always)]
    fn etp_id(&self) -> Option<&str> {
        Some(&self.config.etp_id)
    }

    fn attach_etp(&self, instance: &EtpInstance) -> Result<(), PluginError> {
        match &instance.backend {
            EtpBackend::Elasticsearch(pool) => {
                let _ = self.etp.set(Arc::clone(pool));
                Ok(())
            }
            _ => Err(PluginError::Config(format!(
                "Elasticsearch plugin '{}' requires an 'elasticsearch' ETP for etp_id '{}', got '{}'",
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
                    "Elasticsearch plugin '{}' has no attached ElasticsearchEtp for etp_id '{}'",
                    self.id, self.config.etp_id
                ))
            })?;
            let index = match self.config.index.trim() {
                "" => etp
                    .config
                    .index
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .unwrap_or("velda-logs"),
                explicit => explicit,
            };

            let prebaked_head_prefix = wire::bake_bulk_prefix(&etp.config, &self.config);
            let prebaked_action = prebake_bulk_action_line(self.config.op_type, index);
            let max_batches = self.config.max_drain_batches.clamp(1, 4096);
            let request_timeout = Duration::from_millis(self.config.request_timeout_ms);
            let subscriber = self.telemetry_store.subscribe(max_batches);
            let mut interval =
                tokio::time::interval(Duration::from_millis(self.config.flush_interval_ms.max(10)));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            let mut head_buf = Vec::with_capacity(prebaked_head_prefix.len() + 32);
            let mut payload_buf = Vec::with_capacity(128 * 1024);
            let mut compress_buf = Vec::with_capacity(64 * 1024);
            let mut resp_buf = Vec::with_capacity(1024);
            let mut consecutive_failures = 0u32;
            let mut cooldown_until: Option<tokio::time::Instant> = None;

            loop {
                let is_shutting_down = tokio::select! {
                    _ = interval.tick() => false,
                    _ = shutdown.changed() => *shutdown.borrow(),
                };

                if cooldown_until.is_some_and(|until| tokio::time::Instant::now() < until) {
                    let _ = subscriber.drain_batches(max_batches);
                    tracing::debug!(plugin_id = %self.id, "Elasticsearch log shipper in 30s cool-down; discarded batches");
                } else {
                    if cooldown_until.is_some() {
                        cooldown_until = None;
                        consecutive_failures = 0;
                        tracing::info!(plugin_id = %self.id, "Elasticsearch cool-down ended, resuming batch pushes");
                    }

                    let batches = subscriber.drain_batches(max_batches);
                    if !batches.is_empty() {
                        payload_buf.clear();
                        let encoded =
                            format_bulk_ndjson(&batches, &prebaked_action, &mut payload_buf);
                        drop(batches);

                        if encoded > 0 && !payload_buf.is_empty() {
                            let wire_payload: &[u8] = match self.config.compression {
                                EsCompression::Gzip => {
                                    compress_buf.clear();
                                    let mut encoder =
                                        GzEncoder::new(&mut compress_buf, Compression::fast());
                                    encoder.write_all(&payload_buf).map_err(|e| {
                                        PluginError::Http(format!("ES gzip encode error: {e}"))
                                    })?;
                                    encoder.finish().map_err(|e| {
                                        PluginError::Http(format!("ES gzip finish error: {e}"))
                                    })?;
                                    &compress_buf
                                }
                                EsCompression::None => &payload_buf,
                            };

                            match wire::push_bulk_payload(
                                etp,
                                &prebaked_head_prefix,
                                &mut head_buf,
                                wire_payload,
                                &mut resp_buf,
                                request_timeout,
                            )
                            .await
                            {
                                Ok((status, has_errors)) if (200..300).contains(&status) => {
                                    consecutive_failures = 0;
                                    self.shipped_records
                                        .fetch_add(encoded as u64, Ordering::Relaxed);
                                    if has_errors {
                                        tracing::warn!(plugin_id = %self.id, records = encoded, "Elasticsearch _bulk reported partial item errors");
                                    }
                                }
                                Ok((status, _)) => {
                                    consecutive_failures += 1;
                                    self.failed_pushes.fetch_add(1, Ordering::Relaxed);
                                    tracing::warn!(plugin_id = %self.id, status, records = encoded, attempt = consecutive_failures, "Elasticsearch _bulk returned non-2xx status");
                                }
                                Err(e) => {
                                    consecutive_failures += 1;
                                    self.failed_pushes.fetch_add(1, Ordering::Relaxed);
                                    tracing::warn!(plugin_id = %self.id, error = %e, records = encoded, attempt = consecutive_failures, "Failed to push batch to Elasticsearch");
                                }
                            }
                            if consecutive_failures > 10 {
                                tracing::warn!(plugin_id = %self.id, "Elasticsearch shipper failed 10 consecutive times; entering 30s cool-down");
                                cooldown_until =
                                    Some(tokio::time::Instant::now() + Duration::from_secs(30));
                                consecutive_failures = 0;
                            }
                        }
                    }
                }

                if is_shutting_down {
                    tracing::info!(plugin_id = %self.id, "Elasticsearch log shipper shutting down");
                    break;
                }
            }

            Ok(())
        })
    }
}
