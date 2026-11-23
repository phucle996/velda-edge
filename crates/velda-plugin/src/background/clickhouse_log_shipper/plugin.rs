//! ClickHouse Log Shipper Background Coordinator (`clickhouse_log_shipper::plugin`).
//!
//! ```text
//!   Startup wiring (wire_plugin_etps / with_etp) attaches Arc<ClickHouseEtp> into OnceLock
//!         │
//!         ▼
//!   run(shutdown) ──► wire::bake_insert_prefix + wire::bake_ddl_request
//!         │
//!         ├─► Iteration 0: wire::ensure_table(&etp, &ddl_request, ...)
//!         └─► Interval / shutdown select loop:
//!               ├─ in 30s cooldown? ──► drain & discard batches (RAM guard)
//!               └─ drain_batches(64) -> format RowBinary/JSONEachRow into payload_buf
//!                     └─► wire::push_batch(&etp, &prefix, &mut head_buf, &payload_buf, ...)
//!                           ├─ 2xx      ──► ++shipped_records, reset failure counter
//!                           └─ err/!2xx ──► ++failed_pushes (> 10 -> 30s cooldown)
//!
//!   Coordinator Struct Layout:
//!   ┌────────────────────────────────────────────────────────────────────────────┐
//!   │ ClickHouseLogShipperPlugin                                                 │
//!   │   id: String | config: ClickHouseLogShipperConfig (etp_id)                 │
//!   │   etp: OnceLock<Arc<ClickHouseEtp>> | telemetry_store: Arc<TelemetryStore> │
//!   │   shipped_records: Arc<AtomicU64> | failed_pushes: Arc<AtomicU64>          │
//!   └────────────────────────────────────────────────────────────────────────────┘
//! ```

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use tokio::sync::watch;
use velda_etp::clickhouse::ClickHouseEtp;
use velda_etp::{EtpBackend, EtpInstance};
use velda_observability::{TelemetryStore, global_telemetry_store};

use crate::background::BackgroundPlugin;
use crate::capability::{Direction, PluginCapability, SupportedProtocols};
use crate::config::PluginConfig;
use crate::error::PluginError;

use super::config::{ClickHouseLogShipperConfig, VELDA_ACCESS_LOGS_TABLE};
use super::format::{ClickHouseFormat, format_json_each_row, format_row_binary};
use super::wire;

/// Background ClickHouse Log Shipper Plugin.
pub struct ClickHouseLogShipperPlugin {
    pub id: String,
    pub config: ClickHouseLogShipperConfig,
    pub etp: OnceLock<Arc<ClickHouseEtp>>,
    pub telemetry_store: Arc<TelemetryStore>,
    pub shipped_records: Arc<AtomicU64>,
    pub failed_pushes: Arc<AtomicU64>,
}

impl ClickHouseLogShipperPlugin {
    pub fn new(
        id: impl Into<String>,
        config: ClickHouseLogShipperConfig,
    ) -> Result<Self, PluginError> {
        Self::with_store(id, config, Arc::clone(global_telemetry_store()))
    }

    pub fn with_store(
        id: impl Into<String>,
        config: ClickHouseLogShipperConfig,
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
        config: ClickHouseLogShipperConfig,
        etp: Arc<ClickHouseEtp>,
        telemetry_store: Arc<TelemetryStore>,
    ) -> Result<Self, PluginError> {
        let plugin = Self::with_store(id, config, telemetry_store)?;
        let _ = plugin.etp.set(etp);
        Ok(plugin)
    }

    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let validated =
            super::validation::validate_clickhouse_log_shipper_config(&config.id, &config.config)?;
        Self::with_store(&config.id, validated, Arc::clone(global_telemetry_store()))
    }
}

impl BackgroundPlugin for ClickHouseLogShipperPlugin {
    fn id(&self) -> &str {
        &self.id
    }
    #[inline(always)]
    fn pbww(&self) -> u32 {
        75
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
            EtpBackend::ClickHouse(pool) => {
                let _ = self.etp.set(Arc::clone(pool));
                Ok(())
            }
            _ => Err(PluginError::Config(format!(
                "ClickHouse plugin '{}' requires a 'clickhouse' ETP for etp_id '{}', got '{}'",
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
                    "ClickHouse plugin '{}' has no attached ClickHouseEtp for etp_id '{}'",
                    self.id, self.config.etp_id
                ))
            })?;
            let database = if self.config.database.trim().is_empty() {
                etp.config.database.as_str()
            } else {
                self.config.database.trim()
            };
            if database.is_empty() {
                return Err(PluginError::Config(
                    "ClickHouse target database resolved to empty string".into(),
                ));
            }

            let format = self.config.parsed_format();
            let request_timeout = Duration::from_millis(self.config.request_timeout_ms);
            let prebaked_insert_prefix = wire::bake_insert_prefix(&etp.config, database, format);
            let ddl_request = wire::bake_ddl_request(&etp.config, database);

            let subscriber = self.telemetry_store.subscribe(256);
            let mut interval = tokio::time::interval(Duration::from_millis(
                self.config.flush_interval_ms.max(100),
            ));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            let mut head_buf = Vec::with_capacity(prebaked_insert_prefix.len() + 32);
            let mut payload_buf = Vec::with_capacity(64 * 1024);
            let mut resp_buf = Vec::with_capacity(1024);
            let mut ensured_table = false;
            let mut consecutive_failures: u32 = 0;
            let mut cooldown_until: Option<tokio::time::Instant> = None;

            loop {
                if !ensured_table {
                    ensured_table = true;
                    match wire::ensure_table(etp, &ddl_request, &mut resp_buf, request_timeout)
                        .await
                    {
                        Ok(status) if (200..300).contains(&status) => {
                            tracing::info!(database = %database, table = %VELDA_ACCESS_LOGS_TABLE, "Verified ClickHouse access logs table exists")
                        }
                        Ok(status) => {
                            tracing::warn!(plugin_id = %self.id, status, database = %database, "Could not ensure ClickHouse table on startup")
                        }
                        Err(e) => {
                            tracing::warn!(plugin_id = %self.id, error = %e, database = %database, "Could not ensure ClickHouse table on startup")
                        }
                    }
                    continue;
                }

                let is_shutting_down = tokio::select! {
                    _ = interval.tick() => false,
                    _ = shutdown.changed() => *shutdown.borrow(),
                };

                if cooldown_until.is_some_and(|until| tokio::time::Instant::now() < until) {
                    let _ = subscriber.drain_batches(64);
                    tracing::debug!(plugin_id = %self.id, "ClickHouse log shipper in 30s cool-down; discarded batches");
                } else {
                    if cooldown_until.is_some() {
                        cooldown_until = None;
                        consecutive_failures = 0;
                        tracing::info!(plugin_id = %self.id, "ClickHouse cool-down ended, resuming batch pushes");
                    }

                    let batches = subscriber.drain_batches(64);
                    if !batches.is_empty() {
                        payload_buf.clear();
                        let encoded_recs = match format {
                            ClickHouseFormat::RowBinary => {
                                format_row_binary(&batches, &mut payload_buf)
                            }
                            ClickHouseFormat::JsonEachRow => {
                                format_json_each_row(&batches, &mut payload_buf)
                            }
                        };
                        drop(batches);

                        if encoded_recs > 0 && !payload_buf.is_empty() {
                            match wire::push_batch(
                                etp,
                                &prebaked_insert_prefix,
                                &mut head_buf,
                                &payload_buf,
                                &mut resp_buf,
                                request_timeout,
                            )
                            .await
                            {
                                Ok(status) if (200..300).contains(&status) => {
                                    consecutive_failures = 0;
                                    self.shipped_records
                                        .fetch_add(encoded_recs as u64, Ordering::Relaxed);
                                }
                                Ok(status) => {
                                    consecutive_failures += 1;
                                    self.failed_pushes.fetch_add(1, Ordering::Relaxed);
                                    tracing::warn!(plugin_id = %self.id, status, records = encoded_recs, attempt = consecutive_failures, "ClickHouse returned non-2xx HTTP status");
                                }
                                Err(e) => {
                                    consecutive_failures += 1;
                                    self.failed_pushes.fetch_add(1, Ordering::Relaxed);
                                    tracing::warn!(plugin_id = %self.id, error = %e, records = encoded_recs, attempt = consecutive_failures, "Failed to push batch to ClickHouse");
                                }
                            }
                            if consecutive_failures > 10 {
                                tracing::warn!(plugin_id = %self.id, "ClickHouse shipper failed 10 consecutive times; entering 30s cool-down");
                                cooldown_until =
                                    Some(tokio::time::Instant::now() + Duration::from_secs(30));
                                consecutive_failures = 0;
                            }
                        }
                    }
                }

                if is_shutting_down {
                    tracing::info!(plugin_id = %self.id, "ClickHouse log shipper shutting down");
                    break;
                }
            }

            Ok(())
        })
    }
}
