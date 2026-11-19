//! ClickHouse Log Shipper Background Coordinator (`clickhouse_log_shipper::plugin`).
//!
//! Subscribes to core-local log streams via [`LogStore`], encodes access log batches
//! into high-performance ClickHouse wire formats, and pushes them directly to ClickHouse HTTP API.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::sync::watch;
use velda_observability::{LogStore, global_log_store};

use crate::background::BackgroundPlugin;
use crate::capability::{Direction, PluginCapability, SupportedProtocols};
use crate::config::PluginConfig;
use crate::error::PluginError;

use super::client::ClickHouseHttpClient;
use super::config::ClickHouseLogShipperConfig;
use super::format::ClickHouseFormat;

/// Background ClickHouse Log Shipper Plugin.
pub struct ClickHouseLogShipperPlugin {
    pub id: String,
    pub config: ClickHouseLogShipperConfig,
    log_store: Arc<LogStore>,
    shipped_records: Arc<AtomicU64>,
    failed_pushes: Arc<AtomicU64>,
}

impl ClickHouseLogShipperPlugin {
    /// Constructs a new shipper bound to the global singleton [`LogStore`].
    pub fn new(
        id: impl Into<String>,
        config: ClickHouseLogShipperConfig,
    ) -> Result<Self, PluginError> {
        Self::with_store(id, config, global_log_store())
    }

    /// Constructs a new shipper bound to an explicit [`LogStore`].
    pub fn with_store(
        id: impl Into<String>,
        config: ClickHouseLogShipperConfig,
        log_store: Arc<LogStore>,
    ) -> Result<Self, PluginError> {
        Ok(Self {
            id: id.into(),
            config,
            log_store,
            shipped_records: Arc::new(AtomicU64::new(0)),
            failed_pushes: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Instantiates and validates this plugin from declarative [`PluginConfig`].
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let validated =
            super::validation::validate_clickhouse_log_shipper_config(&config.id, &config.config)
                .map_err(|e| PluginError::Config(e.to_string()))?;

        Self::with_store(&config.id, validated, global_log_store())
    }

    /// Returns cumulative total of successfully shipped log records.
    pub fn shipped_records(&self) -> u64 {
        self.shipped_records.load(Ordering::Relaxed)
    }

    /// Returns cumulative total of failed push operations.
    pub fn failed_pushes(&self) -> u64 {
        self.failed_pushes.load(Ordering::Relaxed)
    }
}

impl BackgroundPlugin for ClickHouseLogShipperPlugin {
    fn id(&self) -> &str {
        &self.id
    }

    fn weight(&self) -> u32 {
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

    fn run(
        self: Arc<Self>,
        mut shutdown: watch::Receiver<bool>,
    ) -> Pin<Box<dyn Future<Output = Result<(), PluginError>> + Send>> {
        Box::pin(async move {
            let subscriber = self.log_store.subscribe(256);
            let mut interval = tokio::time::interval(Duration::from_millis(
                self.config.flush_interval_ms.max(100),
            ));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            let format = self.config.parsed_format();
            let mut client = Some(ClickHouseHttpClient::new(&self.config)?);

            // Automatically ensure that target table exists upon startup
            if let Some(c) = client.as_mut()
                && let Err(e) = c.ensure_table().await
            {
                tracing::warn!(
                    plugin_id = %self.id,
                    error = %e,
                    database = %self.config.database,
                    "Could not ensure ClickHouse table on startup (will attempt during push)"
                );
            }

            let mut payload_buf = Vec::with_capacity(64 * 1024);
            let mut consecutive_failures: u32 = 0;
            let mut cooldown_until: Option<tokio::time::Instant> = None;

            loop {
                let is_shutting_down = tokio::select! {
                    _ = interval.tick() => false,
                    _ = shutdown.changed() => true,
                };

                // 1. Cool-down state: discard incoming batches to prevent RAM buildup
                if cooldown_until.is_some_and(|until| tokio::time::Instant::now() < until) {
                    let _ = subscriber.drain_batches(64);
                    tracing::debug!(
                        plugin_id = %self.id,
                        "ClickHouse log shipper in 30s cool-down; discarded batches to protect memory"
                    );
                    if is_shutting_down {
                        break;
                    }
                    continue;
                }

                // 2. Cool-down expired: re-instantiate client and reset failure count
                if cooldown_until.is_some() {
                    cooldown_until = None;
                    consecutive_failures = 0;
                    client = Some(ClickHouseHttpClient::new(&self.config)?);
                    tracing::info!(
                        plugin_id = %self.id,
                        "ClickHouse cool-down period ended, reconnecting client"
                    );
                }

                // 3. Normal drain and push
                let batches = subscriber.drain_batches(64);

                if !batches.is_empty() {
                    payload_buf.clear();

                    let encoded_recs = match format {
                        ClickHouseFormat::RowBinary => {
                            super::format::format_row_binary(&batches, &mut payload_buf)
                        }
                        ClickHouseFormat::JsonEachRow => {
                            super::format::format_json_each_row(&batches, &mut payload_buf)
                        }
                    };

                    drop(batches);

                    if encoded_recs > 0 && !payload_buf.is_empty() {
                        if client.is_none() {
                            client = Some(ClickHouseHttpClient::new(&self.config)?);
                        }

                        let cl = client.as_mut().unwrap();
                        match cl.push_batch(&payload_buf).await {
                            Ok(status) if (200..300).contains(&status) => {
                                consecutive_failures = 0;
                                self.shipped_records
                                    .fetch_add(encoded_recs as u64, Ordering::Relaxed);
                            }
                            Err(e) => {
                                consecutive_failures += 1;
                                self.failed_pushes.fetch_add(1, Ordering::Relaxed);
                                // Kill socket on error to prevent corrupt/half-closed connection reuse
                                cl.close_connection();

                                tracing::warn!(
                                    plugin_id = %self.id,
                                    error = %e,
                                    records = encoded_recs,
                                    attempt = consecutive_failures,
                                    "Failed to push batch to ClickHouse (dropped without retry)"
                                );

                                if consecutive_failures > 10 {
                                    tracing::warn!(
                                        plugin_id = %self.id,
                                        "ClickHouse shipper failed 10 consecutive times; entering 30s cool-down"
                                    );
                                    client = None;
                                    cooldown_until =
                                        Some(tokio::time::Instant::now() + Duration::from_secs(30));
                                    consecutive_failures = 0;
                                }
                            }
                            Ok(status) => {
                                consecutive_failures += 1;
                                self.failed_pushes.fetch_add(1, Ordering::Relaxed);
                                cl.close_connection();

                                tracing::warn!(
                                    plugin_id = %self.id,
                                    status = status,
                                    records = encoded_recs,
                                    attempt = consecutive_failures,
                                    "ClickHouse returned unexpected HTTP status code"
                                );

                                if consecutive_failures > 10 {
                                    tracing::warn!(
                                        plugin_id = %self.id,
                                        "ClickHouse shipper failed 10 consecutive times; entering 30s cool-down"
                                    );
                                    client = None;
                                    cooldown_until =
                                        Some(tokio::time::Instant::now() + Duration::from_secs(30));
                                    consecutive_failures = 0;
                                }
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
