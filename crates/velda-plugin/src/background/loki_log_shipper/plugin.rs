//! Grafana Loki Log Shipper Background Coordinator (`loki_log_shipper::plugin`).
//!
//! ```text
//! ┌────────────────────────────────────────────────────────────────────────────────────────┐
//! │                        VELDA EDGE DATA PLANE ARCHITECTURE                              │
//! │                                                                                        │
//! │   FRONTLINE TRAFFIC WORKERS (Core 0..N, SCHED_OTHER)                                  │
//! │   ┌────────────────────────────────────────────────────────────────────────────────┐   │
//! │   │ Client Ingress -> Route Matching -> Upstream Proxying -> Client Response       │   │
//! │   │ [Hot-Path Telemetry Hook]                                                      │   │
//! │   │ 64-byte AccessLogRecord ──► Core-Local SPSC Ring (< 70 CPU clock cycles)       │   │
//! │   └───────────────────────────────────────┬────────────────────────────────────────┘   │
//! │                                           │ Atomic In-Memory Hand-off                  │
//! │                                           ▼                                            │
//! │   SHADOW LOGISTICS WORKER (velda-shadow-0, SCHED_IDLE)                                 │
//! │   ┌────────────────────────────────────────────────────────────────────────────────┐   │
//! │   │ LogStore::flush_all_lanes() ──► Batched Arc<[AccessLogRecord]> Slices          │   │
//! │   │                                                                                │   │
//! │   │ LokiLogShipperPlugin::run()                                                    │   │
//! │   │  ├── is_grpc?                                                                  │   │
//! │   │  │    ├── YES ──► proto::LokiProtoFormatter + proto::LokiLogsGrpcClient        │   │
//! │   │  │    └── NO  ──► json::LokiJsonFormatter   + json::LokiLogClient              │   │
//! │   │  └── Batch Drop: Atomic reference decrement (Zero lock contention)            │   │
//! │   └────────────────────────────────────────────────────────────────────────────────┘   │
//! └────────────────────────────────────────────────────────────────────────────────────────┘
//! ```

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use http::HeaderMap;
use tokio::sync::watch;
use velda_observability::{LogStore, global_log_store};
use velda_sync::post_sync::plugin::PluginConfig;

pub use super::config::LokiLogShipperConfig;
use super::json::{LokiJsonFormatter, LokiLogClient};
use super::proto::{LokiLogsGrpcClient, LokiProtoFormatter};
use crate::background::BackgroundPlugin;
use crate::capability::{Direction, PluginCapability, SupportedProtocols};
use crate::error::PluginError;

/// Background Grafana Loki Log Shipper Coordinator Plugin.
pub struct LokiLogShipperPlugin {
    pub id: String,
    pub config: LokiLogShipperConfig,
    log_store: Arc<LogStore>,
    custom_headers: HeaderMap,
    shipped_records: Arc<AtomicU64>,
    failed_pushes: Arc<AtomicU64>,
}

impl LokiLogShipperPlugin {
    /// Constructs a new Loki log shipper bound to global singleton [`LogStore`].
    pub fn new(id: impl Into<String>, config: LokiLogShipperConfig) -> Result<Self, PluginError> {
        Self::with_store(id, config, global_log_store())
    }

    /// Constructs a new shipper bound to an explicit [`LogStore`] (for testing & multi-tenant isolation).
    pub fn with_store(
        id: impl Into<String>,
        config: LokiLogShipperConfig,
        log_store: Arc<LogStore>,
    ) -> Result<Self, PluginError> {
        Ok(Self {
            id: id.into(),
            config,
            log_store,
            custom_headers: HeaderMap::new(),
            shipped_records: Arc::new(AtomicU64::new(0)),
            failed_pushes: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Instantiates and validates this plugin from declarative [`PluginConfig`].
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let validated = velda_sync::validate_loki_log_shipper_config(&config.id, &config.config)
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

impl BackgroundPlugin for LokiLogShipperPlugin {
    fn id(&self) -> &str {
        &self.id
    }

    fn run(
        self: Arc<Self>,
        mut shutdown: watch::Receiver<bool>,
    ) -> Pin<Box<dyn Future<Output = Result<(), PluginError>> + Send>> {
        Box::pin(async move {
            let is_grpc = self.config.protocol.eq_ignore_ascii_case("grpc")
                || self.config.endpoint.starts_with("grpc://")
                || self.config.endpoint.starts_with("grpcs://");

            let subscriber = self.log_store.subscribe(256);
            let mut interval =
                tokio::time::interval(Duration::from_millis(self.config.flush_interval_ms.max(50)));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            let connect_timeout = Duration::from_millis(self.config.connect_timeout_ms);
            let request_timeout = Duration::from_millis(self.config.request_timeout_ms);

            if is_grpc {
                let mut grpc_client = LokiLogsGrpcClient::new(
                    &self.config.endpoint,
                    &self.config.tenant_id,
                    connect_timeout,
                    request_timeout,
                )?;
                let mut formatter =
                    LokiProtoFormatter::new(&self.config.service_name, &self.config.labels);
                let mut proto_buf = Vec::with_capacity(32 * 1024);

                loop {
                    let is_shutting_down = tokio::select! {
                        _ = interval.tick() => false,
                        _ = shutdown.changed() => true,
                    };

                    self.log_store.flush_all_lanes(self.config.max_batch_size);
                    let batches = subscriber.drain_batches(64);

                    if !batches.is_empty() {
                        let record_count = formatter.format_batches(&batches, &mut proto_buf);
                        drop(batches);

                        if record_count > 0 {
                            match grpc_client
                                .send_logs(&proto_buf, &self.custom_headers)
                                .await
                            {
                                Ok(()) => {
                                    self.shipped_records
                                        .fetch_add(record_count as u64, Ordering::Relaxed);
                                }
                                Err(_) => {
                                    self.failed_pushes.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                        }
                    }

                    if is_shutting_down {
                        break;
                    }
                }
            } else {
                let mut http_client = LokiLogClient::new(
                    &self.config.endpoint,
                    &self.config.tenant_id,
                    connect_timeout,
                    request_timeout,
                )?;
                let mut formatter =
                    LokiJsonFormatter::new(&self.config.service_name, &self.config.labels);
                let mut json_buf = Vec::with_capacity(32 * 1024);

                loop {
                    let is_shutting_down = tokio::select! {
                        _ = interval.tick() => false,
                        _ = shutdown.changed() => true,
                    };

                    self.log_store.flush_all_lanes(self.config.max_batch_size);
                    let batches = subscriber.drain_batches(64);

                    if !batches.is_empty() {
                        let record_count = formatter.format_batches(&batches, &mut json_buf);
                        drop(batches);

                        if record_count > 0 {
                            match http_client.send_logs(&json_buf, &self.custom_headers).await {
                                Ok(status) if (200..300).contains(&status) => {
                                    self.shipped_records
                                        .fetch_add(record_count as u64, Ordering::Relaxed);
                                }
                                _ => {
                                    self.failed_pushes.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                        }
                    }

                    if is_shutting_down {
                        break;
                    }
                }
            }

            Ok(())
        })
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
}
