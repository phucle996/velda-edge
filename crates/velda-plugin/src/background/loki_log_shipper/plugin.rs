//! Grafana Loki Log Shipper Background Coordinator (`loki_log_shipper::plugin`).
//!
//! ```text
//! ┌────────────────────────────────────────────────────────────────────────────────────────┐
//! │                        VELDA EDGE DATA PLANE ARCHITECTURE                              │
//! │                                                                                        │
//! │   FRONTLINE TRAFFIC WORKERS (Cores 0..N-1, SCHED_OTHER)                                │
//! │   ┌────────────────────────────────────────────────────────────────────────────────┐   │
//! │   │ Client Ingress -> Route Matching -> Upstream Proxying -> Client Response       │   │
//! │   │ [Hot-Path Telemetry Hook]                                                      │   │
//! │   │ 64-byte AccessLogRecord ──► Core-Local SPSC Ring (Zero-lock push)             │   │
//! │   └───────────────────────────────────────┬────────────────────────────────────────┘   │
//! │                                           │ Atomic In-Memory Hand-off                  │
//! │                                           ▼                                            │
//! │   SHADOW LOGISTICS WORKER (velda-shadow-{i}, SCHED_IDLE, Assigned Core i)              │
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

use crate::config::PluginConfig;
use http::HeaderMap;
use tokio::sync::watch;
use velda_observability::{LogStore, global_log_store};

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
        let validated =
            super::validation::validate_loki_log_shipper_config(&config.id, &config.config)
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

    #[inline(always)]
    fn weight(&self) -> u32 {
        80
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
                let mut grpc_client = Some(LokiLogsGrpcClient::new(
                    &self.config.endpoint,
                    &self.config.tenant_id,
                    connect_timeout,
                    request_timeout,
                    &self.config.tls,
                )?);
                let mut formatter =
                    LokiProtoFormatter::new(&self.config.service_name, &self.config.labels);
                let mut proto_buf = Vec::with_capacity(32 * 1024);
                let mut consecutive_failures: u32 = 0;
                let mut cooldown_until: Option<tokio::time::Instant> = None;

                loop {
                    let is_shutting_down = tokio::select! {
                        _ = interval.tick() => false,
                        _ = shutdown.changed() => true,
                    };

                    if cooldown_until.is_some_and(|until| tokio::time::Instant::now() < until) {
                        let _ = subscriber.drain_batches(64);
                        tracing::debug!(
                            plugin_id = %self.id,
                            "Loki gRPC log shipper in cool-down; discarded batches to free RAM"
                        );
                        if is_shutting_down {
                            break;
                        }
                        continue;
                    }

                    if cooldown_until.is_some() {
                        cooldown_until = None;
                        grpc_client = Some(LokiLogsGrpcClient::new(
                            &self.config.endpoint,
                            &self.config.tenant_id,
                            connect_timeout,
                            request_timeout,
                            &self.config.tls,
                        )?);
                        consecutive_failures = 0;
                    }

                    let batches = subscriber.drain_batches(64);

                    if !batches.is_empty() {
                        let record_count = formatter.format_batches(&batches, &mut proto_buf);
                        drop(batches);

                        if record_count > 0 {
                            if grpc_client.is_none() {
                                grpc_client = Some(LokiLogsGrpcClient::new(
                                    &self.config.endpoint,
                                    &self.config.tenant_id,
                                    connect_timeout,
                                    request_timeout,
                                    &self.config.tls,
                                )?);
                            }

                            let client = grpc_client.as_mut().unwrap();
                            match client.send_logs(&proto_buf, &self.custom_headers).await {
                                Ok(()) => {
                                    consecutive_failures = 0;
                                    self.shipped_records
                                        .fetch_add(record_count as u64, Ordering::Relaxed);
                                }
                                Err(_) => {
                                    consecutive_failures += 1;
                                    self.failed_pushes.fetch_add(1, Ordering::Relaxed);

                                    if consecutive_failures <= 10 {
                                        let backoff_ms = (100u64.saturating_mul(
                                            1u64 << (consecutive_failures - 1).min(31),
                                        ))
                                        .min(2000);
                                        tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                                    } else {
                                        tracing::warn!(
                                            plugin_id = %self.id,
                                            "Shipper failed 10 consecutive times; tearing down connection and entering 30s cool-down"
                                        );
                                        grpc_client = None;
                                        cooldown_until = Some(
                                            tokio::time::Instant::now() + Duration::from_secs(30),
                                        );
                                        consecutive_failures = 0;
                                    }
                                }
                            }
                        }
                    }

                    if is_shutting_down {
                        break;
                    }
                }
            } else {
                let mut http_client = Some(LokiLogClient::new(
                    &self.config.endpoint,
                    &self.config.tenant_id,
                    connect_timeout,
                    request_timeout,
                    &self.config.tls,
                )?);
                let mut formatter =
                    LokiJsonFormatter::new(&self.config.service_name, &self.config.labels);
                let mut json_buf = Vec::with_capacity(32 * 1024);
                let mut consecutive_failures: u32 = 0;
                let mut cooldown_until: Option<tokio::time::Instant> = None;

                loop {
                    let is_shutting_down = tokio::select! {
                        _ = interval.tick() => false,
                        _ = shutdown.changed() => true,
                    };

                    if cooldown_until.is_some_and(|until| tokio::time::Instant::now() < until) {
                        let _ = subscriber.drain_batches(64);
                        tracing::debug!(
                            plugin_id = %self.id,
                            "Loki HTTP log shipper in cool-down; discarded batches to free RAM"
                        );
                        if is_shutting_down {
                            break;
                        }
                        continue;
                    }

                    if cooldown_until.is_some() {
                        cooldown_until = None;
                        http_client = Some(LokiLogClient::new(
                            &self.config.endpoint,
                            &self.config.tenant_id,
                            connect_timeout,
                            request_timeout,
                            &self.config.tls,
                        )?);
                        consecutive_failures = 0;
                    }

                    let batches = subscriber.drain_batches(64);

                    if !batches.is_empty() {
                        let record_count = formatter.format_batches(&batches, &mut json_buf);
                        drop(batches);

                        if record_count > 0 {
                            if http_client.is_none() {
                                http_client = Some(LokiLogClient::new(
                                    &self.config.endpoint,
                                    &self.config.tenant_id,
                                    connect_timeout,
                                    request_timeout,
                                    &self.config.tls,
                                )?);
                            }

                            let client = http_client.as_mut().unwrap();
                            match client.send_logs(&json_buf, &self.custom_headers).await {
                                Ok(status) if (200..300).contains(&status) => {
                                    consecutive_failures = 0;
                                    self.shipped_records
                                        .fetch_add(record_count as u64, Ordering::Relaxed);
                                }
                                _ => {
                                    consecutive_failures += 1;
                                    self.failed_pushes.fetch_add(1, Ordering::Relaxed);

                                    if consecutive_failures <= 10 {
                                        let backoff_ms = (100u64.saturating_mul(
                                            1u64 << (consecutive_failures - 1).min(31),
                                        ))
                                        .min(2000);
                                        tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                                    } else {
                                        tracing::warn!(
                                            plugin_id = %self.id,
                                            "Shipper failed 10 consecutive times; tearing down connection and entering 30s cool-down"
                                        );
                                        http_client = None;
                                        cooldown_until = Some(
                                            tokio::time::Instant::now() + Duration::from_secs(30),
                                        );
                                        consecutive_failures = 0;
                                    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_exponential_backoff_progression() {
        for failures in 1..=10 {
            let backoff_ms = (100u64.saturating_mul(1u64 << (failures - 1).min(31))).min(2000);
            match failures {
                1 => assert_eq!(backoff_ms, 100),
                2 => assert_eq!(backoff_ms, 200),
                3 => assert_eq!(backoff_ms, 400),
                4 => assert_eq!(backoff_ms, 800),
                5 => assert_eq!(backoff_ms, 1600),
                6..=10 => assert_eq!(backoff_ms, 2000),
                _ => unreachable!(),
            }
        }
    }

    #[tokio::test]
    async fn test_loki_log_shipper_shutdown() {
        let store = Arc::new(LogStore::new());
        let config = LokiLogShipperConfig {
            protocol: "http".to_string(),
            endpoint: "http://127.0.0.1:3100/loki/api/v1/push".to_string(),
            tenant_id: "test".to_string(),
            service_name: "test-service".to_string(),
            flush_interval_ms: 50,
            max_batch_size: 100,
            connect_timeout_ms: 1000,
            request_timeout_ms: 1000,
            labels: HashMap::new(),
            tls: Default::default(),
        };

        let plugin =
            Arc::new(LokiLogShipperPlugin::with_store("loki_test", config, store).unwrap());
        let (tx, rx) = watch::channel(false);

        let handle = tokio::spawn(async move { plugin.run(rx).await });

        tx.send(true).unwrap();
        let res = handle.await.unwrap();
        assert!(res.is_ok());
    }
}
