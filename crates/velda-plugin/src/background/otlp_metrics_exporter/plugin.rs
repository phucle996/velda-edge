//! OpenTelemetry Metrics Exporter Background Plugin (`otlp_metrics_exporter::plugin`).
//!
//! Periodically captures lock-free point-in-time [`MetricsSnapshot`] values from
//! [`velda_observability::MetricsStore`] and pushes compliant OpenTelemetry ResourceMetrics
//! JSON payloads (`POST /v1/metrics`) or binary Protobuf over gRPC to an OTLP collector.
//!
//! # Architecture & Mechanism
//!
//! ```text
//! ╔══════════════════════════════════════════════════════════════════════════════════════════════════╗
//! ║                        VELDA EDGE — OTLP METRICS EXPORTER PIPELINE                               ║
//! ║                  (crates/velda-plugin: otlp_metrics_exporter::plugin)                             ║
//! ╚══════════════════════════════════════════════════════════════════════════════════════════════════╝
//!
//!  [FRONTLINE WORKERS (Cores 0..N-1)]         [SHADOW WORKER: velda-shadow-{i} (SCHED_IDLE)]
//!   velda-worker-0..n (Cores 0..N-1)          interval_secs periodic ticker (Core i)
//!         │                                                │
//!         ▼ (Atomic writes)                                ▼
//!   ┌───────────┐   Lock-Free Atomic Extraction    ┌───────────────────────────────┐
//!   │ 36 Slots  │ ───────────────────────────────► │   MetricsStore::snapshot()    │
//!   │ Counters  │     (Relaxed atomic reduce)  │   (36 Pipelines, 288 DPs)     │
//!   └───────────┘                                  └───────────────┬───────────────┘
//!                                                                  │
//!                                                  ┌───────────────┴───────────────┐
//!                                                  │                               │
//!                                            [mode: "grpc"]                  [mode: "http"]
//!                                                  ▼                               ▼
//!                                       ┌─────────────────────┐         ┌─────────────────────┐
//!                                       │  proto/template.rs  │         │   json/template.rs  │
//!                                       │ Pre-Baked Wire      │         │ Pre-Baked Chunk     │
//!                                       │ Skeleton (Zero-Alloc│         │ Interleaving (JSON) │
//!                                       │ direct protobuf)    │         │ (LUT-accelerated)   │
//!                                       └──────────┬──────────┘         └──────────┬──────────┘
//!                                                  │                               │
//!                                                  ▼                               ▼
//!                                       ┌─────────────────────┐         ┌─────────────────────┐
//!                                       │   proto/client.rs   │         │    json/client.rs   │
//!                                       │ Persistent HTTP/2   │         │ Persistent HTTP/1.1 │
//!                                       │ 5-byte Framing      │         │ Keep-Alive (TCP/TLS)│
//!                                       └──────────┬──────────┘         └──────────┬──────────┘
//!                                                  │                               │
//!                                                  ▼                               ▼
//!                                      gRPC Port 4317 (Protobuf)        HTTP Port 4318 (JSON)
//!                                      ┌──────────────────────────────────────────────────────┐
//!                                      │               OpenTelemetry Collector                │
//!                                      └──────────────────────────────────────────────────────┘
//! ```
//!
//! # Strict Monorepo Invariants
//! - **Shadow Execution**: Runs strictly on Shadow Worker supervisory threads with idle priority.
//! - **Zero Hot-Path Contention**: Telemetry is extracted via lock-free atomic snapshots.
//! - **Connection Reuse**: Uses a self-contained persistent HTTP/1.1 or HTTP/2 Keep-Alive socket.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::config::PluginConfig;
use http::HeaderMap;
use tokio::sync::watch;
use velda_observability::{MetricsStore, global_metrics};

use super::config::OtlpMetricsExporterConfig;
use super::json::{OtlpJsonTemplate, OtlpMetricsClient};
use super::proto::{OtlpMetricsGrpcClient, OtlpProtoTemplate, format_otlp_metrics_proto_into};
use crate::background::BackgroundPlugin;
use crate::capability::{Direction, PluginCapability, SupportedProtocols};
use crate::error::PluginError;

/// Standalone Background OTLP Metrics Exporter Plugin.
pub struct OtlpMetricsExporterPlugin {
    pub id: String,
    pub config: OtlpMetricsExporterConfig,
    metrics_store: Arc<MetricsStore>,
    custom_headers: HeaderMap,
    flushed_count: Arc<AtomicU64>,
    failed_count: Arc<AtomicU64>,
}

impl OtlpMetricsExporterPlugin {
    /// Constructs a new exporter plugin from configuration.
    pub fn new(
        id: impl Into<String>,
        config: OtlpMetricsExporterConfig,
    ) -> Result<Self, PluginError> {
        Self::with_store(id, config, global_metrics())
    }

    /// Constructs a new exporter plugin with an explicit [`MetricsStore`] (useful for testing).
    pub fn with_store(
        id: impl Into<String>,
        config: OtlpMetricsExporterConfig,
        store: Arc<MetricsStore>,
    ) -> Result<Self, PluginError> {
        let mut custom_headers = HeaderMap::new();
        for (k, v) in &config.headers {
            let header_name = http::header::HeaderName::from_bytes(k.as_bytes())
                .map_err(|e| PluginError::Config(format!("Invalid header name '{k}': {e}")))?;
            let header_value = http::header::HeaderValue::from_str(v)
                .map_err(|e| PluginError::Config(format!("Invalid header value '{v}': {e}")))?;
            custom_headers.insert(header_name, header_value);
        }

        Ok(Self {
            id: id.into(),
            config,
            metrics_store: store,
            custom_headers,
            flushed_count: Arc::new(AtomicU64::new(0)),
            failed_count: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Deserializes and validates this plugin from declarative [`PluginConfig`].
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let parsed: OtlpMetricsExporterConfig = serde_json::from_value(config.config.clone())
            .map_err(|e| {
                PluginError::Config(format!("Failed to parse otlp_metrics_exporter config: {e}"))
            })?;
        Self::new(&config.id, parsed)
    }

    /// Returns cumulative number of successfully flushed metrics batches.
    pub fn flushed_count(&self) -> u64 {
        self.flushed_count.load(Ordering::Relaxed)
    }

    /// Returns cumulative number of failed metrics flush attempts.
    pub fn failed_count(&self) -> u64 {
        self.failed_count.load(Ordering::Relaxed)
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

    fn run(
        self: Arc<Self>,
        mut shutdown: watch::Receiver<bool>,
    ) -> Pin<Box<dyn Future<Output = Result<(), PluginError>> + Send>> {
        Box::pin(async move {
            let is_grpc = self.config.protocol.eq_ignore_ascii_case("grpc")
                || self.config.endpoint.starts_with("grpc://")
                || self.config.endpoint.starts_with("grpcs://");

            let interval_duration = Duration::from_secs(self.config.interval_secs.max(1));
            let mut ticker = tokio::time::interval(interval_duration);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            tracing::info!(
                plugin_id = %self.id,
                endpoint = %self.config.endpoint,
                is_grpc = is_grpc,
                interval_secs = self.config.interval_secs,
                "Starting OTLP metrics background exporter loop"
            );

            let connect_timeout = Duration::from_millis(self.config.connect_timeout_ms);
            let request_timeout = Duration::from_millis(self.config.request_timeout_ms);

            if is_grpc {
                let mut grpc_client = Some(OtlpMetricsGrpcClient::new(
                    &self.config.endpoint,
                    connect_timeout,
                    request_timeout,
                    &self.config.tls,
                )?);
                let mut proto_template = OtlpProtoTemplate::new(&self.config.service_name);
                let mut proto_buf = Vec::with_capacity(32 * 1024);
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
                            "Shutting down OTLP metrics background exporter (gRPC)"
                        );
                        break;
                    }

                    if cooldown_until.is_some_and(|until| tokio::time::Instant::now() < until) {
                        tracing::debug!(
                            plugin_id = %self.id,
                            "OTLP gRPC metrics exporter in cool-down; skipping snapshot transmission"
                        );
                        continue;
                    }

                    if cooldown_until.is_some() {
                        cooldown_until = None;
                        grpc_client = Some(OtlpMetricsGrpcClient::new(
                            &self.config.endpoint,
                            connect_timeout,
                            request_timeout,
                            &self.config.tls,
                        )?);
                        consecutive_failures = 0;
                    }

                    let snapshot = self.metrics_store.snapshot();
                    format_otlp_metrics_proto_into(&mut proto_template, &snapshot, &mut proto_buf);

                    if grpc_client.is_none() {
                        grpc_client = Some(OtlpMetricsGrpcClient::new(
                            &self.config.endpoint,
                            connect_timeout,
                            request_timeout,
                            &self.config.tls,
                        )?);
                    }

                    let client = grpc_client.as_mut().unwrap();
                    match client.send_metrics(&proto_buf, &self.custom_headers).await {
                        Ok(()) => {
                            consecutive_failures = 0;
                            self.flushed_count.fetch_add(1, Ordering::Relaxed);
                            tracing::debug!(
                                plugin_id = %self.id,
                                "Flushed OTLP metrics batch via gRPC successfully"
                            );
                        }
                        Err(e) => {
                            consecutive_failures += 1;
                            self.failed_count.fetch_add(1, Ordering::Relaxed);
                            tracing::error!(
                                plugin_id = %self.id,
                                error = %e,
                                "Failed to send OTLP metrics via gRPC to collector"
                            );

                            if consecutive_failures <= 10 {
                                let backoff_ms = (100u64
                                    .saturating_mul(1u64 << (consecutive_failures - 1).min(31)))
                                .min(2000);
                                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                            } else {
                                tracing::warn!(
                                    plugin_id = %self.id,
                                    "Shipper failed 10 consecutive times; tearing down connection and entering 30s cool-down"
                                );
                                grpc_client = None;
                                cooldown_until =
                                    Some(tokio::time::Instant::now() + Duration::from_secs(30));
                                consecutive_failures = 0;
                            }
                        }
                    }
                }
            } else {
                let mut http_client = Some(OtlpMetricsClient::new(
                    &self.config.endpoint,
                    connect_timeout,
                    request_timeout,
                    &self.config.tls,
                )?);
                let json_template = OtlpJsonTemplate::new(&self.config.service_name);
                let mut json_buf = Vec::with_capacity(64 * 1024);
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
                            "OTLP HTTP metrics exporter in cool-down; skipping snapshot transmission"
                        );
                        continue;
                    }

                    if cooldown_until.is_some() {
                        cooldown_until = None;
                        http_client = Some(OtlpMetricsClient::new(
                            &self.config.endpoint,
                            connect_timeout,
                            request_timeout,
                            &self.config.tls,
                        )?);
                        consecutive_failures = 0;
                    }

                    let snapshot = self.metrics_store.snapshot();
                    json_template.format_into(&snapshot, &mut json_buf);

                    if http_client.is_none() {
                        http_client = Some(OtlpMetricsClient::new(
                            &self.config.endpoint,
                            connect_timeout,
                            request_timeout,
                            &self.config.tls,
                        )?);
                    }

                    let client = http_client.as_mut().unwrap();
                    let send_result = client.send_metrics(&json_buf, &self.custom_headers).await;
                    match send_result {
                        Ok(status) if (200..300).contains(&status) => {
                            consecutive_failures = 0;
                            self.flushed_count.fetch_add(1, Ordering::Relaxed);
                            tracing::debug!(
                                plugin_id = %self.id,
                                status = status,
                                "Flushed OTLP metrics batch successfully"
                            );
                        }
                        Ok(status) => {
                            consecutive_failures += 1;
                            self.failed_count.fetch_add(1, Ordering::Relaxed);
                            tracing::warn!(
                                plugin_id = %self.id,
                                status = status,
                                "OTLP metrics collector rejected payload"
                            );

                            if consecutive_failures <= 10 {
                                let backoff_ms = (100u64
                                    .saturating_mul(1u64 << (consecutive_failures - 1).min(31)))
                                .min(2000);
                                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                            } else {
                                tracing::warn!(
                                    plugin_id = %self.id,
                                    "Shipper failed 10 consecutive times; tearing down connection and entering 30s cool-down"
                                );
                                http_client = None;
                                cooldown_until =
                                    Some(tokio::time::Instant::now() + Duration::from_secs(30));
                                consecutive_failures = 0;
                            }
                        }
                        Err(e) => {
                            consecutive_failures += 1;
                            self.failed_count.fetch_add(1, Ordering::Relaxed);
                            tracing::error!(
                                plugin_id = %self.id,
                                error = %e,
                                "Failed to send OTLP metrics to collector"
                            );

                            if consecutive_failures <= 10 {
                                let backoff_ms = (100u64
                                    .saturating_mul(1u64 << (consecutive_failures - 1).min(31)))
                                .min(2000);
                                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                            } else {
                                tracing::warn!(
                                    plugin_id = %self.id,
                                    "Shipper failed 10 consecutive times; tearing down connection and entering 30s cool-down"
                                );
                                http_client = None;
                                cooldown_until =
                                    Some(tokio::time::Instant::now() + Duration::from_secs(30));
                                consecutive_failures = 0;
                            }
                        }
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

    #[test]
    fn test_otlp_metrics_exporter_from_config() {
        let p_cfg = PluginConfig {
            id: "otlp_metrics".to_string(),
            config: serde_json::json!({
                "protocol": "grpc",
                "endpoint": "grpc://127.0.0.1:4317",
                "interval_secs": 15,
                "connect_timeout_ms": 5000,
                "request_timeout_ms": 10000,
                "service_name": "velda-edge",
                "headers": {}
            }),
        };

        let plugin =
            OtlpMetricsExporterPlugin::from_config(&p_cfg).expect("Must parse valid config");
        assert_eq!(plugin.config.protocol, "grpc");
        assert_eq!(plugin.config.interval_secs, 15);
    }

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
    async fn test_otlp_metrics_exporter_shutdown() {
        let store = Arc::new(MetricsStore::new());
        let config: OtlpMetricsExporterConfig = serde_json::from_value(serde_json::json!({
            "protocol": "http",
            "endpoint": "http://127.0.0.1:4318/v1/metrics",
            "interval_secs": 1,
            "connect_timeout_ms": 1000,
            "request_timeout_ms": 1000,
            "service_name": "velda-edge",
            "headers": {}
        }))
        .unwrap();

        let plugin = Arc::new(
            OtlpMetricsExporterPlugin::with_store("otlp_metrics_test", config, store).unwrap(),
        );
        let (tx, rx) = watch::channel(false);

        let handle = tokio::spawn(async move { plugin.run(rx).await });

        tx.send(true).unwrap();
        let res = handle.await.unwrap();
        assert!(res.is_ok());
    }
}
