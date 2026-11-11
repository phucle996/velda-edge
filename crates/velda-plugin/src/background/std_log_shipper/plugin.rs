//! Standard Stream Log Shipper Shadow Worker Coordinator (`std_log_shipper::plugin`).
//!
//! ```text
//! ┌────────────────────────────────────────────────────────────────────────────────────────┐
//! │                          SHADOW WORKER COORDINATOR ARCHITECTURE                        │
//! │                                                                                        │
//! │   Traffic Workers (Cores 0..N, SCHED_OTHER)                                            │
//! │        │                                                                               │
//! │        ▼ AccessLogRecord (64B)                                                         │
//! │   [LogStore SPSC Ring Buffers]                                                         │
//! │        │                                                                               │
//! │        ▼ Periodic flush_all_lanes() & subscriber.drain_batches()                       │
//! │   Batched Arc<[AccessLogRecord]> Slices                                                │
//! │        │                                                                               │
//! │        ▼ format_std_log_payload() (Zero Heap Reallocations)                            │
//! │   out_buf (Pre-reserved Vector, Direct Byte Stamping)                                  │
//! │        │                                                                               │
//! │        ▼ sink.write_all(&out_buf).await (Single Open FD, Zero Open/Close Churn)        │
//! │   StdLogSink (Persistent File / Stdout / Stderr)                                       │
//! │        │                                                                               │
//! │        ▼ Drop Arc<[AccessLogRecord]> ──► Lock-free memory reclamation                  │
//! └────────────────────────────────────────────────────────────────────────────────────────┘
//! ```

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::sync::watch;
use velda_observability::{LogStore, global_log_store};
pub use velda_sync::post_sync::plugin::PluginConfig;

pub use super::config::{StdLogShipperConfig, TargetStream};
pub use super::format::format_std_log_payload;
pub use super::sink::StdLogSink;
use crate::background::BackgroundPlugin;
use crate::error::PluginError;

/// Standalone Background Standard Stream Log Shipper Plugin.
pub struct StdLogShipperPlugin {
    pub id: String,
    pub config: StdLogShipperConfig,
    pub target: TargetStream,
    pub log_store: Arc<LogStore>,
    pub shipped_records: Arc<AtomicU64>,
}

impl StdLogShipperPlugin {
    pub fn with_store(
        id: impl Into<String>,
        config: StdLogShipperConfig,
        log_store: Arc<LogStore>,
    ) -> Result<Self, PluginError> {
        let target = TargetStream::parse(&config.target);
        Ok(Self {
            id: id.into(),
            config,
            target,
            log_store,
            shipped_records: Arc::new(AtomicU64::new(0)),
        })
    }

    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let validated = velda_sync::validate_std_log_shipper_config(&config.id, &config.config)
            .map_err(|e| PluginError::Config(e.to_string()))?;

        Self::with_store(&config.id, validated, global_log_store())
    }
}

impl BackgroundPlugin for StdLogShipperPlugin {
    fn id(&self) -> &str {
        &self.id
    }

    fn run(
        self: Arc<Self>,
        mut shutdown: watch::Receiver<bool>,
    ) -> Pin<Box<dyn Future<Output = Result<(), PluginError>> + Send>> {
        Box::pin(async move {
            let mut sink = StdLogSink::init(&self.target).await?;
            let subscriber = self.log_store.subscribe(256);
            let mut interval =
                tokio::time::interval(Duration::from_millis(self.config.flush_interval_ms.max(50)));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            let mut out_buf = Vec::with_capacity(16384);

            loop {
                let is_shutting_down = tokio::select! {
                    _ = interval.tick() => false,
                    _ = shutdown.changed() => true,
                };

                self.log_store.flush_all_lanes(self.config.max_batch_size);

                let batches = subscriber.drain_batches(64);
                let record_count =
                    format_std_log_payload(&batches, &self.config.format, &mut out_buf);
                drop(batches);

                if record_count > 0 && !out_buf.is_empty() {
                    if let Err(e) = sink.write_all(&out_buf).await {
                        tracing::error!(
                            plugin_id = %self.id,
                            error = %e,
                            "Failed to write access log batch to target sink"
                        );
                    }

                    self.shipped_records
                        .fetch_add(record_count as u64, Ordering::Relaxed);
                }

                if is_shutting_down {
                    break;
                }
            }

            Ok(())
        })
    }
}
