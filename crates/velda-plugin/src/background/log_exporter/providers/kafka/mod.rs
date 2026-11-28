//! Kafka Access Log Provider (`log_exporter::providers::kafka`).

pub mod compress;
pub mod format;
pub mod protocol;
pub mod wire;

use std::sync::Arc;

use velda_etp::kafka::KafkaEtp;
use velda_observability::{TelemetryBatch, TelemetryRecord};

use crate::background::log_exporter::config::KafkaTargetConfig;
use crate::error::PluginError;

use self::wire::KafkaWireState;

pub struct KafkaLogProvider {
    pub etp_id: String,
    pub topic: String,
    pub flush_interval_ms: u64,
    pub max_batch_size: usize,
    wire_state: KafkaWireState,
    records_buf: Vec<TelemetryRecord>,
}

impl KafkaLogProvider {
    pub fn new(etp: Arc<KafkaEtp>, cfg: &KafkaTargetConfig) -> Result<Self, PluginError> {
        let etp_id = etp.config.id.clone();
        let wire_state = KafkaWireState::new(etp, cfg)?;
        Ok(Self {
            etp_id,
            topic: cfg.topic.clone(),
            flush_interval_ms: cfg.flush_interval_ms.max(100),
            max_batch_size: cfg.max_batch_size,
            wire_state,
            records_buf: Vec::with_capacity(cfg.max_batch_size),
        })
    }

    pub fn name(&self) -> &'static str {
        "kafka"
    }

    pub async fn prepare(&mut self) -> Result<(), PluginError> {
        if let Err(e) = self.wire_state.refresh_metadata().await {
            tracing::warn!(
                etp_id = %self.etp_id,
                error = %e,
                topic = %self.topic,
                "Could not fetch Kafka topic metadata on startup (will retry during push)"
            );
        }
        Ok(())
    }

    pub async fn flush_batches(
        &mut self,
        batches: &[TelemetryBatch],
    ) -> Result<usize, PluginError> {
        self.records_buf.clear();
        for b in batches {
            self.records_buf.extend_from_slice(b.as_ref());
            if self.records_buf.len() >= self.max_batch_size {
                break;
            }
        }
        if self.records_buf.is_empty() {
            return Ok(0);
        }

        self.wire_state.produce_records(&self.records_buf).await
    }
}
