//! S3-Compatible Object Storage Access Log Provider (`log_exporter::providers::s3`).

pub mod format;
pub mod sigv4;
pub mod wire;

use std::sync::Arc;

use velda_etp::s3::S3Etp;
use velda_observability::TelemetryBatch;

use crate::background::log_exporter::config::S3TargetConfig;
use crate::error::PluginError;

use self::format::format_ndjson;
use self::wire::S3WireState;

pub struct S3LogProvider {
    pub etp_id: String,
    pub max_drain_batches: usize,
    pub flush_interval_ms: u64,
    wire_state: S3WireState,
    payload_buf: Vec<u8>,
}

impl S3LogProvider {
    pub fn new(etp: Arc<S3Etp>, cfg: &S3TargetConfig) -> Self {
        let etp_id = etp.config.id.clone();
        let wire_state = S3WireState::new(etp, cfg);
        Self {
            etp_id,
            max_drain_batches: cfg.max_drain_batches.clamp(1, 4096),
            flush_interval_ms: cfg.flush_interval_ms.max(10),
            wire_state,
            payload_buf: Vec::with_capacity(256 * 1024),
        }
    }

    pub fn name(&self) -> &'static str {
        "s3"
    }

    pub async fn flush_batches(
        &mut self,
        batches: &[TelemetryBatch],
    ) -> Result<usize, PluginError> {
        self.payload_buf.clear();
        let encoded = format_ndjson(batches, &mut self.payload_buf);
        if encoded == 0 || self.payload_buf.is_empty() {
            return Ok(0);
        }

        let status = wire::upload_batch(&mut self.wire_state, &self.payload_buf).await?;
        if (200..300).contains(&status) {
            Ok(encoded)
        } else {
            Err(PluginError::Http(format!(
                "S3 PUT returned HTTP {status} for {encoded} records"
            )))
        }
    }
}
