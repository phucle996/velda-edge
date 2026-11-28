//! Elasticsearch / OpenSearch Access Log Provider (`log_exporter::providers::elasticsearch`).

pub mod format;
pub mod wire;

use std::io::Write;
use std::sync::Arc;
use std::time::Duration;

use flate2::Compression;
use flate2::write::GzEncoder;
use velda_etp::elasticsearch::ElasticsearchEtp;
use velda_observability::TelemetryBatch;

use crate::background::log_exporter::config::{ElasticsearchTargetConfig, EsCompression};
use crate::error::PluginError;

use self::format::{format_bulk_ndjson, prebake_bulk_action_line};

pub struct ElasticsearchLogProvider {
    pub etp: Arc<ElasticsearchEtp>,
    pub compression: EsCompression,
    pub max_drain_batches: usize,
    pub flush_interval_ms: u64,
    pub request_timeout: Duration,
    prebaked_head_prefix: Vec<u8>,
    prebaked_action: Vec<u8>,
    head_buf: Vec<u8>,
    payload_buf: Vec<u8>,
    compress_buf: Vec<u8>,
    resp_buf: Vec<u8>,
}

impl ElasticsearchLogProvider {
    pub fn new(etp: Arc<ElasticsearchEtp>, cfg: &ElasticsearchTargetConfig) -> Self {
        let index = match cfg.index.trim() {
            "" => etp
                .config
                .index
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .unwrap_or("velda-logs"),
            explicit => explicit,
        };
        let prebaked_head_prefix = wire::bake_bulk_prefix(&etp.config, cfg);
        let prebaked_action = prebake_bulk_action_line(cfg.op_type, index);
        let head_cap = prebaked_head_prefix.len() + 32;

        Self {
            etp,
            compression: cfg.compression,
            max_drain_batches: cfg.max_drain_batches.clamp(1, 4096),
            flush_interval_ms: cfg.flush_interval_ms.max(10),
            request_timeout: Duration::from_millis(cfg.request_timeout_ms),
            prebaked_head_prefix,
            prebaked_action,
            head_buf: Vec::with_capacity(head_cap),
            payload_buf: Vec::with_capacity(128 * 1024),
            compress_buf: Vec::with_capacity(64 * 1024),
            resp_buf: Vec::with_capacity(1024),
        }
    }

    pub fn name(&self) -> &'static str {
        "elasticsearch"
    }

    pub async fn flush_batches(
        &mut self,
        batches: &[TelemetryBatch],
    ) -> Result<usize, PluginError> {
        self.payload_buf.clear();
        let encoded = format_bulk_ndjson(batches, &self.prebaked_action, &mut self.payload_buf);
        if encoded == 0 || self.payload_buf.is_empty() {
            return Ok(0);
        }

        let wire_payload: &[u8] = match self.compression {
            EsCompression::Gzip => {
                self.compress_buf.clear();
                let mut encoder = GzEncoder::new(&mut self.compress_buf, Compression::fast());
                encoder
                    .write_all(&self.payload_buf)
                    .map_err(|e| PluginError::Http(format!("ES gzip encode error: {e}")))?;
                encoder
                    .finish()
                    .map_err(|e| PluginError::Http(format!("ES gzip finish error: {e}")))?;
                &self.compress_buf
            }
            EsCompression::None => &self.payload_buf,
        };

        let (status, has_errors) = wire::push_bulk_payload(
            &self.etp,
            &self.prebaked_head_prefix,
            &mut self.head_buf,
            wire_payload,
            &mut self.resp_buf,
            self.request_timeout,
        )
        .await
        .map_err(|e| PluginError::Http(format!("Elasticsearch push failed: {e}")))?;

        if (200..300).contains(&status) {
            if has_errors {
                tracing::warn!(
                    etp_id = %self.etp.config.id,
                    records = encoded,
                    "Elasticsearch _bulk reported partial item errors"
                );
            }
            Ok(encoded)
        } else {
            Err(PluginError::Http(format!(
                "Elasticsearch _bulk returned HTTP {status} for {encoded} records"
            )))
        }
    }
}
