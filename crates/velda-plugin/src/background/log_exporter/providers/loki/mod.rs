//! Grafana Loki Access Log Provider (`log_exporter::providers::loki`).

pub mod json;
pub mod proto;
pub mod wire;

use std::sync::Arc;
use std::time::Duration;

use velda_etp::loki::{LokiEtp, LokiProtocol};
use velda_observability::TelemetryBatch;

use crate::background::log_exporter::config::LokiTargetConfig;
use crate::error::PluginError;

use self::json::LokiJsonFormatter;
use self::proto::LokiProtoFormatter;
use self::wire::LokiWireTemplate;

enum LokiFormatter {
    Json(LokiJsonFormatter),
    Proto(LokiProtoFormatter),
}

pub struct LokiLogProvider {
    pub etp: Arc<LokiEtp>,
    pub flush_interval_ms: u64,
    pub request_timeout: Duration,
    wire_template: LokiWireTemplate,
    formatter: LokiFormatter,
    head_buf: Vec<u8>,
    payload_buf: Vec<u8>,
    resp_buf: Vec<u8>,
}

impl LokiLogProvider {
    pub fn new(etp: Arc<LokiEtp>, cfg: &LokiTargetConfig) -> Result<Self, PluginError> {
        let wire_template = wire::bake_wire_template(&etp.config)?;
        let formatter = match etp.config.protocol {
            LokiProtocol::Http => {
                LokiFormatter::Json(LokiJsonFormatter::new(&cfg.service_name, &cfg.labels))
            }
            LokiProtocol::Grpc => {
                LokiFormatter::Proto(LokiProtoFormatter::new(&cfg.service_name, &cfg.labels))
            }
        };
        Ok(Self {
            etp,
            flush_interval_ms: cfg.flush_interval_ms.max(50),
            request_timeout: Duration::from_millis(cfg.request_timeout_ms),
            wire_template,
            formatter,
            head_buf: Vec::with_capacity(512),
            payload_buf: Vec::with_capacity(32 * 1024),
            resp_buf: Vec::with_capacity(1024),
        })
    }

    pub fn name(&self) -> &'static str {
        "loki"
    }

    pub async fn flush_batches(
        &mut self,
        batches: &[TelemetryBatch],
    ) -> Result<usize, PluginError> {
        let record_count = match &mut self.formatter {
            LokiFormatter::Json(f) => f.format_batches(batches, &mut self.payload_buf),
            LokiFormatter::Proto(f) => f.format_batches(batches, &mut self.payload_buf),
        };
        if record_count == 0 || self.payload_buf.is_empty() {
            return Ok(0);
        }

        wire::push_logs(
            &self.etp,
            &self.wire_template,
            &mut self.head_buf,
            &self.payload_buf,
            &mut self.resp_buf,
            self.request_timeout,
        )
        .await?;

        Ok(record_count)
    }
}
