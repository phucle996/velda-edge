//! OpenTelemetry OTLP Access Log Provider (`log_exporter::providers::otlp`).

pub mod json;
pub mod proto;
pub mod wire;

use std::sync::Arc;
use std::time::Duration;

use velda_etp::otlp::{OtlpEtp, OtlpProtocol};
use velda_observability::TelemetryBatch;

use crate::background::log_exporter::config::OtlpLogTargetConfig;
use crate::error::PluginError;

use self::json::{OtlpJsonEnvelope, format_otlp_records_json};
use self::proto::OtlpProtoWireEnvelope;
use self::wire::OtlpLogsWireTemplate;

enum OtlpLogFormatter {
    Json(OtlpJsonEnvelope),
    Proto(OtlpProtoWireEnvelope),
}

pub struct OtlpLogProvider {
    pub etp: Arc<OtlpEtp>,
    pub flush_interval_ms: u64,
    pub request_timeout: Duration,
    wire_template: OtlpLogsWireTemplate,
    formatter: OtlpLogFormatter,
    head_buf: Vec<u8>,
    payload_buf: Vec<u8>,
    proto_records_buf: Vec<u8>,
    resp_buf: Vec<u8>,
}

impl OtlpLogProvider {
    pub fn new(etp: Arc<OtlpEtp>, cfg: &OtlpLogTargetConfig) -> Result<Self, PluginError> {
        let wire_template = wire::bake_wire_template(&etp.config)?;
        let formatter = if matches!(etp.config.protocol, OtlpProtocol::HttpJson) {
            OtlpLogFormatter::Json(OtlpJsonEnvelope::new(&cfg.service_name))
        } else {
            OtlpLogFormatter::Proto(OtlpProtoWireEnvelope::new(&cfg.service_name))
        };
        Ok(Self {
            etp,
            flush_interval_ms: cfg.flush_interval_ms.max(50),
            request_timeout: Duration::from_millis(cfg.request_timeout_ms),
            wire_template,
            formatter,
            head_buf: Vec::with_capacity(512),
            payload_buf: Vec::with_capacity(32 * 1024),
            proto_records_buf: Vec::with_capacity(32 * 1024),
            resp_buf: Vec::with_capacity(1024),
        })
    }

    pub fn name(&self) -> &'static str {
        "otlp"
    }

    pub async fn flush_batches(
        &mut self,
        batches: &[TelemetryBatch],
    ) -> Result<usize, PluginError> {
        let record_count = match &self.formatter {
            OtlpLogFormatter::Json(env) => {
                format_otlp_records_json(batches, env, &mut self.payload_buf)
            }
            OtlpLogFormatter::Proto(env) => {
                env.format_records_wire(batches, &mut self.proto_records_buf, &mut self.payload_buf)
            }
        };
        if record_count == 0 {
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
