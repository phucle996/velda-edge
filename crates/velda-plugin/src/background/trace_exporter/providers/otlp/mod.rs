//! OpenTelemetry OTLP Trace Provider (`trace_exporter::providers::otlp`).

pub mod json;
pub mod proto;
pub mod wire;

use std::sync::Arc;
use std::time::Duration;

use velda_etp::otlp::{OtlpEtp, OtlpProtocol};
use velda_observability::TelemetryBatch;

use crate::background::trace_exporter::config::TraceTargetConfig;
use crate::error::PluginError;

use self::json::OtlpTraceJsonEnvelope;
use self::proto::OtlpTraceProtoEnvelope;
use self::wire::OtlpTracesWireTemplate;

enum OtlpTraceEncoder {
    Json(OtlpTraceJsonEnvelope),
    Proto(OtlpTraceProtoEnvelope),
}

pub struct OtlpTraceProvider {
    pub etp: Arc<OtlpEtp>,
    pub flush_interval_ms: u64,
    pub request_timeout: Duration,
    wire_template: OtlpTracesWireTemplate,
    encoder: OtlpTraceEncoder,
    head_buf: Vec<u8>,
    json_buf: String,
    proto_buf: Vec<u8>,
    resp_buf: Vec<u8>,
}

impl OtlpTraceProvider {
    pub fn new(etp: Arc<OtlpEtp>, cfg: &TraceTargetConfig) -> Result<Self, PluginError> {
        let wire_template = wire::bake_wire_template(&etp.config)?;
        let use_json = matches!(etp.config.protocol, OtlpProtocol::HttpJson);
        let encoder = if use_json {
            OtlpTraceEncoder::Json(OtlpTraceJsonEnvelope::new(&cfg.service_name))
        } else {
            OtlpTraceEncoder::Proto(OtlpTraceProtoEnvelope::new(&cfg.service_name))
        };
        Ok(Self {
            etp,
            flush_interval_ms: cfg.flush_interval_ms.max(10),
            request_timeout: Duration::from_millis(cfg.request_timeout_ms),
            wire_template,
            encoder,
            head_buf: Vec::with_capacity(512),
            json_buf: String::with_capacity(if use_json { 32 * 1024 } else { 0 }),
            proto_buf: Vec::with_capacity(if use_json { 0 } else { 32 * 1024 }),
            resp_buf: Vec::with_capacity(1024),
        })
    }

    pub async fn flush_batches(
        &mut self,
        batches: &[TelemetryBatch],
    ) -> Result<usize, PluginError> {
        let (span_count, is_json) = match &mut self.encoder {
            OtlpTraceEncoder::Json(env) => {
                (env.format_spans_json(batches, &mut self.json_buf), true)
            }
            OtlpTraceEncoder::Proto(env) => {
                (env.format_spans_wire(batches, &mut self.proto_buf), false)
            }
        };
        if span_count == 0 {
            return Ok(0);
        }

        let payload = if is_json {
            self.json_buf.as_bytes()
        } else {
            &self.proto_buf
        };

        wire::push_traces(
            &self.etp,
            &self.wire_template,
            &mut self.head_buf,
            payload,
            &mut self.resp_buf,
            self.request_timeout,
        )
        .await?;

        Ok(span_count)
    }
}
