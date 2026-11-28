//! OpenTelemetry OTLP Metrics Push Provider (`metrics_exporter::providers::otlp`).

pub mod json;
pub mod proto;
pub mod wire;

use std::sync::Arc;
use std::time::Duration;

use velda_etp::otlp::{OtlpEtp, OtlpProtocol};
use velda_observability::{MetricsSnapshot, MetricsStore};

use crate::background::metrics_exporter::config::{OtlpMetricsPushConfig, OtlpTemporality};
use crate::error::PluginError;

use self::json::OtlpJsonTemplate;
use self::proto::{OtlpProtoTemplate, format_otlp_metrics_proto_into};
use self::wire::OtlpMetricsWireTemplate;

enum OtlpMetricsEncoder {
    Json(OtlpJsonTemplate),
    Proto(OtlpProtoTemplate),
}

pub struct OtlpMetricsProvider {
    pub etp: Arc<OtlpEtp>,
    pub interval: Duration,
    pub request_timeout: Duration,
    pub temporality: OtlpTemporality,
    wire_template: OtlpMetricsWireTemplate,
    encoder: OtlpMetricsEncoder,
    head_buf: Vec<u8>,
    payload_buf: Vec<u8>,
    resp_buf: Vec<u8>,
    prev_snapshot: MetricsSnapshot,
}

impl OtlpMetricsProvider {
    pub fn new(etp: Arc<OtlpEtp>, cfg: &OtlpMetricsPushConfig) -> Result<Self, PluginError> {
        let wire_template = wire::bake_wire_template(&etp.config)?;
        let encoder = if matches!(etp.config.protocol, OtlpProtocol::HttpJson) {
            OtlpMetricsEncoder::Json(OtlpJsonTemplate::new(&cfg.service_name, cfg.temporality))
        } else {
            OtlpMetricsEncoder::Proto(OtlpProtoTemplate::new(&cfg.service_name, cfg.temporality))
        };
        Ok(Self {
            etp,
            interval: Duration::from_secs(cfg.interval_secs),
            request_timeout: Duration::from_millis(cfg.request_timeout_ms),
            temporality: cfg.temporality,
            wire_template,
            encoder,
            head_buf: Vec::with_capacity(512),
            payload_buf: Vec::with_capacity(32 * 1024),
            resp_buf: Vec::with_capacity(1024),
            prev_snapshot: MetricsSnapshot::zero(),
        })
    }

    pub async fn push_tick(&mut self, store: &MetricsStore) -> Result<(), PluginError> {
        let snapshot = store.snapshot();
        match self.temporality {
            OtlpTemporality::Delta => {
                let delta = snapshot.delta_since(&self.prev_snapshot);
                self.encode_snapshot(&delta);
            }
            OtlpTemporality::Cumulative => {
                self.encode_snapshot(&snapshot);
            }
        }

        wire::push_metrics(
            &self.etp,
            &self.wire_template,
            &mut self.head_buf,
            &self.payload_buf,
            &mut self.resp_buf,
            self.request_timeout,
        )
        .await?;

        self.prev_snapshot = snapshot;
        Ok(())
    }

    fn encode_snapshot(&mut self, snapshot: &MetricsSnapshot) {
        match &mut self.encoder {
            OtlpMetricsEncoder::Json(t) => t.format_into(snapshot, &mut self.payload_buf),
            OtlpMetricsEncoder::Proto(t) => {
                format_otlp_metrics_proto_into(t, snapshot, &mut self.payload_buf)
            }
        }
    }
}
