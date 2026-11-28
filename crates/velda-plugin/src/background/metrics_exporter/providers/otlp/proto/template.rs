//! OpenTelemetry ResourceMetrics Protobuf Template & Serializer (`metrics_exporter::providers::otlp::proto::template`).
//!
//! ```text
//!   [Initialization: OtlpProtoTemplate::new(service_name, temporality)]
//!   Pre-bakes:
//!   ├── resource_header + scope_header + metric0_meta + sum_trailer
//!   ├── req_fragments[64 * 41]:  pre-encoded field-1 NumberDataPoint wire slices (+ ts/val offsets)
//!   └── static_metrics_wire:     pre-encoded active_streams + bytes_rx + bytes_tx + LE offsets
//!
//!   [Runtime: update_and_encode()]
//!   1. Bit-scan `p.status_mask` (`trailing_zeros`, `mask &= mask - 1`) -> splice active DPs
//!      directly into `sum_dps_buf` and patch fixed64 timestamp + sfixed64 count in place
//!   2. Frame ExportMetricsServiceRequest -> ResourceMetrics -> ScopeMetrics -> [Metric0 | StaticMetrics]
//!   3. Patch 64x3 static metric timestamps & values directly in output buffer
//! ```

use prost::Message;
use velda_core::lut::{InternalStatus, PipeKind};
use velda_observability::MetricsSnapshot;

use super::model::{
    Gauge, InstrumentationScope, KeyValue, Metric, NumberDataPoint, Resource, ScopeMetrics, Sum,
    metric, number_data_point,
};
use crate::background::metrics_exporter::config::OtlpTemporality;

const MAGIC_TIMESTAMP: u64 = 0xFEED_FACE_CAFE_BABE;

#[inline(always)]
fn make_request_marker(idx: usize) -> i64 {
    ((0x51u64 << 56) | (idx as u64)) as i64
}

#[inline(always)]
fn make_active_marker(idx: usize) -> i64 {
    ((0x52u64 << 56) | (idx as u64)) as i64
}

#[inline(always)]
fn make_rx_marker(idx: usize) -> i64 {
    ((0x53u64 << 56) | (idx as u64)) as i64
}

#[inline(always)]
fn make_tx_marker(idx: usize) -> i64 {
    ((0x54u64 << 56) | (idx as u64)) as i64
}

#[inline(always)]
fn varint_len(mut v: usize) -> usize {
    let mut len = 1;
    while v >= 0x80 {
        v >>= 7;
        len += 1;
    }
    len
}

#[inline(always)]
fn write_varint(buf: &mut Vec<u8>, mut v: usize) {
    while v >= 0x80 {
        buf.push(((v as u8) & 0x7f) | 0x80);
        v >>= 7;
    }
    buf.push(v as u8);
}

#[inline(always)]
fn write_len_field(buf: &mut Vec<u8>, tag: u8, payload_len: usize) {
    buf.push(tag);
    write_varint(buf, payload_len);
}

fn find_fixed64_offset(wire: &[u8], tag: u8, needle: [u8; 8]) -> usize {
    let mut j = 0;
    while j + 9 <= wire.len() {
        if wire[j] == tag && wire[j + 1..j + 9] == needle {
            return j + 1;
        }
        j += 1;
    }
    panic!("Expected pre-baked fixed64 marker offset");
}

struct PrebakedDpFragment {
    raw: Box<[u8]>,
    ts_offset: usize,
    val_offset: usize,
}

/// Pre-allocated, zero-heap-churn template for OTLP Protobuf metrics export.
pub struct OtlpProtoTemplate {
    resource_header: Vec<u8>,
    scope_header: Vec<u8>,
    metric0_meta: Vec<u8>,
    sum_trailer: Vec<u8>,
    req_fragments: Vec<PrebakedDpFragment>,
    static_metrics_wire: Vec<u8>,
    static_ts_offsets: Vec<usize>,
    active_offsets: Vec<usize>,
    rx_offsets: Vec<usize>,
    tx_offsets: Vec<usize>,
    sum_dps_buf: Vec<u8>,
}

impl OtlpProtoTemplate {
    /// Creates a new template pre-populated with metric definitions and static dimension attributes.
    pub fn new(service_name: &str, temporality: OtlpTemporality) -> Self {
        let agg_proto = temporality.as_otlp_proto_int();

        // 1. Pre-bake Resource field inside ResourceMetrics (tag 1: 0x0a)
        let resource = Resource {
            attributes: vec![
                KeyValue::new_string("service.name", service_name),
                KeyValue::new_string("telemetry.sdk.name", "velda-edge"),
                KeyValue::new_string("telemetry.sdk.language", "rust"),
            ],
            dropped_attributes_count: 0,
        };
        let mut res_encoded = Vec::new();
        resource.encode(&mut res_encoded).expect("encode resource");
        let mut resource_header = Vec::with_capacity(res_encoded.len() + 4);
        write_len_field(&mut resource_header, 0x0a, res_encoded.len());
        resource_header.extend_from_slice(&res_encoded);

        // 2. Pre-bake InstrumentationScope field inside ScopeMetrics (tag 1: 0x0a)
        let scope = InstrumentationScope {
            name: "velda-edge".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            attributes: Vec::new(),
            dropped_attributes_count: 0,
        };
        let mut scope_encoded = Vec::new();
        scope.encode(&mut scope_encoded).expect("encode scope");
        let mut scope_header = Vec::with_capacity(scope_encoded.len() + 4);
        write_len_field(&mut scope_header, 0x0a, scope_encoded.len());
        scope_header.extend_from_slice(&scope_encoded);

        // 3. Pre-bake Metric 0 metadata prefix (`name`, `description`, `unit`, without `data`)
        let metric0_stub = Metric {
            name: "velda.requests.total".to_string(),
            description: "Total number of HTTP requests processed across pipeline dimensions"
                .to_string(),
            unit: "1".to_string(),
            data: None,
        };
        let mut metric0_meta = Vec::new();
        metric0_stub
            .encode(&mut metric0_meta)
            .expect("encode metric0 meta");

        // 4. Pre-bake empty Sum trailer (`aggregation_temporality`, `is_monotonic`)
        let empty_sum = Sum {
            data_points: Vec::new(),
            aggregation_temporality: agg_proto,
            is_monotonic: true,
        };
        let mut sum_trailer = Vec::new();
        empty_sum.encode(&mut sum_trailer).expect("encode sum meta");

        // 5. Pre-bake 64 * InternalStatus::COUNT repeated field-1 NumberDataPoint fragments
        let mut req_fragments = Vec::with_capacity(PipeKind::COUNT * InternalStatus::COUNT);
        let mut req_idx = 0usize;
        let ts_needle = MAGIC_TIMESTAMP.to_le_bytes();
        let mut dp_tmp = Vec::with_capacity(128);

        for &pipe in &PipeKind::ALL {
            let down = pipe.downstream_protocol().as_str();
            let up = pipe.upstream_protocol().as_str();
            let strat = pipe.strategy_str();
            for &status in &InternalStatus::ALL {
                let dp = NumberDataPoint {
                    attributes: vec![
                        KeyValue::new_string("downstream", down),
                        KeyValue::new_string("upstream", up),
                        KeyValue::new_string("pipe", strat),
                        KeyValue::new_string("internal_status", status.as_str()),
                    ],
                    start_time_unix_nano: 0,
                    time_unix_nano: MAGIC_TIMESTAMP,
                    value: Some(number_data_point::Value::AsInt(make_request_marker(
                        req_idx,
                    ))),
                    flags: 0,
                };
                dp_tmp.clear();
                dp.encode(&mut dp_tmp).expect("encode dp");
                let mut framed = Vec::with_capacity(dp_tmp.len() + 3);
                write_len_field(&mut framed, 0x0a, dp_tmp.len());
                framed.extend_from_slice(&dp_tmp);

                let ts_offset = find_fixed64_offset(&framed, 0x19, ts_needle);
                let val_offset =
                    find_fixed64_offset(&framed, 0x31, make_request_marker(req_idx).to_le_bytes());

                req_fragments.push(PrebakedDpFragment {
                    raw: framed.into_boxed_slice(),
                    ts_offset,
                    val_offset,
                });
                req_idx += 1;
            }
        }

        // 6. Pre-bake fixed metrics 1..=3 (`active_streams`, `bytes_received_total`, `bytes_transmitted_total`)
        let mut active_datapoints = Vec::with_capacity(PipeKind::COUNT);
        let mut rx_datapoints = Vec::with_capacity(PipeKind::COUNT);
        let mut tx_datapoints = Vec::with_capacity(PipeKind::COUNT);
        for (slot_idx, &pipe) in PipeKind::ALL.iter().enumerate() {
            let down = pipe.downstream_protocol().as_str();
            let up = pipe.upstream_protocol().as_str();
            let strat = pipe.strategy_str();
            active_datapoints.push(NumberDataPoint {
                attributes: vec![
                    KeyValue::new_string("downstream", down),
                    KeyValue::new_string("upstream", up),
                    KeyValue::new_string("pipe", strat),
                ],
                start_time_unix_nano: 0,
                time_unix_nano: MAGIC_TIMESTAMP,
                value: Some(number_data_point::Value::AsInt(make_active_marker(
                    slot_idx,
                ))),
                flags: 0,
            });
            rx_datapoints.push(NumberDataPoint {
                attributes: vec![
                    KeyValue::new_string("downstream", down),
                    KeyValue::new_string("upstream", up),
                    KeyValue::new_string("pipe", strat),
                ],
                start_time_unix_nano: 0,
                time_unix_nano: MAGIC_TIMESTAMP,
                value: Some(number_data_point::Value::AsInt(make_rx_marker(slot_idx))),
                flags: 0,
            });
            tx_datapoints.push(NumberDataPoint {
                attributes: vec![
                    KeyValue::new_string("downstream", down),
                    KeyValue::new_string("upstream", up),
                    KeyValue::new_string("pipe", strat),
                ],
                start_time_unix_nano: 0,
                time_unix_nano: MAGIC_TIMESTAMP,
                value: Some(number_data_point::Value::AsInt(make_tx_marker(slot_idx))),
                flags: 0,
            });
        }

        let static_scope = ScopeMetrics {
            scope: None,
            metrics: vec![
                Metric {
                    name: "velda.active_streams".to_string(),
                    description: "Number of streams actively in-flight".to_string(),
                    unit: "1".to_string(),
                    data: Some(metric::Data::Gauge(Gauge {
                        data_points: active_datapoints,
                    })),
                },
                Metric {
                    name: "velda.bytes_received_total".to_string(),
                    description: "Total bytes received from downstream clients".to_string(),
                    unit: "By".to_string(),
                    data: Some(metric::Data::Sum(Sum {
                        data_points: rx_datapoints,
                        aggregation_temporality: agg_proto,
                        is_monotonic: true,
                    })),
                },
                Metric {
                    name: "velda.bytes_transmitted_total".to_string(),
                    description: "Total bytes transmitted to downstream clients".to_string(),
                    unit: "By".to_string(),
                    data: Some(metric::Data::Sum(Sum {
                        data_points: tx_datapoints,
                        aggregation_temporality: agg_proto,
                        is_monotonic: true,
                    })),
                },
            ],
            schema_url: String::new(),
        };

        let mut static_metrics_wire = Vec::with_capacity(24 * 1024);
        static_scope
            .encode(&mut static_metrics_wire)
            .expect("encode static metrics");

        let mut static_ts_offsets = Vec::with_capacity(PipeKind::COUNT * 3);
        let mut i = 0;
        while i + 9 <= static_metrics_wire.len() {
            if static_metrics_wire[i] == 0x19 && static_metrics_wire[i + 1..i + 9] == ts_needle {
                static_ts_offsets.push(i + 1);
                i += 9;
            } else {
                i += 1;
            }
        }
        assert_eq!(static_ts_offsets.len(), PipeKind::COUNT * 3);

        let mut active_offsets = Vec::with_capacity(PipeKind::COUNT);
        let mut rx_offsets = Vec::with_capacity(PipeKind::COUNT);
        let mut tx_offsets = Vec::with_capacity(PipeKind::COUNT);
        for idx in 0..PipeKind::COUNT {
            active_offsets.push(find_fixed64_offset(
                &static_metrics_wire,
                0x31,
                make_active_marker(idx).to_le_bytes(),
            ));
            rx_offsets.push(find_fixed64_offset(
                &static_metrics_wire,
                0x31,
                make_rx_marker(idx).to_le_bytes(),
            ));
            tx_offsets.push(find_fixed64_offset(
                &static_metrics_wire,
                0x31,
                make_tx_marker(idx).to_le_bytes(),
            ));
        }

        Self {
            resource_header,
            scope_header,
            metric0_meta,
            sum_trailer,
            req_fragments,
            static_metrics_wire,
            static_ts_offsets,
            active_offsets,
            rx_offsets,
            tx_offsets,
            sum_dps_buf: Vec::with_capacity(16 * 1024),
        }
    }

    /// Encodes `snapshot` into `buf` with bit-scan filtering on `p.status_mask`.
    #[inline]
    pub fn update_and_encode(&mut self, snapshot: &MetricsSnapshot, buf: &mut Vec<u8>) {
        let now_nanos = if snapshot.captured_at_nanos > 0 {
            snapshot.captured_at_nanos
        } else {
            velda_core::clock::now_unix_nanos()
        };
        let now_bytes = now_nanos.to_le_bytes();

        // 1. Assemble only active Sum NumberDataPoints via bit-scan on status_mask
        self.sum_dps_buf.clear();
        for p in &snapshot.pipelines {
            let base = (p.pipe as usize) * InternalStatus::COUNT;
            let mut mask = p.status_mask;
            while mask != 0 {
                let status_idx = mask.trailing_zeros() as usize;
                mask &= mask - 1;
                let frag = &self.req_fragments[base + status_idx];
                let start = self.sum_dps_buf.len();
                self.sum_dps_buf.extend_from_slice(&frag.raw);
                self.sum_dps_buf[start + frag.ts_offset..start + frag.ts_offset + 8]
                    .copy_from_slice(&now_bytes);
                self.sum_dps_buf[start + frag.val_offset..start + frag.val_offset + 8]
                    .copy_from_slice(&(p.status_counts[status_idx] as i64).to_le_bytes());
            }
        }

        let sum_len = self.sum_dps_buf.len() + self.sum_trailer.len();
        let metric0_len = self.metric0_meta.len() + 1 + varint_len(sum_len) + sum_len;
        let scope_len = self.scope_header.len()
            + 1
            + varint_len(metric0_len)
            + metric0_len
            + self.static_metrics_wire.len();
        let rm_len = self.resource_header.len() + 1 + varint_len(scope_len) + scope_len;

        buf.clear();
        buf.reserve(1 + varint_len(rm_len) + rm_len);

        write_len_field(buf, 0x0a, rm_len);
        buf.extend_from_slice(&self.resource_header);
        write_len_field(buf, 0x12, scope_len);
        buf.extend_from_slice(&self.scope_header);
        write_len_field(buf, 0x12, metric0_len);
        buf.extend_from_slice(&self.metric0_meta);
        write_len_field(buf, 0x3a, sum_len);
        buf.extend_from_slice(&self.sum_dps_buf);
        buf.extend_from_slice(&self.sum_trailer);

        let static_base = buf.len();
        buf.extend_from_slice(&self.static_metrics_wire);

        // 2. Patch static metric timestamps + active/rx/tx values in place
        let slice = buf.as_mut_slice();
        for &off in &self.static_ts_offsets {
            slice[static_base + off..static_base + off + 8].copy_from_slice(&now_bytes);
        }
        for (i, p) in snapshot.pipelines.iter().enumerate() {
            if let Some(&off) = self.active_offsets.get(i) {
                slice[static_base + off..static_base + off + 8]
                    .copy_from_slice(&p.active_streams.to_le_bytes());
            }
            if let Some(&off) = self.rx_offsets.get(i) {
                slice[static_base + off..static_base + off + 8]
                    .copy_from_slice(&(p.bytes_rx as i64).to_le_bytes());
            }
            if let Some(&off) = self.tx_offsets.get(i) {
                slice[static_base + off..static_base + off + 8]
                    .copy_from_slice(&(p.bytes_tx as i64).to_le_bytes());
            }
        }
    }
}

/// Encodes a [`MetricsSnapshot`] into compliant OpenTelemetry binary Protobuf bytes.
pub fn format_otlp_metrics_proto(
    snapshot: &MetricsSnapshot,
    service_name: &str,
    temporality: OtlpTemporality,
) -> Vec<u8> {
    let mut template = OtlpProtoTemplate::new(service_name, temporality);
    let mut buf = Vec::with_capacity(32 * 1024);
    template.update_and_encode(snapshot, &mut buf);
    buf
}

/// In-place updates an existing [`OtlpProtoTemplate`] and encodes into `buf`.
pub fn format_otlp_metrics_proto_into(
    template: &mut OtlpProtoTemplate,
    snapshot: &MetricsSnapshot,
    buf: &mut Vec<u8>,
) {
    template.update_and_encode(snapshot, buf);
}

#[cfg(test)]
mod tests {
    use super::super::model::{ExportMetricsServiceRequest, any_value};
    use super::*;
    use velda_observability::{MetricsStore, StreamCompletion};

    #[test]
    fn test_format_otlp_metrics_proto() {
        let store = MetricsStore::new();
        store.record_stream_completion_on_core(
            0,
            PipeKind::BufferedH1H1,
            StreamCompletion::new(InternalStatus::Completed, 120, 240, 1_000_000, 200_000),
        );
        store.record_stream_completion_on_core(
            0,
            PipeKind::BufferedH1H1,
            StreamCompletion::new(InternalStatus::UpstreamTimeout, 10, 0, 5_000_000, 0),
        );
        let snapshot = store.snapshot();
        let bytes = format_otlp_metrics_proto(&snapshot, "test-service", OtlpTemporality::Delta);
        assert!(!bytes.is_empty());

        let decoded =
            ExportMetricsServiceRequest::decode(&bytes[..]).expect("Must decode successfully");
        let rm = &decoded.resource_metrics[0];
        assert_eq!(
            rm.resource.as_ref().unwrap().attributes[0]
                .value
                .as_ref()
                .unwrap()
                .value,
            Some(any_value::Value::StringValue("test-service".to_string()))
        );

        let official = <opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest as prost_otlp::Message>::decode(&bytes[..])
            .expect("OTLP Proto metrics must decode against official opentelemetry-proto schema");
        let off_metrics = &official.resource_metrics[0].scope_metrics[0].metrics;
        assert_eq!(off_metrics.len(), 4);
        if let Some(opentelemetry_proto::tonic::metrics::v1::metric::Data::Sum(sum)) =
            &off_metrics[0].data
        {
            assert_eq!(sum.aggregation_temporality, 1);
            assert_eq!(sum.data_points.len(), 2);
            assert_eq!(sum.data_points[0].attributes[3].key, "internal_status");
        } else {
            panic!("Expected Sum metric");
        }
        if let Some(opentelemetry_proto::tonic::metrics::v1::metric::Data::Sum(rx_sum)) =
            &off_metrics[2].data
        {
            assert_eq!(rx_sum.data_points.len(), PipeKind::COUNT);
        } else {
            panic!("Expected rx Sum metric");
        }
    }

    #[test]
    fn test_proto_template_reuse() {
        let store = MetricsStore::new();
        let mut template = OtlpProtoTemplate::new("reuse-service", OtlpTemporality::Cumulative);
        let mut buf = Vec::with_capacity(32 * 1024);

        let snap1 = store.snapshot();
        template.update_and_encode(&snap1, &mut buf);
        let len1 = buf.len();

        let snap2 = store.snapshot();
        template.update_and_encode(&snap2, &mut buf);
        assert_eq!(buf.len(), len1);
    }
}
