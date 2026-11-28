//! OpenTelemetry ResourceMetrics JSON Formatter (`metrics_exporter::providers::otlp::json::template`).
//!
//! ```text
//!   [Initialization: OtlpJsonTemplate::new(service_name, temporality)]
//!   Pre-bakes static JSON fragments:
//!   ├── root_header (service.name, scope, velda.requests.total Sum header + temporality)
//!   ├── req_tails[64 * 41]:  b"\",\"attributes\":[...,\"internal_status\":\"...\"}]}"
//!   ├── active_tails[64] / rx_tails[64] / tx_tails[64]
//!   └── metric separators + ROOT_FOOTER
//!
//!   [Runtime: format_into()]
//!   1. fast_format_ts_19digits_simd(ts)
//!   2. For each PipelineSnapshot: bit-scan `p.status_mask` (`trailing_zeros`, `mask &= mask - 1`)
//!      └── copy prefix + fast_write_u64(count) + prebaked req_tails[pipe * COUNT + idx]
//!   3. Emit active_streams / bytes_rx / bytes_tx
//! ```

use velda_core::lut::decimal::{fast_format_ts_19digits_simd, fast_write_i64, fast_write_u64};
use velda_core::lut::{InternalStatus, PipeKind};
use velda_observability::MetricsSnapshot;

use crate::background::metrics_exporter::config::OtlpTemporality;

const METRIC1_TO_METRIC2: &[u8] = b"]}},{\"name\":\"velda.active_streams\",\"description\":\"Number of streams actively in-flight\",\"unit\":\"1\",\"gauge\":{\"dataPoints\":[";
const METRIC2_TO_METRIC3_DELTA: &[u8] = b"]}},{\"name\":\"velda.bytes_received_total\",\"description\":\"Total bytes received from downstream clients\",\"unit\":\"By\",\"sum\":{\"aggregationTemporality\":1,\"isMonotonic\":true,\"dataPoints\":[";
const METRIC2_TO_METRIC3_CUMULATIVE: &[u8] = b"]}},{\"name\":\"velda.bytes_received_total\",\"description\":\"Total bytes received from downstream clients\",\"unit\":\"By\",\"sum\":{\"aggregationTemporality\":2,\"isMonotonic\":true,\"dataPoints\":[";
const METRIC3_TO_METRIC4_DELTA: &[u8] = b"]}},{\"name\":\"velda.bytes_transmitted_total\",\"description\":\"Total bytes transmitted to downstream clients\",\"unit\":\"By\",\"sum\":{\"aggregationTemporality\":1,\"isMonotonic\":true,\"dataPoints\":[";
const METRIC3_TO_METRIC4_CUMULATIVE: &[u8] = b"]}},{\"name\":\"velda.bytes_transmitted_total\",\"description\":\"Total bytes transmitted to downstream clients\",\"unit\":\"By\",\"sum\":{\"aggregationTemporality\":2,\"isMonotonic\":true,\"dataPoints\":[";
const ROOT_FOOTER: &[u8] = b"]}}]}]}]}";

/// Pre-baked static chunk interleaving template for OpenTelemetry ResourceMetrics JSON.
pub struct OtlpJsonTemplate {
    temporality: OtlpTemporality,
    root_header: Vec<u8>,
    req_tails: Vec<Vec<u8>>,
    active_tails: Vec<Vec<u8>>,
    rx_tails: Vec<Vec<u8>>,
    tx_tails: Vec<Vec<u8>>,
}

impl OtlpJsonTemplate {
    /// Constructs a new [`OtlpJsonTemplate`] with pre-baked JSON chunks.
    pub fn new(service_name: &str, temporality: OtlpTemporality) -> Self {
        let agg = match temporality {
            OtlpTemporality::Delta => "1",
            OtlpTemporality::Cumulative => "2",
        };
        let mut root_header = Vec::with_capacity(512);
        root_header.extend_from_slice(b"{\"resourceMetrics\":[{\"resource\":{\"attributes\":[{\"key\":\"service.name\",\"value\":{\"stringValue\":\"");
        root_header.extend_from_slice(service_name.as_bytes());
        root_header.extend_from_slice(b"\"}},{\"key\":\"telemetry.sdk.name\",\"value\":{\"stringValue\":\"velda-edge\"}},{\"key\":\"telemetry.sdk.language\",\"value\":{\"stringValue\":\"rust\"}}]},\"scopeMetrics\":[{\"scope\":{\"name\":\"velda-edge\",\"version\":\"");
        root_header.extend_from_slice(env!("CARGO_PKG_VERSION").as_bytes());
        root_header.extend_from_slice(b"\"},\"metrics\":[{\"name\":\"velda.requests.total\",\"description\":\"Total number of HTTP requests processed across pipeline dimensions\",\"unit\":\"1\",\"sum\":{\"aggregationTemporality\":");
        root_header.extend_from_slice(agg.as_bytes());
        root_header.extend_from_slice(b",\"isMonotonic\":true,\"dataPoints\":[");

        // 1. Pre-bake 64 * InternalStatus::COUNT requests tails
        let mut req_tails = Vec::with_capacity(PipeKind::COUNT * InternalStatus::COUNT);
        for &pipe in &PipeKind::ALL {
            let down = pipe.downstream_protocol().as_str();
            let up = pipe.upstream_protocol().as_str();
            let strat = pipe.strategy_str();
            for &status in &InternalStatus::ALL {
                let mut tail = Vec::with_capacity(256);
                tail.extend_from_slice(
                    b"\",\"attributes\":[{\"key\":\"downstream\",\"value\":{\"stringValue\":\"",
                );
                tail.extend_from_slice(down.as_bytes());
                tail.extend_from_slice(b"\"}},{\"key\":\"upstream\",\"value\":{\"stringValue\":\"");
                tail.extend_from_slice(up.as_bytes());
                tail.extend_from_slice(b"\"}},{\"key\":\"pipe\",\"value\":{\"stringValue\":\"");
                tail.extend_from_slice(strat.as_bytes());
                tail.extend_from_slice(
                    b"\"}},{\"key\":\"internal_status\",\"value\":{\"stringValue\":\"",
                );
                tail.extend_from_slice(status.as_bytes());
                tail.extend_from_slice(b"\"}}]}");
                req_tails.push(tail);
            }
        }

        // 2. Pre-bake 64 active streams & bytes tails
        let mut active_tails = Vec::with_capacity(PipeKind::COUNT);
        let mut rx_tails = Vec::with_capacity(PipeKind::COUNT);
        let mut tx_tails = Vec::with_capacity(PipeKind::COUNT);
        for &pipe in &PipeKind::ALL {
            let down = pipe.downstream_protocol().as_str();
            let up = pipe.upstream_protocol().as_str();
            let strat = pipe.strategy_str();
            let mut tail = Vec::with_capacity(200);
            tail.extend_from_slice(
                b"\",\"attributes\":[{\"key\":\"downstream\",\"value\":{\"stringValue\":\"",
            );
            tail.extend_from_slice(down.as_bytes());
            tail.extend_from_slice(b"\"}},{\"key\":\"upstream\",\"value\":{\"stringValue\":\"");
            tail.extend_from_slice(up.as_bytes());
            tail.extend_from_slice(b"\"}},{\"key\":\"pipe\",\"value\":{\"stringValue\":\"");
            tail.extend_from_slice(strat.as_bytes());
            tail.extend_from_slice(b"\"}}]}");
            active_tails.push(tail.clone());
            rx_tails.push(tail.clone());
            tx_tails.push(tail);
        }

        Self {
            temporality,
            root_header,
            req_tails,
            active_tails,
            rx_tails,
            tx_tails,
        }
    }

    /// Formats a snapshot into `buf` using bit-scan filtering on `p.status_mask`.
    pub fn format_into(&self, snapshot: &MetricsSnapshot, buf: &mut Vec<u8>) {
        buf.clear();
        buf.reserve(64 * 1024);

        let now_nanos = if snapshot.captured_at_nanos > 0 {
            snapshot.captured_at_nanos as u128
        } else {
            velda_core::clock::now_unix_nanos() as u128
        };

        let mut ts_bytes = [0u8; 19];
        fast_format_ts_19digits_simd(now_nanos, &mut ts_bytes);

        let mut prefix_first = [0u8; 47];
        prefix_first[..17].copy_from_slice(b"{\"timeUnixNano\":\"");
        prefix_first[17..36].copy_from_slice(&ts_bytes);
        prefix_first[36..47].copy_from_slice(b"\",\"asInt\":\"");

        let mut prefix_next = [0u8; 48];
        prefix_next[0] = b',';
        prefix_next[1..18].copy_from_slice(b"{\"timeUnixNano\":\"");
        prefix_next[18..37].copy_from_slice(&ts_bytes);
        prefix_next[37..48].copy_from_slice(b"\",\"asInt\":\"");

        // 1. Root Header & Metric 1 Header
        buf.extend_from_slice(&self.root_header);

        // Metric 1: velda.requests.total (sparse active status bitmask entries)
        let mut first = true;
        for p in &snapshot.pipelines {
            let base = (p.pipe as usize) * InternalStatus::COUNT;
            let mut mask = p.status_mask;
            while mask != 0 {
                let status_idx = mask.trailing_zeros() as usize;
                mask &= mask - 1;
                if first {
                    buf.extend_from_slice(&prefix_first);
                    first = false;
                } else {
                    buf.extend_from_slice(&prefix_next);
                }
                fast_write_u64(buf, p.status_counts[status_idx]);
                buf.extend_from_slice(&self.req_tails[base + status_idx]);
            }
        }

        // 2. Metric 2: velda.active_streams
        buf.extend_from_slice(METRIC1_TO_METRIC2);
        first = true;
        for (p, tail) in snapshot.pipelines.iter().zip(&self.active_tails) {
            if first {
                buf.extend_from_slice(&prefix_first);
                first = false;
            } else {
                buf.extend_from_slice(&prefix_next);
            }
            fast_write_i64(buf, p.active_streams);
            buf.extend_from_slice(tail);
        }

        // 3. Metric 3: velda.bytes_received_total
        let rx_sep = match self.temporality {
            OtlpTemporality::Delta => METRIC2_TO_METRIC3_DELTA,
            OtlpTemporality::Cumulative => METRIC2_TO_METRIC3_CUMULATIVE,
        };
        buf.extend_from_slice(rx_sep);
        first = true;
        for (p, tail) in snapshot.pipelines.iter().zip(&self.rx_tails) {
            if first {
                buf.extend_from_slice(&prefix_first);
                first = false;
            } else {
                buf.extend_from_slice(&prefix_next);
            }
            fast_write_u64(buf, p.bytes_rx);
            buf.extend_from_slice(tail);
        }

        // 4. Metric 4: velda.bytes_transmitted_total
        let tx_sep = match self.temporality {
            OtlpTemporality::Delta => METRIC3_TO_METRIC4_DELTA,
            OtlpTemporality::Cumulative => METRIC3_TO_METRIC4_CUMULATIVE,
        };
        buf.extend_from_slice(tx_sep);
        first = true;
        for (p, tail) in snapshot.pipelines.iter().zip(&self.tx_tails) {
            if first {
                buf.extend_from_slice(&prefix_first);
                first = false;
            } else {
                buf.extend_from_slice(&prefix_next);
            }
            fast_write_u64(buf, p.bytes_tx);
            buf.extend_from_slice(tail);
        }

        // 5. Close JSON Envelope
        buf.extend_from_slice(ROOT_FOOTER);
    }

    /// Formats a snapshot into a newly allocated UTF-8 [`String`].
    pub fn format(&self, snapshot: &MetricsSnapshot) -> String {
        let mut buf = Vec::with_capacity(64 * 1024);
        self.format_into(snapshot, &mut buf);
        // SAFETY: Output is strictly valid ASCII JSON.
        unsafe { String::from_utf8_unchecked(buf) }
    }
}

thread_local! {
    static CACHED_JSON_TEMPLATE: std::cell::RefCell<Option<(String, OtlpTemporality, OtlpJsonTemplate)>> =
        const { std::cell::RefCell::new(None) };
}

/// Encodes a [`MetricsSnapshot`] directly into `buf` without intermediate allocations.
pub fn format_otlp_metrics_into(
    snapshot: &MetricsSnapshot,
    service_name: &str,
    temporality: OtlpTemporality,
    buf: &mut Vec<u8>,
) {
    CACHED_JSON_TEMPLATE.with(|cell| {
        let mut borrow = cell.borrow_mut();
        if borrow.as_ref().map(|(s, t, _)| (s.as_str(), *t)) != Some((service_name, temporality)) {
            *borrow = Some((
                service_name.to_string(),
                temporality,
                OtlpJsonTemplate::new(service_name, temporality),
            ));
        }
        borrow.as_ref().unwrap().2.format_into(snapshot, buf);
    });
}

/// Encodes a [`MetricsSnapshot`] into compliant OpenTelemetry ResourceMetrics JSON.
pub fn format_otlp_metrics_json(
    snapshot: &MetricsSnapshot,
    service_name: &str,
    temporality: OtlpTemporality,
) -> String {
    let mut buf = Vec::with_capacity(64 * 1024);
    format_otlp_metrics_into(snapshot, service_name, temporality, &mut buf);
    // SAFETY: Output is strictly valid ASCII JSON.
    unsafe { String::from_utf8_unchecked(buf) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use velda_observability::{MetricsStore, StreamCompletion};

    #[test]
    fn test_format_otlp_metrics_json() {
        let store = Arc::new(MetricsStore::new());
        store.record_stream_completion_on_core(
            0,
            PipeKind::BufferedH1H1,
            StreamCompletion::new(InternalStatus::Completed, 42, 84, 1_000_000, 200_000),
        );
        let snapshot = store.snapshot();
        let json_str = format_otlp_metrics_json(&snapshot, "test-service", OtlpTemporality::Delta);

        let parsed: serde_json::Value = serde_json::from_str(&json_str).expect("Valid JSON");
        assert!(parsed["resourceMetrics"].is_array());
        assert_eq!(
            parsed["resourceMetrics"][0]["resource"]["attributes"][0]["value"]["stringValue"],
            "test-service"
        );

        let official: opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest =
            serde_json::from_str(&json_str)
                .expect("OTLP JSON metrics must deserialize into official opentelemetry-proto schema");
        let off_metrics = &official.resource_metrics[0].scope_metrics[0].metrics;
        assert_eq!(off_metrics.len(), 4);
        if let Some(opentelemetry_proto::tonic::metrics::v1::metric::Data::Sum(sum)) =
            &off_metrics[0].data
        {
            assert_eq!(sum.aggregation_temporality, 1);
            assert_eq!(sum.data_points.len(), 1);
            assert_eq!(sum.data_points[0].attributes[3].key, "internal_status");
        } else {
            panic!("Expected Sum metric");
        }
    }

    #[test]
    fn test_otlp_json_template_reuse() {
        let store = Arc::new(MetricsStore::new());
        let snapshot = store.snapshot();
        let template = OtlpJsonTemplate::new("reuse-service", OtlpTemporality::Cumulative);

        let mut buf1 = Vec::new();
        template.format_into(&snapshot, &mut buf1);
        assert!(!buf1.is_empty());

        let mut buf2 = Vec::new();
        template.format_into(&snapshot, &mut buf2);
        assert_eq!(buf1.len(), buf2.len());
    }
}
