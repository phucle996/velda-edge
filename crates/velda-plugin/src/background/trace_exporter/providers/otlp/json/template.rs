//! JSON Formatting Template for Unified Trace Span Batches (`otlp_trace_exporter::json::template`).

use velda_observability::{TelemetryBatch, TelemetryRecord};

pub struct OtlpTraceJsonEnvelope {
    service_name: String,
}

impl OtlpTraceJsonEnvelope {
    pub fn new(service_name: impl Into<String>) -> Self {
        Self {
            service_name: service_name.into(),
        }
    }

    /// Formats batches of unified [`TelemetryRecord`] into an OTLP-compliant JSON string.
    pub fn format_spans_json(&self, batches: &[TelemetryBatch], buf: &mut String) -> usize {
        buf.clear();
        let mut total_spans = 0;
        for b in batches {
            total_spans += b.len();
        }

        if total_spans == 0 {
            return 0;
        }

        let mut itoa_buf = itoa::Buffer::new();

        buf.push_str("{\"resourceSpans\":[{\"resource\":{\"attributes\":[{\"key\":\"service.name\",\"value\":{\"stringValue\":\"");
        buf.push_str(&self.service_name);
        buf.push_str(
            "\"}}]},\"scopeSpans\":[{\"scope\":{\"name\":\"velda.edge.router\",\"version\":\"",
        );
        buf.push_str(env!("CARGO_PKG_VERSION"));
        buf.push_str("\"},\"spans\":[");

        let mut first = true;
        for batch in batches {
            for record in batch.iter() {
                if !first {
                    buf.push(',');
                }
                first = false;

                Self::format_single_span_json(record, buf, &mut itoa_buf);
            }
        }

        buf.push_str("]}]}]}");
        total_spans
    }

    fn format_single_span_json(
        record: &TelemetryRecord,
        buf: &mut String,
        itoa_buf: &mut itoa::Buffer,
    ) {
        const HEX_CHARS: &[u8; 16] = b"0123456789abcdef";

        let trace_id = record.trace_id_bytes();
        let mut trace_hex = [0u8; 32];
        for (i, &b) in trace_id.iter().enumerate() {
            trace_hex[i * 2] = HEX_CHARS[(b >> 4) as usize];
            trace_hex[i * 2 + 1] = HEX_CHARS[(b & 0x0f) as usize];
        }
        let trace_str = unsafe { std::str::from_utf8_unchecked(&trace_hex) };

        let span_id = record.span_id_bytes();
        let mut span_hex = [0u8; 16];
        for (i, &b) in span_id.iter().enumerate() {
            span_hex[i * 2] = HEX_CHARS[(b >> 4) as usize];
            span_hex[i * 2 + 1] = HEX_CHARS[(b & 0x0f) as usize];
        }
        let span_str = unsafe { std::str::from_utf8_unchecked(&span_hex) };

        buf.push_str("{\"traceId\":\"");
        buf.push_str(trace_str);
        buf.push_str("\",\"spanId\":\"");
        buf.push_str(span_str);
        buf.push('"');

        buf.push_str(",\"name\":\"");
        buf.push_str(record.method.as_str());
        buf.push(' ');
        velda_router::with_route_bytes(record.route_id, |route_bytes| {
            if let Ok(p) = std::str::from_utf8(route_bytes) {
                buf.push_str(p);
            } else {
                buf.push_str("unmatched");
            }
        });
        let end_time_unix_nano = record.ingress_time_nanos.saturating_add(record.duration_ns);
        buf.push_str("\",\"kind\":2,\"startTimeUnixNano\":");
        buf.push_str(itoa_buf.format(record.ingress_time_nanos));
        buf.push_str(",\"endTimeUnixNano\":");
        buf.push_str(itoa_buf.format(end_time_unix_nano));
        buf.push_str(",\"status\":{\"code\":");
        buf.push_str(if record.internal_status.is_success() {
            "1"
        } else {
            "2"
        });
        buf.push_str(
            "},\"attributes\":[{\"key\":\"velda.internal_status\",\"value\":{\"stringValue\":\"",
        );
        buf.push_str(record.internal_status.as_str());
        buf.push_str("\"}},{\"key\":\"http.method\",\"value\":{\"stringValue\":\"");
        buf.push_str(record.method.as_str());
        buf.push_str("\"}},{\"key\":\"http.target\",\"value\":{\"stringValue\":\"");
        velda_router::with_route_bytes(record.route_id, |route_bytes| {
            if let Ok(p) = std::str::from_utf8(route_bytes) {
                buf.push_str(p);
            } else {
                buf.push_str("unmatched");
            }
        });
        buf.push_str("\"}}");

        if record.route_id != velda_core::UNMATCHED_ROUTE_ID {
            buf.push_str(",{\"key\":\"velda.route_id\",\"value\":{\"intValue\":");
            buf.push_str(itoa_buf.format(record.route_id));
            buf.push_str("}}");
        }

        if record.upstream_id != velda_core::UNASSIGNED_UPSTREAM_ID {
            buf.push_str(",{\"key\":\"net.peer.name\",\"value\":{\"stringValue\":\"");
            velda_upstream::with_upstream_bytes(record.upstream_id, |up_bytes| {
                if let Ok(up) = std::str::from_utf8(up_bytes) {
                    buf.push_str(up);
                } else {
                    buf.push_str("unmatched");
                }
            });
            buf.push_str("\"}}");
        }

        if record.peer_ip != 0 {
            buf.push_str(",{\"key\":\"net.peer.ip\",\"value\":{\"stringValue\":\"");
            let (bytes, len) = velda_core::lut::ip::format_ip_bytes(record.peer_ip);
            buf.push_str(unsafe { std::str::from_utf8_unchecked(&bytes[..len]) });
            buf.push_str("\"}}");
        }

        buf.push_str("]}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn json_encode() {
        let envelope = OtlpTraceJsonEnvelope::new("order-gateway");
        let mut rec = TelemetryRecord::zeroed();
        rec.ingress_time_nanos = 0x0102_0304_0506_0708;
        rec.duration_ns = 15_000_000;
        rec.request_id = 0x090a_0b0c_0d0e_0f10;
        rec.internal_status = velda_core::InternalStatus::Completed;
        rec.route_id = velda_core::UNMATCHED_ROUTE_ID;
        rec.method = velda_core::HttpMethod::Get;
        rec.peer_ip = velda_core::lut::ip::encode_ipv4_u128(u32::from_be_bytes([192, 168, 1, 10]));

        let batch = TelemetryBatch::from_arc(Arc::from([rec]));
        let mut json_buf = String::new();
        let total = envelope.format_spans_json(&[batch], &mut json_buf);

        assert_eq!(total, 1);
        let parsed: serde_json::Value =
            serde_json::from_str(&json_buf).expect("Valid OTLP JSON output");
        let s = &parsed["resourceSpans"][0]["scopeSpans"][0]["spans"][0];
        assert_eq!(s["name"], "GET unmatched");
        assert_eq!(s["traceId"], "0102030405060708090a0b0c0d0e0f10");
        assert_eq!(s["spanId"], "090a0b0c0d0e0f10");
        assert_eq!(s["kind"], 2);
        assert_eq!(s["attributes"][0]["key"], "velda.internal_status");
        assert_eq!(s["attributes"][0]["value"]["stringValue"], "completed");
    }
}
