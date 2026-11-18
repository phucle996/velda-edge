//! JSON Formatting Template for Span Batches (`otlp_trace_exporter::json::template`).

use std::sync::Arc;
use velda_observability::SpanRecord;

pub struct OtlpTraceJsonEnvelope {
    service_name: String,
}

impl OtlpTraceJsonEnvelope {
    pub fn new(service_name: impl Into<String>) -> Self {
        Self {
            service_name: service_name.into(),
        }
    }

    /// Formats batches of [`SpanRecord`] into an OTLP-compliant JSON string.
    pub fn format_spans_json(&self, batches: &[Arc<[SpanRecord]>], buf: &mut String) -> usize {
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

    fn format_single_span_json(record: &SpanRecord, buf: &mut String, itoa_buf: &mut itoa::Buffer) {
        let mut trace_hex = [0u8; 32];
        record.format_trace_id(&mut trace_hex);
        let trace_str = unsafe { std::str::from_utf8_unchecked(&trace_hex) };

        let mut span_hex = [0u8; 16];
        record.format_span_id(&mut span_hex);
        let span_str = unsafe { std::str::from_utf8_unchecked(&span_hex) };

        buf.push_str("{\"traceId\":\"");
        buf.push_str(trace_str);
        buf.push_str("\",\"spanId\":\"");
        buf.push_str(span_str);
        buf.push('"');

        buf.push_str(",\"name\":\"");
        buf.push_str(record.method_str());
        buf.push(' ');
        velda_router::with_route_bytes(record.route_id, |route_bytes| {
            if let Ok(p) = std::str::from_utf8(route_bytes) {
                buf.push_str(p);
            } else {
                buf.push_str("unmatched");
            }
        });
        buf.push_str("\",\"kind\":2,\"startTimeUnixNano\":");
        buf.push_str(itoa_buf.format(record.start_time_unix_nano));
        buf.push_str(",\"endTimeUnixNano\":");
        buf.push_str(itoa_buf.format(record.end_time_unix_nano));
        buf.push_str(",\"status\":{\"code\":");
        buf.push_str(itoa_buf.format(record.otlp_status));
        buf.push_str("},\"attributes\":[{\"key\":\"http.status_code\",\"value\":{\"intValue\":");
        buf.push_str(itoa_buf.format(record.status_code));
        buf.push_str("}},{\"key\":\"http.method\",\"value\":{\"stringValue\":\"");
        buf.push_str(record.method_str());
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

        if record.ip_kind != 0 {
            buf.push_str(",{\"key\":\"net.peer.ip\",\"value\":{\"stringValue\":\"");
            record.format_client_ip(buf);
            buf.push_str("\"}}");
        }

        buf.push_str("]}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_encode() {
        let envelope = OtlpTraceJsonEnvelope::new("order-gateway");
        let mut span = SpanRecord::empty();
        span.start_time_unix_nano = 1_700_000_000_000_000_000;
        span.end_time_unix_nano = 1_700_000_000_015_000_000;
        span.status_code = 200;
        span.otlp_status = 1;
        span.route_id = velda_core::UNMATCHED_ROUTE_ID;
        span.method = 1; // GET
        span.span_kind = 2; // SERVER
        span.ip_kind = 4;
        span.client_ip = [192, 168, 1, 10];
        span.trace_id = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
        span.span_id = [10, 20, 30, 40, 50, 60, 70, 80];

        let batch: Arc<[SpanRecord]> = Arc::from([span]);
        let mut json_buf = String::new();
        let total = envelope.format_spans_json(&[batch], &mut json_buf);

        assert_eq!(total, 1);
        let parsed: serde_json::Value =
            serde_json::from_str(&json_buf).expect("Valid OTLP JSON output");
        let s = &parsed["resourceSpans"][0]["scopeSpans"][0]["spans"][0];
        assert_eq!(s["name"], "GET unmatched");
        assert_eq!(s["traceId"], "0102030405060708090a0b0c0d0e0f10");
        assert_eq!(s["spanId"], "0a141e28323c4650");
        assert_eq!(s["kind"], 2);
        assert_eq!(s["attributes"][0]["key"], "http.status_code");
        assert_eq!(s["attributes"][0]["value"]["intValue"], 200);
    }
}
