//! Direct Protobuf Wire Serializer (`otlp_trace_exporter::proto::template`).
//!
//! ```text
//!  SpanRecord (64B) ──► encode_single_span_wire() ──► ExportTraceServiceRequest Wire Stream
//!                            ├── trace_id (16B) / span_id (8B)
//!                            ├── name ("<method> <route_bytes>") ◄── RouteCatalog &[u8]
//!                            ├── timestamps (fixed64)
//!                            └── attributes ◄───────────────────── UpstreamCatalog &[u8]
//! ```

use std::sync::Arc;
use velda_observability::SpanRecord;

/// Fast inline varint encoding for u64.
#[inline(always)]
pub fn write_varint(buf: &mut Vec<u8>, mut val: u64) {
    while val >= 0x80 {
        buf.push((val as u8 & 0x7F) | 0x80);
        val >>= 7;
    }
    buf.push(val as u8);
}

/// Computes length in bytes of a varint without writing it.
#[inline(always)]
pub const fn varint_len(mut val: u64) -> usize {
    let mut len = 1;
    while val >= 0x80 {
        len += 1;
        val >>= 7;
    }
    len
}

/// Serializes a `KeyValue { key, value: AnyValue::StringValue(val) }` as Span attribute tag 9.
#[inline(always)]
fn write_str_attribute(buf: &mut Vec<u8>, key: &[u8], val: &[u8]) {
    let str_val_len = 1 + varint_len(val.len() as u64) + val.len();
    let kv_len = 1
        + varint_len(key.len() as u64)
        + key.len()
        + 1
        + varint_len(str_val_len as u64)
        + str_val_len;

    buf.push(0x4A); // Span.attributes tag 9 -> (9 << 3) | 2 = 0x4A
    write_varint(buf, kv_len as u64);

    buf.push(0x0A); // KeyValue.key tag 1
    write_varint(buf, key.len() as u64);
    buf.extend_from_slice(key);

    buf.push(0x12); // KeyValue.value tag 2
    write_varint(buf, str_val_len as u64);
    buf.push(0x0A); // AnyValue.string_value tag 1
    write_varint(buf, val.len() as u64);
    buf.extend_from_slice(val);
}

/// Serializes a `KeyValue { key, value: AnyValue::IntValue(val) }` as Span attribute tag 9.
#[inline(always)]
fn write_int_attribute(buf: &mut Vec<u8>, key: &[u8], val: i64) {
    let val_u64 = val as u64;
    let int_val_len = 1 + varint_len(val_u64);
    let kv_len = 1
        + varint_len(key.len() as u64)
        + key.len()
        + 1
        + varint_len(int_val_len as u64)
        + int_val_len;

    buf.push(0x4A); // Span.attributes tag 9
    write_varint(buf, kv_len as u64);

    buf.push(0x0A); // KeyValue.key tag 1
    write_varint(buf, key.len() as u64);
    buf.extend_from_slice(key);

    buf.push(0x12); // KeyValue.value tag 2
    write_varint(buf, int_val_len as u64);
    buf.push(0x18); // AnyValue.int_value tag 3
    write_varint(buf, val_u64);
}

/// Stack-based IPv4 octets to ASCII dotted-decimal formatter.
#[inline(always)]
fn format_ipv4(octets: &[u8; 4], buf: &mut [u8; 16]) -> usize {
    let mut idx = 0;
    for (i, &oct) in octets.iter().enumerate() {
        if i > 0 {
            buf[idx] = b'.';
            idx += 1;
        }
        if oct < 10 {
            buf[idx] = b'0' + oct;
            idx += 1;
        } else if oct < 100 {
            buf[idx] = b'0' + (oct / 10);
            buf[idx + 1] = b'0' + (oct % 10);
            idx += 2;
        } else {
            buf[idx] = b'0' + (oct / 100);
            buf[idx + 1] = b'0' + ((oct / 10) % 10);
            buf[idx + 2] = b'0' + (oct % 10);
            idx += 3;
        }
    }
    idx
}

pub struct OtlpTraceProtoEnvelope {
    resource_wire: Vec<u8>,
    scope_wire: Vec<u8>,
    spans_wire: Vec<u8>,
    single_span: Vec<u8>,
}

impl OtlpTraceProtoEnvelope {
    /// Pre-bakes static Resource (`service.name`) and InstrumentationScope wire fragments.
    pub fn new(service_name: impl Into<String>) -> Self {
        let service_bytes = service_name.into().into_bytes();

        let mut kv = Vec::new();
        kv.push(0x0A);
        write_varint(&mut kv, b"service.name".len() as u64);
        kv.extend_from_slice(b"service.name");

        let mut any_val = Vec::new();
        any_val.push(0x0A);
        write_varint(&mut any_val, service_bytes.len() as u64);
        any_val.extend_from_slice(&service_bytes);

        kv.push(0x12);
        write_varint(&mut kv, any_val.len() as u64);
        kv.extend_from_slice(&any_val);

        let mut resource_wire = Vec::new();
        resource_wire.push(0x0A);
        write_varint(&mut resource_wire, kv.len() as u64);
        resource_wire.extend_from_slice(&kv);

        let mut scope_wire = Vec::new();
        scope_wire.push(0x0A);
        write_varint(&mut scope_wire, b"velda.edge.router".len() as u64);
        scope_wire.extend_from_slice(b"velda.edge.router");

        let version_bytes = env!("CARGO_PKG_VERSION").as_bytes();
        scope_wire.push(0x12);
        write_varint(&mut scope_wire, version_bytes.len() as u64);
        scope_wire.extend_from_slice(version_bytes);

        Self {
            resource_wire,
            scope_wire,
            spans_wire: Vec::with_capacity(16 * 1024),
            single_span: Vec::with_capacity(256),
        }
    }

    /// Serializes batches of [`SpanRecord`] into an OTLP `ExportTraceServiceRequest` protobuf byte buffer.
    pub fn format_spans_wire(
        &mut self,
        batches: &[Arc<[SpanRecord]>],
        out_buf: &mut Vec<u8>,
    ) -> usize {
        let mut total_spans = 0;
        for b in batches {
            total_spans += b.len();
        }

        if total_spans == 0 {
            out_buf.clear();
            return 0;
        }

        self.spans_wire.clear();
        for batch in batches {
            for record in batch.iter() {
                self.single_span.clear();
                Self::encode_single_span_wire(record, &mut self.single_span);

                // ScopeSpans.spans is tag 2 -> 0x12
                self.spans_wire.push(0x12);
                write_varint(&mut self.spans_wire, self.single_span.len() as u64);
                self.spans_wire.extend_from_slice(&self.single_span);
            }
        }

        let scope_part_len = 1 + varint_len(self.scope_wire.len() as u64) + self.scope_wire.len();
        let scope_spans_len = scope_part_len + self.spans_wire.len();

        let res_part_len =
            1 + varint_len(self.resource_wire.len() as u64) + self.resource_wire.len();
        let scope_spans_part_len = 1 + varint_len(scope_spans_len as u64) + scope_spans_len;
        let res_spans_len = res_part_len + scope_spans_part_len;

        let total_req_len = 1 + varint_len(res_spans_len as u64) + res_spans_len;

        out_buf.clear();
        out_buf.reserve(total_req_len);

        out_buf.push(0x0A); // ExportTraceServiceRequest.resource_spans tag 1
        write_varint(out_buf, res_spans_len as u64);

        out_buf.push(0x0A); // ResourceSpans.resource tag 1
        write_varint(out_buf, self.resource_wire.len() as u64);
        out_buf.extend_from_slice(&self.resource_wire);

        out_buf.push(0x12); // ResourceSpans.scope_spans tag 2
        write_varint(out_buf, scope_spans_len as u64);

        out_buf.push(0x0A); // ScopeSpans.scope tag 1
        write_varint(out_buf, self.scope_wire.len() as u64);
        out_buf.extend_from_slice(&self.scope_wire);

        out_buf.extend_from_slice(&self.spans_wire);

        total_spans
    }

    #[inline]
    fn encode_single_span_wire(record: &SpanRecord, buf: &mut Vec<u8>) {
        // Tag 1: trace_id (16B)
        buf.push(0x0A);
        buf.push(16);
        buf.extend_from_slice(&record.trace_id);

        // Tag 2: span_id (8B)
        buf.push(0x12);
        buf.push(8);
        buf.extend_from_slice(&record.span_id);

        // Tag 5: name = "<method> <route>"
        let method_bytes = record.method_str().as_bytes();
        velda_router::with_route_bytes(record.route_id, |route_bytes| {
            let name_len = method_bytes.len() + 1 + route_bytes.len();
            buf.push(0x2A);
            write_varint(buf, name_len as u64);
            buf.extend_from_slice(method_bytes);
            buf.push(b' ');
            buf.extend_from_slice(route_bytes);
        });

        // Tag 6: kind
        buf.push(0x30);
        write_varint(buf, record.span_kind as u64);

        // Tag 7: start_time_unix_nano (fixed64)
        buf.push(0x39);
        buf.extend_from_slice(&record.start_time_unix_nano.to_le_bytes());

        // Tag 8: end_time_unix_nano (fixed64)
        buf.push(0x41);
        buf.extend_from_slice(&record.end_time_unix_nano.to_le_bytes());

        // Tag 9: attributes
        write_int_attribute(buf, b"http.status_code", record.status_code as i64);
        write_str_attribute(buf, b"http.method", method_bytes);

        velda_router::with_route_bytes(record.route_id, |route_bytes| {
            write_str_attribute(buf, b"http.target", route_bytes);
        });

        if record.route_id != velda_core::UNMATCHED_ROUTE_ID {
            write_int_attribute(buf, b"velda.route_id", record.route_id as i64);
        }

        if record.upstream_id != velda_core::UNASSIGNED_UPSTREAM_ID {
            velda_upstream::with_upstream_bytes(record.upstream_id, |up_bytes| {
                write_str_attribute(buf, b"net.peer.name", up_bytes);
            });
        }

        if record.ip_kind != 0 {
            let mut ip_buf = [0u8; 16];
            let ip_len = format_ipv4(&record.client_ip, &mut ip_buf);
            write_str_attribute(buf, b"net.peer.ip", &ip_buf[..ip_len]);
        }

        // Tag 15: status
        let status_code = record.otlp_status as u64;
        let status_len = 1 + varint_len(status_code);
        buf.push(0x7A);
        write_varint(buf, status_len as u64);
        buf.push(0x18);
        write_varint(buf, status_code);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_encode_decode() {
        let route_cat = velda_router::RouteCatalog::build(["/api/health", "/api/orders"]);
        velda_router::set_global_route_catalog(route_cat);
        let up_cat = velda_upstream::UpstreamCatalog::build(["orders-cluster"]);
        velda_upstream::set_global_upstream_catalog(up_cat);

        let mut envelope = OtlpTraceProtoEnvelope::new("order-gateway");
        let mut span = SpanRecord::empty();
        span.start_time_unix_nano = 1_700_000_000_000_000_000;
        span.end_time_unix_nano = 1_700_000_000_015_000_000;
        span.status_code = 200;
        span.otlp_status = 1;
        span.route_id = 0; // first route: "/api/health"
        span.upstream_id = 0; // first upstream: "orders-cluster"
        span.method = 1; // GET
        span.span_kind = 2; // SPAN_KIND_SERVER
        span.ip_kind = 4;
        span.client_ip = [10, 20, 30, 40];
        span.trace_id = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
        span.span_id = [10, 20, 30, 40, 50, 60, 70, 80];

        let batch: Arc<[SpanRecord]> = Arc::from([span]);
        let mut out_buf = Vec::new();
        let total = envelope.format_spans_wire(&[batch], &mut out_buf);

        assert_eq!(total, 1);
        assert!(!out_buf.is_empty());

        let official = <opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest as prost_otlp::Message>::decode(&out_buf[..])
            .expect("Official opentelemetry-proto decode wire buffer");

        assert_eq!(official.resource_spans.len(), 1);
        let res_spans = &official.resource_spans[0];
        let resource = res_spans.resource.as_ref().unwrap();
        assert_eq!(resource.attributes[0].key, "service.name");

        let scope_spans = &res_spans.scope_spans[0];
        let scope = scope_spans.scope.as_ref().unwrap();
        assert_eq!(scope.name, "velda.edge.router");

        let os = &scope_spans.spans[0];
        assert_eq!(
            os.trace_id,
            &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
        );
        assert_eq!(os.span_id, &[10, 20, 30, 40, 50, 60, 70, 80]);
        assert_eq!(os.name, "GET /api/health");
        assert_eq!(os.kind, 2);
        assert_eq!(os.start_time_unix_nano, 1_700_000_000_000_000_000);
        assert_eq!(os.end_time_unix_nano, 1_700_000_000_015_000_000);
        assert_eq!(os.status.as_ref().unwrap().code, 1);

        let keys: Vec<&str> = os.attributes.iter().map(|kv| kv.key.as_str()).collect();
        assert!(keys.contains(&"http.status_code"));
        assert!(keys.contains(&"http.method"));
        assert!(keys.contains(&"http.target"));
        assert!(keys.contains(&"velda.route_id"));
        assert!(keys.contains(&"net.peer.name"));
        assert!(keys.contains(&"net.peer.ip"));
    }
}
