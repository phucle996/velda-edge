//! Direct Protobuf Wire Format Serializer (`otlp_log_shipper::proto::template`).
//!
//! ```text
//! ┌────────────────────────────────────────────────────────────────────────────────────────┐
//! │                OTLP PROTOBUF PRE-BAKED WIRE SKELETON ARCHITECTURE                      │
//! │                                                                                        │
//! │               TelemetryRecord (64-byte integer scalar from SPSC Ring)                  │
//! │             ┌─────────────────────────────────────────────────────────┐                │
//! │             │ timestamp_sec: u32, duration_us: u32, peer_ip: u128 ... │                │
//! │             └────────────────────────────┬────────────────────────────┘                │
//! │                                          │                                             │
//! │        ┌─────────────────────────────────┼─────────────────────────────────┐           │
//! │        ▼                                 ▼                                 ▼           │
//! │ [ Dual-Timestamp SIMD Store ]  [ Pre-Baked Wire Skeleton ]   [ 32-Hex Request ID SIMD ]│
//! │   ts_nano = ts_sec * 10^9        Tags 3..5: Sev + Body(19B)    (ts_sec << 64) | entropy│
//! │            │                     "HTTP access log" static                  │           │
//! │            ▼                     + Request ID Prefix (18B)                 ▼           │
//! │   _mm_set1_epi64x broadcast                  │                     _mm_shuffle_epi8    │
//! │ ┌───────────────────────────┐                ▼                     ┌─────────────────┐ │
//! │ │ Tag 1: time_unix_nano (8B)│     SKELETON_INFO/WARN/ERROR (45-46B)│ 32-Byte Hex     │ │
//! │ │ Tag 2: obs_time_nano (8B) │     Vectorized Single Copy Splicing  │ Stamped In-place│ │
//! │ └───────────────────────────┘                │                     └─────────────────┘ │
//! │        │                                     │                             │           │
//! │        └─────────────────────────────────────┼─────────────────────────────┘           │
//! │                                              ▼                                         │
//! │          Zero-Copy Wire Stream (Zero Prost AST, Zero Dynamic Body String)              │
//! │ ┌────────┬──────────────┬──────────┬──────────────────────────┬──────────────────────┐ │
//! │ │  0x12  │ 2-byte varint│ Tags 1&2 │ SKELETON: Sev + Body +   │ Tag 6: 13 KeyValues  │ │
//! │ │ (tag 2)│ [backpatch]  │ fixed64  │ ReqId Prefix + 32-hex    │ Fast LUT / Varint    │ │
//! │ └────────┴──────────────┴──────────┴──────────────────────────┴──────────────────────┘ │
//! └────────────────────────────────────────────────────────────────────────────────────────┘
//! ```

use velda_observability::{TelemetryBatch, TelemetryRecord};

/// Canonical OpenTelemetry Schema URL byte constant (zero heap allocation).
pub const OTLP_SCHEMA_URL: &[u8] = b"https://opentelemetry.io/schemas/1.26.0";

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

/// Helper to serialize a Protobuf StringValue attribute:
/// `KeyValue { key, value: Some(AnyValue { value: Some(StringValue(s)) }) }`
#[inline(always)]
fn write_str_attribute(buf: &mut Vec<u8>, field_tag: u8, key_wire_bytes: &[u8], s: &[u8]) {
    let str_val_len = 1 + varint_len(s.len() as u64) + s.len(); // tag 0x0A + len + bytes
    let any_val_len = str_val_len;
    let kv_len = key_wire_bytes.len() + 1 + varint_len(any_val_len as u64) + any_val_len;

    buf.push(field_tag); // 0x0A for Resource.attributes (field 1), 0x32 for LogRecord.attributes (field 6)
    write_varint(buf, kv_len as u64);
    buf.extend_from_slice(key_wire_bytes);
    buf.push(0x12); // KeyValue field 2
    write_varint(buf, any_val_len as u64);
    buf.push(0x0A); // AnyValue field 1 (string_value)
    write_varint(buf, s.len() as u64);
    buf.extend_from_slice(s);
}

// Pre-baked wire skeletons combining canonical opentelemetry.proto.logs.v1.LogRecord tags:
// - Tag 2 (0x10): severity_number (enum/varint)
// - Tag 3 (0x1A): severity_text (string)
// - Tag 5 (0x2A): body (static "HTTP access log" AnyValue message)
// - Tag 6 (0x32): repeated KeyValue attribute 0 ("request_id" key + AnyValue stringValue prefix)
//
// Lengths:
// Body message: Tag 5 (0x2A), len 17 (0x11), Tag 1 (0x0A), len 15 (0x0F), "HTTP access log" -> 19 bytes.
// Request ID prefix: Tag 6 (0x32), len 48 (0x30), Tag 1 (0x0A), len 10 (0x0A), "request_id", Tag 2 (0x12), len 34 (0x22), Tag 1 (0x0A), len 32 (0x20) -> 18 bytes.
// Combined Body + ReqId prefix = 19 + 18 = 37 bytes.
//
// INFO: Tag 2 (0x10, 0x09), Tag 3 (0x1A, 0x04, "INFO") -> 8 bytes. Total = 8 + 37 = 45 bytes.
// WARN: Tag 2 (0x10, 0x0D), Tag 3 (0x1A, 0x04, "WARN") -> 8 bytes. Total = 8 + 37 = 45 bytes.
// ERROR: Tag 2 (0x10, 0x11), Tag 3 (0x1A, 0x05, "ERROR") -> 9 bytes. Total = 9 + 37 = 46 bytes.
const SKELETON_INFO: &[u8] = b"\x10\x09\x1a\x04INFO\x2a\x11\x0a\x0fHTTP access log\x32\x30\x0a\x0arequest_id\x12\x22\x0a\x20";
const SKELETON_WARN: &[u8] = b"\x10\x0d\x1a\x04WARN\x2a\x11\x0a\x0fHTTP access log\x32\x30\x0a\x0arequest_id\x12\x22\x0a\x20";
const SKELETON_ERROR: &[u8] = b"\x10\x11\x1a\x05ERROR\x2a\x11\x0a\x0fHTTP access log\x32\x30\x0a\x0arequest_id\x12\x22\x0a\x20";

// Pre-baked Protobuf IntValue attribute prefixes:
// Format: [0x32, kv_len_placeholder, 0x0A, key_len, <key>, 0x12, any_val_len_placeholder, 0x18]
const PRE_DURATION_NS: &[u8] = b"\x32\x00\x0a\x0bduration_ns\x12\x00\x18";
const PRE_RESP_BODY_SIZE: &[u8] = b"\x32\x00\x0a\x17http.response.body.size\x12\x00\x18";
const PRE_REQ_BODY_SIZE: &[u8] = b"\x32\x00\x0a\x16http.request.body.size\x12\x00\x18";
const PRE_ROUTE_ID: &[u8] = b"\x32\x00\x0a\x0evelda.route_id\x12\x00\x18";
const PRE_UA_MAJOR: &[u8] = b"\x32\x00\x0a\x18user_agent.major_version\x12\x00\x18";

// Pre-baked Protobuf StringValue attribute prefix for client.address:
// Format: [0x32, kv_len_placeholder, 0x0A, 0x0E, "client.address", 0x12, any_val_len_placeholder, 0x0A, str_len_placeholder]
const PRE_CLIENT_ADDR: &[u8] = b"\x32\x00\x0a\x0eclient.address\x12\x00\x0a\x00";

/// Upper bound of one raw-stamped LogRecord (worst-case enum names, IPv6, 184B URL, 10-byte varints).
/// Verified by `test_max_log_record_fits_reserve`; the 2-byte length backpatch also caps it at 16 KiB.
const MAX_LOG_RECORD_BYTES: usize = 1024;

#[inline(always)]
unsafe fn write_varint_ptr(mut p: *mut u8, mut v: usize) -> *mut u8 {
    unsafe {
        while v >= 0x80 {
            *p = (v as u8 & 0x7F) | 0x80;
            p = p.add(1);
            v >>= 7;
        }
        *p = v as u8;
        p.add(1)
    }
}

/// Ultra-fast raw pointer stamping of a Protobuf string KeyValue attribute directly into output stream.
#[inline(always)]
unsafe fn stamp_str_attribute(p: *mut u8, key_wire_bytes: &[u8], s: &[u8]) -> *mut u8 {
    unsafe {
        let s_len = s.len();
        let any_val_len = 1 + varint_len(s_len as u64) + s_len;
        let kv_len = key_wire_bytes.len() + 1 + varint_len(any_val_len as u64) + any_val_len;

        *p = 0x32; // Tag 6 (KeyValue attribute)
        let mut cur = write_varint_ptr(p.add(1), kv_len);
        core::ptr::copy_nonoverlapping(key_wire_bytes.as_ptr(), cur, key_wire_bytes.len());
        cur = cur.add(key_wire_bytes.len());
        *cur = 0x12; // Tag 2 (value)
        cur = write_varint_ptr(cur.add(1), any_val_len);
        *cur = 0x0A; // Tag 1 (string_value)
        cur = write_varint_ptr(cur.add(1), s_len);
        core::ptr::copy_nonoverlapping(s.as_ptr(), cur, s_len);
        cur.add(s_len)
    }
}

/// Ultra-fast raw pointer stamping of an integer attribute directly into output stream.
#[inline(always)]
unsafe fn stamp_int_attribute(p: *mut u8, prefix: &[u8], val: u64) -> *mut u8 {
    unsafe {
        core::ptr::copy_nonoverlapping(prefix.as_ptr(), p, prefix.len());
        let kv_len_ptr = p.add(1);
        let any_val_len_ptr = p.add(prefix.len() - 2);
        let val_ptr = p.add(prefix.len());

        let v_len = if val < 0x80 {
            *val_ptr = val as u8;
            1
        } else if val < 0x4000 {
            *val_ptr = (val as u8 & 0x7F) | 0x80;
            *val_ptr.add(1) = (val >> 7) as u8;
            2
        } else {
            let mut v = val;
            let mut count = 0;
            while v >= 0x80 {
                *val_ptr.add(count) = (v as u8 & 0x7F) | 0x80;
                v >>= 7;
                count += 1;
            }
            *val_ptr.add(count) = v as u8;
            count + 1
        };

        *kv_len_ptr = (prefix.len() - 2 + v_len) as u8;
        *any_val_len_ptr = (1 + v_len) as u8;
        val_ptr.add(v_len)
    }
}

/// Ultra-fast raw pointer stamping of client.address attribute.
#[inline(always)]
unsafe fn stamp_ip_attribute(p: *mut u8, peer_ip: u128) -> *mut u8 {
    unsafe {
        core::ptr::copy_nonoverlapping(PRE_CLIENT_ADDR.as_ptr(), p, PRE_CLIENT_ADDR.len());
        let kv_len_ptr = p.add(1);
        let any_val_len_ptr = p.add(19);
        let str_len_ptr = p.add(21);
        let ip_dest = p.add(22);

        let (ip_buf, ip_len) = velda_core::lut::ip::format_ip_bytes(peer_ip);
        core::ptr::copy_nonoverlapping(ip_buf.as_ptr(), ip_dest, ip_len);

        *kv_len_ptr = (20 + ip_len) as u8;
        *any_val_len_ptr = (2 + ip_len) as u8;
        *str_len_ptr = ip_len as u8;
        ip_dest.add(ip_len)
    }
}

/// Pre-baked binary wire envelope for OTLP gRPC Protobuf logs.
#[derive(Debug, Clone)]
pub struct OtlpProtoWireEnvelope {
    pub resource_wire: Vec<u8>,
    pub scope_wire: Vec<u8>,
}

impl OtlpProtoWireEnvelope {
    /// Constructs a new wire envelope, pre-baking static Resource, Scope, and schema URL.
    pub fn new(service_name: &str) -> Self {
        let mut resource_payload = Vec::new();
        write_str_attribute(
            &mut resource_payload,
            0x0A,
            b"\x0a\x0cservice.name",
            service_name.as_bytes(),
        );
        write_str_attribute(
            &mut resource_payload,
            0x0A,
            b"\x0a\x12telemetry.sdk.name",
            b"velda-edge",
        );
        write_str_attribute(
            &mut resource_payload,
            0x0A,
            b"\x0a\x16telemetry.sdk.language",
            b"rust",
        );

        // ResourceLogs wire:
        // tag 1 (0x0A) -> Resource
        let mut resource_wire = Vec::new();
        resource_wire.push(0x0A);
        write_varint(&mut resource_wire, resource_payload.len() as u64);
        resource_wire.extend_from_slice(&resource_payload);
        // tag 3 (0x1A) -> schema_url
        resource_wire.push(0x1A);
        write_varint(&mut resource_wire, OTLP_SCHEMA_URL.len() as u64);
        resource_wire.extend_from_slice(OTLP_SCHEMA_URL);

        // ScopeLogs wire prefix:
        // tag 1 (0x0A) -> Scope ("velda.access_log")
        let mut scope_payload = Vec::new();
        scope_payload.push(0x0A);
        write_varint(&mut scope_payload, 16);
        scope_payload.extend_from_slice(b"velda.access_log");
        scope_payload.push(0x12);
        write_varint(&mut scope_payload, 5);
        scope_payload.extend_from_slice(b"0.1.0");

        let mut scope_wire = Vec::new();
        scope_wire.push(0x0A);
        write_varint(&mut scope_wire, scope_payload.len() as u64);
        scope_wire.extend_from_slice(&scope_payload);
        // tag 3 (0x1A) -> schema_url
        scope_wire.push(0x1A);
        write_varint(&mut scope_wire, OTLP_SCHEMA_URL.len() as u64);
        scope_wire.extend_from_slice(OTLP_SCHEMA_URL);

        Self {
            resource_wire,
            scope_wire,
        }
    }

    /// Serializes a single [`TelemetryRecord`] directly into `out` with pre-baked wire skeleton and SIMD stamping.
    #[inline(always)]
    pub fn encode_log_record_wire(&self, rec: &TelemetryRecord, out: &mut Vec<u8>) {
        out.reserve(MAX_LOG_RECORD_BYTES);

        let internal_status = rec.internal_status;
        let ts_nano = rec.ingress_time_nanos;
        let req_hi = rec.ingress_time_nanos;
        let req_lo = rec.request_id;

        unsafe {
            let base_ptr = out.as_mut_ptr().add(out.len());
            let mut p = base_ptr;

            // Repeated LogRecord header (tag 2 = 0x12) + 2 bytes for length varint placeholder
            *p = 0x12;
            let len_ptr = p.add(1);
            p = p.add(3);
            let start_rec = p;

            // 1. Dual-timestamp (18 bytes): time_unix_nano (tag 1, fixed64 = 0x09) & observed_time_unix_nano (tag 11, fixed64 = 0x59)
            #[cfg(target_arch = "x86_64")]
            {
                use core::arch::x86_64::*;
                *p = 0x09;
                let ts_vec = _mm_set1_epi64x(ts_nano as i64);
                _mm_storeu_si128(p.add(1) as *mut __m128i, ts_vec);
                *p.add(9) = 0x59;
                _mm_storeu_si64(p.add(10), ts_vec);
                p = p.add(18);
            }
            #[cfg(not(target_arch = "x86_64"))]
            {
                *p = 0x09;
                core::ptr::copy_nonoverlapping(ts_nano.to_le_bytes().as_ptr(), p.add(1), 8);
                *p.add(9) = 0x59;
                core::ptr::copy_nonoverlapping(ts_nano.to_le_bytes().as_ptr(), p.add(10), 8);
                p = p.add(18);
            }

            // 2. Pre-baked wire skeleton: Severity + Body + Request ID attribute prefix
            let skeleton = if internal_status.is_server_error() {
                SKELETON_ERROR
            } else if internal_status.is_aborted() {
                SKELETON_WARN
            } else {
                SKELETON_INFO
            };
            core::ptr::copy_nonoverlapping(skeleton.as_ptr(), p, skeleton.len());
            p = p.add(skeleton.len());

            // 3. Stamping request_id: exactly 32 lowercase hex characters via SSSE3 PSHUFB SIMD
            let hex_bytes = velda_core::request_id::format_hex32_bytes(req_hi, req_lo);
            core::ptr::copy_nonoverlapping(hex_bytes.as_ptr(), p, 32);
            p = p.add(32);

            // 4. Attribute 1 & 2: downstream_protocol & upstream_protocol
            p = stamp_str_attribute(
                p,
                b"\x0a\x13downstream_protocol",
                rec.downstream_protocol().as_bytes(),
            );
            p = stamp_str_attribute(
                p,
                b"\x0a\x11upstream_protocol",
                rec.upstream_protocol().as_bytes(),
            );

            // 5. Attribute 2: http.request.method
            p = stamp_str_attribute(p, b"\x0a\x13http.request.method", rec.method.as_bytes());

            // 6. Attribute 3: url
            p = stamp_str_attribute(p, b"\x0a\x03url", rec.url().as_bytes());

            // 7. Attribute 4: velda.internal_status
            p = stamp_str_attribute(
                p,
                b"\x0a\x15velda.internal_status",
                internal_status.as_bytes(),
            );

            // 8. Attribute 5: duration_ns
            p = stamp_int_attribute(p, PRE_DURATION_NS, rec.duration_ns);

            // 9. Attribute 6: http.response.body.size
            p = stamp_int_attribute(p, PRE_RESP_BODY_SIZE, rec.bytes_tx as u64);

            // 10. Attribute 7: http.request.body.size
            p = stamp_int_attribute(p, PRE_REQ_BODY_SIZE, rec.bytes_rx as u64);

            // 11. Attribute 8: velda.route_id
            p = stamp_int_attribute(p, PRE_ROUTE_ID, rec.route_id as u64);

            // 12. Attribute 9: client.address
            p = stamp_ip_attribute(p, rec.peer_ip);

            // 13. Attribute 10: user_agent.family
            p = stamp_str_attribute(p, b"\x0a\x11user_agent.family", rec.ua_family.as_bytes());

            // 15. Attribute 12: user_agent.major_version
            p = stamp_int_attribute(p, PRE_UA_MAJOR, rec.ua_major_version as u64);

            // 16. Attribute 13: user_agent.os
            p = stamp_str_attribute(p, b"\x0a\x0duser_agent.os", rec.ua_os.as_bytes());

            // 17. Backpatch length of LogRecord:
            let rec_len = p as usize - start_rec as usize;
            *len_ptr = (rec_len as u8 & 0x7F) | 0x80;
            *len_ptr.add(1) = (rec_len >> 7) as u8;

            let written = p as usize - base_ptr as usize;
            out.set_len(out.len() + written);
        }
    }

    /// Serializes a batch of [`TelemetryRecord`] directly into compliant Protobuf wire bytes (`out`).
    #[inline(always)]
    pub fn format_records_wire(
        &self,
        batches: &[TelemetryBatch],
        records_buf: &mut Vec<u8>,
        out: &mut Vec<u8>,
    ) -> usize {
        records_buf.clear();
        out.clear();

        let total_recs: usize = batches.iter().map(|b| b.len()).sum();
        if total_recs == 0 {
            return 0;
        }

        records_buf.reserve(total_recs * 480);

        for batch in batches {
            let recs = batch.as_ref();
            for (idx, rec) in recs.iter().enumerate() {
                #[cfg(target_arch = "x86_64")]
                if idx + 4 < recs.len() {
                    unsafe {
                        use core::arch::x86_64::*;
                        let next_rec =
                            recs.get_unchecked(idx + 4) as *const TelemetryRecord as *const i8;
                        _mm_prefetch(next_rec, _MM_HINT_T1);
                        _mm_prefetch(next_rec.add(64), _MM_HINT_T1);
                        _mm_prefetch(next_rec.add(128), _MM_HINT_T1);
                        _mm_prefetch(next_rec.add(192), _MM_HINT_T1);
                    }
                }
                self.encode_log_record_wire(rec, records_buf);
            }
        }

        let scope_payload_len = self.scope_wire.len() + records_buf.len();
        let scope_msg_header_len = 1 + varint_len(scope_payload_len as u64);
        let res_payload_len = self.resource_wire.len() + scope_msg_header_len + scope_payload_len;
        let total_root_len = 1 + varint_len(res_payload_len as u64) + res_payload_len;
        out.reserve(total_root_len);

        out.push(0x0A); // ExportLogsServiceRequest field 1 (repeated ResourceLogs)
        write_varint(out, res_payload_len as u64);
        out.extend_from_slice(&self.resource_wire);
        out.push(0x12); // ResourceLogs field 2 (repeated ScopeLogs)
        write_varint(out, scope_payload_len as u64);
        out.extend_from_slice(&self.scope_wire);
        out.extend_from_slice(records_buf);

        total_recs
    }
}

#[cfg(test)]
mod tests {
    use super::super::model::{ExportLogsServiceRequest, any_value};
    use super::*;
    use prost::Message;
    use std::sync::Arc;
    use velda_core::{HttpMethod, InternalStatus, PipeKind};

    #[test]
    fn test_otlp_proto_wire_template() {
        let envelope = OtlpProtoWireEnvelope::new("edge-prod");
        let mut rec = TelemetryRecord::zeroed();
        rec.request_id = 0x4bf9_2f35_77b3_4da6;
        rec.ingress_time_nanos = 1_710_000_000_000_000_000;
        rec.duration_ns = 2500;
        rec.peer_ip = velda_core::lut::ip::encode_ip_u128("192.168.1.50".parse().unwrap());
        rec.upstream_ip = velda_core::lut::ip::encode_upstream_ip_u32("10.0.0.10".parse().unwrap());
        rec.upstream_port = 8080;
        rec.bytes_rx = 100;
        rec.bytes_tx = 200;
        rec.route_id = 1;
        rec.pipe = PipeKind::BufferedH1H1;
        rec.internal_status = InternalStatus::Completed;
        rec.method = HttpMethod::Get;
        let (fam, os, ver) = velda_core::lut::classify_user_agent(b"curl/8.6.0");
        rec.ua_family = fam;
        rec.ua_os = os;
        rec.ua_major_version = ver;

        let batches = vec![TelemetryBatch::from_arc(Arc::from(vec![rec]))];
        let mut records_buf = Vec::new();
        let mut out = Vec::new();

        let count = envelope.format_records_wire(&batches, &mut records_buf, &mut out);
        assert_eq!(count, 1);
        assert!(!out.is_empty());
        assert_eq!(out[0], 0x0A);

        // Decode wire bytes back through official Prost model to verify 100% standard compliance
        let decoded =
            ExportLogsServiceRequest::decode(&out[..]).expect("Valid OTLP Protobuf wire format");
        assert_eq!(decoded.resource_logs.len(), 1);
        let res_logs = &decoded.resource_logs[0];
        assert_eq!(
            res_logs.schema_url,
            "https://opentelemetry.io/schemas/1.26.0"
        );
        assert_eq!(res_logs.scope_logs.len(), 1);
        let scope_logs = &res_logs.scope_logs[0];
        assert_eq!(scope_logs.log_records.len(), 1);
        let log_rec = &scope_logs.log_records[0];

        // Verify timestamps
        assert_eq!(log_rec.time_unix_nano, 1_710_000_000_000_000_000);
        assert_eq!(log_rec.observed_time_unix_nano, 1_710_000_000_000_000_000);

        // Verify body is static "HTTP access log"
        assert_eq!(
            log_rec.body.as_ref().and_then(|b| b.value.as_ref()),
            Some(&any_value::Value::StringValue(
                "HTTP access log".to_string()
            ))
        );

        // Verify attributes
        assert_eq!(log_rec.attributes.len(), 14);
        assert_eq!(log_rec.attributes[0].key, "request_id");
        assert_eq!(log_rec.attributes[1].key, "downstream_protocol");
        assert_eq!(log_rec.attributes[2].key, "upstream_protocol");
        if let Some(any_value::Value::StringValue(s)) = log_rec.attributes[0]
            .value
            .as_ref()
            .and_then(|v| v.value.as_ref())
        {
            assert_eq!(s.len(), 32);
            assert_eq!(s, "17bb23f0a5eb00004bf92f3577b34da6");
        } else {
            panic!("request_id attribute is not string");
        }

        // Anti-circular check: decode with canonical upstream opentelemetry-proto generated schema
        let official = <opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest as prost_otlp::Message>::decode(&out[..])
            .expect("Must decode cleanly against official OpenTelemetry proto schema");
        assert_eq!(official.resource_logs.len(), 1);
        let off_res = official.resource_logs[0].resource.as_ref().unwrap();
        assert_eq!(off_res.attributes.len(), 3);
        assert_eq!(off_res.attributes[0].key, "service.name");
        assert_eq!(
            off_res.attributes[0].value.as_ref().unwrap().value,
            Some(
                opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                    "edge-prod".into()
                )
            )
        );
        assert_eq!(off_res.attributes[1].key, "telemetry.sdk.name");
        assert_eq!(off_res.attributes[2].key, "telemetry.sdk.language");

        let off_rec = &official.resource_logs[0].scope_logs[0].log_records[0];
        assert_eq!(off_rec.time_unix_nano, 1_710_000_000 * 1_000_000_000);
        assert_eq!(
            off_rec.observed_time_unix_nano,
            1_710_000_000 * 1_000_000_000
        );
        assert_eq!(off_rec.severity_number, 9);
        assert_eq!(off_rec.severity_text, "INFO");
        assert_eq!(off_rec.attributes.len(), 14);
    }

    #[test]
    fn test_max_log_record_fits_reserve() {
        let envelope = OtlpProtoWireEnvelope::new("edge-prod");
        let mut rec = TelemetryRecord::zeroed();
        rec.request_id = u64::MAX;
        rec.ingress_time_nanos = u64::MAX;
        rec.duration_ns = u64::MAX;
        rec.peer_ip = 0x2001_0db8_85a3_1234_5678_8a2e_0370_7334;
        rec.bytes_rx = u32::MAX;
        rec.bytes_tx = u32::MAX;
        rec.route_id = u16::MAX;
        rec.ua_major_version = u16::MAX;
        rec.pipe = PipeKind::ServerStreamGrpcUdpUdp;
        rec.internal_status = InternalStatus::BadRequest;
        rec.method = HttpMethod::Options;
        rec.set_url("/a/long/url/path?query=1");
        rec.ua_family = velda_core::UaFamily::ALL[velda_core::UaFamily::COUNT - 1];

        let mut out = Vec::new();
        envelope.encode_log_record_wire(&rec, &mut out);
        assert!(
            out.len() < MAX_LOG_RECORD_BYTES,
            "worst case {} B",
            out.len()
        );
        assert!(out.len() < 16 * 1024);
    }

    #[test]
    fn test_otlp_proto_severity() {
        let envelope = OtlpProtoWireEnvelope::new("edge-prod");
        let mut warn_rec = TelemetryRecord::zeroed();
        warn_rec.request_id = 1;
        warn_rec.ingress_time_nanos = 1_710_000_000;
        warn_rec.duration_ns = 100;
        warn_rec.peer_ip = velda_core::lut::ip::encode_ip_u128("127.0.0.1".parse().unwrap());
        warn_rec.route_id = 1;
        warn_rec.pipe = PipeKind::BufferedH1H1;
        warn_rec.internal_status = InternalStatus::RouteNotFound;
        warn_rec.method = HttpMethod::Get;
        let (fam, os, ver) = velda_core::lut::classify_user_agent(b"curl/8.6.0");
        warn_rec.ua_family = fam;
        warn_rec.ua_os = os;
        warn_rec.ua_major_version = ver;

        let mut err_rec = TelemetryRecord::zeroed();
        err_rec.request_id = 2;
        err_rec.ingress_time_nanos = 1_710_000_001;
        err_rec.duration_ns = 200;
        err_rec.peer_ip = velda_core::lut::ip::encode_ip_u128("127.0.0.1".parse().unwrap());
        err_rec.route_id = 2;
        err_rec.pipe = PipeKind::BufferedGrpcTcpTcp;
        err_rec.internal_status = InternalStatus::UpstreamError;
        err_rec.method = HttpMethod::Post;
        let (fam, os, ver) = velda_core::lut::classify_user_agent(b"grpc-go/1.64.0");
        err_rec.ua_family = fam;
        err_rec.ua_os = os;
        err_rec.ua_major_version = ver;
        let batches = vec![TelemetryBatch::from_arc(Arc::from(vec![warn_rec, err_rec]))];
        let mut records_buf = Vec::new();
        let mut out = Vec::new();
        let count = envelope.format_records_wire(&batches, &mut records_buf, &mut out);
        assert_eq!(count, 2);

        let official = <opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest as prost_otlp::Message>::decode(&out[..])
            .expect("WARN/ERROR records must decode against official OpenTelemetry proto schema");
        let recs = &official.resource_logs[0].scope_logs[0].log_records;
        assert_eq!(recs[0].severity_number, 13);
        assert_eq!(recs[0].severity_text, "WARN");
        assert_eq!(recs[1].severity_number, 17);
        assert_eq!(recs[1].severity_text, "ERROR");
    }
}
