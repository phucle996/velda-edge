//! Direct Protobuf Wire Format Serializer (`otlp_log_shipper::proto::template`).
//!
//! ```text
//! ┌────────────────────────────────────────────────────────────────────────────────────────┐
//! │                OTLP PROTOBUF PRE-BAKED WIRE SKELETON ARCHITECTURE                      │
//! │                                                                                        │
//! │               AccessLogRecord (64-byte integer scalar from SPSC Ring)                  │
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

use std::sync::Arc;
use velda_observability::{
    AccessLogRecord, LogGrpcStatus, LogHttpMethod, LogProtocol, UaFamily, UaOs,
};

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
const PRE_STATUS_CODE: &[u8] = b"\x32\x00\x0a\x19http.response.status_code\x12\x00\x18";
const PRE_DURATION_NS: &[u8] = b"\x32\x00\x0a\x0bduration_ns\x12\x00\x18";
const PRE_RESP_BODY_SIZE: &[u8] = b"\x32\x00\x0a\x17http.response.body.size\x12\x00\x18";
const PRE_REQ_BODY_SIZE: &[u8] = b"\x32\x00\x0a\x16http.request.body.size\x12\x00\x18";
const PRE_ROUTE_ID: &[u8] = b"\x32\x00\x0a\x0evelda.route_id\x12\x00\x18";
const PRE_CLIENT_PORT: &[u8] = b"\x32\x00\x0a\x0bclient.port\x12\x00\x18";
const PRE_UA_MAJOR: &[u8] = b"\x32\x00\x0a\x18user_agent.major_version\x12\x00\x18";

// Pre-baked Protobuf StringValue attribute prefix for client.address:
// Format: [0x32, kv_len_placeholder, 0x0A, 0x0E, "client.address", 0x12, any_val_len_placeholder, 0x0A, str_len_placeholder]
const PRE_CLIENT_ADDR: &[u8] = b"\x32\x00\x0a\x0eclient.address\x12\x00\x0a\x00";

/// Fast division-free IPv4 and zero-alloc IPv6 formatting directly into stack buffer.
#[inline(always)]
fn format_ip_to_slice(ip: u128, buf: &mut [u8; 40]) -> usize {
    if (ip >> 48) == 0 && ((ip >> 32) & 0xFFFF) == 0xFFFF {
        let ip_u32 = ip as u32;
        let octets = ip_u32.to_be_bytes();
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
                let rem = oct as usize * 2;
                buf[idx] = super::super::lut::DIGIT_PAIRS[rem];
                buf[idx + 1] = super::super::lut::DIGIT_PAIRS[rem + 1];
                idx += 2;
            } else {
                buf[idx] = b'0' + oct / 100;
                let rem = (oct % 100) as usize * 2;
                buf[idx + 1] = super::super::lut::DIGIT_PAIRS[rem];
                buf[idx + 2] = super::super::lut::DIGIT_PAIRS[rem + 1];
                idx += 3;
            }
        }
        idx
    } else {
        super::super::lut::format_ipv6_to_slice(ip, buf)
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

        let mut ip_buf = [0u8; 40];
        let ip_len = format_ip_to_slice(peer_ip, &mut ip_buf);
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
    // Pre-baked static attribute wire slices
    downstream_protocol_attrs: [Vec<u8>; 7],
    upstream_protocol_attrs: [Vec<u8>; 7],
    method_attrs: [Vec<u8>; 10],
    grpc_attrs: [Vec<u8>; 18],
    ua_fam_attrs: [Vec<u8>; 34],
    ua_os_attrs: [Vec<u8>; 8],
}

impl OtlpProtoWireEnvelope {
    /// Constructs a new wire envelope, pre-baking static Resource, Scope, schema URL,
    /// and enum attribute wire bytes once at startup.
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

        // Static attribute tables (LogRecord field 6 = 0x32):
        let mut downstream_protocol_attrs: [Vec<u8>; 7] = Default::default();
        let mut upstream_protocol_attrs: [Vec<u8>; 7] = Default::default();
        for i in 0..7 {
            let p = LogProtocol::from_u8(i as u8);
            let mut down_buf = Vec::new();
            write_str_attribute(
                &mut down_buf,
                0x32,
                b"\x0a\x13downstream_protocol",
                p.as_str().as_bytes(),
            );
            downstream_protocol_attrs[i] = down_buf;

            let mut up_buf = Vec::new();
            write_str_attribute(
                &mut up_buf,
                0x32,
                b"\x0a\x11upstream_protocol",
                p.as_str().as_bytes(),
            );
            upstream_protocol_attrs[i] = up_buf;
        }

        let mut method_attrs: [Vec<u8>; 10] = Default::default();
        for (i, slot) in method_attrs.iter_mut().enumerate() {
            let m = LogHttpMethod::from_u8(i as u8);
            let mut buf = Vec::new();
            write_str_attribute(
                &mut buf,
                0x32,
                b"\x0a\x13http.request.method",
                m.as_str().as_bytes(),
            );
            *slot = buf;
        }

        let mut grpc_attrs: [Vec<u8>; 18] = Default::default();
        for (i, slot) in grpc_attrs.iter_mut().enumerate() {
            let g = if i <= 16 {
                LogGrpcStatus::from_u8(i as u8)
            } else {
                LogGrpcStatus::NonGrpc
            };
            let mut buf = Vec::new();
            write_str_attribute(
                &mut buf,
                0x32,
                b"\x0a\x14rpc.grpc.status_code",
                g.as_str().as_bytes(),
            );
            *slot = buf;
        }

        let mut ua_fam_attrs: [Vec<u8>; 34] = core::array::from_fn(|_| Vec::new());
        for (i, slot) in ua_fam_attrs.iter_mut().enumerate() {
            let u = if i <= 32 {
                UaFamily::from_u8(i as u8)
            } else {
                UaFamily::Custom
            };
            let mut buf = Vec::new();
            write_str_attribute(
                &mut buf,
                0x32,
                b"\x0a\x11user_agent.family",
                u.as_str().as_bytes(),
            );
            *slot = buf;
        }

        let mut ua_os_attrs: [Vec<u8>; 8] = Default::default();
        for (i, slot) in ua_os_attrs.iter_mut().enumerate() {
            let o = UaOs::from_u8(i as u8);
            let mut buf = Vec::new();
            write_str_attribute(
                &mut buf,
                0x32,
                b"\x0a\x0duser_agent.os",
                o.as_str().as_bytes(),
            );
            *slot = buf;
        }

        Self {
            resource_wire,
            scope_wire,
            downstream_protocol_attrs,
            upstream_protocol_attrs,
            method_attrs,
            grpc_attrs,
            ua_fam_attrs,
            ua_os_attrs,
        }
    }

    /// Serializes a single [`AccessLogRecord`] directly into `out` with pre-baked wire skeleton and SIMD stamping.
    #[inline(always)]
    pub fn encode_log_record_wire(&self, rec: &AccessLogRecord, out: &mut Vec<u8>) {
        out.reserve(600);

        let grpc_st = LogGrpcStatus::from_u8(rec.grpc_status);
        let ts_nano = (rec.timestamp_sec as u64).saturating_mul(1_000_000_000);
        let req_hi = rec.timestamp_sec as u64;
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

            // 2. Pre-baked wire skeleton: Severity + Body ("HTTP access log") + Request ID attribute prefix
            let skeleton = if rec.status_code >= 500
                || (grpc_st != LogGrpcStatus::Ok && grpc_st != LogGrpcStatus::NonGrpc)
            {
                SKELETON_ERROR
            } else if rec.status_code >= 400 {
                SKELETON_WARN
            } else {
                SKELETON_INFO
            };
            core::ptr::copy_nonoverlapping(skeleton.as_ptr(), p, skeleton.len());
            p = p.add(skeleton.len());

            // 3. Stamping request_id: exactly 32 lowercase hex characters via SSSE3 PSHUFB SIMD
            let hex_bytes = super::super::lut::format_hex32_bytes(req_hi, req_lo);
            core::ptr::copy_nonoverlapping(hex_bytes.as_ptr(), p, 32);
            p = p.add(32);

            // 4. Attribute 1 & 2: downstream_protocol & upstream_protocol
            let down_idx = (rec.downstream_protocol() as usize).min(6);
            let down_slice = &self.downstream_protocol_attrs[down_idx];
            core::ptr::copy_nonoverlapping(down_slice.as_ptr(), p, down_slice.len());
            p = p.add(down_slice.len());

            let up_idx = (rec.upstream_protocol() as usize).min(6);
            let up_slice = &self.upstream_protocol_attrs[up_idx];
            core::ptr::copy_nonoverlapping(up_slice.as_ptr(), p, up_slice.len());
            p = p.add(up_slice.len());

            // 5. Attribute 2: http.request.method
            let method_idx = if (rec.method as usize) < 10 {
                rec.method as usize
            } else {
                0
            };
            let method_slice = &self.method_attrs[method_idx];
            core::ptr::copy_nonoverlapping(method_slice.as_ptr(), p, method_slice.len());
            p = p.add(method_slice.len());

            // 6. Attribute 3: http.response.status_code
            p = stamp_int_attribute(p, PRE_STATUS_CODE, rec.status_code as u64);

            // 7. Attribute 4: rpc.grpc.status_code
            let grpc_idx = if (rec.grpc_status as usize) <= 16 {
                rec.grpc_status as usize
            } else {
                17
            };
            let grpc_slice = &self.grpc_attrs[grpc_idx];
            core::ptr::copy_nonoverlapping(grpc_slice.as_ptr(), p, grpc_slice.len());
            p = p.add(grpc_slice.len());

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

            // 13. Attribute 10: client.port
            p = stamp_int_attribute(p, PRE_CLIENT_PORT, rec.peer_port as u64);

            // 14. Attribute 11: user_agent.family
            let ua_fam_idx = if (rec.ua_family as usize) <= 32 {
                rec.ua_family as usize
            } else {
                33
            };
            let ua_fam_slice = &self.ua_fam_attrs[ua_fam_idx];
            core::ptr::copy_nonoverlapping(ua_fam_slice.as_ptr(), p, ua_fam_slice.len());
            p = p.add(ua_fam_slice.len());

            // 15. Attribute 12: user_agent.major_version
            p = stamp_int_attribute(p, PRE_UA_MAJOR, rec.ua_major_version as u64);

            // 16. Attribute 13: user_agent.os
            let ua_os_idx = if (rec.ua_os as usize) <= 7 {
                rec.ua_os as usize
            } else {
                0
            };
            let ua_os_slice = &self.ua_os_attrs[ua_os_idx];
            core::ptr::copy_nonoverlapping(ua_os_slice.as_ptr(), p, ua_os_slice.len());
            p = p.add(ua_os_slice.len());

            // 17. Backpatch length of LogRecord:
            let rec_len = p as usize - start_rec as usize;
            *len_ptr = (rec_len as u8 & 0x7F) | 0x80;
            *len_ptr.add(1) = (rec_len >> 7) as u8;

            let written = p as usize - base_ptr as usize;
            out.set_len(out.len() + written);
        }
    }

    /// Serializes a batch of [`AccessLogRecord`] directly into compliant Protobuf wire bytes (`out`).
    #[inline(always)]
    pub fn format_records_wire(
        &self,
        batches: &[Arc<[AccessLogRecord]>],
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
            for rec in batch.iter() {
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
    use velda_observability::PipeKind;

    #[test]
    fn test_otlp_proto_wire_template() {
        let envelope = OtlpProtoWireEnvelope::new("edge-prod");
        let rec = AccessLogRecord::from_scalars(
            0x4bf9_2f35_77b3_4da6,
            1_710_000_000,
            2500,
            "192.168.1.50:12345".parse().unwrap(),
            Some("10.0.0.10:8080".parse().unwrap()),
            100,
            200,
            200,
            1,
            0,
            PipeKind::BufferedH1H1,
            velda_observability::LogCompletionReason::Complete,
            LogHttpMethod::Get,
            LogGrpcStatus::NonGrpc,
            b"curl/8.6.0",
        );

        let batches = vec![Arc::from(vec![rec])];
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
        assert_eq!(log_rec.time_unix_nano, 1_710_000_000 * 1_000_000_000);
        assert_eq!(
            log_rec.observed_time_unix_nano,
            1_710_000_000 * 1_000_000_000
        );

        // Verify body is static "HTTP access log"
        assert_eq!(
            log_rec.body.as_ref().and_then(|b| b.value.as_ref()),
            Some(&any_value::Value::StringValue(
                "HTTP access log".to_string()
            ))
        );

        // Verify attributes
        assert_eq!(log_rec.attributes.len(), 15);
        assert_eq!(log_rec.attributes[0].key, "request_id");
        assert_eq!(log_rec.attributes[1].key, "downstream_protocol");
        assert_eq!(log_rec.attributes[2].key, "upstream_protocol");
        if let Some(any_value::Value::StringValue(s)) = log_rec.attributes[0]
            .value
            .as_ref()
            .and_then(|v| v.value.as_ref())
        {
            assert_eq!(s.len(), 32);
            assert_eq!(s, "0000000065ec87804bf92f3577b34da6");
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
        assert_eq!(off_rec.attributes.len(), 15);
    }

    #[test]
    fn test_otlp_proto_wire_warn_and_error_severity_official() {
        let envelope = OtlpProtoWireEnvelope::new("edge-prod");
        let warn_rec = AccessLogRecord::from_scalars(
            1,
            1_710_000_000,
            100,
            "127.0.0.1:1000".parse().unwrap(),
            None,
            0,
            0,
            404,
            1,
            0,
            PipeKind::BufferedH1H1,
            velda_observability::LogCompletionReason::Complete,
            LogHttpMethod::Get,
            LogGrpcStatus::NonGrpc,
            b"curl/8.6.0",
        );
        let err_rec = AccessLogRecord::from_scalars(
            2,
            1_710_000_001,
            200,
            "127.0.0.1:1001".parse().unwrap(),
            None,
            0,
            0,
            200,
            2,
            0,
            PipeKind::BufferedGrpcTcpTcp,
            velda_observability::LogCompletionReason::UpstreamError,
            LogHttpMethod::Post,
            LogGrpcStatus::Unimplemented,
            b"grpc-go/1.64.0",
        );
        let batches = vec![Arc::from(vec![warn_rec, err_rec])];
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
