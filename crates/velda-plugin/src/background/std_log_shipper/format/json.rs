//! NDJSON Access Log Formatting (`std_log_shipper::format::json`).
//!
//! ```text
//! ┌────────────────────────────────────────────────────────────────────────────────────────┐
//! │                              NDJSON LOG RECORD SERIALIZATION                           │
//! │                                                                                        │
//! │  rec: &AccessLogRecord (64B)                                                           │
//! │   ├─ JSON_K0: {"request_id":"                                                          │
//! │   ├─ fast_write_hex32(req_hi, req_lo) ──► Exact 32 lowercase hex chars via SIMD SSSE3  │
//! │   ├─ JSON_K1: ","ts":                 ──► fast_write_u64 (reciprocal mul, no DIV)      │
//! │   ├─ JSON_K2: ,"dur_us":              ──► fast_write_u64                               │
//! │   ├─ JSON_K3: ,"proto":"              ──► proto.as_str()                               │
//! │   ├─ JSON_K4: ","up_proto":"          ──► up_proto.as_str()                            │
//! │   ├─ JSON_K5: ","pipe":"              ──► pipe.as_str()                                │
//! │   ├─ JSON_K6: ","method":"            ──► method.as_str()                              │
//! │   ├─ JSON_K7: ","status":             ──► fast_write_u64                               │
//! │   ├─ JSON_K8: ,"grpc_status":"        ──► grpc_st.as_str()                             │
//! │   ├─ JSON_K9: ,"rx":                  ──► fast_write_u64                               │
//! │   ├─ JSON_K10: ,"tx":                 ──► fast_write_u64                               │
//! │   ├─ JSON_K11: ,"msgs":               ──► fast_write_u64                               │
//! │   ├─ JSON_K12: ,"peer":"              ──► fast_write_ip (RFC 5952 zero-alloc) : port  │
//! │   ├─ JSON_K13: ","upstream":"         ──► fast_write_ipv4 : port                       │
//! │   ├─ JSON_K14: ,"route_id":           ──► fast_write_u64                               │
//! │   ├─ JSON_K15: ,"ua":"                ──► ua_fam / major_ver                           │
//! │   ├─ JSON_K16: ","os":"               ──► ua_os.as_str()                               │
//! │   ├─ JSON_K17: ","completion":"       ──► completion.as_str()                          │
//! │   └─ JSON_END: "}\n                                                                    │
//! └────────────────────────────────────────────────────────────────────────────────────────┘
//! ```

use std::sync::Arc;
use velda_observability::{
    AccessLogRecord, LogCompletionReason, LogGrpcStatus, LogHttpMethod, LogProtocol, PipeStrategy,
    UaFamily, UaOs,
};

use super::super::lut::{
    fast_write_hex32, fast_write_ip, fast_write_ipv4, fast_write_ts_sec, fast_write_u64,
};

// Pre-baked static frame chunks for NDJSON output
const JSON_K0: &[u8] = b"{\"request_id\":\"";
const JSON_K1: &[u8] = b"\",\"ts\":";
const JSON_K2: &[u8] = b",\"dur_us\":";
const JSON_K3: &[u8] = b",\"proto\":\"";
const JSON_K4: &[u8] = b"\",\"up_proto\":\"";
const JSON_K5: &[u8] = b"\",\"pipe\":\"";
const JSON_K6: &[u8] = b"\",\"method\":\"";
const JSON_K7: &[u8] = b"\",\"status\":";
const JSON_K8: &[u8] = b",\"grpc_status\":\"";
const JSON_K9: &[u8] = b"\",\"rx\":";
const JSON_K10: &[u8] = b",\"tx\":";
const JSON_K11: &[u8] = b",\"msgs\":";
const JSON_K12: &[u8] = b",\"peer\":\"";
const JSON_K13: &[u8] = b"\",\"upstream\":\"";
const JSON_K14: &[u8] = b"\",\"route_id\":";
const JSON_K15: &[u8] = b",\"ua\":\"";
const JSON_K16: &[u8] = b"\",\"os\":\"";
const JSON_K17: &[u8] = b"\",\"completion\":\"";
const JSON_END: &[u8] = b"\"}\n";

/// Formats batches of [`AccessLogRecord`] into NDJSON format directly into `out`.
///
/// Pre-reserves capacity upfront to guarantee 0 heap reallocations during formatting.
pub fn format_ndjson_records(
    batches: &[Arc<[AccessLogRecord]>],
    total_recs: usize,
    out: &mut Vec<u8>,
) {
    out.reserve(total_recs * 320);

    for batch in batches {
        for rec in batch.iter() {
            let proto = LogProtocol::from_u8(rec.downstream_proto);
            let up_proto = LogProtocol::from_u8(rec.upstream_proto);
            let pipe = PipeStrategy::from_discriminant(rec.pipe_strategy)
                .unwrap_or(PipeStrategy::Buffered);
            let completion = LogCompletionReason::from_u8(rec.completion);
            let method = LogHttpMethod::from_u8(rec.method);
            let grpc_st = LogGrpcStatus::from_u8(rec.grpc_status);
            let ua_fam = UaFamily::from_u8(rec.ua_family);
            let ua_os = UaOs::from_u8(rec.ua_os);

            out.extend_from_slice(JSON_K0);
            fast_write_hex32(out, rec.timestamp_sec as u64, rec.request_id);
            out.extend_from_slice(JSON_K1);
            fast_write_ts_sec(out, rec.timestamp_sec);
            out.extend_from_slice(JSON_K2);
            fast_write_u64(out, rec.duration_us as u64);
            out.extend_from_slice(JSON_K3);
            out.extend_from_slice(proto.as_str().as_bytes());
            out.extend_from_slice(JSON_K4);
            out.extend_from_slice(up_proto.as_str().as_bytes());
            out.extend_from_slice(JSON_K5);
            out.extend_from_slice(pipe.as_str().as_bytes());
            out.extend_from_slice(JSON_K6);
            out.extend_from_slice(method.as_str().as_bytes());
            out.extend_from_slice(JSON_K7);
            fast_write_u64(out, rec.status_code as u64);
            out.extend_from_slice(JSON_K8);
            out.extend_from_slice(grpc_st.as_str().as_bytes());
            out.extend_from_slice(JSON_K9);
            fast_write_u64(out, rec.bytes_rx as u64);
            out.extend_from_slice(JSON_K10);
            fast_write_u64(out, rec.bytes_tx as u64);
            out.extend_from_slice(JSON_K11);
            fast_write_u64(out, rec.rpc_msgs_or_flags as u64);
            out.extend_from_slice(JSON_K12);
            fast_write_ip(out, rec.peer_ip);
            out.push(b':');
            fast_write_u64(out, rec.peer_port as u64);
            out.extend_from_slice(JSON_K13);
            fast_write_ipv4(out, rec.upstream_ip);
            out.push(b':');
            fast_write_u64(out, rec.upstream_port as u64);
            out.extend_from_slice(JSON_K14);
            fast_write_u64(out, rec.route_id as u64);
            out.extend_from_slice(JSON_K15);
            out.extend_from_slice(ua_fam.as_str().as_bytes());
            out.push(b'/');
            fast_write_u64(out, rec.ua_major_version as u64);
            out.extend_from_slice(JSON_K16);
            out.extend_from_slice(ua_os.as_str().as_bytes());
            out.extend_from_slice(JSON_K17);
            out.extend_from_slice(completion.as_str().as_bytes());
            out.extend_from_slice(JSON_END);
        }
    }
}
