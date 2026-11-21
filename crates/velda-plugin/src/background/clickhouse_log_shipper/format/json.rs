//! ClickHouse JSONEachRow Format Serializer (`clickhouse_log_shipper::format::json`).
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────────────────────┐
//! │                   CLICKHOUSE JSONEACHROW SERIALIZER SPECIFICATION                       │
//! ├───────────────────┬───────────────┬────────────────┬───────────────┬────────────────────┤
//! │ JSON Field Key    │ Wire Type     │ Value Source   │ Est. Size     │ Stamping Mechanics │
//! ├───────────────────┼───────────────┼────────────────┼───────────────┼────────────────────┤
//! │ "timestamp"       │ Decimal ASCII │ ingress_nanos  │ 19 digits     │ fast_write_u64_ptr │
//! │ "request_id"      │ Decimal ASCII │ request_id     │ 1..=20 digits │ fast_write_u64_ptr │
//! │ "duration_ns"     │ Decimal ASCII │ duration_ns    │ 1..=19 digits │ fast_write_u64_ptr │
//! │ "peer_ip"         │ Quoted String │ peer_ip        │ 3..=39 chars  │ fast_write_ip_ptr  │
//! │ "upstream_ip"     │ Quoted String │ upstream_ip    │ 4..=15 chars  │ fast_write_ipv4_ptr│
//! │ "upstream_port"   │ Decimal ASCII │ upstream_port  │ 1..=5 digits  │ fast_write_u16_ptr │
//! │ "route_id"        │ Decimal ASCII │ route_id       │ 1..=5 digits  │ fast_write_u16_ptr │
//! │ "status_code"     │ Decimal ASCII │ status_code    │ 3 digits      │ fast_write_u16_ptr │
//! │ "pipe"            │ Quoted String │ rec.pipe       │ 10..=24 chars │ Core .as_bytes()   │
//! │ "method"          │ Quoted String │ rec.method     │ 3..=7 chars   │ Core .as_bytes()   │
//! │ "url"             │ Quoted String │ rec.url()      │ N chars       │ JSON-escaped copy  │
//! │ "bytes_rx"        │ Decimal ASCII │ bytes_rx       │ 1..=10 digits │ fast_write_u64_ptr │
//! │ "bytes_tx"        │ Decimal ASCII │ bytes_tx       │ 1..=10 digits │ fast_write_u64_ptr │
//! │ "completion_reason│ Quoted String │ rec.completion │ 4..=14 chars  │ Core .as_bytes()   │
//! │ "grpc_status"     │ Quoted String │ rec.grpc_status│ 2..=19 chars  │ Core .as_bytes()   │
//! │ "user_agent_family│ Quoted String │ rec.ua_family  │ 4..=12 chars  │ Core .as_bytes()   │
//! │ "user_agent_os"   │ Quoted String │ rec.ua_os      │ 3..=7 chars   │ Core .as_bytes()   │
//! ├───────────────────┴───────────────┴────────────────┼───────────────┴────────────────────┤
//! │ Total Record NDJSON Size:                          │ ~240..=380 bytes per row + '\n'   │
//! └────────────────────────────────────────────────────┴────────────────────────────────────┘
//!
//!  Memory Pipeline & Direct Pointer Stamping:
//!
//!    TelemetryBatch (L2 Prefetch _MM_HINT_T1)
//!         │
//!         ▼
//!    [ TelemetryRecord ] ──► Reservation: upfront estimate + per-row MAX_ROW_BYTES guard
//!         │
//!         ├─── Static Syntax Literals (,"key":") ──► LLVM Immediate Register Stores
//!         ├─── Integers (ts, id, dur, ports)     ──► fast_write_u16/u64_ptr (DIGIT_PAIRS)
//!         ├─── IP Addrs (peer_ip, upstream)      ──► fast_write_ip_ptr (velda-core::lut::ip)
//!         ├─── Enums (pipe, meth, comp, ...)     ──► rec.<enum>.as_bytes() (Core vocabulary)
//!         └─── Dynamic URL (path, query)         ──► write_json_escaped_ptr
//!         │
//!         ▼
//!    [ Continuous NDJSON Wire Buffer ] ──► Terminal '}\n' ──► Single out.set_len(cursor)
//!
//!  JSONEachRow Framing Structure:
//!  {"timestamp":1732011000123456789,"request_id":987654321,...,"user_agent_os":"linux"}\n
//! ```
//!
//! Produces newline-delimited JSON objects matching ClickHouse's native `JSONEachRow` wire format.
//! Pre-reserves capacity upfront to guarantee zero heap reallocations during formatting.

use velda_observability::{TelemetryBatch, TelemetryRecord};

use velda_core::lut::decimal::{fast_write_u16_ptr, fast_write_u64_ptr};
use velda_core::lut::ip::{fast_write_ip_ptr, fast_write_ipv4_ptr};

/// Upper bound of one serialized row: fixed keys + numerics + IPv6 + enum names (< 512 B)
/// plus a fully control-character URL escaped at 6 bytes per input byte (`\u00XX`).
const MAX_ROW_BYTES: usize = 512 + 184 * 6;

const HEX_DIGITS: &[u8; 16] = b"0123456789abcdef";

#[inline]
unsafe fn write_json_escaped_ptr(ptr: *mut u8, s: &[u8]) -> usize {
    let mut p = ptr;
    for &b in s {
        unsafe {
            match b {
                b'"' | b'\\' => {
                    *p = b'\\';
                    *p.add(1) = b;
                    p = p.add(2);
                }
                0x00..=0x1F => {
                    core::ptr::copy_nonoverlapping(b"\\u00".as_ptr(), p, 4);
                    *p.add(4) = HEX_DIGITS[(b >> 4) as usize];
                    *p.add(5) = HEX_DIGITS[(b & 0x0f) as usize];
                    p = p.add(6);
                }
                _ => {
                    *p = b;
                    p = p.add(1);
                }
            }
        }
    }
    p as usize - ptr as usize
}

const JSON_PREFIX: &[u8] = b"{\"timestamp\":";
const JSON_REQ_ID: &[u8] = b",\"request_id\":";
const JSON_DURATION: &[u8] = b",\"duration_ns\":";
const JSON_PEER_IP: &[u8] = b",\"peer_ip\":\"";
const JSON_UPSTREAM_IP: &[u8] = b"\",\"upstream_ip\":\"";
const JSON_NONE: &[u8] = b"none";
const JSON_UPSTREAM_PORT: &[u8] = b"\",\"upstream_port\":";
const JSON_ROUTE_ID: &[u8] = b",\"route_id\":";
const JSON_STATUS: &[u8] = b",\"status_code\":";
const JSON_PIPE: &[u8] = b",\"pipe\":\"";
const JSON_METHOD: &[u8] = b"\",\"method\":\"";
const JSON_URL: &[u8] = b"\",\"url\":\"";
const JSON_BYTES_RX: &[u8] = b"\",\"bytes_rx\":";
const JSON_BYTES_TX: &[u8] = b",\"bytes_tx\":";
const JSON_COMPLETION: &[u8] = b",\"completion_reason\":\"";
const JSON_GRPC: &[u8] = b"\",\"grpc_status\":\"";
const JSON_UA_FAMILY: &[u8] = b"\",\"user_agent_family\":\"";
const JSON_UA_OS: &[u8] = b"\",\"user_agent_os\":\"";
const JSON_SUFFIX: &[u8] = b"\"}\n";

/// Formats batches of [`TelemetryRecord`] into ClickHouse `JSONEachRow` format directly into `out`.
///
/// Employs direct pointer stamping, compile-time key literals, and zero-allocation integer formatting.
/// Employs `_MM_HINT_T1` prefetching to keep the record batch in L2 without polluting L1d cache.
/// Returns the number of records actually serialized into `out`.
#[inline(never)]
pub fn format_json_each_row(batches: &[TelemetryBatch], out: &mut Vec<u8>) -> usize {
    let total_recs: usize = batches.iter().map(|b| b.len()).sum();
    out.reserve(total_recs * 384);
    let mut serialized_count = 0;

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

            serialized_count += 1;

            // Per-row capacity guard: the raw stamping below never exceeds `MAX_ROW_BYTES`.
            out.reserve(MAX_ROW_BYTES);

            unsafe {
                let base_ptr = out.as_mut_ptr();
                let mut ptr = base_ptr.add(out.len());

                // 1. Prefix: `{"timestamp":`
                core::ptr::copy_nonoverlapping(JSON_PREFIX.as_ptr(), ptr, JSON_PREFIX.len());
                ptr = ptr.add(JSON_PREFIX.len());

                // 2. timestamp (ingress_time_nanos)
                let n_len = fast_write_u64_ptr(ptr, rec.ingress_time_nanos);
                ptr = ptr.add(n_len);

                // 3. `,"request_id":`
                core::ptr::copy_nonoverlapping(JSON_REQ_ID.as_ptr(), ptr, JSON_REQ_ID.len());
                ptr = ptr.add(JSON_REQ_ID.len());

                // 4. request_id
                let n_len = fast_write_u64_ptr(ptr, rec.request_id);
                ptr = ptr.add(n_len);

                // 5. `,"duration_ns":`
                core::ptr::copy_nonoverlapping(JSON_DURATION.as_ptr(), ptr, JSON_DURATION.len());
                ptr = ptr.add(JSON_DURATION.len());

                // 6. duration_ns
                let n_len = fast_write_u64_ptr(ptr, rec.duration_ns);
                ptr = ptr.add(n_len);

                // 7. `,"peer_ip":"`
                core::ptr::copy_nonoverlapping(JSON_PEER_IP.as_ptr(), ptr, JSON_PEER_IP.len());
                ptr = ptr.add(JSON_PEER_IP.len());

                // 8. peer_ip
                let ip_len = fast_write_ip_ptr(ptr, rec.peer_ip);
                ptr = ptr.add(ip_len);

                // 9. `","upstream_ip":"`
                core::ptr::copy_nonoverlapping(
                    JSON_UPSTREAM_IP.as_ptr(),
                    ptr,
                    JSON_UPSTREAM_IP.len(),
                );
                ptr = ptr.add(JSON_UPSTREAM_IP.len());

                // 10. upstream_ip
                if rec.upstream_ip != 0 {
                    let up_len = fast_write_ipv4_ptr(ptr, rec.upstream_ip);
                    ptr = ptr.add(up_len);
                } else {
                    core::ptr::copy_nonoverlapping(JSON_NONE.as_ptr(), ptr, JSON_NONE.len());
                    ptr = ptr.add(JSON_NONE.len());
                }

                // 13. `","upstream_port":`
                core::ptr::copy_nonoverlapping(
                    JSON_UPSTREAM_PORT.as_ptr(),
                    ptr,
                    JSON_UPSTREAM_PORT.len(),
                );
                ptr = ptr.add(JSON_UPSTREAM_PORT.len());

                // 14. upstream_port
                let n_len = fast_write_u16_ptr(ptr, rec.upstream_port);
                ptr = ptr.add(n_len);

                // 15. `,"route_id":`
                core::ptr::copy_nonoverlapping(JSON_ROUTE_ID.as_ptr(), ptr, JSON_ROUTE_ID.len());
                ptr = ptr.add(JSON_ROUTE_ID.len());

                // 16. route_id
                let n_len = fast_write_u16_ptr(ptr, rec.route_id);
                ptr = ptr.add(n_len);

                // 17. `,"status_code":`
                core::ptr::copy_nonoverlapping(JSON_STATUS.as_ptr(), ptr, JSON_STATUS.len());
                ptr = ptr.add(JSON_STATUS.len());

                // 18. status_code
                let n_len = fast_write_u16_ptr(ptr, rec.status_code);
                ptr = ptr.add(n_len);

                // 19. pipe
                core::ptr::copy_nonoverlapping(JSON_PIPE.as_ptr(), ptr, JSON_PIPE.len());
                ptr = ptr.add(JSON_PIPE.len());
                let pipe = rec.pipe.as_bytes();
                core::ptr::copy_nonoverlapping(pipe.as_ptr(), ptr, pipe.len());
                ptr = ptr.add(pipe.len());

                // 20. method
                core::ptr::copy_nonoverlapping(JSON_METHOD.as_ptr(), ptr, JSON_METHOD.len());
                ptr = ptr.add(JSON_METHOD.len());
                let meth = rec.method.as_bytes();
                core::ptr::copy_nonoverlapping(meth.as_ptr(), ptr, meth.len());
                ptr = ptr.add(meth.len());

                // 21. url
                core::ptr::copy_nonoverlapping(JSON_URL.as_ptr(), ptr, JSON_URL.len());
                ptr = ptr.add(JSON_URL.len());
                ptr = ptr.add(write_json_escaped_ptr(ptr, rec.url().as_bytes()));

                // 22. bytes_rx
                core::ptr::copy_nonoverlapping(JSON_BYTES_RX.as_ptr(), ptr, JSON_BYTES_RX.len());
                ptr = ptr.add(JSON_BYTES_RX.len());
                let n_len = fast_write_u64_ptr(ptr, rec.bytes_rx as u64);
                ptr = ptr.add(n_len);

                // 24. bytes_tx
                core::ptr::copy_nonoverlapping(JSON_BYTES_TX.as_ptr(), ptr, JSON_BYTES_TX.len());
                ptr = ptr.add(JSON_BYTES_TX.len());
                let n_len = fast_write_u64_ptr(ptr, rec.bytes_tx as u64);
                ptr = ptr.add(n_len);

                // 26. completion_reason
                core::ptr::copy_nonoverlapping(
                    JSON_COMPLETION.as_ptr(),
                    ptr,
                    JSON_COMPLETION.len(),
                );
                ptr = ptr.add(JSON_COMPLETION.len());
                let comp = rec.completion.as_bytes();
                core::ptr::copy_nonoverlapping(comp.as_ptr(), ptr, comp.len());
                ptr = ptr.add(comp.len());

                // 27. grpc_status
                core::ptr::copy_nonoverlapping(JSON_GRPC.as_ptr(), ptr, JSON_GRPC.len());
                ptr = ptr.add(JSON_GRPC.len());
                let grpc = rec.grpc_status.as_bytes();
                core::ptr::copy_nonoverlapping(grpc.as_ptr(), ptr, grpc.len());
                ptr = ptr.add(grpc.len());

                // 28. user_agent_family
                core::ptr::copy_nonoverlapping(JSON_UA_FAMILY.as_ptr(), ptr, JSON_UA_FAMILY.len());
                ptr = ptr.add(JSON_UA_FAMILY.len());
                let fam = rec.ua_family.as_bytes();
                core::ptr::copy_nonoverlapping(fam.as_ptr(), ptr, fam.len());
                ptr = ptr.add(fam.len());

                // 29. user_agent_os
                core::ptr::copy_nonoverlapping(JSON_UA_OS.as_ptr(), ptr, JSON_UA_OS.len());
                ptr = ptr.add(JSON_UA_OS.len());
                let os = rec.ua_os.as_bytes();
                core::ptr::copy_nonoverlapping(os.as_ptr(), ptr, os.len());
                ptr = ptr.add(os.len());

                // 30. suffix `"}\n"`
                core::ptr::copy_nonoverlapping(JSON_SUFFIX.as_ptr(), ptr, JSON_SUFFIX.len());
                ptr = ptr.add(JSON_SUFFIX.len());

                out.set_len(ptr as usize - base_ptr as usize);
            }
        }
    }

    serialized_count
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use velda_core::{CompletionReason, GrpcStatus, HttpMethod, PipeKind, UaFamily, UaOs};
    use velda_observability::TelemetryRecord;

    fn make_test_record() -> TelemetryRecord {
        let mut rec = TelemetryRecord::zeroed();
        rec.ingress_time_nanos = 1_732_011_000_123_456_789;
        rec.request_id = 987654321;
        rec.duration_ns = 250_000;
        // IPv4-mapped IPv6: 192.168.1.100 -> (192<<24 | 168<<16 | 1<<8 | 100) = 0xC0A80164
        rec.peer_ip = 0x0000_0000_0000_0000_0000_FFFF_C0A8_0164;
        // Upstream IPv4: 10.0.0.1 -> 0x0A000001
        rec.upstream_ip = 0x0A000001;
        rec.upstream_port = 8080;
        rec.route_id = 42;
        rec.status_code = 200;
        rec.pipe = PipeKind::BufferedH1H1;
        rec.method = HttpMethod::Get;
        rec.completion = CompletionReason::Complete;
        rec.grpc_status = GrpcStatus::Ok;
        rec.ua_family = UaFamily::Curl;
        rec.ua_os = UaOs::Linux;
        rec.bytes_rx = 128;
        rec.bytes_tx = 4096;
        rec.set_url("/api/v1/orders?id=123");
        rec
    }

    #[test]
    fn test_json_each_row_serialization() {
        let rec = make_test_record();
        let batch = TelemetryBatch::from_arc(Arc::new([rec]));
        let mut out = Vec::new();

        let count = format_json_each_row(&[batch], &mut out);
        assert_eq!(count, 1);
        assert!(!out.is_empty());

        let json_str = std::str::from_utf8(&out).unwrap();
        assert!(json_str.starts_with("{\"timestamp\":1732011000123456789"));
        assert!(json_str.contains("\"request_id\":987654321"));
        assert!(json_str.contains("\"duration_ns\":250000"));
        assert!(json_str.contains("\"peer_ip\":\"192.168.1.100\""));
        assert!(json_str.contains("\"upstream_ip\":\"10.0.0.1\""));
        assert!(json_str.contains("\"upstream_port\":8080"));
        assert!(json_str.contains("\"route_id\":42"));
        assert!(json_str.contains("\"status_code\":200"));
        assert!(json_str.contains("\"pipe\":\"buffered_h1_h1\""));
        assert!(json_str.contains("\"method\":\"GET\""));
        assert!(json_str.contains("\"url\":\"/api/v1/orders?id=123\""));
        assert!(json_str.contains("\"bytes_rx\":128"));
        assert!(json_str.contains("\"bytes_tx\":4096"));
        assert!(json_str.contains("\"completion_reason\":\"complete\""));
        assert!(json_str.contains("\"grpc_status\":\"OK\""));
        assert!(json_str.contains("\"user_agent_family\":\"curl\""));
        assert!(json_str.contains("\"user_agent_os\":\"linux\""));
        assert!(json_str.ends_with("}\n"));

        // Verify valid JSON parse
        let parsed: serde_json::Value = serde_json::from_str(json_str.trim_end()).unwrap();
        assert_eq!(parsed["request_id"], 987654321);
        assert_eq!(parsed["peer_ip"], "192.168.1.100");
        assert_eq!(parsed["status_code"], 200);
        assert_eq!(parsed["pipe"], "buffered_h1_h1");
        assert_eq!(parsed["method"], "GET");
    }

    #[test]
    fn test_json_url_escape_and_max_row() {
        let mut rec = make_test_record();
        rec.peer_ip = 0x2001_0db8_85a3_1234_5678_8a2e_0370_7334;
        rec.ingress_time_nanos = u64::MAX;
        rec.request_id = u64::MAX;
        rec.duration_ns = u64::MAX;
        rec.pipe = PipeKind::ServerStreamGrpcUdpUdp;
        rec.grpc_status = GrpcStatus::FailedPrecondition;
        // 186 bytes of adversarial control/quote characters
        let evil = "/a\"b\\c\n\r\t".repeat(21);
        rec.set_url(&evil);

        let batch = TelemetryBatch::from_arc(Arc::new([rec]));
        let mut out = Vec::new();
        let count = format_json_each_row(&[batch], &mut out);
        assert_eq!(count, 1);

        let line = std::str::from_utf8(&out).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(line.trim_end()).unwrap();
        assert_eq!(parsed["url"].as_str().unwrap(), rec.url());
    }
}
