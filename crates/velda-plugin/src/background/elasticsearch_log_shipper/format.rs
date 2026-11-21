//! Direct Pointer-Stamping Elasticsearch `_bulk` NDJSON Formatter.
//!
//! ```text
//! Boot Time:
//!   prebaked_action = b"{\"create\":{\"_index\":\"velda-logs\"}}\n"  (1-time bake)
//!
//! Per-Batch Hot Path (0 serde_json, 0 String allocs, L2 _MM_HINT_T1 prefetch):
//!   [prebaked_action] + {"@timestamp":<ms>,"timestamp":<ns>,...,"user_agent_os":"..."}\n
//! ```

use velda_core::lut::decimal::{fast_write_u16_ptr, fast_write_u64_ptr};
use velda_core::lut::ip::{fast_write_ip_ptr, fast_write_ipv4_ptr};
use velda_observability::{TelemetryBatch, TelemetryRecord};

use super::config::EsBulkOpType;

const MAX_ROW_BYTES: usize = 512 + 184 * 6;
const HEX_DIGITS: &[u8; 16] = b"0123456789abcdef";

const JSON_PREFIX: &[u8] = b"{\"@timestamp\":";
const JSON_TS_NS: &[u8] = b",\"timestamp\":";
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

/// Prebakes the immutable `_bulk` action line once at plugin initialization.
pub fn prebake_bulk_action_line(op_type: EsBulkOpType, index: &str) -> Vec<u8> {
    let op = match op_type {
        EsBulkOpType::Create => "create",
        EsBulkOpType::Index => "index",
    };
    format!("{{\"{op}\":{{\"_index\":\"{index}\"}}}}\n").into_bytes()
}

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

/// Formats batches into Elasticsearch `_bulk` NDJSON directly into `out` via pointer stamping.
#[inline(never)]
pub fn format_bulk_ndjson(
    batches: &[TelemetryBatch],
    prebaked_action: &[u8],
    out: &mut Vec<u8>,
) -> usize {
    let total_recs: usize = batches.iter().map(|b| b.len()).sum();
    out.reserve(total_recs * (prebaked_action.len() + 384));
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
            out.reserve(prebaked_action.len() + MAX_ROW_BYTES);

            unsafe {
                let base_ptr = out.as_mut_ptr();
                let mut ptr = base_ptr.add(out.len());

                // 0. Prebaked `_bulk` action line
                core::ptr::copy_nonoverlapping(
                    prebaked_action.as_ptr(),
                    ptr,
                    prebaked_action.len(),
                );
                ptr = ptr.add(prebaked_action.len());

                // 1. `{"@timestamp":` (epoch ms for ES native date field)
                core::ptr::copy_nonoverlapping(JSON_PREFIX.as_ptr(), ptr, JSON_PREFIX.len());
                ptr = ptr.add(JSON_PREFIX.len());
                ptr = ptr.add(fast_write_u64_ptr(ptr, rec.ingress_time_nanos / 1_000_000));

                // 2. `,"timestamp":`
                core::ptr::copy_nonoverlapping(JSON_TS_NS.as_ptr(), ptr, JSON_TS_NS.len());
                ptr = ptr.add(JSON_TS_NS.len());
                ptr = ptr.add(fast_write_u64_ptr(ptr, rec.ingress_time_nanos));

                // 3. `,"request_id":`
                core::ptr::copy_nonoverlapping(JSON_REQ_ID.as_ptr(), ptr, JSON_REQ_ID.len());
                ptr = ptr.add(JSON_REQ_ID.len());
                ptr = ptr.add(fast_write_u64_ptr(ptr, rec.request_id));

                // 4. `,"duration_ns":`
                core::ptr::copy_nonoverlapping(JSON_DURATION.as_ptr(), ptr, JSON_DURATION.len());
                ptr = ptr.add(JSON_DURATION.len());
                ptr = ptr.add(fast_write_u64_ptr(ptr, rec.duration_ns));

                // 5. `,"peer_ip":"`
                core::ptr::copy_nonoverlapping(JSON_PEER_IP.as_ptr(), ptr, JSON_PEER_IP.len());
                ptr = ptr.add(JSON_PEER_IP.len());
                ptr = ptr.add(fast_write_ip_ptr(ptr, rec.peer_ip));

                // 6. `","upstream_ip":"`
                core::ptr::copy_nonoverlapping(
                    JSON_UPSTREAM_IP.as_ptr(),
                    ptr,
                    JSON_UPSTREAM_IP.len(),
                );
                ptr = ptr.add(JSON_UPSTREAM_IP.len());
                if rec.upstream_ip != 0 {
                    ptr = ptr.add(fast_write_ipv4_ptr(ptr, rec.upstream_ip));
                } else {
                    core::ptr::copy_nonoverlapping(JSON_NONE.as_ptr(), ptr, JSON_NONE.len());
                    ptr = ptr.add(JSON_NONE.len());
                }

                // 7. `","upstream_port":`
                core::ptr::copy_nonoverlapping(
                    JSON_UPSTREAM_PORT.as_ptr(),
                    ptr,
                    JSON_UPSTREAM_PORT.len(),
                );
                ptr = ptr.add(JSON_UPSTREAM_PORT.len());
                ptr = ptr.add(fast_write_u16_ptr(ptr, rec.upstream_port));

                // 8. `,"route_id":`
                core::ptr::copy_nonoverlapping(JSON_ROUTE_ID.as_ptr(), ptr, JSON_ROUTE_ID.len());
                ptr = ptr.add(JSON_ROUTE_ID.len());
                ptr = ptr.add(fast_write_u16_ptr(ptr, rec.route_id));

                // 9. `,"status_code":`
                core::ptr::copy_nonoverlapping(JSON_STATUS.as_ptr(), ptr, JSON_STATUS.len());
                ptr = ptr.add(JSON_STATUS.len());
                ptr = ptr.add(fast_write_u16_ptr(ptr, rec.status_code));

                // 10. `,"pipe":"`
                core::ptr::copy_nonoverlapping(JSON_PIPE.as_ptr(), ptr, JSON_PIPE.len());
                ptr = ptr.add(JSON_PIPE.len());
                let pipe = rec.pipe.as_bytes();
                core::ptr::copy_nonoverlapping(pipe.as_ptr(), ptr, pipe.len());
                ptr = ptr.add(pipe.len());

                // 11. `","method":"`
                core::ptr::copy_nonoverlapping(JSON_METHOD.as_ptr(), ptr, JSON_METHOD.len());
                ptr = ptr.add(JSON_METHOD.len());
                let meth = rec.method.as_bytes();
                core::ptr::copy_nonoverlapping(meth.as_ptr(), ptr, meth.len());
                ptr = ptr.add(meth.len());

                // 12. `","url":"`
                core::ptr::copy_nonoverlapping(JSON_URL.as_ptr(), ptr, JSON_URL.len());
                ptr = ptr.add(JSON_URL.len());
                ptr = ptr.add(write_json_escaped_ptr(ptr, rec.url().as_bytes()));

                // 13. `","bytes_rx":`
                core::ptr::copy_nonoverlapping(JSON_BYTES_RX.as_ptr(), ptr, JSON_BYTES_RX.len());
                ptr = ptr.add(JSON_BYTES_RX.len());
                ptr = ptr.add(fast_write_u64_ptr(ptr, rec.bytes_rx as u64));

                // 14. `,"bytes_tx":`
                core::ptr::copy_nonoverlapping(JSON_BYTES_TX.as_ptr(), ptr, JSON_BYTES_TX.len());
                ptr = ptr.add(JSON_BYTES_TX.len());
                ptr = ptr.add(fast_write_u64_ptr(ptr, rec.bytes_tx as u64));

                // 15. `,"completion_reason":"`
                core::ptr::copy_nonoverlapping(
                    JSON_COMPLETION.as_ptr(),
                    ptr,
                    JSON_COMPLETION.len(),
                );
                ptr = ptr.add(JSON_COMPLETION.len());
                let comp = rec.completion.as_bytes();
                core::ptr::copy_nonoverlapping(comp.as_ptr(), ptr, comp.len());
                ptr = ptr.add(comp.len());

                // 16. `","grpc_status":"`
                core::ptr::copy_nonoverlapping(JSON_GRPC.as_ptr(), ptr, JSON_GRPC.len());
                ptr = ptr.add(JSON_GRPC.len());
                let grpc = rec.grpc_status.as_bytes();
                core::ptr::copy_nonoverlapping(grpc.as_ptr(), ptr, grpc.len());
                ptr = ptr.add(grpc.len());

                // 17. `","user_agent_family":"`
                core::ptr::copy_nonoverlapping(JSON_UA_FAMILY.as_ptr(), ptr, JSON_UA_FAMILY.len());
                ptr = ptr.add(JSON_UA_FAMILY.len());
                let fam = rec.ua_family.as_bytes();
                core::ptr::copy_nonoverlapping(fam.as_ptr(), ptr, fam.len());
                ptr = ptr.add(fam.len());

                // 18. `","user_agent_os":"`
                core::ptr::copy_nonoverlapping(JSON_UA_OS.as_ptr(), ptr, JSON_UA_OS.len());
                ptr = ptr.add(JSON_UA_OS.len());
                let os = rec.ua_os.as_bytes();
                core::ptr::copy_nonoverlapping(os.as_ptr(), ptr, os.len());
                ptr = ptr.add(os.len());

                // 19. `"}\n`
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

    #[test]
    fn test_bulk_ndjson_formatting() {
        let mut rec = TelemetryRecord::zeroed();
        rec.ingress_time_nanos = 1_732_011_000_123_456_789;
        rec.request_id = 42;
        rec.duration_ns = 12_500;
        rec.peer_ip = 0x0000_0000_0000_0000_0000_FFFF_C0A8_010A;
        rec.upstream_ip = 0x0A00_0001;
        rec.upstream_port = 8081;
        rec.route_id = 7;
        rec.status_code = 200;
        rec.pipe = PipeKind::BufferedH1H1;
        rec.method = HttpMethod::Get;
        rec.completion = CompletionReason::Complete;
        rec.grpc_status = GrpcStatus::Ok;
        rec.ua_family = UaFamily::Curl;
        rec.ua_os = UaOs::Linux;
        rec.set_url("/v1/items?a=\"1\"");

        let action = prebake_bulk_action_line(EsBulkOpType::Create, "velda-logs");
        let batch = TelemetryBatch::from_arc(Arc::new([rec]));
        let mut out = Vec::new();
        let count = format_bulk_ndjson(&[batch], &action, &mut out);
        assert_eq!(count, 1);

        let text = std::str::from_utf8(&out).unwrap();
        let mut lines = text.lines();
        assert_eq!(
            lines.next().unwrap(),
            r#"{"create":{"_index":"velda-logs"}}"#
        );
        let doc: serde_json::Value = serde_json::from_str(lines.next().unwrap()).unwrap();
        assert_eq!(doc["@timestamp"], 1_732_011_000_123u64);
        assert_eq!(doc["status_code"], 200);
        assert_eq!(doc["peer_ip"], "192.168.1.10");
        assert_eq!(doc["url"], "/v1/items?a=\"1\"");
    }
}
