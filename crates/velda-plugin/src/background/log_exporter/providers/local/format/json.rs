//! Unified NDJSON Access Log Formatting (`std_log_shipper::format::json`).
//!
//! Emits uniform NDJSON access log records across all protocols using canonical
//! `internal_status` (`snake_case`).

use velda_observability::{TelemetryBatch, TelemetryRecord};

use super::fast_write_iso8601_utc;
use velda_core::lut::decimal::fast_write_u64;
use velda_core::lut::ip::{fast_write_ip, fast_write_ipv4};
use velda_core::request_id::fast_write_hex32;

// Common header chunks
const JSON_PREFIX: &[u8] = b"{\"timestamp\":\"";
const JSON_TS_EPOCH: &[u8] = b"\",\"timestamp_epoch_sec\":";
const JSON_REQ_ID: &[u8] = b",\"request_id\":\"";
const JSON_CLIENT: &[u8] = b"\",\"client\":\"";
const JSON_ENDPOINT: &[u8] = b"\",\"endpoint\":\"";

const JSON_METHOD: &[u8] = b"\",\"method\":\"";
const JSON_URL: &[u8] = b"\",\"url\":\"";
const JSON_ROUTE: &[u8] = b"\",\"route\":\"";
const JSON_INTERNAL_STATUS: &[u8] = b"\",\"internal_status\":\"";
const JSON_DURATION_NS: &[u8] = b"\",\"duration_ns\":";
const JSON_BYTES_RX: &[u8] = b",\"bytes_rx\":";
const JSON_BYTES_TX: &[u8] = b",\"bytes_tx\":";
const JSON_USER_AGENT: &[u8] = b",\"user_agent\":\"";
const JSON_CLIENT_OS: &[u8] = b"\",\"client_os\":\"";
const JSON_PIPE: &[u8] = b"\",\"pipe\":\"";
const JSON_SUFFIX: &[u8] = b"\"}\n";

#[inline]
fn write_json_escaped(out: &mut Vec<u8>, s: &[u8]) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut start = 0;
    for (i, &b) in s.iter().enumerate() {
        let escaped = match b {
            b'"' | b'\\' => {
                out.extend_from_slice(&s[start..i]);
                out.push(b'\\');
                out.push(b);
                true
            }
            0x00..=0x1F => {
                out.extend_from_slice(&s[start..i]);
                out.extend_from_slice(b"\\u00");
                out.push(HEX[(b >> 4) as usize]);
                out.push(HEX[(b & 0x0f) as usize]);
                true
            }
            _ => false,
        };
        if escaped {
            start = i + 1;
        }
    }
    out.extend_from_slice(&s[start..]);
}

/// Formats batches of [`TelemetryRecord`] into APISIX-aligned NDJSON format directly into `out`.
///
/// Pre-reserves capacity upfront to guarantee 0 heap reallocations during formatting.
pub fn format_ndjson_records(batches: &[TelemetryBatch], total_recs: usize, out: &mut Vec<u8>) {
    out.reserve(total_recs * 320);

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
            let internal_status = rec.internal_status;
            let ua_fam = rec.ua_family;
            let ua_os = rec.ua_os;
            let method = rec.method;

            // 1. Common prefix: timestamp, request_id, client socket, endpoint
            let ts_sec = (rec.ingress_time_nanos / 1_000_000_000) as u32;
            out.extend_from_slice(JSON_PREFIX);
            fast_write_iso8601_utc(out, ts_sec);
            out.extend_from_slice(JSON_TS_EPOCH);
            fast_write_u64(out, ts_sec as u64);
            out.extend_from_slice(JSON_REQ_ID);
            fast_write_hex32(out, rec.ingress_time_nanos, rec.request_id);
            out.extend_from_slice(JSON_CLIENT);
            fast_write_ip(out, rec.peer_ip);
            out.extend_from_slice(JSON_ENDPOINT);
            if rec.upstream_ip != 0 {
                fast_write_ipv4(out, rec.upstream_ip);
                out.push(b':');
                fast_write_u64(out, rec.upstream_port as u64);
            } else {
                out.extend_from_slice(b"none");
            }

            // 2. Unified protocol-agnostic fields
            out.extend_from_slice(JSON_METHOD);
            out.extend_from_slice(method.as_str().as_bytes());
            out.extend_from_slice(JSON_URL);
            write_json_escaped(out, rec.url().as_bytes());
            out.extend_from_slice(JSON_ROUTE);
            velda_router::with_route_bytes(rec.route_id, |bytes| {
                out.extend_from_slice(bytes);
            });
            out.extend_from_slice(JSON_INTERNAL_STATUS);
            out.extend_from_slice(internal_status.as_str().as_bytes());
            out.extend_from_slice(JSON_DURATION_NS);
            fast_write_u64(out, rec.duration_ns);
            out.extend_from_slice(JSON_BYTES_RX);
            fast_write_u64(out, rec.bytes_rx as u64);
            out.extend_from_slice(JSON_BYTES_TX);
            fast_write_u64(out, rec.bytes_tx as u64);
            out.extend_from_slice(JSON_USER_AGENT);
            out.extend_from_slice(ua_fam.as_str().as_bytes());
            out.push(b'/');
            fast_write_u64(out, rec.ua_major_version as u64);
            out.extend_from_slice(JSON_CLIENT_OS);
            out.extend_from_slice(ua_os.as_str().as_bytes());
            out.extend_from_slice(JSON_PIPE);
            out.extend_from_slice(rec.pipe_name().as_bytes());
            out.extend_from_slice(JSON_SUFFIX);
        }
    }
}
