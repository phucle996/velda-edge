//! Text & Common Log Format (CLF) Access Log Formatting (`std_log_shipper::format::text`).

use velda_observability::{TelemetryBatch, TelemetryRecord};

use super::fast_write_iso8601_utc;
use velda_core::lut::decimal::fast_write_u64;
use velda_core::lut::ip::{fast_write_ip, fast_write_ipv4};

// Pre-baked static text chunks
const TEXT_OPEN_BRACKET: &[u8] = b" [";
const TEXT_CLOSE_BRACKET_QUOTE: &[u8] = b"] \"";
const TEXT_QUOTE_SPACE: &[u8] = b"\" ";
const TEXT_DURATION_NS: &[u8] = b" duration_ns=";
const TEXT_PIPE: &[u8] = b" pipe=";
const TEXT_PATH: &[u8] = b" path=";
const TEXT_ROUTE: &[u8] = b" route=";
const TEXT_UPSTREAM: &[u8] = b" endpoint=";
const TEXT_UA: &[u8] = b" user_agent=\"";
const TEXT_INTERNAL_STATUS: &[u8] = b"\" internal_status=";
const TEXT_BYTES_RX: &[u8] = b" bytes_rx=";
const CLF_PREFIX: &[u8] = b" - - [";

/// Formats batches of [`TelemetryRecord`] into Extended Text format directly into `out`.
///
/// Pre-reserves buffer capacity to guarantee 0 heap reallocations.
pub fn format_text_records(batches: &[TelemetryBatch], total_recs: usize, out: &mut Vec<u8>) {
    out.reserve(total_recs * 340);

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
            let downstream_protocol = rec.downstream_protocol();
            let internal_status = rec.internal_status;
            let method = rec.method;
            let ua_fam = rec.ua_family;

            let ts_sec = (rec.ingress_time_nanos / 1_000_000_000) as u32;
            fast_write_ip(out, rec.peer_ip);
            out.extend_from_slice(TEXT_OPEN_BRACKET);
            fast_write_iso8601_utc(out, ts_sec);
            out.extend_from_slice(TEXT_CLOSE_BRACKET_QUOTE);

            out.extend_from_slice(method.as_str().as_bytes());
            out.push(b' ');
            if !rec.url().is_empty() {
                out.extend_from_slice(rec.url().as_bytes());
                out.push(b' ');
            } else {
                out.extend_from_slice(b"- ");
            }
            out.extend_from_slice(downstream_protocol.as_str().as_bytes());
            out.extend_from_slice(TEXT_QUOTE_SPACE);
            fast_write_u64(out, rec.bytes_tx as u64);
            out.extend_from_slice(TEXT_DURATION_NS);
            fast_write_u64(out, rec.duration_ns);
            out.extend_from_slice(TEXT_PIPE);
            out.extend_from_slice(rec.pipe_name().as_bytes());
            out.extend_from_slice(TEXT_PATH);
            out.extend_from_slice(rec.url().as_bytes());
            out.extend_from_slice(TEXT_BYTES_RX);
            fast_write_u64(out, rec.bytes_rx as u64);
            out.extend_from_slice(TEXT_ROUTE);
            velda_router::with_route_bytes(rec.route_id, |bytes| {
                out.extend_from_slice(bytes);
            });
            out.extend_from_slice(TEXT_UPSTREAM);
            fast_write_ipv4(out, rec.upstream_ip);
            out.push(b':');
            fast_write_u64(out, rec.upstream_port as u64);
            out.extend_from_slice(TEXT_UA);
            out.extend_from_slice(ua_fam.as_str().as_bytes());
            out.push(b'/');
            fast_write_u64(out, rec.ua_major_version as u64);
            out.extend_from_slice(TEXT_INTERNAL_STATUS);
            out.extend_from_slice(internal_status.as_str().as_bytes());
            out.push(b'\n');
        }
    }
}

/// Formats batches of [`TelemetryRecord`] into Standard Common Log Format (CLF) directly into `out`.
pub fn format_clf_records(batches: &[TelemetryBatch], total_recs: usize, out: &mut Vec<u8>) {
    out.reserve(total_recs * 160);

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
            let downstream_protocol = rec.downstream_protocol();
            let method = rec.method;

            let ts_sec = (rec.ingress_time_nanos / 1_000_000_000) as u32;
            fast_write_ip(out, rec.peer_ip);
            out.extend_from_slice(CLF_PREFIX);
            fast_write_iso8601_utc(out, ts_sec);
            out.extend_from_slice(TEXT_CLOSE_BRACKET_QUOTE);

            out.extend_from_slice(method.as_str().as_bytes());
            out.push(b' ');
            if !rec.url().is_empty() {
                out.extend_from_slice(rec.url().as_bytes());
                out.push(b' ');
            } else {
                out.extend_from_slice(b"- ");
            }
            out.extend_from_slice(downstream_protocol.as_str().as_bytes());
            out.extend_from_slice(TEXT_QUOTE_SPACE);
            out.extend_from_slice(rec.internal_status.as_bytes());
            out.push(b' ');
            fast_write_u64(out, rec.bytes_tx as u64);
            out.push(b'\n');
        }
    }
}
