//! Standard Stream Access Log Formatting Subsystem (`std_log_shipper::format`).
//!
//! ```text
//! ┌────────────────────────────────────────────────────────────────────────────────────────┐
//! │                              FORMATTING PIPELINE ARCHITECTURE                          │
//! │                                                                                        │
//! │  batches: &[Arc<[TelemetryRecord]>]                                                    │
//! │        │                                                                               │
//! │        ├─► Total Records Count Scan (O(batches)) ──► Pre-reserve out buffer capacity   │
//! │        │                                                                               │
//! │        ├── format == "json" ──► format_ndjson_records (format/json.rs)                 │
//! │        │                        ├─ Pre-baked NDJSON key chunks                         │
//! │        │                        ├─ 128-bit ReqId SSSE3 PSHUFB Hex Stamping (32 chars)  │
//! │        │                        └─ Zero-Alloc RFC 5952 IPv6 compression                │
//! │        │                                                                               │
//! │        ├── format == "clf"  ──► format_clf_records (format/text.rs)                    │
//! │        │                        └─ NCSA Common Log Format                              │
//! │        │                                                                               │
//! │        └── format == "text" ──► format_text_records (format/text.rs)                   │
//! │            (Extended)           ├─ Pre-baked Text chunks                               │
//! │                                 ├─ Division-free 2-digit ASCII LUT integer conversions │
//! │                                 └─ Multi-protocol dispatch (HTTP / gRPC / Raw L4)      │
//! └────────────────────────────────────────────────────────────────────────────────────────┘
//! ```

pub mod json;
pub mod text;

use velda_observability::TelemetryBatch;

pub use json::format_ndjson_records;
pub use text::{format_clf_records, format_text_records};

/// Formats an exact 20-character ISO-8601 UTC timestamp (`YYYY-MM-DDTHH:MM:SSZ`) into ASCII bytes
/// directly into `buf` using Howard Hinnant's branchless integer date algorithm (0 heap allocations).
#[inline(always)]
pub fn fast_write_iso8601_utc(buf: &mut Vec<u8>, ts: u32) {
    let sec = (ts % 60) as u8;
    let min = ((ts / 60) % 60) as u8;
    let hour = ((ts / 3600) % 24) as u8;
    let days = (ts / 86400) as i64;

    let z = days + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = (yoe as i64) + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };

    let mut out = [0u8; 20];
    out[0] = b'0' + ((y / 1000) % 10) as u8;
    out[1] = b'0' + ((y / 100) % 10) as u8;
    out[2] = b'0' + ((y / 10) % 10) as u8;
    out[3] = b'0' + (y % 10) as u8;
    out[4] = b'-';
    out[5] = b'0' + (m / 10) as u8;
    out[6] = b'0' + (m % 10) as u8;
    out[7] = b'-';
    out[8] = b'0' + (d / 10) as u8;
    out[9] = b'0' + (d % 10) as u8;
    out[10] = b'T';
    out[11] = b'0' + (hour / 10);
    out[12] = b'0' + (hour % 10);
    out[13] = b':';
    out[14] = b'0' + (min / 10);
    out[15] = b'0' + (min % 10);
    out[16] = b':';
    out[17] = b'0' + (sec / 10);
    out[18] = b'0' + (sec % 10);
    out[19] = b'Z';

    buf.extend_from_slice(&out);
}

/// Formats reference-counted 64-byte [`TelemetryRecord`] batches directly into bytes
/// by splicing static frame chunks and LUT-converted integers (zero runtime format parsing).
pub fn format_std_log_payload(
    batches: &[TelemetryBatch],
    format: &str,
    out: &mut Vec<u8>,
) -> usize {
    out.clear();
    let total_recs: usize = batches.iter().map(|b| b.len()).sum();
    if total_recs == 0 {
        return 0;
    }

    if format.eq_ignore_ascii_case("json") {
        json::format_ndjson_records(batches, total_recs, out);
    } else if format.eq_ignore_ascii_case("clf") {
        text::format_clf_records(batches, total_recs, out);
    } else {
        text::format_text_records(batches, total_recs, out);
    }

    total_recs
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use velda_core::{CompletionReason, GrpcStatus, HttpMethod, PipeKind};
    use velda_observability::TelemetryRecord;

    #[test]
    fn test_format_std_log_payload_all_modes() {
        let mut http_rec = TelemetryRecord::zeroed();
        http_rec.request_id = 0x1234_5678;
        http_rec.ingress_time_nanos = 1_710_000_000_000_000_000;
        http_rec.duration_ns = 250;
        http_rec.peer_ip = velda_core::lut::ip::encode_ip_u128("192.168.1.100".parse().unwrap());
        http_rec.upstream_ip =
            velda_core::lut::ip::encode_upstream_ip_u32("10.0.0.1".parse().unwrap());
        http_rec.upstream_port = 8080;
        http_rec.bytes_rx = 64;
        http_rec.bytes_tx = 512;
        http_rec.status_code = 200;
        http_rec.route_id = 1;
        http_rec.pipe = PipeKind::BufferedH1H1;
        http_rec.completion = CompletionReason::Complete;
        http_rec.method = HttpMethod::Get;
        http_rec.grpc_status = GrpcStatus::NonGrpc;
        let (fam, os, ver) = velda_core::lut::classify_user_agent(b"curl/8.6.0");
        http_rec.ua_family = fam;
        http_rec.ua_os = os;
        http_rec.ua_major_version = ver;

        let batch: Arc<[TelemetryRecord]> = Arc::from(vec![http_rec]);
        let batches = vec![TelemetryBatch::from_arc(batch)];

        // 1. Text Format
        let mut text_out = Vec::new();
        let count = format_std_log_payload(&batches, "text", &mut text_out);
        assert_eq!(count, 1);
        let text_str = String::from_utf8(text_out).unwrap();
        assert!(text_str.contains("192.168.1.100"));
        assert!(text_str.contains("duration_ns=250 pipe=buffered_h1_h1"));

        // 2. JSON Format
        let mut json_out = Vec::new();
        let count = format_std_log_payload(&batches, "json", &mut json_out);
        assert_eq!(count, 1);
        let json_str = String::from_utf8(json_out).unwrap();
        assert!(json_str.contains("\"pipe\":\"buffered_h1_h1\""));
        assert!(json_str.contains("\"duration_ns\":250"));
        assert!(!json_str.contains("\"downstream_protocol\""));
        assert!(!json_str.contains("\"upstream_protocol\""));
        assert!(json_str.contains("\"status\":200"));
        // 32-char hex request ID check (timestamp_nanos 1_710_000_000_000_000_000 = 0x17bb23f0a5eb0000, id = 0x12345678)
        assert!(json_str.contains("\"request_id\":\"17bb23f0a5eb00000000000012345678\""));

        // 3. CLF Format
        let mut clf_out = Vec::new();
        let count = format_std_log_payload(&batches, "clf", &mut clf_out);
        assert_eq!(count, 1);
        let clf_str = String::from_utf8(clf_out).unwrap();
        assert!(
            clf_str
                .contains("192.168.1.100 - - [2024-03-09T16:00:00Z] \"GET - http/1.1\" 200 512\n")
        );
    }
}
