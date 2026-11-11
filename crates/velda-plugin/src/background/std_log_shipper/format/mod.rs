//! Standard Stream Access Log Formatting Subsystem (`std_log_shipper::format`).
//!
//! ```text
//! ┌────────────────────────────────────────────────────────────────────────────────────────┐
//! │                              FORMATTING PIPELINE ARCHITECTURE                          │
//! │                                                                                        │
//! │  batches: &[Arc<[AccessLogRecord]>]                                                    │
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

use std::sync::Arc;
use velda_observability::AccessLogRecord;

pub use json::format_ndjson_records;
pub use text::{format_clf_records, format_text_records};

/// Formats reference-counted 64-byte [`AccessLogRecord`] batches directly into bytes
/// by splicing static frame chunks and LUT-converted integers (zero runtime format parsing).
pub fn format_std_log_payload(
    batches: &[Arc<[AccessLogRecord]>],
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
    use velda_observability::{
        LogCompletionReason, LogGrpcStatus, LogHttpMethod, LogProtocol, PipeStrategy,
    };

    #[test]
    fn test_format_std_log_payload_all_modes() {
        let http_rec = AccessLogRecord::from_scalars(
            0x1234_5678,
            1_710_000_000,
            250,
            "192.168.1.100:44321".parse().unwrap(),
            Some("10.0.0.1:8080".parse().unwrap()),
            64,
            512,
            200,
            1,
            0,
            LogProtocol::H1,
            LogProtocol::H1,
            PipeStrategy::Buffered,
            LogCompletionReason::Complete,
            LogHttpMethod::Get,
            LogGrpcStatus::NonGrpc,
            b"curl/8.6.0",
        );

        let batch: Arc<[AccessLogRecord]> = Arc::from(vec![http_rec]);
        let batches = vec![batch];

        // 1. Text Format
        let mut text_out = Vec::new();
        let count = format_std_log_payload(&batches, "text", &mut text_out);
        assert_eq!(count, 1);
        let text_str = String::from_utf8(text_out).unwrap();
        assert!(text_str.contains("192.168.1.100:44321"));
        assert!(text_str.contains("\"GET http/1.1\" 200 512 250us"));

        // 2. JSON Format
        let mut json_out = Vec::new();
        let count = format_std_log_payload(&batches, "json", &mut json_out);
        assert_eq!(count, 1);
        let json_str = String::from_utf8(json_out).unwrap();
        assert!(json_str.contains("\"proto\":\"http/1.1\""));
        assert!(json_str.contains("\"status\":200"));
        // 32-char hex request ID check (timestamp 1_710_000_000 = 0x65ec8780, id = 0x12345678)
        assert!(json_str.contains("\"request_id\":\"0000000065ec87800000000012345678\""));

        // 3. CLF Format
        let mut clf_out = Vec::new();
        let count = format_std_log_payload(&batches, "clf", &mut clf_out);
        assert_eq!(count, 1);
        let clf_str = String::from_utf8(clf_out).unwrap();
        assert!(clf_str.contains("192.168.1.100 - - [1710000000] \"GET http/1.1\" 200 512\n"));
    }
}
