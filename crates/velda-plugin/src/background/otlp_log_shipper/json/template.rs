//! OpenTelemetry JSON Envelope & Record Template Formatter (`otlp_log_shipper::json::template`).
//!
//! ```text
//! ┌──────────────────────────────────────────────────────────────────────────────────────────────┐
//! │                            OTLP JSON TEMPLATE FRAMING ARCHITECTURE                           │
//! │                                                                                              │
//! │  1. Pre-Baked Static Prefix (~350 Bytes, baked once at new()):                               │
//! │  {"resourceLogs":[{"resource":{"attributes":[{"key":"service.name",...}]},                   │
//! │   "schemaUrl":"https://opentelemetry.io/schemas/1.26.0",                                     │
//! │   "scopeLogs":[{"scope":{"name":"velda.access_log",...},"logRecords":[                      │
//! └───────────────────────────────────────────────┬──────────────────────────────────────────────┘
//!                                                 │
//!        ┌────────────────────────────────────────┴───────────────────────────────────────┐
//!        ▼                                                                                ▼
//! ┌────────────────────────────────────────┐                            ┌────────────────────────────────────────┐
//! │ LogRecord Slot 1                       │ ──► [,] ──► ... ──►        │ LogRecord Slot N                       │
//! │ ├─ OTLP_K0: {"timeUnixNano":"          │                            │ ├─ OTLP_K0: {"timeUnixNano":"          │
//! │ ├─ ts_nano via fast_write_u64          │                            │ ├─ ts_nano via fast_write_u64          │
//! │ ├─ JSON_SEV_*_BODY_REQ: severity/body  │                            │ ├─ JSON_SEV_*_BODY_REQ: severity/body  │
//! │ ├─ body: "HTTP access log" (static)    │                            │ ├─ body: "HTTP access log" (static)    │
//! │ └─ attributes: 14 KeyValues (LUT/hex32)│                            │ └─ attributes: 14 KeyValues (LUT/hex32)│
//! └────────────────────────────────────────┘                            └────────────────────────────────────────┘
//!                                                 │
//!                                                 ▼
//! ┌──────────────────────────────────────────────────────────────────────────────────────────────┐
//! │  2. Pre-Baked Static Suffix (8 Bytes):                                                       │
//! │  ]}]}]}                                                                                      │
//! └──────────────────────────────────────────────────────────────────────────────────────────────┘
//! ```

use std::sync::Arc;
use velda_observability::{
    AccessLogRecord, LogGrpcStatus, LogHttpMethod, LogProtocol, UaFamily, UaOs,
};

use super::super::lut::{fast_write_hex32, fast_write_ip, fast_write_ts_nano, fast_write_u64};

/// Canonical OpenTelemetry Schema URL byte constant (zero heap allocation).
pub const OTLP_SCHEMA_URL: &[u8] = b"https://opentelemetry.io/schemas/1.26.0";

/// Pre-baked static outer envelope for OTLP JSON logs export.
/// Constructed once at plugin instantiation time (`new()`).
#[derive(Debug, Clone)]
pub struct OtlpJsonEnvelope {
    pub prefix: Vec<u8>,
    pub suffix: &'static [u8],
}

impl OtlpJsonEnvelope {
    /// Builds the pre-baked static JSON envelope with service name and canonical schema URL.
    pub fn new(service_name: &str) -> Self {
        let mut prefix = Vec::with_capacity(512);
        prefix.extend_from_slice(b"{\"resourceLogs\":[{\"resource\":{\"attributes\":[{\"key\":\"service.name\",\"value\":{\"stringValue\":\"");
        prefix.extend_from_slice(service_name.as_bytes());
        prefix.extend_from_slice(b"\"}},{\"key\":\"telemetry.sdk.name\",\"value\":{\"stringValue\":\"velda-edge\"}}]},\"schemaUrl\":\"");
        prefix.extend_from_slice(OTLP_SCHEMA_URL);
        prefix.extend_from_slice(b"\",\"scopeLogs\":[{\"scope\":{\"name\":\"velda.access_log\",\"version\":\"0.1.0\"},\"schemaUrl\":\"");
        prefix.extend_from_slice(OTLP_SCHEMA_URL);
        prefix.extend_from_slice(b"\",\"logRecords\":[");

        Self {
            prefix,
            suffix: b"]}]}]}",
        }
    }
}

// Pre-baked static JSON frame chunks for individual OTLP logRecord serialization
const OTLP_K0: &[u8] = b"{\"timeUnixNano\":\"";

// Pre-baked static chunks combining severity level/text, static log body ("HTTP access log"),
// and the request_id attribute prefix (zero dynamic string concatenation).
const JSON_SEV_INFO_BODY_REQ: &[u8] = b"\",\"severityNumber\":9,\"severityText\":\"INFO\",\"body\":{\"stringValue\":\"HTTP access log\"},\"attributes\":[{\"key\":\"request_id\",\"value\":{\"stringValue\":\"";
const JSON_SEV_WARN_BODY_REQ: &[u8] = b"\",\"severityNumber\":13,\"severityText\":\"WARN\",\"body\":{\"stringValue\":\"HTTP access log\"},\"attributes\":[{\"key\":\"request_id\",\"value\":{\"stringValue\":\"";
const JSON_SEV_ERROR_BODY_REQ: &[u8] = b"\",\"severityNumber\":17,\"severityText\":\"ERROR\",\"body\":{\"stringValue\":\"HTTP access log\"},\"attributes\":[{\"key\":\"request_id\",\"value\":{\"stringValue\":\"";

const OTLP_K6: &[u8] = b"\"}},{\"key\":\"network.protocol.name\",\"value\":{\"stringValue\":\"";
const OTLP_K7: &[u8] = b"\"}},{\"key\":\"http.request.method\",\"value\":{\"stringValue\":\"";
const OTLP_K8: &[u8] = b"\"}},{\"key\":\"http.response.status_code\",\"value\":{\"intValue\":\"";
const OTLP_K9: &[u8] = b"\"}},{\"key\":\"rpc.grpc.status_code\",\"value\":{\"stringValue\":\"";
const OTLP_K10: &[u8] = b"\"}},{\"key\":\"duration_us\",\"value\":{\"intValue\":\"";
const OTLP_K11: &[u8] = b"\"}},{\"key\":\"http.response.body.size\",\"value\":{\"intValue\":\"";
const OTLP_K12: &[u8] = b"\"}},{\"key\":\"http.request.body.size\",\"value\":{\"intValue\":\"";
const OTLP_K13: &[u8] = b"\"}},{\"key\":\"velda.route_id\",\"value\":{\"intValue\":\"";
const OTLP_K14: &[u8] = b"\"}},{\"key\":\"client.address\",\"value\":{\"stringValue\":\"";
const OTLP_K15: &[u8] = b"\"}},{\"key\":\"client.port\",\"value\":{\"intValue\":\"";
const OTLP_K16: &[u8] = b"\"}},{\"key\":\"user_agent.family\",\"value\":{\"stringValue\":\"";
const OTLP_K17: &[u8] = b"\"}},{\"key\":\"user_agent.major_version\",\"value\":{\"intValue\":\"";
const OTLP_K18: &[u8] = b"\"}},{\"key\":\"user_agent.os\",\"value\":{\"stringValue\":\"";
const OTLP_REC_END: &[u8] = b"\"}}]}";

/// Formats batches of reference-counted [`AccessLogRecord`] into standard OTLP JSON bytes directly.
///
/// Pre-reserves capacity to guarantee 0 heap reallocations during formatting.
pub fn format_otlp_records_json(
    batches: &[Arc<[AccessLogRecord]>],
    envelope: &OtlpJsonEnvelope,
    out: &mut Vec<u8>,
) -> usize {
    out.clear();
    let total_recs: usize = batches.iter().map(|b| b.len()).sum();
    if total_recs == 0 {
        return 0;
    }

    // Pre-reserve total buffer capacity: guaranteed 0 heap reallocations during runtime stamping
    out.reserve(envelope.prefix.len() + total_recs * 520 + envelope.suffix.len());

    let mut total_records = 0usize;

    for batch in batches {
        for rec in batch.iter() {
            if total_records == 0 {
                out.extend_from_slice(&envelope.prefix);
            } else {
                out.push(b',');
            }
            total_records += 1;

            let proto = LogProtocol::from_u8(rec.downstream_proto);
            let method = LogHttpMethod::from_u8(rec.method);
            let grpc_st = LogGrpcStatus::from_u8(rec.grpc_status);
            let ua_fam = UaFamily::from_u8(rec.ua_family);
            let ua_os = UaOs::from_u8(rec.ua_os);

            // Reconstruct full 128-bit RequestId: (high 64-bit timestamp, low 64-bit entropy)
            let req_hi = rec.timestamp_sec as u64;
            let req_lo = rec.request_id;

            // 1. Static header & timestamp
            out.extend_from_slice(OTLP_K0);
            fast_write_ts_nano(out, rec.timestamp_sec);

            // 2. Pre-baked Severity, static Body ("HTTP access log"), and request_id attribute prefix
            if rec.status_code >= 500
                || (grpc_st != LogGrpcStatus::Ok && grpc_st != LogGrpcStatus::NonGrpc)
            {
                out.extend_from_slice(JSON_SEV_ERROR_BODY_REQ);
            } else if rec.status_code >= 400 {
                out.extend_from_slice(JSON_SEV_WARN_BODY_REQ);
            } else {
                out.extend_from_slice(JSON_SEV_INFO_BODY_REQ);
            }

            // 3. Stamping request_id: exact 32-character lowercase hex via SSSE3 PSHUFB SIMD
            fast_write_hex32(out, req_hi, req_lo);

            // 4. Attributes:
            // Attr: network.protocol.name
            out.extend_from_slice(OTLP_K6);
            out.extend_from_slice(proto.as_str().as_bytes());

            // Attr: http.request.method
            out.extend_from_slice(OTLP_K7);
            out.extend_from_slice(method.as_str().as_bytes());

            // Attr: http.response.status_code
            out.extend_from_slice(OTLP_K8);
            fast_write_u64(out, rec.status_code as u64);

            // Attr: rpc.grpc.status_code
            out.extend_from_slice(OTLP_K9);
            out.extend_from_slice(grpc_st.as_str().as_bytes());

            // Attr: duration_us
            out.extend_from_slice(OTLP_K10);
            fast_write_u64(out, rec.duration_us as u64);

            // Attr: http.response.body.size (bytes_tx)
            out.extend_from_slice(OTLP_K11);
            fast_write_u64(out, rec.bytes_tx as u64);

            // Attr: http.request.body.size (bytes_rx)
            out.extend_from_slice(OTLP_K12);
            fast_write_u64(out, rec.bytes_rx as u64);

            // Attr: velda.route_id
            out.extend_from_slice(OTLP_K13);
            fast_write_u64(out, rec.route_id as u64);

            // Attr: client.address (RFC 4291 / RFC 5952 zero-alloc IP)
            out.extend_from_slice(OTLP_K14);
            fast_write_ip(out, rec.peer_ip);

            // Attr: client.port
            out.extend_from_slice(OTLP_K15);
            fast_write_u64(out, rec.peer_port as u64);

            // Attr: user_agent.family
            out.extend_from_slice(OTLP_K16);
            out.extend_from_slice(ua_fam.as_str().as_bytes());

            // Attr: user_agent.major_version
            out.extend_from_slice(OTLP_K17);
            fast_write_u64(out, rec.ua_major_version as u64);

            // Attr: user_agent.os
            out.extend_from_slice(OTLP_K18);
            out.extend_from_slice(ua_os.as_str().as_bytes());

            out.extend_from_slice(OTLP_REC_END);
        }
    }

    if total_records > 0 {
        out.extend_from_slice(envelope.suffix);
    }

    total_records
}

#[cfg(test)]
mod tests {
    use super::*;
    use velda_observability::PipeStrategy;

    #[test]
    fn test_format_otlp_records_json_schema_and_req_id() {
        let envelope = OtlpJsonEnvelope::new("edge-test");
        let rec = AccessLogRecord::from_scalars(
            0x4bf9_2f35_77b3_4da6,
            1_710_000_000,
            1500,
            "192.168.1.50:8080".parse().unwrap(),
            None,
            100,
            200,
            200,
            1,
            0,
            LogProtocol::H1,
            LogProtocol::H1,
            PipeStrategy::Buffered,
            velda_observability::LogCompletionReason::Complete,
            LogHttpMethod::Get,
            LogGrpcStatus::NonGrpc,
            b"curl/8.6.0",
        );
        let batches = vec![Arc::from(vec![rec])];
        let mut out = Vec::new();
        let count = format_otlp_records_json(&batches, &envelope, &mut out);
        assert_eq!(count, 1);

        let json_str = std::str::from_utf8(&out).expect("Valid UTF-8 JSON");
        assert!(json_str.contains("https://opentelemetry.io/schemas/1.26.0"));
        assert!(json_str.contains("\"service.name\",\"value\":{\"stringValue\":\"edge-test\"}"));
        assert!(json_str.contains("\"body\":{\"stringValue\":\"HTTP access log\"}"));
        assert!(
            json_str.contains("\"client.address\",\"value\":{\"stringValue\":\"192.168.1.50\"}")
        );
        // 32-char hex check
        assert!(json_str.contains("0000000065ec87804bf92f3577b34da6"));
    }
}
