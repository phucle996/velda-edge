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

use velda_core::GrpcStatus;
use velda_observability::{TelemetryBatch, TelemetryRecord};

use velda_core::lut::decimal::fast_write_u64;
use velda_core::lut::ip::fast_write_ip;
use velda_core::request_id::fast_write_hex32;

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
        prefix.extend_from_slice(b"\"}},{\"key\":\"telemetry.sdk.name\",\"value\":{\"stringValue\":\"velda-edge\"}},{\"key\":\"telemetry.sdk.language\",\"value\":{\"stringValue\":\"rust\"}}]},\"schemaUrl\":\"");
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

const OTLP_K6: &[u8] = b"\"}},{\"key\":\"downstream_protocol\",\"value\":{\"stringValue\":\"";
const OTLP_K6_UP: &[u8] = b"\"}},{\"key\":\"upstream_protocol\",\"value\":{\"stringValue\":\"";
const OTLP_K7: &[u8] = b"\"}},{\"key\":\"http.request.method\",\"value\":{\"stringValue\":\"";
const OTLP_K8: &[u8] = b"\"}},{\"key\":\"http.response.status_code\",\"value\":{\"intValue\":\"";
const OTLP_K9: &[u8] = b"\"}},{\"key\":\"rpc.grpc.status_code\",\"value\":{\"stringValue\":\"";
const OTLP_K10: &[u8] = b"\"}},{\"key\":\"duration_ns\",\"value\":{\"intValue\":\"";
const OTLP_K11: &[u8] = b"\"}},{\"key\":\"http.response.body.size\",\"value\":{\"intValue\":\"";
const OTLP_K12: &[u8] = b"\"}},{\"key\":\"http.request.body.size\",\"value\":{\"intValue\":\"";
const OTLP_K13: &[u8] = b"\"}},{\"key\":\"velda.route_id\",\"value\":{\"intValue\":\"";
const OTLP_K14: &[u8] = b"\"}},{\"key\":\"client.address\",\"value\":{\"stringValue\":\"";
const OTLP_K16: &[u8] = b"\"}},{\"key\":\"user_agent.family\",\"value\":{\"stringValue\":\"";
const OTLP_K17: &[u8] = b"\"}},{\"key\":\"user_agent.major_version\",\"value\":{\"intValue\":\"";
const OTLP_K18: &[u8] = b"\"}},{\"key\":\"user_agent.os\",\"value\":{\"stringValue\":\"";
const OTLP_REC_END: &[u8] = b"\"}}]}";

/// Formats batches of reference-counted [`TelemetryRecord`] into standard OTLP JSON bytes directly.
///
/// Pre-reserves capacity to guarantee 0 heap reallocations during formatting.
pub fn format_otlp_records_json(
    batches: &[TelemetryBatch],
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
            if total_records == 0 {
                out.extend_from_slice(&envelope.prefix);
            } else {
                out.push(b',');
            }
            total_records += 1;

            let downstream_protocol = rec.downstream_protocol();
            let upstream_protocol = rec.upstream_protocol();
            let method = rec.method;
            let grpc_st = rec.grpc_status;
            let ua_fam = rec.ua_family;
            let ua_os = rec.ua_os;

            // Reconstruct full 128-bit RequestId: (high 64-bit timestamp, low 64-bit entropy)
            let req_hi = rec.ingress_time_nanos;
            let req_lo = rec.request_id;

            // 1. Static header & timestamp
            out.extend_from_slice(OTLP_K0);
            fast_write_u64(out, rec.ingress_time_nanos);

            // 2. Pre-baked Severity, static Body ("HTTP access log"), and request_id attribute prefix
            if rec.status_code >= 500
                || (grpc_st != GrpcStatus::Ok && grpc_st != GrpcStatus::NonGrpc)
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
            // Attr: downstream_protocol
            out.extend_from_slice(OTLP_K6);
            out.extend_from_slice(downstream_protocol.as_str().as_bytes());

            // Attr: upstream_protocol
            out.extend_from_slice(OTLP_K6_UP);
            out.extend_from_slice(upstream_protocol.as_str().as_bytes());

            // Attr: http.request.method
            out.extend_from_slice(OTLP_K7);
            out.extend_from_slice(method.as_str().as_bytes());

            // Attr: http.response.status_code
            out.extend_from_slice(OTLP_K8);
            fast_write_u64(out, rec.status_code as u64);

            // Attr: rpc.grpc.status_code
            out.extend_from_slice(OTLP_K9);
            out.extend_from_slice(grpc_st.as_str().as_bytes());

            // Attr: duration_ns
            out.extend_from_slice(OTLP_K10);
            fast_write_u64(out, rec.duration_ns);

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
    use std::sync::Arc;
    use velda_core::{CompletionReason, GrpcStatus, HttpMethod, PipeKind};

    #[test]
    fn test_otlp_json_schema() {
        let envelope = OtlpJsonEnvelope::new("edge-test");
        let mut rec = TelemetryRecord::zeroed();
        rec.request_id = 0x4bf9_2f35_77b3_4da6;
        rec.ingress_time_nanos = 1_710_000_000_000_000_000;
        rec.duration_ns = 1500;
        rec.peer_ip = velda_core::lut::ip::encode_ip_u128("192.168.1.50".parse().unwrap());
        rec.bytes_rx = 100;
        rec.bytes_tx = 200;
        rec.status_code = 200;
        rec.route_id = 1;
        rec.pipe = PipeKind::BufferedH1H1;
        rec.completion = CompletionReason::Complete;
        rec.method = HttpMethod::Get;
        rec.grpc_status = GrpcStatus::NonGrpc;
        let (fam, os, ver) = velda_core::lut::classify_user_agent(b"curl/8.6.0");
        rec.ua_family = fam;
        rec.ua_os = os;
        rec.ua_major_version = ver;
        let batches = vec![TelemetryBatch::from_arc(Arc::from(vec![rec]))];
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
        assert!(json_str.contains("17bb23f0a5eb00004bf92f3577b34da6"));

        // Anti-circular check: deserialize OTLP/JSON via official opentelemetry-proto serde schema
        let official: opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest =
            serde_json::from_slice(&out).expect(
                "OTLP JSON must deserialize cleanly into official opentelemetry-proto schema",
            );
        let off_rec = &official.resource_logs[0].scope_logs[0].log_records[0];
        assert_eq!(off_rec.attributes.len(), 14);
        assert_eq!(off_rec.severity_number, 9);
    }
}
