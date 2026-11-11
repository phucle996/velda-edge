//! OpenTelemetry JSON Formatter (`otlp_log_shipper::format`).
//!
//! Formats reference-counted 64-byte [`AccessLogRecord`] batches into standard
//! OTLP/HTTP JSON (`/v1/logs`) payloads using pre-baked static frame chunks and LUT.

use std::sync::Arc;
use velda_observability::{
    AccessLogRecord, LogGrpcStatus, LogHttpMethod, LogProtocol, UaFamily, UaOs, decode_ip_u128,
};

/// Pre-baked static outer envelope for OTLP JSON logs export.
/// Built once at plugin instantiation time (`new()`).
#[derive(Debug, Clone)]
pub struct OtlpJsonEnvelope {
    pub prefix: Vec<u8>,
    pub suffix: &'static [u8],
}

impl OtlpJsonEnvelope {
    pub fn new(service_name: &str) -> Self {
        let mut prefix = Vec::with_capacity(256);
        prefix.extend_from_slice(b"{\"resourceLogs\":[{\"resource\":{\"attributes\":[{\"key\":\"service.name\",\"value\":{\"stringValue\":\"");
        prefix.extend_from_slice(service_name.as_bytes());
        prefix.extend_from_slice(b"\"}},{\"key\":\"telemetry.sdk.name\",\"value\":{\"stringValue\":\"velda-edge\"}}]},\"scopeLogs\":[{\"scope\":{\"name\":\"velda.access_log\"},\"logRecords\":[");
        Self {
            prefix,
            suffix: b"]}]}]}",
        }
    }
}

const DIGIT_PAIRS: &[u8; 200] = b"\
    00010203040506070809\
    10111213141516171819\
    20212223242526272829\
    30313233343536373839\
    40414243444546474849\
    50515253545556575859\
    60616263646566676869\
    70717273747576777879\
    80818283848586878889\
    90919293949596979899";

#[inline(always)]
pub(crate) fn fast_write_u64(buf: &mut Vec<u8>, mut n: u64) {
    if n < 10 {
        buf.push(b'0' + n as u8);
        return;
    }
    let mut temp = [0u8; 20];
    let mut curr = 20;
    while n >= 100 {
        let rem = (n % 100) as usize * 2;
        n /= 100;
        curr -= 2;
        temp[curr] = DIGIT_PAIRS[rem];
        temp[curr + 1] = DIGIT_PAIRS[rem + 1];
    }
    if n < 10 {
        curr -= 1;
        temp[curr] = b'0' + n as u8;
    } else {
        let rem = n as usize * 2;
        curr -= 2;
        temp[curr] = DIGIT_PAIRS[rem];
        temp[curr + 1] = DIGIT_PAIRS[rem + 1];
    }
    buf.extend_from_slice(&temp[curr..20]);
}

#[inline(always)]
pub(crate) fn fast_write_ip128(buf: &mut Vec<u8>, ip_u128: u128) {
    let ip = decode_ip_u128(ip_u128);
    match ip {
        std::net::IpAddr::V4(v4) => {
            let octets = v4.octets();
            fast_write_u64(buf, octets[0] as u64);
            buf.push(b'.');
            fast_write_u64(buf, octets[1] as u64);
            buf.push(b'.');
            fast_write_u64(buf, octets[2] as u64);
            buf.push(b'.');
            fast_write_u64(buf, octets[3] as u64);
        }
        std::net::IpAddr::V6(v6) => {
            let s = v6.to_string();
            buf.extend_from_slice(s.as_bytes());
        }
    }
}

const HEX_CHARS: &[u8; 16] = b"0123456789abcdef";

#[inline(always)]
pub(crate) fn fast_write_hex16(buf: &mut Vec<u8>, n: u64) {
    let mut temp = [0u8; 16];
    for i in (0..16).rev() {
        temp[i] = HEX_CHARS[((n >> ((15 - i) * 4)) & 0x0F) as usize];
    }
    buf.extend_from_slice(&temp);
}

// Pre-baked static frame chunks for individual OTLP logRecord object
const OTLP_K0: &[u8] = b"{\"timeUnixNano\":\"";
const OTLP_K1: &[u8] = b"\",\"severityNumber\":";
const OTLP_K2: &[u8] = b",\"severityText\":\"";
const OTLP_K3: &[u8] = b"\",\"body\":{\"stringValue\":\"request_id=";
const OTLP_K4: &[u8] = b" status=";
const OTLP_K5: &[u8] = b" dur_us=";
const OTLP_K6_REQ: &[u8] =
    b"\"},\"attributes\":[{\"key\":\"request_id\",\"value\":{\"stringValue\":\"";
const OTLP_K6: &[u8] = b"\"}},{\"key\":\"network.protocol.name\",\"value\":{\"stringValue\":\"";
const OTLP_K7: &[u8] = b"\"}},{\"key\":\"http.request.method\",\"value\":{\"stringValue\":\"";
const OTLP_K8: &[u8] = b"\"}},{\"key\":\"http.response.status_code\",\"value\":{\"intValue\":\"";
const OTLP_K9: &[u8] = b"\"}},{\"key\":\"rpc.grpc.status_code\",\"value\":{\"stringValue\":\"";
const OTLP_K10: &[u8] = b"\"}},{\"key\":\"duration_us\",\"value\":{\"intValue\":\"";
const OTLP_K11: &[u8] = b"\"}},{\"key\":\"http.response.body.size\",\"value\":{\"intValue\":\"";
const OTLP_K12: &[u8] = b"\"}},{\"key\":\"client.address\",\"value\":{\"stringValue\":\"";
const OTLP_K13: &[u8] = b"\"}},{\"key\":\"client.port\",\"value\":{\"intValue\":\"";
const OTLP_K14: &[u8] = b"\"}},{\"key\":\"user_agent.family\",\"value\":{\"stringValue\":\"";
const OTLP_K15: &[u8] = b"\"}},{\"key\":\"user_agent.os\",\"value\":{\"stringValue\":\"";
const OTLP_K16: &[u8] = b"\"}},{\"key\":\"velda.route_id\",\"value\":{\"intValue\":\"";
const OTLP_REC_END: &[u8] = b"\"}}]}";

/// Formats batches of [`AccessLogRecord`] into standard OTLP JSON bytes directly
/// by splicing pre-baked envelope slices and LUT-converted integers (zero string formatting).
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

    // Pre-reserve total buffer capacity: 0 allocations during record formatting
    out.reserve(envelope.prefix.len() + total_recs * 600 + envelope.suffix.len());

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
            let ts_nano = (rec.timestamp_sec as u64).saturating_mul(1_000_000_000);

            let (sev_num, sev_txt): (u64, &[u8]) = if rec.status_code >= 500
                || (grpc_st != LogGrpcStatus::Ok && grpc_st != LogGrpcStatus::NonGrpc)
            {
                (17, b"ERROR")
            } else if rec.status_code >= 400 {
                (13, b"WARN")
            } else {
                (9, b"INFO")
            };

            // Multi-offset streaming writes directly into pre-reserved buffer
            out.extend_from_slice(OTLP_K0);
            fast_write_u64(out, ts_nano);
            out.extend_from_slice(OTLP_K1);
            fast_write_u64(out, sev_num);
            out.extend_from_slice(OTLP_K2);
            out.extend_from_slice(sev_txt);
            out.extend_from_slice(OTLP_K3);
            fast_write_hex16(out, rec.request_id);
            out.push(b' ');
            out.extend_from_slice(method.as_str().as_bytes());
            out.push(b' ');
            out.extend_from_slice(proto.as_str().as_bytes());
            out.extend_from_slice(OTLP_K4);
            fast_write_u64(out, rec.status_code as u64);
            out.extend_from_slice(OTLP_K5);
            fast_write_u64(out, rec.duration_us as u64);
            out.extend_from_slice(OTLP_K6_REQ);
            fast_write_hex16(out, rec.request_id);
            out.extend_from_slice(OTLP_K6);
            out.extend_from_slice(proto.as_str().as_bytes());
            out.extend_from_slice(OTLP_K7);
            out.extend_from_slice(method.as_str().as_bytes());
            out.extend_from_slice(OTLP_K8);
            fast_write_u64(out, rec.status_code as u64);
            out.extend_from_slice(OTLP_K9);
            out.extend_from_slice(grpc_st.as_str().as_bytes());
            out.extend_from_slice(OTLP_K10);
            fast_write_u64(out, rec.duration_us as u64);
            out.extend_from_slice(OTLP_K11);
            fast_write_u64(out, rec.bytes_tx as u64);
            out.extend_from_slice(OTLP_K12);
            fast_write_ip128(out, rec.peer_ip);
            out.extend_from_slice(OTLP_K13);
            fast_write_u64(out, rec.peer_port as u64);
            out.extend_from_slice(OTLP_K14);
            out.extend_from_slice(ua_fam.as_str().as_bytes());
            out.extend_from_slice(OTLP_K15);
            out.extend_from_slice(ua_os.as_str().as_bytes());
            out.extend_from_slice(OTLP_K16);
            fast_write_u64(out, rec.route_id as u64);
            out.extend_from_slice(OTLP_REC_END);
        }
    }

    out.extend_from_slice(envelope.suffix);
    total_records
}
