//! Text & Common Log Format (CLF) Serialization (`kafka_log_shipper::format::text`).
//!
//! Emits single-line text access logs with explicit protocol dispatch:
//! - HTTP (`http/1.1`, `http/2`, `http/3`): `"GET /path http/1.1" 200`
//! - gRPC (`grpc/tcp`, `grpc/udp`): `"POST /path grpc/tcp" 200 grpc_status=OK rpc_messages=0`
//! - Raw L4 (`raw/tcp`, `raw/udp`): `"raw/tcp" 200`

use velda_core::WireProtocol;
use velda_core::lut::decimal::{DIGIT_PAIRS, fast_write_u64};
use velda_core::lut::ip::{fast_write_ip, fast_write_ipv4};
use velda_observability::TelemetryRecord;

/// Formats an exact 20-character ISO-8601 UTC timestamp (`YYYY-MM-DDTHH:MM:SSZ`) into ASCII bytes
/// directly into `buf` using Howard Hinnant's branchless integer date algorithm and 2-digit decimal LUT.
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
    let d = (doy - (153 * mp + 2) / 5 + 1) as u8;
    let m = (if mp < 10 { mp + 3 } else { mp - 9 }) as u8;
    let y = (if m <= 2 { y + 1 } else { y }) as u32;

    let y1 = (y / 100) as u8;
    let y2 = (y % 100) as u8;

    let y1_idx = (y1 as usize) * 2;
    let y2_idx = (y2 as usize) * 2;
    let m_idx = (m as usize) * 2;
    let d_idx = (d as usize) * 2;
    let h_idx = (hour as usize) * 2;
    let min_idx = (min as usize) * 2;
    let s_idx = (sec as usize) * 2;

    let mut out = [0u8; 20];
    unsafe {
        let p = DIGIT_PAIRS.as_ptr();
        out[0] = *p.add(y1_idx);
        out[1] = *p.add(y1_idx + 1);
        out[2] = *p.add(y2_idx);
        out[3] = *p.add(y2_idx + 1);
        out[4] = b'-';
        out[5] = *p.add(m_idx);
        out[6] = *p.add(m_idx + 1);
        out[7] = b'-';
        out[8] = *p.add(d_idx);
        out[9] = *p.add(d_idx + 1);
        out[10] = b'T';
        out[11] = *p.add(h_idx);
        out[12] = *p.add(h_idx + 1);
        out[13] = b':';
        out[14] = *p.add(min_idx);
        out[15] = *p.add(min_idx + 1);
        out[16] = b':';
        out[17] = *p.add(s_idx);
        out[18] = *p.add(s_idx + 1);
        out[19] = b'Z';
    }

    buf.extend_from_slice(&out);
}

const TEXT_OPEN_BRACKET: &[u8] = b" [";
const TEXT_CLOSE_BRACKET_QUOTE: &[u8] = b"] \"";
const TEXT_QUOTE_SPACE: &[u8] = b"\" ";
const TEXT_DURATION_NS: &[u8] = b" duration_ns=";
const TEXT_PIPE: &[u8] = b" pipe=";
const TEXT_ROUTE: &[u8] = b" route=";
const TEXT_UPSTREAM: &[u8] = b" upstream=";
const TEXT_BYTES_RX: &[u8] = b" bytes_rx=";
const TEXT_BYTES_TX: &[u8] = b" bytes_tx=";
const TEXT_GRPC_STATUS: &[u8] = b" grpc_status=";
const TEXT_RPC_MSGS: &[u8] = b" rpc_messages=";
const TEXT_UA: &[u8] = b" ua=\"";
const TEXT_COMPLETION: &[u8] = b"\" completion=";
const TEXT_POST: &[u8] = b"POST ";

/// Formats an individual [`TelemetryRecord`] into a single-line text representation.
///
/// Automatically formats HTTP vs gRPC vs Raw L4 protocols explicitly.
pub fn format_single_record_text(rec: &TelemetryRecord, out: &mut Vec<u8>) {
    fast_write_ip(out, rec.peer_ip);
    out.extend_from_slice(TEXT_OPEN_BRACKET);
    fast_write_iso8601_utc(out, (rec.ingress_time_nanos / 1_000_000_000) as u32);
    out.extend_from_slice(TEXT_CLOSE_BRACKET_QUOTE);

    let downstream_protocol = rec.downstream_protocol();
    match downstream_protocol {
        WireProtocol::H1 | WireProtocol::H2 | WireProtocol::H3 => {
            out.extend_from_slice(rec.method.as_bytes());
            out.push(b' ');
            if !rec.url().is_empty() {
                out.extend_from_slice(rec.url().as_bytes());
                out.push(b' ');
            }
            out.extend_from_slice(downstream_protocol.as_bytes());
            out.extend_from_slice(TEXT_QUOTE_SPACE);
            fast_write_u64(out, rec.status_code as u64);
        }
        WireProtocol::GrpcTcp | WireProtocol::GrpcUdp => {
            out.extend_from_slice(TEXT_POST);
            if !rec.url().is_empty() {
                out.extend_from_slice(rec.url().as_bytes());
                out.push(b' ');
            }
            out.extend_from_slice(downstream_protocol.as_bytes());
            out.extend_from_slice(TEXT_QUOTE_SPACE);
            fast_write_u64(out, rec.status_code as u64);
            out.extend_from_slice(TEXT_GRPC_STATUS);
            out.extend_from_slice(rec.grpc_status.as_bytes());
            out.extend_from_slice(TEXT_RPC_MSGS);
            fast_write_u64(out, rec.rpc_msgs_or_flags as u64);
        }
        WireProtocol::RawTcp | WireProtocol::RawUdp => {
            out.extend_from_slice(downstream_protocol.as_bytes());
            out.extend_from_slice(TEXT_QUOTE_SPACE);
            fast_write_u64(out, rec.status_code as u64);
        }
    }

    out.extend_from_slice(TEXT_DURATION_NS);
    fast_write_u64(out, rec.duration_ns);

    out.extend_from_slice(TEXT_BYTES_RX);
    fast_write_u64(out, rec.bytes_rx as u64);

    out.extend_from_slice(TEXT_BYTES_TX);
    fast_write_u64(out, rec.bytes_tx as u64);

    out.extend_from_slice(TEXT_PIPE);
    out.extend_from_slice(rec.pipe.as_bytes());

    out.extend_from_slice(TEXT_ROUTE);
    fast_write_u64(out, rec.route_id as u64);

    out.extend_from_slice(TEXT_UPSTREAM);
    if rec.upstream_ip != 0 {
        fast_write_ipv4(out, rec.upstream_ip);
        out.push(b':');
        fast_write_u64(out, rec.upstream_port as u64);
    } else {
        out.extend_from_slice(b"none");
    }

    out.extend_from_slice(TEXT_UA);
    out.extend_from_slice(rec.ua_family.as_bytes());
    out.push(b'/');
    fast_write_u64(out, rec.ua_major_version as u64);

    out.extend_from_slice(TEXT_COMPLETION);
    out.extend_from_slice(rec.completion.as_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;
    use velda_core::{CompletionReason, GrpcStatus, HttpMethod, PipeKind, UaFamily, UaOs};

    #[test]
    fn test_format_single_record_text_http() {
        let mut rec = TelemetryRecord::zeroed();
        rec.ingress_time_nanos = 1_700_000_000_000_000_000;
        rec.request_id = 99;
        rec.duration_ns = 250_000;
        rec.peer_ip = 0x0000_0000_0000_0000_0000_FFFF_7F00_0001; // 127.0.0.1
        rec.status_code = 200;
        rec.pipe = PipeKind::BufferedH1H1;
        rec.method = HttpMethod::Get;
        rec.completion = CompletionReason::Complete;
        rec.grpc_status = GrpcStatus::Ok;
        rec.ua_family = UaFamily::Curl;
        rec.ua_os = UaOs::Linux;
        rec.set_url("/index.html");

        let mut out = Vec::new();
        format_single_record_text(&rec, &mut out);

        let s = String::from_utf8(out).unwrap();
        assert!(s.contains("127.0.0.1"));
        assert!(s.contains("\"GET /index.html http/1.1\" 200"));
        assert!(s.contains("duration_ns=250000"));
    }

    #[test]
    fn test_format_single_record_text_grpc() {
        let mut rec = TelemetryRecord::zeroed();
        rec.ingress_time_nanos = 1_700_000_000_000_000_000;
        rec.peer_ip = 0x0000_0000_0000_0000_0000_FFFF_7F00_0001;
        rec.status_code = 200;
        rec.pipe = PipeKind::BufferedGrpcTcpTcp;
        rec.method = HttpMethod::Post;
        rec.completion = CompletionReason::Complete;
        rec.grpc_status = GrpcStatus::ResourceExhausted;
        rec.rpc_msgs_or_flags = 5;
        rec.set_url("/package.Service/Method");

        let mut out = Vec::new();
        format_single_record_text(&rec, &mut out);

        let s = String::from_utf8(out).unwrap();
        assert!(s.contains("\"POST /package.Service/Method grpc/tcp\" 200"));
        assert!(s.contains("grpc_status=RESOURCE_EXHAUSTED"));
        assert!(s.contains("rpc_messages=5"));
    }
}
