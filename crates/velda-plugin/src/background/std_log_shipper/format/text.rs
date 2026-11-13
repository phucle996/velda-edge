//! Text & Common Log Format (CLF) Access Log Formatting (`std_log_shipper::format::text`).

use std::sync::Arc;
use velda_observability::{
    AccessLogRecord, LogCompletionReason, LogGrpcStatus, LogHttpMethod, LogProtocol, UaFamily,
};

use super::super::lut::{fast_write_ip, fast_write_ipv4, fast_write_iso8601_utc, fast_write_u64};

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
const TEXT_COMPLETION: &[u8] = b"\" completion=";
const TEXT_COMPLETION_RAW: &[u8] = b" completion=";
const TEXT_GRPC_STATUS: &[u8] = b" grpc_status=";
const TEXT_MSGS: &[u8] = b" rpc_messages=";
const TEXT_BYTES_RX: &[u8] = b" bytes_rx=";
const TEXT_BYTES_TX: &[u8] = b" bytes_tx=";
const TEXT_POST: &[u8] = b"POST ";
const CLF_PREFIX: &[u8] = b" - - [";

/// Formats batches of [`AccessLogRecord`] into Extended Text format directly into `out`.
///
/// Pre-reserves buffer capacity to guarantee 0 heap reallocations.
pub fn format_text_records(
    batches: &[Arc<[AccessLogRecord]>],
    total_recs: usize,
    out: &mut Vec<u8>,
) {
    out.reserve(total_recs * 340);

    for batch in batches {
        for rec in batch.iter() {
            let downstream_protocol = rec.downstream_protocol();
            let completion = LogCompletionReason::from_u8(rec.completion);
            let method = LogHttpMethod::from_u8(rec.method);
            let grpc_st = LogGrpcStatus::from_u8(rec.grpc_status);
            let ua_fam = UaFamily::from_u8(rec.ua_family);

            fast_write_ip(out, rec.peer_ip);
            out.push(b':');
            fast_write_u64(out, rec.peer_port as u64);
            out.extend_from_slice(TEXT_OPEN_BRACKET);
            fast_write_iso8601_utc(out, rec.timestamp_sec);
            out.extend_from_slice(TEXT_CLOSE_BRACKET_QUOTE);

            match downstream_protocol {
                LogProtocol::H1 | LogProtocol::H2 | LogProtocol::H3 => {
                    out.extend_from_slice(method.as_str().as_bytes());
                    out.push(b' ');
                    if !rec.path().is_empty() {
                        out.extend_from_slice(rec.path().as_bytes());
                        out.push(b' ');
                    }
                    out.extend_from_slice(downstream_protocol.as_str().as_bytes());
                    out.extend_from_slice(TEXT_QUOTE_SPACE);
                    fast_write_u64(out, rec.status_code as u64);
                    out.push(b' ');
                    fast_write_u64(out, rec.bytes_tx as u64);
                    out.extend_from_slice(TEXT_DURATION_NS);
                    fast_write_u64(out, rec.duration_ns);
                    out.extend_from_slice(TEXT_PIPE);
                    out.extend_from_slice(rec.pipe_name().as_bytes());
                    out.extend_from_slice(TEXT_PATH);
                    out.extend_from_slice(rec.path().as_bytes());
                    out.extend_from_slice(TEXT_ROUTE);
                    velda_observability::with_route_name(rec.route_id, |name| {
                        out.extend_from_slice(name.as_bytes());
                    });
                    out.extend_from_slice(TEXT_UPSTREAM);
                    fast_write_ipv4(out, rec.upstream_ip);
                    out.push(b':');
                    fast_write_u64(out, rec.upstream_port as u64);
                    out.extend_from_slice(TEXT_UA);
                    out.extend_from_slice(ua_fam.as_str().as_bytes());
                    out.push(b'/');
                    fast_write_u64(out, rec.ua_major_version as u64);
                    out.extend_from_slice(TEXT_COMPLETION);
                    out.extend_from_slice(completion.as_str().as_bytes());
                    out.push(b'\n');
                }
                LogProtocol::GrpcTcp | LogProtocol::GrpcUdp => {
                    out.extend_from_slice(TEXT_POST);
                    if !rec.path().is_empty() {
                        out.extend_from_slice(rec.path().as_bytes());
                        out.push(b' ');
                    }
                    out.extend_from_slice(downstream_protocol.as_str().as_bytes());
                    out.extend_from_slice(TEXT_QUOTE_SPACE);
                    fast_write_u64(out, rec.status_code as u64);
                    out.push(b' ');
                    fast_write_u64(out, rec.bytes_tx as u64);
                    out.extend_from_slice(TEXT_DURATION_NS);
                    fast_write_u64(out, rec.duration_ns);
                    out.extend_from_slice(TEXT_PIPE);
                    out.extend_from_slice(rec.pipe_name().as_bytes());
                    out.extend_from_slice(TEXT_PATH);
                    out.extend_from_slice(rec.path().as_bytes());
                    out.extend_from_slice(TEXT_GRPC_STATUS);
                    out.extend_from_slice(grpc_st.as_str().as_bytes());
                    out.extend_from_slice(TEXT_MSGS);
                    fast_write_u64(out, rec.rpc_msgs_or_flags as u64);
                    out.extend_from_slice(TEXT_ROUTE);
                    velda_observability::with_route_name(rec.route_id, |name| {
                        out.extend_from_slice(name.as_bytes());
                    });
                    out.extend_from_slice(TEXT_UPSTREAM);
                    fast_write_ipv4(out, rec.upstream_ip);
                    out.push(b':');
                    fast_write_u64(out, rec.upstream_port as u64);
                    out.extend_from_slice(TEXT_UA);
                    out.extend_from_slice(ua_fam.as_str().as_bytes());
                    out.push(b'/');
                    fast_write_u64(out, rec.ua_major_version as u64);
                    out.extend_from_slice(TEXT_COMPLETION);
                    out.extend_from_slice(completion.as_str().as_bytes());
                    out.push(b'\n');
                }
                LogProtocol::RawTcp | LogProtocol::RawUdp => {
                    out.extend_from_slice(downstream_protocol.as_str().as_bytes());
                    out.extend_from_slice(TEXT_QUOTE_SPACE);
                    fast_write_u64(out, rec.status_code as u64);
                    out.extend_from_slice(TEXT_DURATION_NS);
                    fast_write_u64(out, rec.duration_ns);
                    out.extend_from_slice(TEXT_PIPE);
                    out.extend_from_slice(rec.pipe_name().as_bytes());
                    out.extend_from_slice(TEXT_BYTES_RX);
                    fast_write_u64(out, rec.bytes_rx as u64);
                    out.extend_from_slice(TEXT_BYTES_TX);
                    fast_write_u64(out, rec.bytes_tx as u64);
                    out.extend_from_slice(TEXT_ROUTE);
                    velda_observability::with_route_name(rec.route_id, |name| {
                        out.extend_from_slice(name.as_bytes());
                    });
                    out.extend_from_slice(TEXT_UPSTREAM);
                    fast_write_ipv4(out, rec.upstream_ip);
                    out.push(b':');
                    fast_write_u64(out, rec.upstream_port as u64);
                    out.extend_from_slice(TEXT_COMPLETION_RAW);
                    out.extend_from_slice(completion.as_str().as_bytes());
                    out.push(b'\n');
                }
            }
        }
    }
}

/// Formats batches of [`AccessLogRecord`] into Standard Common Log Format (CLF) directly into `out`.
pub fn format_clf_records(
    batches: &[Arc<[AccessLogRecord]>],
    total_recs: usize,
    out: &mut Vec<u8>,
) {
    out.reserve(total_recs * 160);

    for batch in batches {
        for rec in batch.iter() {
            let downstream_protocol = rec.downstream_protocol();
            let method = LogHttpMethod::from_u8(rec.method);

            fast_write_ip(out, rec.peer_ip);
            out.extend_from_slice(CLF_PREFIX);
            fast_write_iso8601_utc(out, rec.timestamp_sec);
            out.extend_from_slice(TEXT_CLOSE_BRACKET_QUOTE);

            match downstream_protocol {
                LogProtocol::H1 | LogProtocol::H2 | LogProtocol::H3 => {
                    out.extend_from_slice(method.as_str().as_bytes());
                    out.push(b' ');
                    if !rec.path().is_empty() {
                        out.extend_from_slice(rec.path().as_bytes());
                        out.push(b' ');
                    }
                    out.extend_from_slice(downstream_protocol.as_str().as_bytes());
                    out.extend_from_slice(TEXT_QUOTE_SPACE);
                    fast_write_u64(out, rec.status_code as u64);
                    out.push(b' ');
                    fast_write_u64(out, rec.bytes_tx as u64);
                    out.push(b'\n');
                }
                LogProtocol::GrpcTcp | LogProtocol::GrpcUdp => {
                    out.extend_from_slice(TEXT_POST);
                    if !rec.path().is_empty() {
                        out.extend_from_slice(rec.path().as_bytes());
                        out.push(b' ');
                    }
                    out.extend_from_slice(downstream_protocol.as_str().as_bytes());
                    out.extend_from_slice(TEXT_QUOTE_SPACE);
                    fast_write_u64(out, rec.status_code as u64);
                    out.push(b' ');
                    fast_write_u64(out, rec.bytes_tx as u64);
                    out.push(b'\n');
                }
                LogProtocol::RawTcp | LogProtocol::RawUdp => {
                    out.extend_from_slice(downstream_protocol.as_str().as_bytes());
                    out.extend_from_slice(TEXT_QUOTE_SPACE);
                    fast_write_u64(out, rec.status_code as u64);
                    out.push(b' ');
                    fast_write_u64(out, rec.bytes_tx as u64);
                    out.push(b'\n');
                }
            }
        }
    }
}
