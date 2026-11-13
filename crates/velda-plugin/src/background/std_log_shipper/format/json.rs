//! APISIX-Aligned NDJSON Access Log Formatting (`std_log_shipper::format::json`).
//!
//! Produces unambiguous, human-readable, and machine-parsable NDJSON lines strictly segregated
//! by protocol family (HTTP vs gRPC vs Raw L4), with baked unit suffixes (`"duration": "190us"`),
//! physical backend target (`"endpoint": "IP:port"`), and human route names looked up from
//! the global route catalog without hot-path overhead.

use std::sync::Arc;
use velda_observability::{
    AccessLogRecord, LogCompletionReason, LogGrpcStatus, LogHttpMethod, LogProtocol, UaFamily,
    UaOs, with_route_name,
};

use super::super::lut::{
    fast_write_hex32, fast_write_ip, fast_write_ipv4, fast_write_iso8601_utc, fast_write_ts_sec,
    fast_write_u64,
};

// Common header chunks
const JSON_PREFIX: &[u8] = b"{\"timestamp\":\"";
const JSON_TS_EPOCH: &[u8] = b"\",\"timestamp_epoch_sec\":";
const JSON_REQ_ID: &[u8] = b",\"request_id\":\"";
const JSON_CLIENT: &[u8] = b"\",\"client\":\"";
const JSON_ENDPOINT: &[u8] = b"\",\"endpoint\":\"";

// HTTP specific chunks
const JSON_HTTP_METHOD: &[u8] = b"\",\"method\":\"";
const JSON_URL: &[u8] = b"\",\"url\":\"";
const JSON_ROUTE: &[u8] = b"\",\"route\":\"";
const JSON_STATUS: &[u8] = b"\",\"status\":";
const JSON_DURATION_NS: &[u8] = b",\"duration_ns\":";
const JSON_BYTES_RX: &[u8] = b",\"bytes_rx\":";
const JSON_BYTES_TX: &[u8] = b",\"bytes_tx\":";
const JSON_USER_AGENT: &[u8] = b",\"user_agent\":\"";
const JSON_CLIENT_OS: &[u8] = b"\",\"client_os\":\"";
const JSON_PIPE: &[u8] = b"\",\"pipe\":\"";
const JSON_PIPE_RAW: &[u8] = b",\"pipe\":\"";
const JSON_COMPLETION: &[u8] = b"\",\"completion\":\"";
const JSON_SUFFIX: &[u8] = b"\"}\n";

// gRPC specific chunks
const JSON_GRPC_METHOD: &[u8] = b"\",\"method\":\"POST\",\"url\":\"";
const JSON_GRPC_STATUS: &[u8] = b"\",\"grpc_status\":\"";
const JSON_GRPC_STATUS_CODE: &[u8] = b"\",\"grpc_status_code\":";
const JSON_GRPC_HTTP_STATUS: &[u8] = b",\"status\":";
const JSON_RPC_MESSAGES: &[u8] = b",\"rpc_messages\":";

/// Formats batches of [`AccessLogRecord`] into APISIX-aligned NDJSON format directly into `out`.
///
/// Pre-reserves capacity upfront to guarantee 0 heap reallocations during formatting.
pub fn format_ndjson_records(
    batches: &[Arc<[AccessLogRecord]>],
    total_recs: usize,
    out: &mut Vec<u8>,
) {
    out.reserve(total_recs * 320);

    for batch in batches {
        for rec in batch.iter() {
            let downstream_protocol = rec.downstream_protocol();
            let completion = LogCompletionReason::from_u8(rec.completion);
            let ua_fam = UaFamily::from_u8(rec.ua_family);
            let ua_os = UaOs::from_u8(rec.ua_os);

            // 1. Common prefix: timestamp, request_id, client socket, endpoint
            out.extend_from_slice(JSON_PREFIX);
            fast_write_iso8601_utc(out, rec.timestamp_sec);
            out.extend_from_slice(JSON_TS_EPOCH);
            fast_write_ts_sec(out, rec.timestamp_sec);
            out.extend_from_slice(JSON_REQ_ID);
            fast_write_hex32(out, rec.timestamp_sec as u64, rec.request_id);
            out.extend_from_slice(JSON_CLIENT);
            fast_write_ip(out, rec.peer_ip);
            out.push(b':');
            fast_write_u64(out, rec.peer_port as u64);
            out.extend_from_slice(JSON_ENDPOINT);
            if rec.upstream_ip != 0 {
                fast_write_ipv4(out, rec.upstream_ip);
                out.push(b':');
                fast_write_u64(out, rec.upstream_port as u64);
            } else {
                out.extend_from_slice(b"none");
            }

            // 2. Protocol-Isolated Formatting
            match downstream_protocol {
                LogProtocol::H1 | LogProtocol::H2 | LogProtocol::H3 => {
                    let method = LogHttpMethod::from_u8(rec.method);
                    out.extend_from_slice(JSON_HTTP_METHOD);
                    out.extend_from_slice(method.as_str().as_bytes());
                    out.extend_from_slice(JSON_URL);
                    out.extend_from_slice(rec.url().as_bytes());
                    out.extend_from_slice(JSON_ROUTE);
                    with_route_name(rec.route_id, |name| {
                        out.extend_from_slice(name.as_bytes());
                    });
                    out.extend_from_slice(JSON_STATUS);
                    fast_write_u64(out, rec.status_code as u64);
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
                    out.extend_from_slice(JSON_COMPLETION);
                    out.extend_from_slice(completion.as_str().as_bytes());
                    out.extend_from_slice(JSON_SUFFIX);
                }
                LogProtocol::GrpcTcp | LogProtocol::GrpcUdp => {
                    let grpc_st = LogGrpcStatus::from_u8(rec.grpc_status);
                    out.extend_from_slice(JSON_GRPC_METHOD);
                    out.extend_from_slice(rec.url().as_bytes());
                    out.extend_from_slice(JSON_ROUTE);
                    with_route_name(rec.route_id, |name| {
                        out.extend_from_slice(name.as_bytes());
                    });
                    out.extend_from_slice(JSON_GRPC_STATUS);
                    out.extend_from_slice(grpc_st.as_str().as_bytes());
                    out.extend_from_slice(JSON_GRPC_STATUS_CODE);
                    fast_write_u64(out, rec.grpc_status as u64);
                    out.extend_from_slice(JSON_GRPC_HTTP_STATUS);
                    fast_write_u64(out, rec.status_code as u64);
                    out.extend_from_slice(JSON_RPC_MESSAGES);
                    fast_write_u64(out, rec.rpc_msgs_or_flags as u64);
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
                    out.extend_from_slice(JSON_COMPLETION);
                    out.extend_from_slice(completion.as_str().as_bytes());
                    out.extend_from_slice(JSON_SUFFIX);
                }
                LogProtocol::RawTcp | LogProtocol::RawUdp => {
                    out.extend_from_slice(JSON_ROUTE);
                    with_route_name(rec.route_id, |name| {
                        out.extend_from_slice(name.as_bytes());
                    });
                    out.extend_from_slice(b"\"");
                    out.extend_from_slice(JSON_DURATION_NS);
                    fast_write_u64(out, rec.duration_ns);
                    out.extend_from_slice(JSON_BYTES_RX);
                    fast_write_u64(out, rec.bytes_rx as u64);
                    out.extend_from_slice(JSON_BYTES_TX);
                    fast_write_u64(out, rec.bytes_tx as u64);
                    out.extend_from_slice(JSON_PIPE_RAW);
                    out.extend_from_slice(rec.pipe_name().as_bytes());
                    out.extend_from_slice(JSON_COMPLETION);
                    out.extend_from_slice(completion.as_str().as_bytes());
                    out.extend_from_slice(JSON_SUFFIX);
                }
            }
        }
    }
}
