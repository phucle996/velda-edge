//! Standard Stream Log Shipper Background Implementation (`std_log_shipper`).
//!
//! # Architectural Rationale & Mechanism
//! 1. **Protocol-Agnostic Single Log Sink**:
//!    Eliminates separate `std_http_access_log` and `std_grpc_access_log` plugins. A single
//!    background exporter formats records across HTTP/1.1, HTTP/2, HTTP/3, gRPC, and Raw L4.
//! 2. **Zero Hot-Path Work & Zero Queue Duplication**:
//!    The frontline request serving path does zero formatting or string allocations. It only emits
//!    the 64-byte [`AccessLogRecord`] into `velda_observability::LogStore`.
//! 3. **Reference-Counted Memory Management**:
//!    Drains `Arc<[AccessLogRecord]>` batches from `LogStore`. Once formatting into output bytes
//!    completes and the `Arc` goes out of scope, memory reclaims automatically when all exporters finish.

use std::future::Future;
use std::io::Write as _;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::io::AsyncWriteExt;
use tokio::sync::watch;
use velda_observability::{
    AccessLogRecord, LogCompletionReason, LogGrpcStatus, LogHttpMethod, LogProtocol, LogStore,
    PipeStrategy, UaFamily, UaOs, decode_ip_u128, global_log_store,
};
pub use velda_sync::StdLogShipperConfig;
use velda_sync::post_sync::plugin::PluginConfig;

use crate::background::BackgroundPlugin;
use crate::error::PluginError;

/// Output destination target for standard log records.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TargetStream {
    Stdout,
    Stderr,
    File(PathBuf),
}

impl TargetStream {
    pub fn parse(s: &str) -> Self {
        match s.trim().to_ascii_lowercase().as_str() {
            "stdout" => Self::Stdout,
            "stderr" => Self::Stderr,
            _ => Self::File(PathBuf::from(s.trim())),
        }
    }
}

/// Standalone Background Standard Stream Log Shipper Plugin.
pub struct StdLogShipperPlugin {
    pub id: String,
    pub config: StdLogShipperConfig,
    pub target: TargetStream,
    pub log_store: Arc<LogStore>,
    pub shipped_records: Arc<AtomicU64>,
}

impl StdLogShipperPlugin {
    pub fn with_store(
        id: impl Into<String>,
        config: StdLogShipperConfig,
        log_store: Arc<LogStore>,
    ) -> Result<Self, PluginError> {
        let target = TargetStream::parse(&config.target);
        Ok(Self {
            id: id.into(),
            config,
            target,
            log_store,
            shipped_records: Arc::new(AtomicU64::new(0)),
        })
    }

    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let validated = velda_sync::validate_std_log_shipper_config(&config.id, &config.config)
            .map_err(|e| PluginError::Config(e.to_string()))?;

        Self::with_store(&config.id, validated, global_log_store())
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

const HEX_CHARS: &[u8; 16] = b"0123456789abcdef";

#[inline(always)]
fn fast_write_u64(buf: &mut Vec<u8>, mut n: u64) {
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
fn fast_write_hex16(buf: &mut Vec<u8>, n: u64) {
    let mut temp = [0u8; 16];
    for i in (0..16).rev() {
        temp[i] = HEX_CHARS[((n >> ((15 - i) * 4)) & 0x0F) as usize];
    }
    buf.extend_from_slice(&temp);
}

#[inline(always)]
fn fast_write_ipv4(buf: &mut Vec<u8>, ip_bits: u32) {
    let octets = ip_bits.to_be_bytes();
    fast_write_u64(buf, octets[0] as u64);
    buf.push(b'.');
    fast_write_u64(buf, octets[1] as u64);
    buf.push(b'.');
    fast_write_u64(buf, octets[2] as u64);
    buf.push(b'.');
    fast_write_u64(buf, octets[3] as u64);
}

#[inline(always)]
fn fast_write_ip128(buf: &mut Vec<u8>, ip_u128: u128) {
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

// Pre-baked static frame chunks for JSON output
const JSON_K0: &[u8] = b"{\"request_id\":\"";
const JSON_K1: &[u8] = b"\",\"ts\":";
const JSON_K2: &[u8] = b",\"dur_us\":";
const JSON_K3: &[u8] = b",\"proto\":\"";
const JSON_K4: &[u8] = b"\",\"up_proto\":\"";
const JSON_K5: &[u8] = b"\",\"pipe\":\"";
const JSON_K6: &[u8] = b"\",\"method\":\"";
const JSON_K7: &[u8] = b"\",\"status\":";
const JSON_K8: &[u8] = b",\"grpc_status\":\"";
const JSON_K9: &[u8] = b"\",\"rx\":";
const JSON_K10: &[u8] = b",\"tx\":";
const JSON_K11: &[u8] = b",\"msgs\":";
const JSON_K12: &[u8] = b",\"peer\":\"";
const JSON_K13: &[u8] = b"\",\"upstream\":\"";
const JSON_K14: &[u8] = b"\",\"route_id\":";
const JSON_K15: &[u8] = b",\"ua\":\"";
const JSON_K16: &[u8] = b"\",\"os\":\"";
const JSON_K17: &[u8] = b"\",\"completion\":\"";
const JSON_END: &[u8] = b"\"}\n";

/// Formats reference-counted 64-byte [`AccessLogRecord`] batches directly into bytes
/// by splicing static frame chunks and LUT-converted integers (zero runtime format parsing).
pub fn format_std_log_payload(
    batches: &[Arc<[AccessLogRecord]>],
    format: &str,
    out: &mut Vec<u8>,
) -> usize {
    out.clear();
    let is_json = format.eq_ignore_ascii_case("json");
    let mut total_records = 0usize;

    for batch in batches {
        for rec in batch.iter() {
            total_records += 1;

            let proto = LogProtocol::from_u8(rec.downstream_proto);
            let up_proto = LogProtocol::from_u8(rec.upstream_proto);
            let pipe = PipeStrategy::from_discriminant(rec.pipe_strategy)
                .unwrap_or(PipeStrategy::Buffered);
            let completion = LogCompletionReason::from_u8(rec.completion);
            let method = LogHttpMethod::from_u8(rec.method);
            let grpc_st = LogGrpcStatus::from_u8(rec.grpc_status);
            let ua_fam = UaFamily::from_u8(rec.ua_family);
            let ua_os = UaOs::from_u8(rec.ua_os);

            if is_json {
                out.extend_from_slice(JSON_K0);
                fast_write_hex16(out, rec.request_id);
                out.extend_from_slice(JSON_K1);
                fast_write_u64(out, rec.timestamp_sec as u64);
                out.extend_from_slice(JSON_K2);
                fast_write_u64(out, rec.duration_us as u64);
                out.extend_from_slice(JSON_K3);
                out.extend_from_slice(proto.as_str().as_bytes());
                out.extend_from_slice(JSON_K4);
                out.extend_from_slice(up_proto.as_str().as_bytes());
                out.extend_from_slice(JSON_K5);
                out.extend_from_slice(pipe.as_str().as_bytes());
                out.extend_from_slice(JSON_K6);
                out.extend_from_slice(method.as_str().as_bytes());
                out.extend_from_slice(JSON_K7);
                fast_write_u64(out, rec.status_code as u64);
                out.extend_from_slice(JSON_K8);
                out.extend_from_slice(grpc_st.as_str().as_bytes());
                out.extend_from_slice(JSON_K9);
                fast_write_u64(out, rec.bytes_rx as u64);
                out.extend_from_slice(JSON_K10);
                fast_write_u64(out, rec.bytes_tx as u64);
                out.extend_from_slice(JSON_K11);
                fast_write_u64(out, rec.rpc_msgs_or_flags as u64);
                out.extend_from_slice(JSON_K12);
                fast_write_ip128(out, rec.peer_ip);
                out.push(b':');
                fast_write_u64(out, rec.peer_port as u64);
                out.extend_from_slice(JSON_K13);
                fast_write_ipv4(out, rec.upstream_ip);
                out.push(b':');
                fast_write_u64(out, rec.upstream_port as u64);
                out.extend_from_slice(JSON_K14);
                fast_write_u64(out, rec.route_id as u64);
                out.extend_from_slice(JSON_K15);
                out.extend_from_slice(ua_fam.as_str().as_bytes());
                out.push(b'/');
                fast_write_u64(out, rec.ua_major_version as u64);
                out.extend_from_slice(JSON_K16);
                out.extend_from_slice(ua_os.as_str().as_bytes());
                out.extend_from_slice(JSON_K17);
                out.extend_from_slice(completion.as_str().as_bytes());
                out.extend_from_slice(JSON_END);
            } else {
                // Text mode direct slice splicing
                fast_write_ip128(out, rec.peer_ip);
                out.push(b':');
                fast_write_u64(out, rec.peer_port as u64);
                out.extend_from_slice(b" [");
                fast_write_u64(out, rec.timestamp_sec as u64);
                out.extend_from_slice(b"] \"");

                match proto {
                    LogProtocol::H1 | LogProtocol::H2 | LogProtocol::H3 => {
                        out.extend_from_slice(method.as_str().as_bytes());
                        out.push(b' ');
                        out.extend_from_slice(proto.as_str().as_bytes());
                        out.extend_from_slice(b"\" ");
                        fast_write_u64(out, rec.status_code as u64);
                        out.push(b' ');
                        fast_write_u64(out, rec.bytes_tx as u64);
                        out.push(b' ');
                        fast_write_u64(out, rec.duration_us as u64);
                        out.extend_from_slice(b"us route=");
                        fast_write_u64(out, rec.route_id as u64);
                        out.extend_from_slice(b" upstream=");
                        fast_write_ipv4(out, rec.upstream_ip);
                        out.push(b':');
                        fast_write_u64(out, rec.upstream_port as u64);
                        out.extend_from_slice(b" ua=\"");
                        out.extend_from_slice(ua_fam.as_str().as_bytes());
                        out.push(b'/');
                        fast_write_u64(out, rec.ua_major_version as u64);
                        out.extend_from_slice(b"\" completion=");
                        out.extend_from_slice(completion.as_str().as_bytes());
                        out.push(b'\n');
                    }
                    LogProtocol::GrpcTcp | LogProtocol::GrpcUdp => {
                        out.extend_from_slice(b"POST ");
                        out.extend_from_slice(proto.as_str().as_bytes());
                        out.extend_from_slice(b"\" ");
                        fast_write_u64(out, rec.status_code as u64);
                        out.push(b' ');
                        fast_write_u64(out, rec.bytes_tx as u64);
                        out.push(b' ');
                        fast_write_u64(out, rec.duration_us as u64);
                        out.extend_from_slice(b"us grpc_status=");
                        out.extend_from_slice(grpc_st.as_str().as_bytes());
                        out.extend_from_slice(b" msgs=");
                        fast_write_u64(out, rec.rpc_msgs_or_flags as u64);
                        out.extend_from_slice(b" route=");
                        fast_write_u64(out, rec.route_id as u64);
                        out.extend_from_slice(b" upstream=");
                        fast_write_ipv4(out, rec.upstream_ip);
                        out.push(b':');
                        fast_write_u64(out, rec.upstream_port as u64);
                        out.extend_from_slice(b" ua=\"");
                        out.extend_from_slice(ua_fam.as_str().as_bytes());
                        out.push(b'/');
                        fast_write_u64(out, rec.ua_major_version as u64);
                        out.extend_from_slice(b"\" completion=");
                        out.extend_from_slice(completion.as_str().as_bytes());
                        out.push(b'\n');
                    }
                    LogProtocol::RawTcp | LogProtocol::RawUdp => {
                        out.extend_from_slice(proto.as_str().as_bytes());
                        out.extend_from_slice(b"\" ");
                        fast_write_u64(out, rec.status_code as u64);
                        out.extend_from_slice(b" rx=");
                        fast_write_u64(out, rec.bytes_rx as u64);
                        out.extend_from_slice(b" tx=");
                        fast_write_u64(out, rec.bytes_tx as u64);
                        out.push(b' ');
                        fast_write_u64(out, rec.duration_us as u64);
                        out.extend_from_slice(b"us route=");
                        fast_write_u64(out, rec.route_id as u64);
                        out.extend_from_slice(b" upstream=");
                        fast_write_ipv4(out, rec.upstream_ip);
                        out.push(b':');
                        fast_write_u64(out, rec.upstream_port as u64);
                        out.extend_from_slice(b" completion=");
                        out.extend_from_slice(completion.as_str().as_bytes());
                        out.push(b'\n');
                    }
                }
            }
        }
    }

    total_records
}

impl BackgroundPlugin for StdLogShipperPlugin {
    fn id(&self) -> &str {
        &self.id
    }

    fn run(
        self: Arc<Self>,
        mut shutdown: watch::Receiver<bool>,
    ) -> Pin<Box<dyn Future<Output = Result<(), PluginError>> + Send>> {
        Box::pin(async move {
            let subscriber = self.log_store.subscribe(256);
            let mut interval =
                tokio::time::interval(Duration::from_millis(self.config.flush_interval_ms.max(50)));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            let mut out_buf = Vec::with_capacity(16384);

            loop {
                let is_shutting_down = tokio::select! {
                    _ = interval.tick() => false,
                    _ = shutdown.changed() => true,
                };

                self.log_store.flush_all_lanes(self.config.max_batch_size);

                let batches = subscriber.drain_batches(64);
                let record_count =
                    format_std_log_payload(&batches, &self.config.format, &mut out_buf);
                drop(batches);

                if record_count > 0 && !out_buf.is_empty() {
                    match &self.target {
                        TargetStream::Stdout => {
                            let _ = std::io::stdout().write_all(&out_buf);
                            let _ = std::io::stdout().flush();
                        }
                        TargetStream::Stderr => {
                            let _ = std::io::stderr().write_all(&out_buf);
                            let _ = std::io::stderr().flush();
                        }
                        TargetStream::File(path) => {
                            if let Ok(mut f) = tokio::fs::OpenOptions::new()
                                .create(true)
                                .append(true)
                                .open(path)
                                .await
                            {
                                let _ = f.write_all(&out_buf).await;
                                let _ = f.flush().await;
                            }
                        }
                    }

                    self.shipped_records
                        .fetch_add(record_count as u64, Ordering::Relaxed);
                }

                if is_shutting_down {
                    break;
                }
            }

            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_std_log_payload_json_and_text() {
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

        let grpc_rec = AccessLogRecord::from_scalars(
            0x9999_8888,
            1_710_000_001,
            750,
            "192.168.1.101:50051".parse().unwrap(),
            Some("10.0.0.2:9090".parse().unwrap()),
            128,
            256,
            200,
            2,
            5,
            LogProtocol::GrpcTcp,
            LogProtocol::GrpcTcp,
            PipeStrategy::Duplex,
            LogCompletionReason::Complete,
            LogHttpMethod::Post,
            LogGrpcStatus::Ok,
            b"grpc-go/1.64.0",
        );

        let batch: Arc<[AccessLogRecord]> = Arc::from(vec![http_rec, grpc_rec]);
        let batches = vec![batch];

        // 1. Test Text Format
        let mut text_out = Vec::new();
        let count = format_std_log_payload(&batches, "text", &mut text_out);
        assert_eq!(count, 2);
        let text_str = String::from_utf8(text_out).unwrap();
        assert!(text_str.contains("192.168.1.100:44321"));
        assert!(text_str.contains("\"GET http/1.1\" 200 512 250us"));
        assert!(text_str.contains("grpc_status=OK msgs=5"));

        // 2. Test JSON Format
        let mut json_out = Vec::new();
        let count = format_std_log_payload(&batches, "json", &mut json_out);
        assert_eq!(count, 2);
        let json_str = String::from_utf8(json_out).unwrap();
        assert!(json_str.contains("\"proto\":\"http/1.1\""));
        assert!(json_str.contains("\"proto\":\"grpc/tcp\""));
        assert!(json_str.contains("\"grpc_status\":\"OK\""));
        assert!(json_str.contains("\"msgs\":5"));
    }
}
