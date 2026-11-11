//! Grafana Loki Log Shipper Background Implementation (`loki_log_shipper`).
//!
//! # Architectural Rationale & Mechanism
//! 1. **Unified Multi-Stream Export Across Protocol Families**:
//!    Rather than instantiating separate background exporters per protocol (which would multiply
//!    socket pools, timers, and HTTP POST handshakes), a single `LokiLogShipperPlugin` subscribes
//!    to [`velda_observability::LogStore`] and partitions 64-byte [`AccessLogRecord`] structs into
//!    3 bounded Loki stream buckets (`protocol="http"`, `protocol="grpc"`, `protocol="raw"`) inside
//!    a single `/loki/api/v1/push` request.
//! 2. **Bounded Label Cardinality**:
//!    Loki indexes only stream labels (`service`, `protocol`, plus static `labels`). Dynamic
//!    per-request fields (`request_id`, `peer_ip`, `status_code`, `ua_family`, `duration_us`) are
//!    serialized into the log line payload, preventing Loki index cardinality explosion.
//! 3. **Automatic Reference-Counted Memory Reclamation**:
//!    Each drained batch is an `Arc<[AccessLogRecord]>`. Once `format_loki_push_payload` finishes
//!    reading the integer fields and the `Vec<Arc<[AccessLogRecord]>>` goes out of scope, Rust
//!    decrements the `Arc` strong count and deallocates the batch as soon as the last active
//!    exporter completes.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::watch;
use velda_observability::{
    AccessLogRecord, LogCompletionReason, LogGrpcStatus, LogHttpMethod, LogProtocol, LogStore,
    PipeStrategy, UaFamily, UaOs, decode_ip_u128, global_log_store,
};
pub use velda_sync::LokiLogShipperConfig;
use velda_sync::post_sync::plugin::PluginConfig;

use crate::background::BackgroundPlugin;
use crate::error::PluginError;

/// Pre-baked static stream headers for Grafana Loki Push API partitioned by protocol family.
#[derive(Debug, Clone)]
pub struct LokiStreamTemplate {
    pub http_prefix: Vec<u8>,
    pub grpc_prefix: Vec<u8>,
    pub raw_prefix: Vec<u8>,
}

impl LokiStreamTemplate {
    pub fn new(service_name: &str, static_labels: &HashMap<String, String>) -> Self {
        fn build_header(
            service_name: &str,
            protocol: &str,
            static_labels: &HashMap<String, String>,
        ) -> Vec<u8> {
            let mut buf = Vec::with_capacity(128);
            buf.extend_from_slice(b"{\"stream\":{\"service\":\"");
            buf.extend_from_slice(service_name.as_bytes());
            buf.extend_from_slice(b"\",\"protocol\":\"");
            buf.extend_from_slice(protocol.as_bytes());
            buf.push(b'"');
            for (k, v) in static_labels {
                buf.extend_from_slice(b",\"");
                buf.extend_from_slice(k.as_bytes());
                buf.extend_from_slice(b"\":\"");
                buf.extend_from_slice(v.as_bytes());
                buf.push(b'"');
            }
            buf.extend_from_slice(b"},\"values\":[");
            buf
        }

        Self {
            http_prefix: build_header(service_name, "http", static_labels),
            grpc_prefix: build_header(service_name, "grpc", static_labels),
            raw_prefix: build_header(service_name, "raw", static_labels),
        }
    }
}

/// Standalone Background Grafana Loki Log Shipper Plugin.
pub struct LokiLogShipperPlugin {
    pub id: String,
    pub config: LokiLogShipperConfig,
    pub log_store: Arc<LogStore>,
    pub host: String,
    pub port: u16,
    pub path: String,
    pub host_header: String,
    pub header_prefix: Vec<u8>,
    pub stream_template: LokiStreamTemplate,
    pub shipped_records: Arc<AtomicU64>,
    pub failed_pushes: Arc<AtomicU64>,
}

impl LokiLogShipperPlugin {
    pub fn with_store(
        id: impl Into<String>,
        config: LokiLogShipperConfig,
        log_store: Arc<LogStore>,
    ) -> Result<Self, PluginError> {
        let trimmed = config.endpoint.trim();
        let rest = if let Some(s) = trimmed.strip_prefix("http://") {
            s
        } else if let Some(s) = trimmed.strip_prefix("https://") {
            s
        } else {
            return Err(PluginError::Config(format!(
                "Loki endpoint '{}' must start with http:// or https://",
                config.endpoint
            )));
        };

        let (authority, path_part) = match rest.find('/') {
            Some(idx) => (&rest[..idx], &rest[idx..]),
            None => (rest, "/loki/api/v1/push"),
        };

        if authority.is_empty() {
            return Err(PluginError::Config(format!(
                "Loki endpoint '{}' is missing host authority",
                config.endpoint
            )));
        }

        let (host, port) = match authority.rfind(':') {
            Some(idx) if !authority.ends_with(']') => {
                let h = authority[..idx].trim_matches(|c| c == '[' || c == ']');
                let p = authority[idx + 1..].parse::<u16>().map_err(|_| {
                    PluginError::Config(format!(
                        "Invalid port in Loki endpoint '{}'",
                        config.endpoint
                    ))
                })?;
                (h.to_string(), p)
            }
            _ => {
                let h = authority.trim_matches(|c| c == '[' || c == ']');
                (h.to_string(), 3100)
            }
        };

        let path = if path_part.is_empty() {
            "/loki/api/v1/push".to_string()
        } else {
            path_part.to_string()
        };

        let host_header = if port == 80 {
            host.clone()
        } else {
            format!("{host}:{port}")
        };

        let mut header_prefix = Vec::with_capacity(256);
        header_prefix.extend_from_slice(b"POST ");
        header_prefix.extend_from_slice(path.as_bytes());
        header_prefix.extend_from_slice(b" HTTP/1.1\r\nHost: ");
        header_prefix.extend_from_slice(host_header.as_bytes());
        header_prefix
            .extend_from_slice(b"\r\nContent-Type: application/json\r\nConnection: keep-alive\r\n");
        if !config.tenant_id.is_empty() {
            header_prefix.extend_from_slice(b"X-Scope-OrgID: ");
            header_prefix.extend_from_slice(config.tenant_id.as_bytes());
            header_prefix.extend_from_slice(b"\r\n");
        }
        header_prefix.extend_from_slice(b"Content-Length: ");

        let stream_template = LokiStreamTemplate::new(&config.service_name, &config.labels);

        Ok(Self {
            id: id.into(),
            config,
            log_store,
            host,
            port,
            path,
            host_header,
            header_prefix,
            stream_template,
            shipped_records: Arc::new(AtomicU64::new(0)),
            failed_pushes: Arc::new(AtomicU64::new(0)),
        })
    }

    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let validated = velda_sync::validate_loki_log_shipper_config(&config.id, &config.config)
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
fn fast_write_ip128(buf: &mut Vec<u8>, ip: u128) {
    match decode_ip_u128(ip) {
        std::net::IpAddr::V4(v4) => {
            fast_write_ipv4(buf, u32::from(v4));
        }
        std::net::IpAddr::V6(v6) => {
            let mut str_tmp = [0u8; 40];
            let mut cursor = std::io::Cursor::new(&mut str_tmp[..]);
            use std::io::Write as _;
            let _ = write!(cursor, "{v6}");
            let pos = cursor.position() as usize;
            buf.extend_from_slice(&str_tmp[..pos]);
        }
    }
}

/// Serializes reference-counted 64-byte [`AccessLogRecord`] batches into a multi-stream
/// Grafana Loki `/loki/api/v1/push` JSON payload partitioned by protocol family (`http`, `grpc`, `raw`).
pub fn format_loki_push_payload(
    batches: &[Arc<[AccessLogRecord]>],
    template: &LokiStreamTemplate,
    out: &mut Vec<u8>,
) -> usize {
    out.clear();

    let mut http_values = Vec::with_capacity(4096);
    let mut grpc_values = Vec::with_capacity(2048);
    let mut raw_values = Vec::with_capacity(1024);
    let mut total_records = 0usize;

    for batch in batches {
        for rec in batch.iter() {
            total_records += 1;
            let proto = LogProtocol::from_u8(rec.downstream_proto);
            let up_proto = LogProtocol::from_u8(rec.upstream_proto);
            let pipe = PipeStrategy::from_discriminant(rec.pipe_strategy)
                .unwrap_or(PipeStrategy::Buffered);
            let completion = LogCompletionReason::from_u8(rec.completion);
            let ts_ns = (rec.timestamp_sec as u64).saturating_mul(1_000_000_000);

            match proto {
                LogProtocol::H1 | LogProtocol::H2 | LogProtocol::H3 => {
                    if !http_values.is_empty() {
                        http_values.push(b',');
                    }
                    let method = LogHttpMethod::from_u8(rec.method);
                    let ua_fam = UaFamily::from_u8(rec.ua_family);
                    let ua_os = UaOs::from_u8(rec.ua_os);

                    http_values.extend_from_slice(b"[\"");
                    fast_write_u64(&mut http_values, ts_ns);
                    http_values.extend_from_slice(b"\",\"request_id=");
                    fast_write_hex16(&mut http_values, rec.request_id);
                    http_values.extend_from_slice(b" proto=");
                    http_values.extend_from_slice(proto.as_str().as_bytes());
                    http_values.extend_from_slice(b" up_proto=");
                    http_values.extend_from_slice(up_proto.as_str().as_bytes());
                    http_values.extend_from_slice(b" pipe=");
                    http_values.extend_from_slice(pipe.as_str().as_bytes());
                    http_values.extend_from_slice(b" method=");
                    http_values.extend_from_slice(method.as_str().as_bytes());
                    http_values.extend_from_slice(b" status=");
                    fast_write_u64(&mut http_values, rec.status_code as u64);
                    http_values.extend_from_slice(b" route_id=");
                    fast_write_u64(&mut http_values, rec.route_id as u64);
                    http_values.extend_from_slice(b" dur_us=");
                    fast_write_u64(&mut http_values, rec.duration_us as u64);
                    http_values.extend_from_slice(b" rx=");
                    fast_write_u64(&mut http_values, rec.bytes_rx as u64);
                    http_values.extend_from_slice(b" tx=");
                    fast_write_u64(&mut http_values, rec.bytes_tx as u64);
                    http_values.extend_from_slice(b" peer=");
                    fast_write_ip128(&mut http_values, rec.peer_ip);
                    http_values.push(b':');
                    fast_write_u64(&mut http_values, rec.peer_port as u64);
                    http_values.extend_from_slice(b" upstream=");
                    fast_write_ipv4(&mut http_values, rec.upstream_ip);
                    http_values.push(b':');
                    fast_write_u64(&mut http_values, rec.upstream_port as u64);
                    http_values.extend_from_slice(b" ua=");
                    http_values.extend_from_slice(ua_fam.as_str().as_bytes());
                    http_values.push(b'/');
                    fast_write_u64(&mut http_values, rec.ua_major_version as u64);
                    http_values.extend_from_slice(b" os=");
                    http_values.extend_from_slice(ua_os.as_str().as_bytes());
                    http_values.extend_from_slice(b" completion=");
                    http_values.extend_from_slice(completion.as_str().as_bytes());
                    http_values.extend_from_slice(b"\"]");
                }
                LogProtocol::GrpcTcp | LogProtocol::GrpcUdp => {
                    if !grpc_values.is_empty() {
                        grpc_values.push(b',');
                    }
                    let grpc_st = LogGrpcStatus::from_u8(rec.grpc_status);
                    let ua_fam = UaFamily::from_u8(rec.ua_family);

                    grpc_values.extend_from_slice(b"[\"");
                    fast_write_u64(&mut grpc_values, ts_ns);
                    grpc_values.extend_from_slice(b"\",\"request_id=");
                    fast_write_hex16(&mut grpc_values, rec.request_id);
                    grpc_values.extend_from_slice(b" proto=");
                    grpc_values.extend_from_slice(proto.as_str().as_bytes());
                    grpc_values.extend_from_slice(b" grpc_status=");
                    grpc_values.extend_from_slice(grpc_st.as_str().as_bytes());
                    grpc_values.extend_from_slice(b" http_status=");
                    fast_write_u64(&mut grpc_values, rec.status_code as u64);
                    grpc_values.extend_from_slice(b" route_id=");
                    fast_write_u64(&mut grpc_values, rec.route_id as u64);
                    grpc_values.extend_from_slice(b" dur_us=");
                    fast_write_u64(&mut grpc_values, rec.duration_us as u64);
                    grpc_values.extend_from_slice(b" rx=");
                    fast_write_u64(&mut grpc_values, rec.bytes_rx as u64);
                    grpc_values.extend_from_slice(b" tx=");
                    fast_write_u64(&mut grpc_values, rec.bytes_tx as u64);
                    grpc_values.extend_from_slice(b" msgs=");
                    fast_write_u64(&mut grpc_values, rec.rpc_msgs_or_flags as u64);
                    grpc_values.extend_from_slice(b" peer=");
                    fast_write_ip128(&mut grpc_values, rec.peer_ip);
                    grpc_values.push(b':');
                    fast_write_u64(&mut grpc_values, rec.peer_port as u64);
                    grpc_values.extend_from_slice(b" upstream=");
                    fast_write_ipv4(&mut grpc_values, rec.upstream_ip);
                    grpc_values.push(b':');
                    fast_write_u64(&mut grpc_values, rec.upstream_port as u64);
                    grpc_values.extend_from_slice(b" ua=");
                    grpc_values.extend_from_slice(ua_fam.as_str().as_bytes());
                    grpc_values.push(b'/');
                    fast_write_u64(&mut grpc_values, rec.ua_major_version as u64);
                    grpc_values.extend_from_slice(b" completion=");
                    grpc_values.extend_from_slice(completion.as_str().as_bytes());
                    grpc_values.extend_from_slice(b"\"]");
                }
                LogProtocol::RawTcp | LogProtocol::RawUdp => {
                    if !raw_values.is_empty() {
                        raw_values.push(b',');
                    }
                    raw_values.extend_from_slice(b"[\"");
                    fast_write_u64(&mut raw_values, ts_ns);
                    raw_values.extend_from_slice(b"\",\"request_id=");
                    fast_write_hex16(&mut raw_values, rec.request_id);
                    raw_values.extend_from_slice(b" proto=");
                    raw_values.extend_from_slice(proto.as_str().as_bytes());
                    raw_values.extend_from_slice(b" dur_us=");
                    fast_write_u64(&mut raw_values, rec.duration_us as u64);
                    raw_values.extend_from_slice(b" rx=");
                    fast_write_u64(&mut raw_values, rec.bytes_rx as u64);
                    raw_values.extend_from_slice(b" tx=");
                    fast_write_u64(&mut raw_values, rec.bytes_tx as u64);
                    raw_values.extend_from_slice(b" peer=");
                    fast_write_ip128(&mut raw_values, rec.peer_ip);
                    raw_values.push(b':');
                    fast_write_u64(&mut raw_values, rec.peer_port as u64);
                    raw_values.extend_from_slice(b" upstream=");
                    fast_write_ipv4(&mut raw_values, rec.upstream_ip);
                    raw_values.push(b':');
                    fast_write_u64(&mut raw_values, rec.upstream_port as u64);
                    raw_values.extend_from_slice(b" completion=");
                    raw_values.extend_from_slice(completion.as_str().as_bytes());
                    raw_values.extend_from_slice(b"\"]");
                }
            }
        }
    }

    if total_records == 0 {
        return 0;
    }

    out.extend_from_slice(b"{\"streams\":[");
    let mut first_stream = true;

    if !http_values.is_empty() {
        first_stream = false;
        out.extend_from_slice(&template.http_prefix);
        out.extend_from_slice(&http_values);
        out.extend_from_slice(b"]}");
    }

    if !grpc_values.is_empty() {
        if !first_stream {
            out.push(b',');
        }
        first_stream = false;
        out.extend_from_slice(&template.grpc_prefix);
        out.extend_from_slice(&grpc_values);
        out.extend_from_slice(b"]}");
    }

    if !raw_values.is_empty() {
        if !first_stream {
            out.push(b',');
        }
        out.extend_from_slice(&template.raw_prefix);
        out.extend_from_slice(&raw_values);
        out.extend_from_slice(b"]}");
    }

    out.extend_from_slice(b"]}");
    total_records
}

impl BackgroundPlugin for LokiLogShipperPlugin {
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

            let connect_timeout = Duration::from_millis(self.config.connect_timeout_ms);
            let request_timeout = Duration::from_millis(self.config.request_timeout_ms);
            let mut conn: Option<TcpStream> = None;
            let mut payload_buf = Vec::with_capacity(16384);
            let mut req_buf = Vec::with_capacity(16896);

            loop {
                let is_shutting_down = tokio::select! {
                    _ = interval.tick() => false,
                    _ = shutdown.changed() => true,
                };

                // Ensure any remaining records in per-core SPSC rings are fanned out
                self.log_store.flush_all_lanes(self.config.max_batch_size);

                // Drain reference-counted Arc<[AccessLogRecord]> batches. When `batches` goes
                // out of scope at the end of this block, each Arc's strong count decrements.
                let batches = subscriber.drain_batches(64);
                let record_count =
                    format_loki_push_payload(&batches, &self.stream_template, &mut payload_buf);
                drop(batches);

                if record_count > 0 {
                    req_buf.clear();
                    req_buf.extend_from_slice(&self.header_prefix);
                    fast_write_u64(&mut req_buf, payload_buf.len() as u64);
                    req_buf.extend_from_slice(b"\r\n\r\n");
                    req_buf.extend_from_slice(&payload_buf);

                    let mut pushed = false;
                    for _attempt in 0..2 {
                        if conn.is_none() {
                            let addr_str = format!("{}:{}", self.host, self.port);
                            if let Ok(Ok(s)) =
                                tokio::time::timeout(connect_timeout, TcpStream::connect(&addr_str))
                                    .await
                            {
                                let _ = s.set_nodelay(true);
                                conn = Some(s);
                            } else {
                                break;
                            }
                        }

                        if let Some(stream) = conn.as_mut() {
                            let io_res = tokio::time::timeout(request_timeout, async {
                                stream.write_all(&req_buf).await?;
                                let mut resp_buf = [0u8; 256];
                                let n = stream.read(&mut resp_buf).await?;
                                if n >= 12 && (&resp_buf[9..11] == b"20") {
                                    Ok::<bool, std::io::Error>(true)
                                } else {
                                    Ok(false)
                                }
                            })
                            .await;

                            match io_res {
                                Ok(Ok(true)) => {
                                    pushed = true;
                                    break;
                                }
                                _ => {
                                    conn = None;
                                }
                            }
                        }
                    }

                    if pushed {
                        self.shipped_records
                            .fetch_add(record_count as u64, Ordering::Relaxed);
                    } else {
                        self.failed_pushes.fetch_add(1, Ordering::Relaxed);
                    }
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
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn test_loki_log_shipper_multi_protocol_end_to_end() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let received_payload = Arc::new(tokio::sync::Mutex::new(String::new()));
        let received_clone = Arc::clone(&received_payload);

        let server_handle = tokio::spawn(async move {
            if let Ok((mut stream, _)) = listener.accept().await {
                let mut buf = vec![0u8; 8192];
                if let Ok(n) = stream.read(&mut buf).await {
                    let req_str = String::from_utf8_lossy(&buf[..n]).to_string();
                    *received_clone.lock().await = req_str;
                    let _ = stream
                        .write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n")
                        .await;
                }
            }
        });

        let store = Arc::new(LogStore::with_cores_and_capacity(2, 128));

        let mut labels = HashMap::new();
        labels.insert("env".to_string(), "test".to_string());

        let cfg = LokiLogShipperConfig {
            endpoint: format!("http://{addr}/loki/api/v1/push"),
            tenant_id: "velda-tenant".to_string(),
            service_name: "velda-edge".to_string(),
            flush_interval_ms: 100,
            max_batch_size: 256,
            connect_timeout_ms: 1000,
            request_timeout_ms: 1000,
            labels,
        };

        let shipper = Arc::new(
            LokiLogShipperPlugin::with_store("loki_shipper", cfg, Arc::clone(&store)).unwrap(),
        );

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let shipper_clone = Arc::clone(&shipper);
        let bg_task = tokio::spawn(async move {
            shipper_clone.run(shutdown_rx).await.unwrap();
        });

        // Wait briefly for subscriber registration
        tokio::time::sleep(Duration::from_millis(30)).await;

        // Emit 1 HTTP record and 1 gRPC record into the 64-byte integer SPSC ring
        let http_rec = AccessLogRecord::from_scalars(
            0xaaaa_bbbb_cccc_dddd,
            1_710_000_001,
            320,
            "192.168.1.10:50000".parse().unwrap(),
            Some("10.0.0.1:8080".parse().unwrap()),
            128,
            512,
            200,
            1,
            0,
            LogProtocol::H2,
            LogProtocol::H1,
            PipeStrategy::Buffered,
            LogCompletionReason::Complete,
            LogHttpMethod::Get,
            LogGrpcStatus::NonGrpc,
            b"curl/8.6.0",
        );

        let grpc_rec = AccessLogRecord::from_scalars(
            0x1111_2222_3333_4444,
            1_710_000_002,
            890,
            "192.168.1.20:50001".parse().unwrap(),
            Some("10.0.0.2:9090".parse().unwrap()),
            64,
            256,
            200,
            2,
            4,
            LogProtocol::GrpcTcp,
            LogProtocol::GrpcTcp,
            PipeStrategy::Duplex,
            LogCompletionReason::Complete,
            LogHttpMethod::Post,
            LogGrpcStatus::Ok,
            b"grpc-go/1.64.0",
        );

        assert!(store.emit_on_core(0, &http_rec));
        assert!(store.emit_on_core(1, &grpc_rec));

        // Wait for background tick to flush and ship to mock Loki server
        tokio::time::sleep(Duration::from_millis(180)).await;
        let _ = shutdown_tx.send(true);
        let _ = bg_task.await;
        let _ = server_handle.await;

        assert_eq!(shipper.shipped_records.load(Ordering::Relaxed), 2);
        let raw_req = received_payload.lock().await;
        assert!(raw_req.contains("POST /loki/api/v1/push HTTP/1.1"));
        assert!(raw_req.contains("X-Scope-OrgID: velda-tenant"));
        assert!(raw_req.contains("\"protocol\":\"http\""));
        assert!(raw_req.contains("\"protocol\":\"grpc\""));
        assert!(raw_req.contains("ua=curl/8"));
        assert!(raw_req.contains("grpc_status=OK"));
    }
}
