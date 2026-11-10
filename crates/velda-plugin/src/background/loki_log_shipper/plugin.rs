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
use std::fmt::Write as _;
use std::future::Future;
use std::net::Ipv4Addr;
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

/// Standalone Background Grafana Loki Log Shipper Plugin.
pub struct LokiLogShipperPlugin {
    pub id: String,
    pub config: LokiLogShipperConfig,
    pub log_store: Arc<LogStore>,
    pub host: String,
    pub port: u16,
    pub path: String,
    pub host_header: String,
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

        Ok(Self {
            id: id.into(),
            config,
            log_store,
            host,
            port,
            path,
            host_header,
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

/// Serializes reference-counted 64-byte [`AccessLogRecord`] batches into a multi-stream
/// Grafana Loki `/loki/api/v1/push` JSON payload partitioned by protocol family (`http`, `grpc`, `raw`).
pub fn format_loki_push_payload(
    batches: &[Arc<[AccessLogRecord]>],
    service_name: &str,
    static_labels: &HashMap<String, String>,
    out: &mut String,
) -> usize {
    out.clear();

    let mut http_values = String::new();
    let mut grpc_values = String::new();
    let mut raw_values = String::new();
    let mut total_records = 0usize;

    for batch in batches {
        for rec in batch.iter() {
            total_records += 1;
            let proto = LogProtocol::from_u8(rec.downstream_proto);
            let up_proto = LogProtocol::from_u8(rec.upstream_proto);
            let pipe = PipeStrategy::from_discriminant(rec.pipe_strategy)
                .unwrap_or(PipeStrategy::Buffered);
            let completion = LogCompletionReason::from_u8(rec.completion);
            let peer_ip = decode_ip_u128(rec.peer_ip);
            let up_ip = Ipv4Addr::from_bits(rec.upstream_ip);
            let ts_ns = (rec.timestamp_sec as u64).saturating_mul(1_000_000_000);

            match proto {
                LogProtocol::H1 | LogProtocol::H2 | LogProtocol::H3 => {
                    if !http_values.is_empty() {
                        http_values.push(',');
                    }
                    let method = LogHttpMethod::from_u8(rec.method);
                    let ua_fam = UaFamily::from_u8(rec.ua_family);
                    let ua_os = UaOs::from_u8(rec.ua_os);
                    let _ = write!(
                        http_values,
                        "[\"{ts_ns}\",\"req_id={:016x} proto={} up_proto={} pipe={} method={} status={} route_id={} dur_us={} rx={} tx={} peer={}:{} upstream={}:{} ua={}/{} os={} completion={}\"]",
                        rec.request_id,
                        proto.as_str(),
                        up_proto.as_str(),
                        pipe.as_str(),
                        method.as_str(),
                        rec.status_code,
                        rec.route_id,
                        rec.duration_us,
                        rec.bytes_rx,
                        rec.bytes_tx,
                        peer_ip,
                        rec.peer_port,
                        up_ip,
                        rec.upstream_port,
                        ua_fam.as_str(),
                        rec.ua_major_version,
                        ua_os.as_str(),
                        completion.as_str(),
                    );
                }
                LogProtocol::GrpcTcp | LogProtocol::GrpcUdp => {
                    if !grpc_values.is_empty() {
                        grpc_values.push(',');
                    }
                    let grpc_st = LogGrpcStatus::from_u8(rec.grpc_status);
                    let ua_fam = UaFamily::from_u8(rec.ua_family);
                    let _ = write!(
                        grpc_values,
                        "[\"{ts_ns}\",\"req_id={:016x} proto={} grpc_status={} http_status={} route_id={} dur_us={} rx={} tx={} msgs={} peer={}:{} upstream={}:{} ua={}/{} completion={}\"]",
                        rec.request_id,
                        proto.as_str(),
                        grpc_st.as_str(),
                        rec.status_code,
                        rec.route_id,
                        rec.duration_us,
                        rec.bytes_rx,
                        rec.bytes_tx,
                        rec.rpc_msgs_or_flags,
                        peer_ip,
                        rec.peer_port,
                        up_ip,
                        rec.upstream_port,
                        ua_fam.as_str(),
                        rec.ua_major_version,
                        completion.as_str(),
                    );
                }
                LogProtocol::RawTcp | LogProtocol::RawUdp => {
                    if !raw_values.is_empty() {
                        raw_values.push(',');
                    }
                    let _ = write!(
                        raw_values,
                        "[\"{ts_ns}\",\"req_id={:016x} proto={} dur_us={} rx={} tx={} peer={}:{} upstream={}:{} completion={}\"]",
                        rec.request_id,
                        proto.as_str(),
                        rec.duration_us,
                        rec.bytes_rx,
                        rec.bytes_tx,
                        peer_ip,
                        rec.peer_port,
                        up_ip,
                        rec.upstream_port,
                        completion.as_str(),
                    );
                }
            }
        }
    }

    if total_records == 0 {
        return 0;
    }

    out.push_str("{\"streams\":[");
    let mut first_stream = true;

    for (family, values) in [
        ("http", &http_values),
        ("grpc", &grpc_values),
        ("raw", &raw_values),
    ] {
        if values.is_empty() {
            continue;
        }
        if !first_stream {
            out.push(',');
        }
        first_stream = false;

        let _ = write!(
            out,
            "{{\"stream\":{{\"service\":\"{service_name}\",\"protocol\":\"{family}\""
        );
        for (k, v) in static_labels {
            let _ = write!(out, ",\"{k}\":\"{v}\"");
        }
        let _ = write!(out, "}},\"values\":[{values}]}}");
    }

    out.push_str("]}");
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
            let mut payload_buf = String::with_capacity(16384);
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
                let record_count = format_loki_push_payload(
                    &batches,
                    &self.config.service_name,
                    &self.config.labels,
                    &mut payload_buf,
                );
                drop(batches);

                if record_count > 0 {
                    req_buf.clear();
                    let mut header = format!(
                        "POST {} HTTP/1.1\r\nHost: {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: keep-alive\r\n",
                        self.path,
                        self.host_header,
                        payload_buf.len()
                    );
                    if !self.config.tenant_id.is_empty() {
                        let _ = write!(header, "X-Scope-OrgID: {}\r\n", self.config.tenant_id);
                    }
                    header.push_str("\r\n");
                    req_buf.extend_from_slice(header.as_bytes());
                    req_buf.extend_from_slice(payload_buf.as_bytes());

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
