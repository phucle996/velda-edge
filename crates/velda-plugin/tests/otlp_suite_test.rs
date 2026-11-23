//! Comprehensive Test Suite for OTLP Plugins (`otlp_metrics_exporter`, `otlp_log_shipper`, `otlp_trace_exporter`).
//!
//! Validates:
//! 1. Shared `velda_etp::otlp::OtlpEtp` connection reuse (`http/json`, `http/protobuf`, `grpc`).
//! 2. In-place `lease.reconnect().await` resilience when collector closes an idle connection.
//! 3. `wire_plugin_etps` multi-plugin sharing of a single `OtlpEtp` pool across logs, metrics, and traces.
//! 4. gRPC Trailers-Only `grpc-status` error reporting and graceful background shutdown.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use prost::Message;
use rustc_hash::FxHashMap;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::watch;
use velda_core::{CompletionReason, GrpcStatus, HttpMethod, PipeKind};
use velda_etp::otlp::OtlpEtp;
use velda_etp::registry::build_registry;
use velda_etp::{EtpBackend, EtpEntryConfig, EtpKind, EtpPoolSettings};
use velda_observability::{
    MetricsStore, TelemetryBatch, TelemetryRecord, TelemetryStore, global_telemetry_store,
};

use velda_plugin::PluginConfig;
use velda_plugin::background::BackgroundPlugin;
use velda_plugin::background::otlp_log_shipper::json::{
    OtlpJsonEnvelope, format_otlp_records_json,
};
use velda_plugin::background::otlp_log_shipper::proto::{
    ExportLogsServiceRequest, format_otlp_records_proto,
};
use velda_plugin::background::otlp_log_shipper::wire as logs_wire;
use velda_plugin::background::otlp_log_shipper::{OtlpLogShipperConfig, OtlpLogShipperPlugin};
use velda_plugin::background::otlp_metrics_exporter::json::format_otlp_metrics_json;
use velda_plugin::background::otlp_metrics_exporter::proto::format_otlp_metrics_proto;
use velda_plugin::background::otlp_metrics_exporter::wire as metrics_wire;
use velda_plugin::background::otlp_metrics_exporter::{
    OtlpMetricsExporterConfig, OtlpMetricsExporterPlugin,
};
use velda_plugin::background::otlp_trace_exporter::wire as traces_wire;
use velda_plugin::background::otlp_trace_exporter::{
    OtlpTraceExporterConfig, OtlpTraceExporterPlugin,
};
use velda_plugin::factory::{instantiate_plugin, wire_plugin_etps};

fn make_otlp_etp(
    id: &str,
    endpoint: String,
    protocol: &str,
    headers: serde_json::Value,
) -> Arc<OtlpEtp> {
    Arc::new(
        OtlpEtp::from_entry(&EtpEntryConfig {
            id: id.to_string(),
            kind: EtpKind::Otlp,
            pool: EtpPoolSettings {
                pool_size: 1,
                connect_timeout_ms: 2000,
                idle_timeout_ms: 60_000,
            },
            tls: None,
            settings: serde_json::json!({
                "endpoint": endpoint,
                "protocol": protocol,
                "headers": headers
            }),
        })
        .unwrap(),
    )
}

fn make_sample_log_record(
    req_id: u64,
    ts_sec: u64,
    status: u16,
    method: HttpMethod,
) -> TelemetryRecord {
    let mut rec = TelemetryRecord::zeroed();
    rec.request_id = req_id;
    rec.ingress_time_nanos = ts_sec;
    rec.duration_ns = 2500;
    rec.peer_ip = velda_core::lut::ip::encode_ip_u128("192.168.1.50".parse().unwrap());
    rec.upstream_ip = velda_core::lut::ip::encode_upstream_ip_u32("10.0.0.10".parse().unwrap());
    rec.upstream_port = 8080;
    rec.bytes_rx = 100;
    rec.bytes_tx = 200;
    rec.status_code = status;
    rec.route_id = 1;
    rec.pipe = PipeKind::BufferedH1H1;
    rec.completion = CompletionReason::Complete;
    rec.method = method;
    rec.grpc_status = GrpcStatus::NonGrpc;
    let (fam, os, ver) = velda_core::lut::classify_user_agent(b"curl/8.6.0");
    rec.ua_family = fam;
    rec.ua_os = os;
    rec.ua_major_version = ver;
    rec
}

/// Helper: starts a mock OTLP Collector HTTP/1.1 server counting connections and requests.
async fn spawn_mock_collector(
    expected_path: &'static str,
) -> (
    String,
    Arc<AtomicUsize>,
    Arc<AtomicUsize>,
    tokio::sync::oneshot::Sender<()>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let conn_count = Arc::new(AtomicUsize::new(0));
    let req_count = Arc::new(AtomicUsize::new(0));
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    let conn_counter = Arc::clone(&conn_count);
    let req_counter = Arc::clone(&req_count);

    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = &mut shutdown_rx => break,
                res = listener.accept() => {
                    let Ok((mut socket, _)) = res else { break };
                    conn_counter.fetch_add(1, Ordering::SeqCst);
                    let req_c = Arc::clone(&req_counter);

                    tokio::spawn(async move {
                        let mut buf = [0u8; 65536];
                        loop {
                            let n = match socket.read(&mut buf).await {
                                Ok(0) => break,
                                Ok(n) => n,
                                Err(_) => break,
                            };

                            let req_str = String::from_utf8_lossy(&buf[..n]);
                            if req_str.contains(expected_path) || req_str.starts_with("POST ") {
                                req_c.fetch_add(1, Ordering::SeqCst);
                                let resp = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n";
                                if socket.write_all(resp.as_bytes()).await.is_err() {
                                    break;
                                }
                            }
                        }
                    });
                }
            }
        }
    });

    (
        format!("http://{addr}{expected_path}"),
        conn_count,
        req_count,
        shutdown_tx,
    )
}

#[tokio::test]
async fn test_otlp_metrics_conn_reuse() {
    let (url, conns, reqs, _shutdown) = spawn_mock_collector("/v1/metrics").await;

    let etp = make_otlp_etp(
        "otlp-metrics-reuse",
        url,
        "http/json",
        serde_json::json!({}),
    );
    let tpl = metrics_wire::bake_wire_template(&etp.config).unwrap();

    let store = Arc::new(MetricsStore::new());
    let snapshot = store.snapshot();
    let payload = format_otlp_metrics_json(&snapshot, "test-edge");

    let mut head_buf = Vec::with_capacity(256);
    let mut resp_buf = Vec::with_capacity(256);

    for _ in 0..3 {
        metrics_wire::push_metrics(
            &etp,
            &tpl,
            &mut head_buf,
            payload.as_bytes(),
            &mut resp_buf,
            Duration::from_secs(5),
        )
        .await
        .expect("OTLP metrics push must succeed");
    }

    tokio::time::sleep(Duration::from_millis(50)).await;

    assert_eq!(reqs.load(Ordering::SeqCst), 3, "Total 3 requests received");
    assert_eq!(
        conns.load(Ordering::SeqCst),
        1,
        "Only 1 persistent TCP connection was opened"
    );
}

#[tokio::test]
async fn test_otlp_metrics_transparent_reconnect() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let conns = Arc::new(AtomicUsize::new(0));

    let conns_clone = Arc::clone(&conns);
    tokio::spawn(async move {
        // Accept connection 1: handle 1 request then close abruptly to simulate idle timeout
        if let Ok((mut s1, _)) = listener.accept().await {
            conns_clone.fetch_add(1, Ordering::SeqCst);
            let mut buf = [0u8; 4096];
            let _ = s1.read(&mut buf).await;
            let resp = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n";
            let _ = s1.write_all(resp.as_bytes()).await;
            tokio::time::sleep(Duration::from_millis(10)).await;
            drop(s1);
        }

        // Accept connection 2: transparent in-place reconnect retry
        if let Ok((mut s2, _)) = listener.accept().await {
            conns_clone.fetch_add(1, Ordering::SeqCst);
            let mut buf = [0u8; 4096];
            let _ = s2.read(&mut buf).await;
            let resp = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n";
            let _ = s2.write_all(resp.as_bytes()).await;
        }
    });

    let url = format!("http://{addr}/v1/metrics");
    let etp = make_otlp_etp(
        "otlp-metrics-reconn",
        url,
        "http/json",
        serde_json::json!({}),
    );
    let tpl = metrics_wire::bake_wire_template(&etp.config).unwrap();

    let mut head_buf = Vec::with_capacity(256);
    let mut resp_buf = Vec::with_capacity(256);

    metrics_wire::push_metrics(
        &etp,
        &tpl,
        &mut head_buf,
        b"{}",
        &mut resp_buf,
        Duration::from_secs(5),
    )
    .await
    .unwrap();

    tokio::time::sleep(Duration::from_millis(30)).await;

    metrics_wire::push_metrics(
        &etp,
        &tpl,
        &mut head_buf,
        b"{}",
        &mut resp_buf,
        Duration::from_secs(5),
    )
    .await
    .unwrap();

    assert_eq!(
        conns.load(Ordering::SeqCst),
        2,
        "Reconnected transparently in-place on dropped socket"
    );
}

#[tokio::test]
async fn test_otlp_log_shipper_batching_and_send() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut buf = Vec::with_capacity(4096);
        let mut tmp = [0u8; 2048];
        let hdr_end = loop {
            let n = sock.read(&mut tmp).await.unwrap();
            assert!(n > 0);
            buf.extend_from_slice(&tmp[..n]);
            if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                break pos + 4;
            }
        };
        let headers = String::from_utf8_lossy(&buf[..hdr_end]).to_string();
        assert!(headers.starts_with("POST /v1/logs HTTP/1.1\r\n"));
        assert!(headers.contains("Content-Type: application/json\r\n"));
        assert!(headers.contains("x-api-key: log-secret\r\n"));
        sock.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
            .await
            .unwrap();
    });

    let etp = make_otlp_etp(
        "otlp-logs-json",
        format!("http://{addr}"),
        "http/json",
        serde_json::json!({ "x-api-key": "log-secret" }),
    );
    let tpl = logs_wire::bake_wire_template(&etp.config).unwrap();

    let rec1 = make_sample_log_record(0x4bf9_2f35_77b3_4da6, 1_710_000_000, 200, HttpMethod::Get);
    let rec2 = make_sample_log_record(0x5fb3_97be_34d2_3b0f, 1_710_000_001, 500, HttpMethod::Post);
    let batches = vec![TelemetryBatch::from_arc(Arc::from(vec![rec1, rec2]))];
    let envelope = OtlpJsonEnvelope::new("test-edge");
    let mut payload = Vec::with_capacity(1024);
    format_otlp_records_json(&batches, &envelope, &mut payload);

    let mut head_buf = Vec::with_capacity(512);
    let mut resp_buf = Vec::with_capacity(512);
    logs_wire::push_logs(
        &etp,
        &tpl,
        &mut head_buf,
        &payload,
        &mut resp_buf,
        Duration::from_secs(5),
    )
    .await
    .expect("Sent OTLP JSON logs");

    server.await.unwrap();
}

#[tokio::test]
async fn test_otlp_log_shipper_http_protobuf() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut buf = Vec::with_capacity(4096);
        let mut tmp = [0u8; 2048];
        let hdr_end = loop {
            let n = sock.read(&mut tmp).await.unwrap();
            assert!(n > 0);
            buf.extend_from_slice(&tmp[..n]);
            if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                break pos + 4;
            }
        };
        let headers = String::from_utf8_lossy(&buf[..hdr_end]).to_string();
        assert!(headers.starts_with("POST /v1/logs HTTP/1.1\r\n"));
        assert!(headers.contains("Content-Type: application/x-protobuf\r\n"));
        let cl = headers
            .lines()
            .find_map(|l| {
                let (k, v) = l.split_once(':')?;
                k.trim()
                    .eq_ignore_ascii_case("content-length")
                    .then(|| v.trim().parse::<usize>().ok())
                    .flatten()
            })
            .unwrap();
        while buf.len() < hdr_end + cl {
            let n = sock.read(&mut tmp).await.unwrap();
            assert!(n > 0);
            buf.extend_from_slice(&tmp[..n]);
        }
        let decoded =
            ExportLogsServiceRequest::decode(&buf[hdr_end..hdr_end + cl]).expect("valid protobuf");
        assert_eq!(decoded.resource_logs.len(), 1);
        sock.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
            .await
            .unwrap();
    });

    let etp = make_otlp_etp(
        "otlp-logs-http-pb",
        format!("http://{addr}"),
        "http",
        serde_json::json!({}),
    );
    let store = Arc::new(TelemetryStore::with_cores_and_capacity(1, 64));
    let cfg = OtlpLogShipperConfig {
        etp_id: "otlp-logs-http-pb".into(),
        service_name: "velda-http-pb".into(),
        flush_interval_ms: 50,
        ..Default::default()
    };
    let plugin =
        Arc::new(OtlpLogShipperPlugin::with_etp("otlp_pb", cfg, etp, Arc::clone(&store)).unwrap());

    let (tx, rx) = watch::channel(false);
    let runner = tokio::spawn(Arc::clone(&plugin).run(rx));
    while store.active_subscriber_count.load(Ordering::Acquire) == 0 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    store.emit_on_core(
        0,
        &make_sample_log_record(0x7777, 1_710_000_002, 200, HttpMethod::Get),
    );
    store.flush_all_lanes(64);
    server.await.unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(plugin.shipped_records.load(Ordering::Relaxed), 1);

    let _ = tx.send(true);
    runner.await.unwrap().unwrap();
}

/// Helper: starts a mock OTLP gRPC HTTP/2 server counting incoming RPC calls.
async fn spawn_mock_grpc_collector(
    expected_path: &'static str,
) -> (String, Arc<AtomicUsize>, tokio::sync::oneshot::Sender<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let req_count = Arc::new(AtomicUsize::new(0));
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    let req_counter = Arc::clone(&req_count);

    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = &mut shutdown_rx => break,
                res = listener.accept() => {
                    let Ok((socket, _)) = res else { break };
                    let req_c = Arc::clone(&req_counter);

                    tokio::spawn(async move {
                        let mut connection = match h2::server::handshake(socket).await {
                            Ok(conn) => conn,
                            Err(_) => return,
                        };

                        while let Some(Ok((request, mut respond))) = connection.accept().await {
                            req_c.fetch_add(1, Ordering::SeqCst);
                            assert_eq!(request.uri().path(), expected_path);

                            let mut body = request.into_body();
                            while let Some(chunk) = body.data().await {
                                if let Ok(data) = chunk {
                                    let _ = body.flow_control().release_capacity(data.len());
                                }
                            }

                            let response = http::Response::builder()
                                .status(200)
                                .header("content-type", "application/grpc")
                                .header("grpc-status", "0")
                                .body(())
                                .unwrap();
                            let _ = respond.send_response(response, true);
                        }
                    });
                }
            }
        }
    });

    (format!("grpc://{addr}"), req_count, shutdown_tx)
}

#[tokio::test]
async fn test_otlp_metrics_grpc_export() {
    let (endpoint, req_count, _shutdown) =
        spawn_mock_grpc_collector(metrics_wire::OTLP_METRICS_GRPC_PATH).await;

    let store = MetricsStore::new();
    let snapshot = store.snapshot();
    let proto_bytes = format_otlp_metrics_proto(&snapshot, "velda-test-grpc");

    let etp = make_otlp_etp("otlp-metrics-grpc", endpoint, "grpc", serde_json::json!({}));
    let tpl = metrics_wire::bake_wire_template(&etp.config).unwrap();
    let mut head_buf = Vec::new();
    let mut resp_buf = Vec::new();

    for _ in 0..2 {
        metrics_wire::push_metrics(
            &etp,
            &tpl,
            &mut head_buf,
            &proto_bytes,
            &mut resp_buf,
            Duration::from_secs(5),
        )
        .await
        .expect("gRPC metrics send must succeed");
    }
    assert_eq!(req_count.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn test_otlp_logs_grpc_export() {
    let (logs_endpoint, logs_count, _logs_shutdown) =
        spawn_mock_grpc_collector(logs_wire::OTLP_LOGS_GRPC_PATH).await;

    let rec = make_sample_log_record(0x4bf9_2f35_77b3_4da6, 1_729_579_200, 200, HttpMethod::Post);
    let batches = vec![TelemetryBatch::from_arc(Arc::from(vec![rec]))];
    let (req, _) = format_otlp_records_proto(&batches, "pay-service");
    let mut log_proto_bytes = Vec::new();
    req.encode(&mut log_proto_bytes).unwrap();

    let etp = make_otlp_etp(
        "otlp-logs-grpc",
        logs_endpoint,
        "grpc",
        serde_json::json!({}),
    );
    let tpl = logs_wire::bake_wire_template(&etp.config).unwrap();
    let mut head_buf = Vec::new();
    let mut resp_buf = Vec::new();

    logs_wire::push_logs(
        &etp,
        &tpl,
        &mut head_buf,
        &log_proto_bytes,
        &mut resp_buf,
        Duration::from_secs(5),
    )
    .await
    .expect("Logs gRPC export must succeed");
    assert_eq!(logs_count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn test_otlp_grpc_trailers_error() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        while let Ok((socket, _)) = listener.accept().await {
            tokio::spawn(async move {
                let Ok(mut conn) = h2::server::handshake(socket).await else {
                    return;
                };
                while let Some(Ok((req, mut respond))) = conn.accept().await {
                    let mut body = req.into_body();
                    while let Some(Ok(_)) = body.data().await {}
                    let resp = http::Response::builder()
                        .status(200)
                        .header("content-type", "application/grpc")
                        .header("grpc-status", "13")
                        .header("grpc-message", "proto: wrong wireType")
                        .body(())
                        .unwrap();
                    let _ = respond.send_response(resp, true);
                }
            });
        }
    });

    let etp = make_otlp_etp(
        "otlp-grpc-err",
        format!("grpc://{addr}"),
        "grpc",
        serde_json::json!({}),
    );
    let tpl = logs_wire::bake_wire_template(&etp.config).unwrap();
    let mut head_buf = Vec::new();
    let mut resp_buf = Vec::new();

    let err = logs_wire::push_logs(
        &etp,
        &tpl,
        &mut head_buf,
        b"bad",
        &mut resp_buf,
        Duration::from_secs(2),
    )
    .await
    .expect_err("Trailers-Only grpc-status 13 must return Err");
    let msg = err.to_string();
    assert!(
        msg.contains("13"),
        "Expected grpc-status 13 in error: {msg}"
    );
    assert!(
        msg.contains("wrong wireType"),
        "Expected grpc-message in error: {msg}"
    );
}

#[tokio::test]
async fn test_otlp_log_and_metrics_shutdown_and_from_config() {
    let etp = make_otlp_etp(
        "otlp-shutdown",
        "http://127.0.0.1:4318".into(),
        "http/json",
        serde_json::json!({}),
    );

    let log_store = Arc::new(TelemetryStore::with_cores_and_capacity(1, 32));
    let log_plugin = Arc::new(
        OtlpLogShipperPlugin::with_etp(
            "otlp_log_test",
            OtlpLogShipperConfig {
                etp_id: "otlp-shutdown".into(),
                flush_interval_ms: 50,
                ..Default::default()
            },
            Arc::clone(&etp),
            log_store,
        )
        .unwrap(),
    );
    let (ltx, lrx) = watch::channel(false);
    let l_handle = tokio::spawn(async move { log_plugin.run(lrx).await });
    ltx.send(true).unwrap();
    assert!(l_handle.await.unwrap().is_ok());

    let metrics_cfg = PluginConfig {
        id: "otlp_metrics_exporter".into(),
        config: serde_json::json!({
            "etp_id": "otlp-shutdown",
            "interval_secs": 15,
            "request_timeout_ms": 10000,
            "service_name": "velda-edge"
        }),
    };
    let parsed_metrics = OtlpMetricsExporterPlugin::from_config(&metrics_cfg).unwrap();
    assert_eq!(parsed_metrics.etp_id(), Some("otlp-shutdown"));

    let m_store = Arc::new(MetricsStore::new());
    let metrics_plugin = Arc::new(
        OtlpMetricsExporterPlugin::with_etp(
            "otlp_metrics_test",
            OtlpMetricsExporterConfig {
                etp_id: "otlp-shutdown".into(),
                interval_secs: 1,
                ..Default::default()
            },
            etp,
            m_store,
        )
        .unwrap(),
    );
    let (mtx, mrx) = watch::channel(false);
    let m_handle = tokio::spawn(async move { metrics_plugin.run(mrx).await });
    mtx.send(true).unwrap();
    assert!(m_handle.await.unwrap().is_ok());
}

#[tokio::test]
async fn test_otlp_shared_etp_registry_wiring_and_reconnect_recovery() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        // 1. Accept initial HTTP socket and drop abruptly so OTLP metrics exporter performs
        //    in-place `lease.reconnect().await`.
        let (mut s1, _) = listener.accept().await.unwrap();
        let mut scratch = vec![0u8; 4096];
        let _ = s1.read(&mut scratch).await;
        drop(s1);

        // 2. Accept reconnected retry, verify `/v1/metrics` JSON post, return 200 OK.
        let (mut s2, _) = listener.accept().await.unwrap();
        let mut buf = Vec::with_capacity(8192);
        let mut tmp = [0u8; 2048];
        let hdr_end = loop {
            let n = s2.read(&mut tmp).await.unwrap();
            assert!(n > 0);
            buf.extend_from_slice(&tmp[..n]);
            if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                break pos + 4;
            }
        };
        let headers = String::from_utf8_lossy(&buf[..hdr_end]).to_string();
        assert!(headers.starts_with("POST /v1/metrics HTTP/1.1\r\n"));
        assert!(headers.contains("Content-Type: application/json\r\n"));
        s2.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
            .await
            .unwrap();
    });

    let registry = build_registry(&[EtpEntryConfig {
        id: "otlp-main".into(),
        kind: EtpKind::Otlp,
        pool: EtpPoolSettings {
            pool_size: 2,
            connect_timeout_ms: 2000,
            idle_timeout_ms: 60_000,
        },
        tls: None,
        settings: serde_json::json!({
            "endpoint": format!("http://{addr}"),
            "protocol": "http/json",
            "headers": {}
        }),
    }])
    .unwrap();

    let logs_cfg = PluginConfig {
        id: "otlp_log_shipper".into(),
        config: serde_json::json!({
            "etp_id": "otlp-main",
            "service_name": "velda-edge"
        }),
    };
    let metrics_cfg = PluginConfig {
        id: "otlp_metrics_exporter".into(),
        config: serde_json::json!({
            "etp_id": "otlp-main",
            "interval_secs": 1,
            "service_name": "velda-edge"
        }),
    };

    let loaded_logs = instantiate_plugin(&logs_cfg).unwrap();
    let loaded_metrics = instantiate_plugin(&metrics_cfg).unwrap();
    let mut available = FxHashMap::default();
    available.insert(loaded_logs.id().to_string(), loaded_logs);
    available.insert(loaded_metrics.id().to_string(), loaded_metrics.clone());
    wire_plugin_etps(&available, &registry).unwrap();

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let runner = tokio::spawn(loaded_metrics.run_background(shutdown_rx).unwrap());

    tokio::time::timeout(Duration::from_secs(3), server)
        .await
        .unwrap()
        .unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;

    let _ = shutdown_tx.send(true);
    runner.await.unwrap().unwrap();

    // Missing ETP in registry produces an explicit PluginError
    let missing_plugin = instantiate_plugin(&logs_cfg).unwrap();
    let mut missing_map = FxHashMap::default();
    missing_map.insert(missing_plugin.id().to_string(), missing_plugin);
    assert!(wire_plugin_etps(&missing_map, &velda_etp::EtpRegistry::default()).is_err());

    // Wrong-kind ETP in registry produces an explicit PluginError
    let wrong_registry = build_registry(&[EtpEntryConfig {
        id: "otlp-main".into(),
        kind: EtpKind::Loki,
        pool: EtpPoolSettings::default(),
        tls: None,
        settings: serde_json::json!({ "endpoint": "http://127.0.0.1:3100" }),
    }])
    .unwrap();
    let wrong_plugin = instantiate_plugin(&metrics_cfg).unwrap();
    let mut wrong_map = FxHashMap::default();
    wrong_map.insert(wrong_plugin.id().to_string(), wrong_plugin);
    assert!(wire_plugin_etps(&wrong_map, &wrong_registry).is_err());
}

#[tokio::test]
async fn test_all_three_otlp_plugins_share_single_etp() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let seen_paths = Arc::new(tokio::sync::Mutex::new(Vec::<String>::new()));
    let conn_count = Arc::new(AtomicUsize::new(0));
    let (srv_stop_tx, mut srv_stop_rx) = watch::channel(false);

    let paths_c = Arc::clone(&seen_paths);
    let conns_c = Arc::clone(&conn_count);
    let server = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = srv_stop_rx.changed() => break,
                res = listener.accept() => {
                    let Ok((mut sock, _)) = res else { break };
                    conns_c.fetch_add(1, Ordering::SeqCst);
                    let p_s = Arc::clone(&paths_c);
                    tokio::spawn(async move {
                        let mut buf = Vec::with_capacity(8192);
                        let mut tmp = [0u8; 4096];
                        loop {
                            let hdr_end = loop {
                                if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                                    break Some(pos + 4);
                                }
                                let Ok(n) = sock.read(&mut tmp).await else {
                                    break None;
                                };
                                if n == 0 {
                                    break None;
                                }
                                buf.extend_from_slice(&tmp[..n]);
                            };
                            let Some(hdr_end) = hdr_end else { break };
                            let hdr_str = String::from_utf8_lossy(&buf[..hdr_end]).to_string();
                            let first_line = hdr_str.lines().next().unwrap_or("");
                            let path = first_line.split_whitespace().nth(1).unwrap_or("").to_string();
                            assert!(hdr_str.contains("Content-Type: application/x-protobuf\r\n"));

                            let cl = hdr_str
                                .lines()
                                .find_map(|l| {
                                    let (k, v) = l.split_once(':')?;
                                    k.trim()
                                        .eq_ignore_ascii_case("content-length")
                                        .then(|| v.trim().parse::<usize>().ok())
                                        .flatten()
                                })
                                .unwrap_or(0);
                            while buf.len() < hdr_end + cl {
                                let Ok(n) = sock.read(&mut tmp).await else { return };
                                if n == 0 {
                                    return;
                                }
                                buf.extend_from_slice(&tmp[..n]);
                            }
                            buf.drain(..hdr_end + cl);
                            p_s.lock().await.push(path);
                            if sock
                                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n")
                                .await
                                .is_err()
                            {
                                break;
                            }
                        }
                    });
                }
            }
        }
    });

    let registry = build_registry(&[EtpEntryConfig {
        id: "otlp-shared".into(),
        kind: EtpKind::Otlp,
        pool: EtpPoolSettings {
            pool_size: 1,
            connect_timeout_ms: 2000,
            idle_timeout_ms: 60_000,
        },
        tls: None,
        settings: serde_json::json!({
            "endpoint": format!("http://{addr}"),
            "protocol": "http/protobuf",
            "headers": {}
        }),
    }])
    .unwrap();

    let inst = registry.entries.get("otlp-shared").unwrap();
    let EtpBackend::Otlp(ref registry_etp) = inst.backend else {
        panic!("expected Otlp ETP backend");
    };

    // 1. Concrete plugins attach to the exact same Arc<OtlpEtp> instance and reuse its single socket.
    let log_plugin = Arc::new(
        OtlpLogShipperPlugin::with_store(
            "otlp_log_shipper",
            OtlpLogShipperConfig {
                etp_id: "otlp-shared".into(),
                flush_interval_ms: 100,
                ..Default::default()
            },
            Arc::new(TelemetryStore::with_cores_and_capacity(1, 32)),
        )
        .unwrap(),
    );
    let metrics_plugin = Arc::new(
        OtlpMetricsExporterPlugin::with_store(
            "otlp_metrics_exporter",
            OtlpMetricsExporterConfig {
                etp_id: "otlp-shared".into(),
                interval_secs: 1,
                ..Default::default()
            },
            Arc::new(MetricsStore::new()),
        )
        .unwrap(),
    );
    let trace_plugin = Arc::new(
        OtlpTraceExporterPlugin::with_store(
            "otlp_trace_exporter",
            OtlpTraceExporterConfig {
                etp_id: "otlp-shared".into(),
                flush_interval_ms: 20,
                ..Default::default()
            },
            Arc::new(TelemetryStore::with_cores_and_capacity(1, 32)),
        )
        .unwrap(),
    );

    log_plugin.attach_etp(inst).unwrap();
    metrics_plugin.attach_etp(inst).unwrap();
    trace_plugin.attach_etp(inst).unwrap();

    let log_etp = log_plugin.etp.get().unwrap();
    let metrics_etp = metrics_plugin.etp.get().unwrap();
    let trace_etp = trace_plugin.etp.get().unwrap();
    assert!(Arc::ptr_eq(log_etp, registry_etp));
    assert!(Arc::ptr_eq(metrics_etp, registry_etp));
    assert!(Arc::ptr_eq(trace_etp, registry_etp));

    let logs_tpl = logs_wire::bake_wire_template(&log_etp.config).unwrap();
    let metrics_tpl = metrics_wire::bake_wire_template(&metrics_etp.config).unwrap();
    let traces_tpl = traces_wire::bake_wire_template(&trace_etp.config).unwrap();
    let mut head_buf = Vec::new();
    let mut resp_buf = Vec::new();

    logs_wire::push_logs(
        log_etp,
        &logs_tpl,
        &mut head_buf,
        b"\x0a\x00",
        &mut resp_buf,
        Duration::from_secs(2),
    )
    .await
    .unwrap();
    metrics_wire::push_metrics(
        metrics_etp,
        &metrics_tpl,
        &mut head_buf,
        b"\x0a\x00",
        &mut resp_buf,
        Duration::from_secs(2),
    )
    .await
    .unwrap();
    traces_wire::push_traces(
        trace_etp,
        &traces_tpl,
        &mut head_buf,
        b"\x0a\x00",
        &mut resp_buf,
        Duration::from_secs(2),
    )
    .await
    .unwrap();

    assert_eq!(
        conn_count.load(Ordering::SeqCst),
        1,
        "Sequential requests from all 3 OTLP plugins must reuse the single pooled connection"
    );

    // 2. Full factory instantiation + wire_plugin_etps across all 3 plugin IDs.
    let logs_cfg = PluginConfig {
        id: "otlp_log_shipper".into(),
        config: serde_json::json!({
            "etp_id": "otlp-shared",
            "flush_interval_ms": 100
        }),
    };
    let metrics_cfg = PluginConfig {
        id: "otlp_metrics_exporter".into(),
        config: serde_json::json!({
            "etp_id": "otlp-shared",
            "interval_secs": 1
        }),
    };
    let trace_cfg = PluginConfig {
        id: "otlp_trace_exporter".into(),
        config: serde_json::json!({
            "etp_id": "otlp-shared",
            "flush_interval_ms": 20
        }),
    };

    let loaded_logs = instantiate_plugin(&logs_cfg).unwrap();
    let loaded_metrics = instantiate_plugin(&metrics_cfg).unwrap();
    let loaded_trace = instantiate_plugin(&trace_cfg).unwrap();
    let mut available = FxHashMap::default();
    available.insert(loaded_logs.id().to_string(), loaded_logs.clone());
    available.insert(loaded_metrics.id().to_string(), loaded_metrics.clone());
    available.insert(loaded_trace.id().to_string(), loaded_trace.clone());
    wire_plugin_etps(&available, &registry).unwrap();

    let (stop_tx, stop_rx) = watch::channel(false);
    let t_logs = tokio::spawn(loaded_logs.run_background(stop_rx.clone()).unwrap());
    let t_metrics = tokio::spawn(loaded_metrics.run_background(stop_rx.clone()).unwrap());
    let t_trace = tokio::spawn(loaded_trace.run_background(stop_rx).unwrap());

    let global_store = global_telemetry_store();
    while global_store.active_subscriber_count.load(Ordering::Acquire) < 2 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    global_store.emit_on_core(
        0,
        &make_sample_log_record(0xaaaa, 1_710_000_010, 200, HttpMethod::Get),
    );
    global_store.flush_all_lanes(64);

    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let paths = seen_paths.lock().await;
            let count_logs = paths.iter().filter(|p| p.as_str() == "/v1/logs").count();
            let count_metrics = paths.iter().filter(|p| p.as_str() == "/v1/metrics").count();
            let count_traces = paths.iter().filter(|p| p.as_str() == "/v1/traces").count();
            if count_logs >= 2 && count_metrics >= 2 && count_traces >= 2 {
                break;
            }
            drop(paths);
            tokio::time::sleep(Duration::from_millis(15)).await;
        }
    })
    .await
    .expect("Expected all 3 wired OTLP plugins to export over shared OtlpEtp");

    let _ = stop_tx.send(true);
    t_logs.await.unwrap().unwrap();
    t_metrics.await.unwrap().unwrap();
    t_trace.await.unwrap().unwrap();
    let _ = srv_stop_tx.send(true);
    let _ = server.await;
}
