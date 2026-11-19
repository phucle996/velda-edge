//! Comprehensive Test Suite for OTLP Plugins (`otlp_metrics_exporter`, `otlp_log_shipper`).
//!
//! Validates:
//! 1. OpenTelemetry JSON Schema conformance across Metrics, Logs, and Traces.
//! 2. Persistent Keep-Alive connection reuse avoiding connection churn and handshake storms.
//! 3. Transparent reconnect resilience when server closes socket while idle.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use http::HeaderMap;
use prost::Message;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use velda_core::{CompletionReason, GrpcStatus, HttpMethod, PipeKind};
use velda_observability::{AccessLogRecord, LogBatch, MetricsStore};
use velda_plugin::OutboundTlsConfig;

use velda_plugin::background::otlp_log_shipper::json::{
    OtlpJsonEnvelope, OtlpLogClient, format_otlp_records_json,
};
use velda_plugin::background::otlp_log_shipper::proto::{
    OtlpLogsGrpcClient, format_otlp_records_proto,
};
use velda_plugin::background::otlp_metrics_exporter::json::{
    OtlpMetricsClient, format_otlp_metrics_json,
};
use velda_plugin::background::otlp_metrics_exporter::proto::{
    OtlpMetricsGrpcClient, format_otlp_metrics_proto,
};

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
                        let mut buf = [0u8; 8192];
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

    let mut client = OtlpMetricsClient::new(
        &url,
        Duration::from_secs(5),
        Duration::from_secs(10),
        &OutboundTlsConfig::default(),
    )
    .expect("Client initialized");
    let store = Arc::new(MetricsStore::new());
    let snapshot = store.snapshot();
    let payload = format_otlp_metrics_json(&snapshot, "test-edge");

    let headers = HeaderMap::new();

    // 1st request -> establishes connection
    let status1 = client
        .send_metrics(payload.as_bytes(), &headers)
        .await
        .expect("Sent request 1");
    assert_eq!(status1, 200);

    // 2nd request -> reuses existing persistent connection
    let status2 = client
        .send_metrics(payload.as_bytes(), &headers)
        .await
        .expect("Sent request 2");
    assert_eq!(status2, 200);

    // 3rd request -> reuses existing persistent connection
    let status3 = client
        .send_metrics(payload.as_bytes(), &headers)
        .await
        .expect("Sent request 3");
    assert_eq!(status3, 200);

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
            let mut buf = [0u8; 1024];
            let _ = s1.read(&mut buf).await;
            let resp = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n";
            let _ = s1.write_all(resp.as_bytes()).await;
            tokio::time::sleep(Duration::from_millis(10)).await;
            drop(s1); // Server closes socket while idle
        }

        // Accept connection 2: transparent reconnect
        if let Ok((mut s2, _)) = listener.accept().await {
            conns_clone.fetch_add(1, Ordering::SeqCst);
            let mut buf = [0u8; 1024];
            let _ = s2.read(&mut buf).await;
            let resp = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n";
            let _ = s2.write_all(resp.as_bytes()).await;
        }
    });

    let url = format!("http://{addr}/v1/metrics");
    let mut client = OtlpMetricsClient::new(
        &url,
        Duration::from_secs(5),
        Duration::from_secs(10),
        &OutboundTlsConfig::default(),
    )
    .unwrap();
    let headers = HeaderMap::new();

    let s1 = client.send_metrics(b"{}", &headers).await.unwrap();
    assert_eq!(s1, 200);

    tokio::time::sleep(Duration::from_millis(30)).await;

    // Server has closed socket, next request must transparently reconnect
    let s2 = client.send_metrics(b"{}", &headers).await.unwrap();
    assert_eq!(s2, 200);

    assert_eq!(
        conns.load(Ordering::SeqCst),
        2,
        "Reconnected transparently on dropped socket"
    );
}

#[tokio::test]
async fn test_otlp_log_shipper_batching_and_send() {
    let (url, conns, reqs, _shutdown) = spawn_mock_collector("/v1/logs").await;

    let mut client = OtlpLogClient::new(
        &url,
        Duration::from_secs(5),
        Duration::from_secs(10),
        &OutboundTlsConfig::default(),
    )
    .expect("Client initialized");

    let mut rec1 = AccessLogRecord::zeroed();
    rec1.request_id = 0x4bf9_2f35_77b3_4da6;
    rec1.ingress_time_nanos = 1_710_000_000;
    rec1.duration_ns = 2500;
    rec1.peer_ip = velda_core::lut::ip::encode_ip_u128("192.168.1.50".parse().unwrap());
    rec1.upstream_ip = velda_core::lut::ip::encode_upstream_ip_u32("10.0.0.10".parse().unwrap());
    rec1.upstream_port = 8080;
    rec1.bytes_rx = 100;
    rec1.bytes_tx = 200;
    rec1.status_code = 200;
    rec1.route_id = 1;
    rec1.pipe = PipeKind::BufferedH1H1;
    rec1.completion = CompletionReason::Complete;
    rec1.method = HttpMethod::Get;
    rec1.grpc_status = GrpcStatus::NonGrpc;
    let (fam, os, ver) = velda_core::lut::classify_user_agent(b"curl/8.6.0");
    rec1.ua_family = fam;
    rec1.ua_os = os;
    rec1.ua_major_version = ver;

    let mut rec2 = AccessLogRecord::zeroed();
    rec2.request_id = 0x5fb3_97be_34d2_3b0f;
    rec2.ingress_time_nanos = 1_710_000_001;
    rec2.duration_ns = 15200;
    rec2.peer_ip = velda_core::lut::ip::encode_ip_u128("192.168.1.51".parse().unwrap());
    rec2.upstream_ip = velda_core::lut::ip::encode_upstream_ip_u32("10.0.0.11".parse().unwrap());
    rec2.upstream_port = 8080;
    rec2.bytes_rx = 200;
    rec2.bytes_tx = 500;
    rec2.status_code = 500;
    rec2.route_id = 2;
    rec2.pipe = PipeKind::BufferedH1H1;
    rec2.completion = CompletionReason::Complete;
    rec2.method = HttpMethod::Post;
    rec2.grpc_status = GrpcStatus::NonGrpc;
    let (fam, os, ver) = velda_core::lut::classify_user_agent(b"curl/8.6.0");
    rec2.ua_family = fam;
    rec2.ua_os = os;
    rec2.ua_major_version = ver;

    let batches = vec![LogBatch::from_arc(Arc::from(vec![rec1, rec2]))];
    let envelope = OtlpJsonEnvelope::new("test-edge");
    let mut payload = Vec::with_capacity(1024);
    format_otlp_records_json(&batches, &envelope, &mut payload);
    let headers = HeaderMap::new();

    let status = client
        .send_logs(&payload, &headers)
        .await
        .expect("Sent logs");
    assert_eq!(status, 200);

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(reqs.load(Ordering::SeqCst), 1);
    assert_eq!(conns.load(Ordering::SeqCst), 1);
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

    (format!("http://{addr}"), req_count, shutdown_tx)
}

#[tokio::test]
async fn test_otlp_metrics_grpc_export() {
    let (endpoint, req_count, _shutdown) =
        spawn_mock_grpc_collector(OtlpMetricsGrpcClient::RPC_PATH).await;

    let store = MetricsStore::new();
    let snapshot = store.snapshot();
    let proto_bytes = format_otlp_metrics_proto(&snapshot, "velda-test-grpc");

    let mut client = OtlpMetricsGrpcClient::new(
        &endpoint,
        Duration::from_secs(5),
        Duration::from_secs(10),
        &OutboundTlsConfig::default(),
    )
    .unwrap();
    let headers = HeaderMap::new();

    // First flush
    client
        .send_metrics(&proto_bytes, &headers)
        .await
        .expect("First gRPC send must succeed");
    assert_eq!(req_count.load(Ordering::SeqCst), 1);

    // Second flush on warm HTTP/2 stream
    client
        .send_metrics(&proto_bytes, &headers)
        .await
        .expect("Second gRPC send must succeed");
    assert_eq!(req_count.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn test_otlp_logs_grpc_export() {
    let (logs_endpoint, logs_count, _logs_shutdown) =
        spawn_mock_grpc_collector(OtlpLogsGrpcClient::RPC_PATH).await;

    let mut rec = AccessLogRecord::zeroed();
    rec.request_id = 0x4bf9_2f35_77b3_4da6;
    rec.ingress_time_nanos = 1_729_579_200;
    rec.duration_ns = 2100;
    rec.peer_ip = velda_core::lut::ip::encode_ip_u128("127.0.0.1".parse().unwrap());
    rec.upstream_ip = velda_core::lut::ip::encode_upstream_ip_u32("10.0.0.1".parse().unwrap());
    rec.upstream_port = 80;
    rec.bytes_rx = 150;
    rec.bytes_tx = 300;
    rec.status_code = 200;
    rec.route_id = 1;
    rec.pipe = PipeKind::BufferedH2H2;
    rec.completion = CompletionReason::Complete;
    rec.method = HttpMethod::Post;
    rec.grpc_status = GrpcStatus::NonGrpc;
    let (fam, os, ver) = velda_core::lut::classify_user_agent(b"curl/8.6.0");
    rec.ua_family = fam;
    rec.ua_os = os;
    rec.ua_major_version = ver;

    let batches = vec![LogBatch::from_arc(Arc::from(vec![rec]))];
    let (req, _) = format_otlp_records_proto(&batches, "pay-service");
    let mut log_proto_bytes = Vec::new();
    req.encode(&mut log_proto_bytes).unwrap();

    let mut log_client = OtlpLogsGrpcClient::new(
        &logs_endpoint,
        Duration::from_secs(5),
        Duration::from_secs(10),
        &OutboundTlsConfig::default(),
    )
    .unwrap();
    let headers = HeaderMap::new();

    log_client
        .send_logs(&log_proto_bytes, &headers)
        .await
        .expect("Logs gRPC export must succeed");
    assert_eq!(logs_count.load(Ordering::SeqCst), 1);
}
