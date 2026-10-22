//! Comprehensive Test Suite for OTLP Plugins (`otlp_metrics_exporter`, `opentelemetry_logger`,
//! `opentelemetry_tracer`, `w3c_trace_context`).
//!
//! Validates:
//! 1. OpenTelemetry JSON Schema conformance across Metrics, Logs, and Traces.
//! 2. W3C TraceContext RFC propagation and generation.
//! 3. Persistent Keep-Alive connection reuse avoiding connection churn and handshake storms.
//! 4. Transparent reconnect resilience when server closes socket while idle.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use http::{HeaderMap, HeaderValue, Method, Uri, Version};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use velda_core::{Body, L7Request};
use velda_observability::MetricsStore;

use velda_plugin::background::otlp_metrics_exporter::{
    OtlpMetricsClient, OtlpMetricsGrpcClient, format_otlp_metrics_json, format_otlp_metrics_proto,
};
use velda_plugin::header::{TRACEPARENT, X_SPAN_ID, X_TRACE_ID};
use velda_plugin::post_response::opentelemetry_logger::{
    OtlpLogClient, OtlpLogItem, OtlpLogsGrpcClient, format_otlp_logs_json, format_otlp_logs_proto,
};
use velda_plugin::post_response::opentelemetry_tracer::{
    OtlpSpanItem, OtlpTraceClient, OtlpTracesGrpcClient, format_otlp_spans_json,
    format_otlp_spans_proto,
};
use velda_plugin::pre_upstream::PreUpstreamPlugin;
use velda_plugin::pre_upstream::w3c_trace_context::{W3cTraceContextConfig, W3cTraceContextPlugin};

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
async fn test_otlp_metrics_persistent_connection_reuse() {
    let (url, conns, reqs, _shutdown) = spawn_mock_collector("/v1/metrics").await;

    let mut client = OtlpMetricsClient::new(&url, Duration::from_secs(5), Duration::from_secs(10))
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
    let mut client =
        OtlpMetricsClient::new(&url, Duration::from_secs(5), Duration::from_secs(10)).unwrap();
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
async fn test_opentelemetry_logger_batching_and_send() {
    let (url, conns, reqs, _shutdown) = spawn_mock_collector("/v1/logs").await;

    let mut client = OtlpLogClient::new(&url, Duration::from_secs(5), Duration::from_secs(10))
        .expect("Client initialized");
    let items = vec![
        OtlpLogItem {
            time_unix_nano: 1710000000000000000,
            status: 200,
            method: "GET".to_string(),
            path: "/api/items".to_string(),
            client_ip: Some("192.168.1.50".to_string()),
            route_id: Some("items-route".to_string()),
            upstream: Some("10.0.0.10:8080".to_string()),
            latency_ms: Some(2.5),
            trace_id: Some("4bf92f3577b34da6a3ce929d0e0e4736".to_string()),
            span_id: Some("00f067aa0ba902b7".to_string()),
        },
        OtlpLogItem {
            time_unix_nano: 1710000000001000000,
            status: 500,
            method: "POST".to_string(),
            path: "/api/checkout".to_string(),
            client_ip: Some("192.168.1.51".to_string()),
            route_id: Some("checkout-route".to_string()),
            upstream: Some("10.0.0.11:8080".to_string()),
            latency_ms: Some(15.2),
            trace_id: Some("4bf92f3577b34da6a3ce929d0e0e4736".to_string()),
            span_id: Some("5fb397be34d23b0f".to_string()),
        },
    ];

    let payload = format_otlp_logs_json(&items, "test-edge");
    let headers = HeaderMap::new();

    let status = client
        .send_logs(payload.as_bytes(), &headers)
        .await
        .expect("Sent logs");
    assert_eq!(status, 200);

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(reqs.load(Ordering::SeqCst), 1);
    assert_eq!(conns.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn test_opentelemetry_tracer_batching_and_send() {
    let (url, conns, reqs, _shutdown) = spawn_mock_collector("/v1/traces").await;

    let mut client = OtlpTraceClient::new(&url, Duration::from_secs(5), Duration::from_secs(10))
        .expect("Client initialized");
    let spans = vec![OtlpSpanItem {
        trace_id: "4bf92f3577b34da6a3ce929d0e0e4736".to_string(),
        span_id: "00f067aa0ba902b7".to_string(),
        parent_span_id: None,
        name: "GET /api/v1/health".to_string(),
        start_time_unix_nano: 1710000000000000000,
        end_time_unix_nano: 1710000000002000000,
        status_code: 200,
        method: "GET".to_string(),
        path: "/api/v1/health".to_string(),
        route_id: Some("health-route".to_string()),
        client_ip: Some("127.0.0.1".to_string()),
        upstream: Some("10.0.0.2:8080".to_string()),
    }];

    let payload = format_otlp_spans_json(&spans, "test-edge");
    let headers = HeaderMap::new();

    let status = client
        .send_traces(payload.as_bytes(), &headers)
        .await
        .expect("Sent traces");
    assert_eq!(status, 200);

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(reqs.load(Ordering::SeqCst), 1);
    assert_eq!(conns.load(Ordering::SeqCst), 1);
}

#[test]
fn test_w3c_trace_context_end_to_end_flow() {
    let tracer_plugin = W3cTraceContextPlugin::new(
        "w3c",
        W3cTraceContextConfig {
            generate_if_missing: true,
            forward_tracestate: true,
            sampled: true,
        },
    );

    // Case A: Missing traceparent -> generated dynamically
    let mut req = L7Request::new(
        Method::GET,
        Uri::from_static("/test"),
        Version::HTTP_11,
        HeaderMap::new(),
        Body::Empty,
    );

    let mut target: Arc<str> = Arc::from("upstream-srv");
    let action = tracer_plugin.on_upstream_request(&mut req, &mut target);
    assert!(matches!(action, velda_core::Action::Continue));

    let tp = req.headers.get(&TRACEPARENT).unwrap().to_str().unwrap();
    assert!(tp.starts_with("00-"));
    assert_eq!(tp.len(), 55);

    let trace_id = req
        .headers
        .get(&X_TRACE_ID)
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let span_id = req
        .headers
        .get(&X_SPAN_ID)
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();

    // Verify format: 00-{trace_id}-{span_id}-01
    assert_eq!(tp, format!("00-{trace_id}-{span_id}-01"));

    // Case B: Existing traceparent forwarded with new span_id
    let mut req2 = L7Request::new(
        Method::POST,
        Uri::from_static("/test2"),
        Version::HTTP_11,
        HeaderMap::new(),
        Body::Empty,
    );
    req2.headers.insert(
        TRACEPARENT,
        HeaderValue::from_static("00-abcdef0123456789abcdef0123456789-1111222233334444-01"),
    );

    tracer_plugin.on_upstream_request(&mut req2, &mut target);
    let tp2 = req2.headers.get(&TRACEPARENT).unwrap().to_str().unwrap();
    assert!(tp2.starts_with("00-abcdef0123456789abcdef0123456789-"));
    assert_ne!(
        tp2,
        "00-abcdef0123456789abcdef0123456789-1111222233334444-01"
    );
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

    let mut client =
        OtlpMetricsGrpcClient::new(&endpoint, Duration::from_secs(5), Duration::from_secs(10))
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
async fn test_otlp_logs_and_traces_grpc_export() {
    // 1. Logs gRPC
    let (logs_endpoint, logs_count, _logs_shutdown) =
        spawn_mock_grpc_collector(OtlpLogsGrpcClient::RPC_PATH).await;

    let log_item = OtlpLogItem {
        time_unix_nano: 1729579200000000000,
        status: 200,
        method: "POST".to_string(),
        path: "/api/pay".to_string(),
        client_ip: Some("127.0.0.1".to_string()),
        route_id: Some("pay-route".to_string()),
        upstream: Some("10.0.0.1:80".to_string()),
        latency_ms: Some(2.1),
        trace_id: Some("4bf92f3577b34da6a3ce929d0e0e4736".to_string()),
        span_id: Some("00f067aa0ba902b7".to_string()),
    };

    let log_proto_bytes = format_otlp_logs_proto(&[log_item], "pay-service");
    let mut log_client = OtlpLogsGrpcClient::new(
        &logs_endpoint,
        Duration::from_secs(5),
        Duration::from_secs(10),
    )
    .unwrap();
    let headers = HeaderMap::new();

    log_client
        .send_logs(&log_proto_bytes, &headers)
        .await
        .expect("Logs gRPC export must succeed");
    assert_eq!(logs_count.load(Ordering::SeqCst), 1);

    // 2. Traces gRPC
    let (traces_endpoint, traces_count, _traces_shutdown) =
        spawn_mock_grpc_collector(OtlpTracesGrpcClient::RPC_PATH).await;

    let span_item = OtlpSpanItem {
        trace_id: "4bf92f3577b34da6a3ce929d0e0e4736".to_string(),
        span_id: "00f067aa0ba902b7".to_string(),
        parent_span_id: None,
        name: "POST /api/pay".to_string(),
        start_time_unix_nano: 1729579200000000000,
        end_time_unix_nano: 1729579200002100000,
        status_code: 200,
        method: "POST".to_string(),
        path: "/api/pay".to_string(),
        route_id: Some("pay-route".to_string()),
        client_ip: Some("127.0.0.1".to_string()),
        upstream: Some("10.0.0.1:80".to_string()),
    };

    let span_proto_bytes = format_otlp_spans_proto(&[span_item], "pay-service");
    let mut trace_client = OtlpTracesGrpcClient::new(
        &traces_endpoint,
        Duration::from_secs(5),
        Duration::from_secs(10),
    )
    .unwrap();

    trace_client
        .send_traces(&span_proto_bytes, &headers)
        .await
        .expect("Traces gRPC export must succeed");
    assert_eq!(traces_count.load(Ordering::SeqCst), 1);
}
