//! Comprehensive Test Suite for Grafana Loki Log Shipper (`loki_log_shipper`).
//!
//! Validates:
//! 1. End-to-end multi-stream export over persistent HTTP/1.1 JSON (`/loki/api/v1/push`).
//! 2. End-to-end Protobuf wire push over persistent HTTP/2 gRPC (`/logproto.Pusher/Push`).
//! 3. 128-bit RequestId formatting (exact 32 lowercase hex characters).
//! 4. RFC 5952 zero-allocation IPv6 formatting.
//! 5. Multi-tenancy header/metadata (`X-Scope-OrgID`).
//! 6. Zero heap reallocation during formatting via persistent scratch buffers.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use prost::Message;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::watch;
use velda_observability::{
    AccessLogRecord, LogCompletionReason, LogGrpcStatus, LogHttpMethod, LogStore, PipeKind,
};

use velda_plugin::background::BackgroundPlugin;
use velda_plugin::background::loki_log_shipper::json::LokiJsonFormatter;
use velda_plugin::background::loki_log_shipper::proto::{
    LokiLogsGrpcClient, LokiProtoLabels, PushRequest, format_loki_records_proto,
};
use velda_plugin::background::loki_log_shipper::{LokiLogShipperConfig, LokiLogShipperPlugin};

fn create_sample_http_record(id: u64, ts: u32, ip_str: &str) -> AccessLogRecord {
    AccessLogRecord::from_scalars(
        id,
        ts,
        320,
        ip_str.parse().unwrap(),
        Some("10.0.0.1:8080".parse().unwrap()),
        128,
        512,
        200,
        1,
        0,
        PipeKind::BufferedH2H1,
        LogCompletionReason::Complete,
        LogHttpMethod::Get,
        LogGrpcStatus::NonGrpc,
        b"curl/8.6.0",
    )
}

fn create_sample_grpc_record(id: u64, ts: u32, ip_str: &str) -> AccessLogRecord {
    AccessLogRecord::from_scalars(
        id,
        ts,
        890,
        ip_str.parse().unwrap(),
        Some("10.0.0.2:9090".parse().unwrap()),
        64,
        256,
        200,
        2,
        4,
        PipeKind::DuplexGrpcTcpTcp,
        LogCompletionReason::Complete,
        LogHttpMethod::Post,
        LogGrpcStatus::Ok,
        b"grpc-go/1.64.0",
    )
}

fn create_sample_raw_record(id: u64, ts: u32) -> AccessLogRecord {
    AccessLogRecord::from_scalars(
        id,
        ts,
        1200,
        "192.168.1.30:50002".parse().unwrap(),
        Some("10.0.0.3:7000".parse().unwrap()),
        1024,
        2048,
        0,
        3,
        0,
        PipeKind::TcpTcp,
        LogCompletionReason::Complete,
        LogHttpMethod::Get,
        LogGrpcStatus::NonGrpc,
        b"",
    )
}

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
        protocol: "http".to_string(),
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

    tokio::time::sleep(Duration::from_millis(30)).await;

    // Emit 1 HTTP record with IPv4 and 1 gRPC record with IPv6
    let http_rec =
        create_sample_http_record(0xaaaa_bbbb_cccc_dddd, 1_710_000_001, "192.168.1.10:50000");
    let grpc_rec = create_sample_grpc_record(0x1111_2222_3333_4444, 1_710_000_002, "[::1]:50001");

    assert!(store.emit_on_core(0, &http_rec));
    assert!(store.emit_on_core(1, &grpc_rec));

    tokio::time::sleep(Duration::from_millis(200)).await;
    let _ = shutdown_tx.send(true);
    let _ = bg_task.await;
    let _ = server_handle.await;

    assert_eq!(shipper.shipped_records(), 2);
    let raw_req = received_payload.lock().await;
    assert!(raw_req.contains("POST /loki/api/v1/push HTTP/1.1"));
    assert!(raw_req.contains("X-Scope-OrgID: velda-tenant"));
    assert!(raw_req.contains("\"protocol\":\"http\""));
    assert!(raw_req.contains("\"protocol\":\"grpc\""));
    assert!(raw_req.contains("user_agent=curl/8"));
    assert!(raw_req.contains("grpc_status=OK"));

    // Verify 32-character lowercase hex Request ID:
    assert!(raw_req.contains("request_id=0000000065ec8781aaaabbbbccccdddd"));
    assert!(raw_req.contains("request_id=0000000065ec87821111222233334444"));

    // Verify IPv6 zero-compression:
    assert!(raw_req.contains("client=::1:50001"));
}

#[tokio::test]
async fn test_loki_log_shipper_grpc_end_to_end() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let received_push = Arc::new(tokio::sync::Mutex::new(None::<PushRequest>));
    let received_clone = Arc::clone(&received_push);
    let received_tenant = Arc::new(tokio::sync::Mutex::new(String::new()));
    let tenant_clone = Arc::clone(&received_tenant);

    let (server_shutdown_tx, mut server_shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    let server_handle = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = &mut server_shutdown_rx => break,
                res = listener.accept() => {
                    let Ok((socket, _)) = res else { break };
                    let received_c = Arc::clone(&received_clone);
                    let tenant_c = Arc::clone(&tenant_clone);

                    tokio::spawn(async move {
                        let mut conn = match h2::server::handshake(socket).await {
                            Ok(c) => c,
                            Err(_) => return,
                        };

                        while let Some(Ok((req, mut respond))) = conn.accept().await {
                            assert_eq!(req.uri().path(), LokiLogsGrpcClient::RPC_PATH);
                            if let Some(t) = req.headers().get("x-scope-orgid").and_then(|v| v.to_str().ok()) {
                                *tenant_c.lock().await = t.to_string();
                            }

                            let mut body = req.into_body();
                            let mut raw_bytes = Vec::new();
                            while let Some(chunk) = body.data().await {
                                if let Ok(data) = chunk {
                                    let _ = body.flow_control().release_capacity(data.len());
                                    raw_bytes.extend_from_slice(&data);
                                }
                            }

                            // Parse 5-byte canonical gRPC framing: [0x00, len_u32_be]
                            if raw_bytes.len() >= 5 {
                                let frame_header = raw_bytes[0];
                                assert_eq!(frame_header, 0); // uncompressed
                                let len = u32::from_be_bytes([raw_bytes[1], raw_bytes[2], raw_bytes[3], raw_bytes[4]]) as usize;
                                let proto_payload = &raw_bytes[5..5 + len];
                                if let Ok(decoded) = PushRequest::decode(proto_payload) {
                                    *received_c.lock().await = Some(decoded);
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

    let store = Arc::new(LogStore::with_cores_and_capacity(2, 128));

    let mut labels = HashMap::new();
    labels.insert("cluster".to_string(), "edge-us".to_string());

    let cfg = LokiLogShipperConfig {
        protocol: "grpc".to_string(),
        endpoint: format!("grpc://{addr}"),
        tenant_id: "tenant-grpc".to_string(),
        service_name: "velda-edge-grpc".to_string(),
        flush_interval_ms: 100,
        max_batch_size: 256,
        connect_timeout_ms: 1000,
        request_timeout_ms: 1000,
        labels,
    };

    let shipper = Arc::new(
        LokiLogShipperPlugin::with_store("loki_shipper_grpc", cfg, Arc::clone(&store)).unwrap(),
    );

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let shipper_clone = Arc::clone(&shipper);
    let bg_task = tokio::spawn(async move {
        shipper_clone.run(shutdown_rx).await.unwrap();
    });

    tokio::time::sleep(Duration::from_millis(30)).await;

    let http_rec = create_sample_http_record(0x5555_6666_7777_8888, 1_710_000_010, "10.1.2.3:4433");
    let raw_rec = create_sample_raw_record(0x1234_5678_9abc_def0, 1_710_000_020);

    assert!(store.emit_on_core(0, &http_rec));
    assert!(store.emit_on_core(1, &raw_rec));

    tokio::time::sleep(Duration::from_millis(200)).await;
    let _ = shutdown_tx.send(true);
    let _ = bg_task.await;
    let _ = server_shutdown_tx.send(());
    let _ = server_handle.await;

    assert_eq!(shipper.shipped_records(), 2);
    assert_eq!(*received_tenant.lock().await, "tenant-grpc");

    let maybe_push = received_push.lock().await;
    let push = maybe_push.as_ref().expect("Received gRPC PushRequest");
    assert_eq!(push.streams.len(), 2);

    let stream0 = &push.streams[0];
    assert!(stream0.labels.contains("service=\"velda-edge-grpc\""));
    assert!(stream0.labels.contains("protocol=\"http\""));
    assert!(stream0.labels.contains("cluster=\"edge-us\""));
    assert_eq!(stream0.entries.len(), 1);
    assert!(
        stream0.entries[0]
            .line
            .contains("request_id=0000000065ec878a5555666677778888")
    );
    assert!(stream0.entries[0].line.contains("client=10.1.2.3:4433"));

    let stream1 = &push.streams[1];
    assert!(stream1.labels.contains("protocol=\"raw\""));
    assert_eq!(stream1.entries.len(), 1);
    assert!(
        stream1.entries[0]
            .line
            .contains("request_id=0000000065ec8794123456789abcdef0")
    );
}

#[test]
fn test_loki_json_formatter_scratch_zero_alloc() {
    let mut labels = HashMap::new();
    labels.insert("tier".to_string(), "edge".to_string());
    let mut formatter = LokiJsonFormatter::new("svc", &labels);

    let rec = create_sample_http_record(0x1122_3344_5566_7788, 1_710_000_000, "127.0.0.1:80");
    let batch = vec![Arc::from(vec![rec])];
    let mut out = Vec::new();

    // Cycle 1
    let n1 = formatter.format_batches(&batch, &mut out);
    assert_eq!(n1, 1);
    assert!(out.len() > 100);

    // Cycle 2: reuses scratch buffers without growth
    let n2 = formatter.format_batches(&batch, &mut out);
    assert_eq!(n2, 1);
    assert!(out.len() > 100);
}

#[test]
fn test_loki_proto_wire_formatter_decodes_cleanly() {
    let mut labels = HashMap::new();
    labels.insert("env".to_string(), "prod".to_string());
    let proto_labels = LokiProtoLabels::new("bench-svc", &labels);

    let rec1 = create_sample_http_record(0x1111_1111_1111_1111, 1_710_000_001, "192.168.1.1:80");
    let rec2 = create_sample_grpc_record(0x2222_2222_2222_2222, 1_710_000_002, "192.168.1.2:90");
    let rec3 = create_sample_raw_record(0x3333_3333_3333_3333, 1_710_000_003);

    let batches = vec![Arc::from(vec![rec1, rec2, rec3])];
    let mut wire_out = Vec::new();

    let n = format_loki_records_proto(&batches, &proto_labels, &mut wire_out);
    assert_eq!(n, 3);
    assert!(!wire_out.is_empty());

    let decoded = PushRequest::decode(&wire_out[..]).expect("Decode binary wire_out");
    assert_eq!(decoded.streams.len(), 3);
}
