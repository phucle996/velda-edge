//! Comprehensive Test Suite for Grafana Loki Log Shipper (`loki_log_shipper`).
//!
//! Validates:
//! 1. End-to-end multi-stream export over pooled `LokiEtp` HTTP/1.1 keep-alive (`/loki/api/v1/push`).
//! 2. End-to-end Protobuf wire push over pooled `LokiEtp` HTTP/2 gRPC (`/logproto.Pusher/Push`).
//! 3. `wire_plugin_etps()` registry wiring and automatic in-place `lease.reconnect()` recovery
//!    across both HTTP/1.1 and HTTP/2 gRPC sessions.
//! 4. Trailer-only gRPC status error propagation and clean plugin shutdown.
//! 5. 128-bit RequestId formatting, RFC 5952 IPv6 compression, and formatter buffer reuse.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use prost::Message;
use rustc_hash::FxHashMap;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::watch;
use velda_core::{CompletionReason, GrpcStatus, HttpMethod, PipeKind};
use velda_etp::loki::LokiEtp;
use velda_etp::registry::build_registry;
use velda_etp::{EtpEntryConfig, EtpKind, EtpPoolSettings};
use velda_observability::{TelemetryBatch, TelemetryRecord, TelemetryStore};

use velda_plugin::PluginConfig;
use velda_plugin::background::BackgroundPlugin;
use velda_plugin::background::loki_log_shipper::json::LokiJsonFormatter;
use velda_plugin::background::loki_log_shipper::proto::{
    LokiProtoLabels, PushRequest, format_loki_records_proto,
};
use velda_plugin::background::loki_log_shipper::{
    LokiLogShipperConfig, LokiLogShipperPlugin, wire,
};
use velda_plugin::factory::{instantiate_plugin, wire_plugin_etps};

fn parse_ip(s: &str) -> std::net::IpAddr {
    if let Ok(addr) = s.parse::<std::net::SocketAddr>() {
        addr.ip()
    } else {
        s.parse::<std::net::IpAddr>()
            .expect("valid ip or socket addr")
    }
}

fn create_sample_http_record(id: u64, ts: u32, ip_str: &str) -> TelemetryRecord {
    let mut rec = TelemetryRecord::zeroed();
    rec.request_id = id;
    rec.ingress_time_nanos = (ts as u64) * 1_000_000_000;
    rec.duration_ns = 320;
    rec.peer_ip = velda_core::lut::ip::encode_ip_u128(parse_ip(ip_str));
    rec.upstream_ip = velda_core::lut::ip::encode_upstream_ip_u32("10.0.0.1".parse().unwrap());
    rec.upstream_port = 8080;
    rec.bytes_rx = 128;
    rec.bytes_tx = 512;
    rec.status_code = 200;
    rec.route_id = 1;
    rec.pipe = PipeKind::BufferedH2H1;
    rec.completion = CompletionReason::Complete;
    rec.method = HttpMethod::Get;
    rec.grpc_status = GrpcStatus::NonGrpc;
    let (fam, os, ver) = velda_core::lut::classify_user_agent(b"curl/8.6.0");
    rec.ua_family = fam;
    rec.ua_os = os;
    rec.ua_major_version = ver;
    rec
}

fn create_sample_grpc_record(id: u64, ts: u32, ip_str: &str) -> TelemetryRecord {
    let mut rec = TelemetryRecord::zeroed();
    rec.request_id = id;
    rec.ingress_time_nanos = (ts as u64) * 1_000_000_000;
    rec.duration_ns = 890;
    rec.peer_ip = velda_core::lut::ip::encode_ip_u128(parse_ip(ip_str));
    rec.upstream_ip = velda_core::lut::ip::encode_upstream_ip_u32("10.0.0.2".parse().unwrap());
    rec.upstream_port = 9090;
    rec.bytes_rx = 64;
    rec.bytes_tx = 256;
    rec.status_code = 200;
    rec.route_id = 2;
    rec.rpc_msgs_or_flags = 4;
    rec.pipe = PipeKind::DuplexGrpcTcpTcp;
    rec.completion = CompletionReason::Complete;
    rec.method = HttpMethod::Post;
    rec.grpc_status = GrpcStatus::Ok;
    let (fam, os, ver) = velda_core::lut::classify_user_agent(b"grpc-go/1.64.0");
    rec.ua_family = fam;
    rec.ua_os = os;
    rec.ua_major_version = ver;
    rec
}

fn create_sample_raw_record(id: u64, ts: u32) -> TelemetryRecord {
    let mut rec = TelemetryRecord::zeroed();
    rec.request_id = id;
    rec.ingress_time_nanos = (ts as u64) * 1_000_000_000;
    rec.duration_ns = 1200;
    rec.peer_ip = velda_core::lut::ip::encode_ip_u128("192.168.1.30".parse().unwrap());
    rec.upstream_ip = velda_core::lut::ip::encode_upstream_ip_u32("10.0.0.3".parse().unwrap());
    rec.upstream_port = 7000;
    rec.bytes_rx = 1024;
    rec.bytes_tx = 2048;
    rec.status_code = 0;
    rec.route_id = 3;
    rec.pipe = PipeKind::TcpTcp;
    rec.completion = CompletionReason::Complete;
    rec.method = HttpMethod::Get;
    rec.grpc_status = GrpcStatus::NonGrpc;
    rec
}

#[tokio::test]
async fn test_loki_multi_protocol_e2e() {
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

    let etp = Arc::new(
        LokiEtp::from_entry(&EtpEntryConfig {
            id: "loki-http-e2e".into(),
            kind: EtpKind::Loki,
            pool: EtpPoolSettings {
                pool_size: 2,
                connect_timeout_ms: 1000,
                idle_timeout_ms: 60_000,
            },
            tls: None,
            settings: serde_json::json!({
                "protocol": "http",
                "endpoint": format!("http://{addr}/loki/api/v1/push"),
                "tenant_id": "velda-tenant",
                "bearer_token": "loki-secret-tok",
                "headers": {
                    "X-Velda-Source": "edge-gw"
                }
            }),
        })
        .unwrap(),
    );

    let store = Arc::new(TelemetryStore::with_cores_and_capacity(2, 128));
    let mut labels = HashMap::new();
    labels.insert("env".to_string(), "test".to_string());

    let cfg = LokiLogShipperConfig {
        etp_id: "loki-http-e2e".to_string(),
        service_name: "velda-edge".to_string(),
        flush_interval_ms: 50,
        max_batch_size: 256,
        request_timeout_ms: 1000,
        labels,
    };

    let shipper = Arc::new(
        LokiLogShipperPlugin::with_etp("loki_shipper", cfg, etp, Arc::clone(&store)).unwrap(),
    );

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let shipper_clone = Arc::clone(&shipper);
    let bg_task = tokio::spawn(async move {
        shipper_clone.run(shutdown_rx).await.unwrap();
    });

    while store.active_subscriber_count.load(Ordering::Acquire) == 0 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    let http_rec =
        create_sample_http_record(0xaaaa_bbbb_cccc_dddd, 1_710_000_001, "192.168.1.10:50000");
    let grpc_rec = create_sample_grpc_record(0x1111_2222_3333_4444, 1_710_000_002, "[::1]:50001");

    assert!(store.emit_on_core(0, &http_rec));
    assert!(store.emit_on_core(1, &grpc_rec));
    store.flush_all_lanes(256);

    for _ in 0..50 {
        if shipper.shipped_records.load(Ordering::Relaxed) >= 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(15)).await;
    }
    let _ = shutdown_tx.send(true);
    let _ = bg_task.await;
    let _ = server_handle.await;

    assert_eq!(shipper.shipped_records.load(Ordering::Relaxed), 2);
    assert_eq!(shipper.failed_pushes.load(Ordering::Relaxed), 0);
    let raw_req = received_payload.lock().await;
    assert!(raw_req.starts_with("POST /loki/api/v1/push HTTP/1.1\r\n"));
    assert!(raw_req.contains("X-Scope-OrgID: velda-tenant\r\n"));
    assert!(raw_req.contains("Authorization: Bearer loki-secret-tok\r\n"));
    assert!(raw_req.contains("X-Velda-Source: edge-gw\r\n"));
    assert!(raw_req.contains("\"protocol\":\"http\""));
    assert!(raw_req.contains("\"protocol\":\"grpc\""));
    assert!(raw_req.contains("user_agent=curl/8"));
    assert!(raw_req.contains("grpc_status=OK"));

    // Verify 32-character lowercase hex Request ID:
    assert!(raw_req.contains("request_id=17bb23f0e185ca00aaaabbbbccccdddd"));
    assert!(raw_req.contains("request_id=17bb23f11d2094001111222233334444"));

    // Verify IPv6 zero-compression:
    assert!(raw_req.contains("client=::1"));
}

#[tokio::test]
async fn test_loki_log_shipper_grpc_end_to_end() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let received_push = Arc::new(tokio::sync::Mutex::new(None::<PushRequest>));
    let received_clone = Arc::clone(&received_push);
    let received_tenant = Arc::new(tokio::sync::Mutex::new(String::new()));
    let tenant_clone = Arc::clone(&received_tenant);
    let received_meta = Arc::new(tokio::sync::Mutex::new(String::new()));
    let meta_clone = Arc::clone(&received_meta);

    let (server_shutdown_tx, mut server_shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    let server_handle = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = &mut server_shutdown_rx => break,
                res = listener.accept() => {
                    let Ok((socket, _)) = res else { break };
                    let received_c = Arc::clone(&received_clone);
                    let tenant_c = Arc::clone(&tenant_clone);
                    let meta_c = Arc::clone(&meta_clone);

                    tokio::spawn(async move {
                        let mut conn = match h2::server::handshake(socket).await {
                            Ok(c) => c,
                            Err(_) => return,
                        };

                        while let Some(Ok((req, mut respond))) = conn.accept().await {
                            assert_eq!(req.uri().path(), wire::LOKI_GRPC_PATH);
                            if let Some(t) = req.headers().get("x-scope-orgid").and_then(|v| v.to_str().ok()) {
                                *tenant_c.lock().await = t.to_string();
                            }
                            if let Some(m) = req.headers().get("x-grpc-meta").and_then(|v| v.to_str().ok()) {
                                *meta_c.lock().await = m.to_string();
                            }

                            let mut body = req.into_body();
                            let mut raw_bytes = Vec::new();
                            while let Some(chunk) = body.data().await {
                                if let Ok(data) = chunk {
                                    let _ = body.flow_control().release_capacity(data.len());
                                    raw_bytes.extend_from_slice(&data);
                                }
                            }

                            if raw_bytes.len() >= 5 {
                                let frame_header = raw_bytes[0];
                                assert_eq!(frame_header, 0);
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

    let etp = Arc::new(
        LokiEtp::from_entry(&EtpEntryConfig {
            id: "loki-grpc-e2e".into(),
            kind: EtpKind::Loki,
            pool: EtpPoolSettings {
                pool_size: 2,
                connect_timeout_ms: 1000,
                idle_timeout_ms: 60_000,
            },
            tls: None,
            settings: serde_json::json!({
                "endpoint": format!("grpc://{addr}"),
                "tenant_id": "tenant-grpc",
                "headers": {
                    "x-grpc-meta": "ok"
                }
            }),
        })
        .unwrap(),
    );

    let store = Arc::new(TelemetryStore::with_cores_and_capacity(2, 128));
    let mut labels = HashMap::new();
    labels.insert("cluster".to_string(), "edge-us".to_string());

    let cfg = LokiLogShipperConfig {
        etp_id: "loki-grpc-e2e".to_string(),
        service_name: "velda-edge-grpc".to_string(),
        flush_interval_ms: 50,
        max_batch_size: 256,
        request_timeout_ms: 1000,
        labels,
    };

    let shipper = Arc::new(
        LokiLogShipperPlugin::with_etp("loki_shipper_grpc", cfg, etp, Arc::clone(&store)).unwrap(),
    );

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let shipper_clone = Arc::clone(&shipper);
    let bg_task = tokio::spawn(async move {
        shipper_clone.run(shutdown_rx).await.unwrap();
    });

    while store.active_subscriber_count.load(Ordering::Acquire) == 0 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    let http_rec = create_sample_http_record(0x5555_6666_7777_8888, 1_710_000_010, "10.1.2.3:4433");
    let raw_rec = create_sample_raw_record(0x1234_5678_9abc_def0, 1_710_000_020);

    assert!(store.emit_on_core(0, &http_rec));
    assert!(store.emit_on_core(1, &raw_rec));
    store.flush_all_lanes(256);

    for _ in 0..50 {
        if shipper.shipped_records.load(Ordering::Relaxed) >= 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(15)).await;
    }
    let _ = shutdown_tx.send(true);
    let _ = bg_task.await;
    let _ = server_shutdown_tx.send(());
    let _ = server_handle.await;

    assert_eq!(shipper.shipped_records.load(Ordering::Relaxed), 2);
    assert_eq!(shipper.failed_pushes.load(Ordering::Relaxed), 0);
    assert_eq!(*received_tenant.lock().await, "tenant-grpc");
    assert_eq!(*received_meta.lock().await, "ok");

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
            .contains("request_id=17bb23f2f9f6e4005555666677778888")
    );
    assert!(stream0.entries[0].line.contains("client=10.1.2.3"));

    let stream1 = &push.streams[1];
    assert!(stream1.labels.contains("protocol=\"raw\""));
    assert_eq!(stream1.entries.len(), 1);
    assert!(
        stream1.entries[0]
            .line
            .contains("request_id=17bb23f54e02c800123456789abcdef0")
    );
}

#[tokio::test]
async fn test_loki_grpc_trailers_error() {
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
                        .header("grpc-status", "3")
                        .header("grpc-message", "invalid entry timestamp")
                        .body(())
                        .unwrap();
                    let _ = respond.send_response(resp, true);
                }
            });
        }
    });

    let etp = Arc::new(
        LokiEtp::from_entry(&EtpEntryConfig {
            id: "loki-grpc-err".into(),
            kind: EtpKind::Loki,
            pool: EtpPoolSettings::default(),
            tls: None,
            settings: serde_json::json!({ "endpoint": format!("grpc://{addr}") }),
        })
        .unwrap(),
    );
    let template = wire::bake_wire_template(&etp.config).unwrap();
    let mut head_buf = Vec::new();
    let mut resp_buf = Vec::new();
    let err = wire::push_logs(
        &etp,
        &template,
        &mut head_buf,
        b"bad",
        &mut resp_buf,
        Duration::from_secs(2),
    )
    .await
    .expect_err("Trailers-Only grpc-status 3 must return Err");
    assert!(err.to_string().contains("invalid entry timestamp"));
}

#[tokio::test]
async fn test_loki_log_shipper_shutdown() {
    let etp = Arc::new(
        LokiEtp::from_entry(&EtpEntryConfig {
            id: "loki-shutdown".into(),
            kind: EtpKind::Loki,
            pool: EtpPoolSettings::default(),
            tls: None,
            settings: serde_json::json!({ "endpoint": "http://127.0.0.1:3100" }),
        })
        .unwrap(),
    );
    let store = Arc::new(TelemetryStore::with_cores_and_capacity(1, 64));
    let config = LokiLogShipperConfig {
        etp_id: "loki-shutdown".to_string(),
        service_name: "test-service".to_string(),
        flush_interval_ms: 50,
        max_batch_size: 100,
        request_timeout_ms: 1000,
        labels: HashMap::new(),
    };

    let plugin = Arc::new(LokiLogShipperPlugin::with_etp("loki_test", config, etp, store).unwrap());
    let (tx, rx) = watch::channel(false);
    let handle = tokio::spawn(async move { plugin.run(rx).await });

    tx.send(true).unwrap();
    assert!(handle.await.unwrap().is_ok());
}

#[tokio::test]
async fn test_loki_etp_registry_wiring_and_reconnect_recovery() {
    // 1. HTTP/1.1 registry wiring + abrupt idle drop -> in-place reconnect recovery
    let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http_port = http_listener.local_addr().unwrap().port();

    let http_server = tokio::spawn(async move {
        let (mut s1, _) = http_listener.accept().await.unwrap();
        let mut buf = vec![0u8; 8192];
        let _ = s1.read(&mut buf).await;
        drop(s1);

        let (mut s2, _) = http_listener.accept().await.unwrap();
        let n2 = s2.read(&mut buf).await.unwrap();
        let req2 = String::from_utf8_lossy(&buf[..n2]);
        assert!(req2.starts_with("POST /loki/api/v1/push HTTP/1.1\r\n"));
        s2.write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n")
            .await
            .unwrap();
        s2.flush().await.unwrap();
    });

    let registry = build_registry(&[EtpEntryConfig {
        id: "loki-wired".into(),
        kind: EtpKind::Loki,
        pool: EtpPoolSettings {
            pool_size: 1,
            connect_timeout_ms: 2000,
            idle_timeout_ms: 60_000,
        },
        tls: None,
        settings: serde_json::json!({
            "host": "127.0.0.1",
            "port": http_port,
            "protocol": "http",
            "tenant_id": "wired-tenant"
        }),
    }])
    .unwrap();

    let plugin_cfg = PluginConfig {
        id: "loki_log_shipper".into(),
        config: serde_json::json!({
            "etp_id": "loki-wired",
            "service_name": "velda-wired",
            "flush_interval_ms": 100,
            "request_timeout_ms": 2000
        }),
    };
    let loaded = instantiate_plugin(&plugin_cfg, &[]).unwrap();
    let mut available = FxHashMap::default();
    available.insert(loaded.id().to_string(), loaded.clone());
    wire_plugin_etps(&available, &registry).unwrap();

    let store = velda_observability::global_telemetry_store();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let runner = tokio::spawn(loaded.run_background(shutdown_rx).unwrap());

    while store.active_subscriber_count.load(Ordering::Acquire) == 0 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    store.emit_on_core(
        0,
        &create_sample_http_record(0x9999, 1_710_000_030, "10.0.0.9:1234"),
    );
    store.flush_all_lanes(128);

    tokio::time::timeout(Duration::from_secs(3), http_server)
        .await
        .unwrap()
        .unwrap();
    tokio::time::sleep(Duration::from_millis(40)).await;

    let _ = shutdown_tx.send(true);
    runner.await.unwrap().unwrap();

    // Missing ETP produces an explicit error
    let err_plugin = instantiate_plugin(&plugin_cfg, &[]).unwrap();
    let mut missing_map = FxHashMap::default();
    missing_map.insert(err_plugin.id().to_string(), err_plugin);
    assert!(wire_plugin_etps(&missing_map, &velda_etp::EtpRegistry::default()).is_err());

    // 2. HTTP/2 gRPC broken session -> in-place lease.reconnect() recovery
    let grpc_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let grpc_addr = grpc_listener.local_addr().unwrap();

    let grpc_server = tokio::spawn(async move {
        let (s1, _) = grpc_listener.accept().await.unwrap();
        let mut conn1 = h2::server::handshake(s1).await.unwrap();
        if let Some(Ok((_req, respond))) = conn1.accept().await {
            drop(respond);
        }
        drop(conn1);

        let (s2, _) = grpc_listener.accept().await.unwrap();
        let mut conn2 = h2::server::handshake(s2).await.unwrap();
        while let Some(Ok((req, mut respond))) = conn2.accept().await {
            let mut body = req.into_body();
            while let Some(Ok(_)) = body.data().await {}
            let resp = http::Response::builder()
                .status(200)
                .header("content-type", "application/grpc")
                .header("grpc-status", "0")
                .body(())
                .unwrap();
            let _ = respond.send_response(resp, true);
        }
    });

    let grpc_etp = Arc::new(
        LokiEtp::from_entry(&EtpEntryConfig {
            id: "loki-grpc-reconnect".into(),
            kind: EtpKind::Loki,
            pool: EtpPoolSettings {
                pool_size: 1,
                connect_timeout_ms: 2000,
                idle_timeout_ms: 60_000,
            },
            tls: None,
            settings: serde_json::json!({ "endpoint": format!("grpc://{grpc_addr}") }),
        })
        .unwrap(),
    );
    let grpc_template = wire::bake_wire_template(&grpc_etp.config).unwrap();
    let mut h_buf = Vec::new();
    let mut r_buf = Vec::new();
    wire::push_logs(
        &grpc_etp,
        &grpc_template,
        &mut h_buf,
        b"retry-payload",
        &mut r_buf,
        Duration::from_secs(2),
    )
    .await
    .expect("H2 push should recover via lease.reconnect() on attempt 1");
    grpc_server.abort();
}

#[test]
fn test_loki_json_zero_alloc() {
    let mut labels = HashMap::new();
    labels.insert("tier".to_string(), "edge".to_string());
    let mut formatter = LokiJsonFormatter::new("svc", &labels);

    let rec = create_sample_http_record(0x1122_3344_5566_7788, 1_710_000_000, "127.0.0.1:80");
    let batch = vec![TelemetryBatch::from_arc(Arc::from(vec![rec]))];
    let mut out = Vec::new();

    let n1 = formatter.format_batches(&batch, &mut out);
    assert_eq!(n1, 1);
    assert!(out.len() > 100);

    let n2 = formatter.format_batches(&batch, &mut out);
    assert_eq!(n2, 1);
    assert!(out.len() > 100);
}

#[test]
fn test_loki_proto_decode() {
    let mut labels = HashMap::new();
    labels.insert("env".to_string(), "prod".to_string());
    let proto_labels = LokiProtoLabels::new("bench-svc", &labels);

    let rec1 = create_sample_http_record(0x1111_1111_1111_1111, 1_710_000_001, "192.168.1.1:80");
    let rec2 = create_sample_grpc_record(0x2222_2222_2222_2222, 1_710_000_002, "192.168.1.2:90");
    let rec3 = create_sample_raw_record(0x3333_3333_3333_3333, 1_710_000_003);

    let batches = vec![TelemetryBatch::from_arc(Arc::from(vec![rec1, rec2, rec3]))];
    let mut wire_out = Vec::new();

    let n = format_loki_records_proto(&batches, &proto_labels, &mut wire_out);
    assert_eq!(n, 3);
    assert!(!wire_out.is_empty());

    let decoded = PushRequest::decode(&wire_out[..]).expect("Decode binary wire_out");
    assert_eq!(decoded.streams.len(), 3);
}
