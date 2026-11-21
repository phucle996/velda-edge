//! Comprehensive Integration Test Suite for Standard Stream Log Shipper Plugin (`std_log_shipper`).

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::sync::watch;
use velda_core::{CompletionReason, GrpcStatus, HttpMethod, PipeKind};
use velda_observability::{TelemetryBatch, TelemetryRecord, TelemetryStore};
use velda_plugin::PluginConfig;
use velda_plugin::background::BackgroundPlugin;
use velda_plugin::background::std_log_shipper::config::TargetStream;
use velda_plugin::background::std_log_shipper::format::format_std_log_payload;
use velda_plugin::background::std_log_shipper::{StdLogShipperConfig, StdLogShipperPlugin};

#[test]
fn test_std_log_format_all() {
    let mut http1_rec = TelemetryRecord::zeroed();
    http1_rec.request_id = 0x1111_2222;
    http1_rec.ingress_time_nanos = 1_710_000_000_000_000_000;
    http1_rec.duration_ns = 150;
    http1_rec.peer_ip = velda_core::lut::ip::encode_ip_u128("10.0.0.1".parse().unwrap());
    http1_rec.upstream_ip =
        velda_core::lut::ip::encode_upstream_ip_u32("192.168.1.1".parse().unwrap());
    http1_rec.upstream_port = 8080;
    http1_rec.bytes_rx = 120;
    http1_rec.bytes_tx = 1024;
    http1_rec.status_code = 200;
    http1_rec.route_id = 10;
    http1_rec.pipe = PipeKind::BufferedH1H1;
    http1_rec.completion = CompletionReason::Complete;
    http1_rec.method = HttpMethod::Get;
    http1_rec.grpc_status = GrpcStatus::NonGrpc;
    let (fam, os, ver) =
        velda_core::lut::classify_user_agent(b"Mozilla/5.0 (Windows NT 10.0; Win64; x64)");
    http1_rec.ua_family = fam;
    http1_rec.ua_os = os;
    http1_rec.ua_major_version = ver;

    let mut h2_rec = TelemetryRecord::zeroed();
    h2_rec.request_id = 0x3333_4444;
    h2_rec.ingress_time_nanos = 1_710_000_001_000_000_000;
    h2_rec.duration_ns = 220;
    h2_rec.peer_ip = velda_core::lut::ip::encode_ip_u128("10.0.0.2".parse().unwrap());
    h2_rec.upstream_ip =
        velda_core::lut::ip::encode_upstream_ip_u32("192.168.1.2".parse().unwrap());
    h2_rec.upstream_port = 8443;
    h2_rec.bytes_rx = 200;
    h2_rec.bytes_tx = 4096;
    h2_rec.status_code = 200;
    h2_rec.route_id = 11;
    h2_rec.pipe = PipeKind::ServerStreamH2H2;
    h2_rec.completion = CompletionReason::Complete;
    h2_rec.method = HttpMethod::Post;
    h2_rec.grpc_status = GrpcStatus::NonGrpc;
    let (fam, os, ver) = velda_core::lut::classify_user_agent(b"curl/8.6.0");
    h2_rec.ua_family = fam;
    h2_rec.ua_os = os;
    h2_rec.ua_major_version = ver;

    let mut grpc_rec = TelemetryRecord::zeroed();
    grpc_rec.request_id = 0x5555_6666;
    grpc_rec.ingress_time_nanos = 1_710_000_002_000_000_000;
    grpc_rec.duration_ns = 350;
    grpc_rec.peer_ip = velda_core::lut::ip::encode_ip_u128("10.0.0.3".parse().unwrap());
    grpc_rec.upstream_ip =
        velda_core::lut::ip::encode_upstream_ip_u32("192.168.1.3".parse().unwrap());
    grpc_rec.upstream_port = 50051;
    grpc_rec.bytes_rx = 512;
    grpc_rec.bytes_tx = 2048;
    grpc_rec.status_code = 200;
    grpc_rec.route_id = 12;
    grpc_rec.rpc_msgs_or_flags = 10;
    grpc_rec.pipe = PipeKind::DuplexGrpcTcpTcp;
    grpc_rec.completion = CompletionReason::Complete;
    grpc_rec.method = HttpMethod::Post;
    grpc_rec.grpc_status = GrpcStatus::Ok;
    let (fam, os, ver) = velda_core::lut::classify_user_agent(b"grpc-go/1.64.0");
    grpc_rec.ua_family = fam;
    grpc_rec.ua_os = os;
    grpc_rec.ua_major_version = ver;

    let mut raw_rec = TelemetryRecord::zeroed();
    raw_rec.request_id = 0x7777_8888;
    raw_rec.ingress_time_nanos = 1_710_000_003_000_000_000;
    raw_rec.duration_ns = 80;
    raw_rec.peer_ip = velda_core::lut::ip::encode_ip_u128("10.0.0.4".parse().unwrap());
    raw_rec.upstream_ip =
        velda_core::lut::ip::encode_upstream_ip_u32("192.168.1.4".parse().unwrap());
    raw_rec.upstream_port = 9000;
    raw_rec.bytes_rx = 1024;
    raw_rec.bytes_tx = 2048;
    raw_rec.status_code = 0;
    raw_rec.route_id = 13;
    raw_rec.pipe = PipeKind::TcpTcp;
    raw_rec.completion = CompletionReason::Complete;
    raw_rec.method = HttpMethod::Unknown;
    raw_rec.grpc_status = GrpcStatus::NonGrpc;

    let batches = vec![TelemetryBatch::from_arc(Arc::from(vec![
        http1_rec, h2_rec, grpc_rec, raw_rec,
    ]))];

    // Verify Text format
    let mut text_buf = Vec::new();
    let count = format_std_log_payload(&batches, "text", &mut text_buf);
    assert_eq!(count, 4);
    let text = String::from_utf8(text_buf).unwrap();
    assert!(text.contains("10.0.0.1"));
    assert!(text.contains("duration_ns=150 pipe=buffered_h1_h1"));
    assert!(text.contains("duration_ns=220 pipe=server_stream_h2_h2"));
    assert!(text.contains("grpc_status=OK rpc_messages=10"));
    assert!(text.contains("pipe=tcp_tcp"));

    // Verify JSON format
    let mut json_buf = Vec::new();
    let count = format_std_log_payload(&batches, "json", &mut json_buf);
    assert_eq!(count, 4);
    let json_str = String::from_utf8(json_buf).unwrap();
    assert!(json_str.contains("\"pipe\":\"buffered_h1_h1\""));
    assert!(json_str.contains("\"pipe\":\"server_stream_h2_h2\""));
    assert!(json_str.contains("\"pipe\":\"duplex_grpc_tcp_tcp\""));
    assert!(json_str.contains("\"pipe\":\"tcp_tcp\""));
    assert!(json_str.contains("\"duration_ns\":150"));
    assert!(!json_str.contains("\"downstream_protocol\""));
    assert!(!json_str.contains("\"upstream_protocol\""));
    assert!(json_str.contains("\"grpc_status\":\"OK\""));
    assert!(json_str.contains("\"rpc_messages\":10"));
}

#[tokio::test]
async fn test_std_log_shipper_from_plugin_config() {
    let cfg = PluginConfig {
        id: "std_log_shipper".to_string(),
        phase: "background".to_string(),
        config: serde_json::json!({
            "target": "stdout",
            "format": "json",
            "flush_interval_ms": 100,
            "max_batch_size": 256
        }),
    };

    let plugin = StdLogShipperPlugin::from_config(&cfg).expect("Config must be valid");
    assert_eq!(plugin.target, TargetStream::Stdout);
    assert_eq!(plugin.config.format, "json");
    assert_eq!(plugin.config.flush_interval_ms, 100);
    assert_eq!(plugin.config.max_batch_size, 256);
}

#[tokio::test]
async fn test_std_log_subscription() {
    let telemetry_store = Arc::new(TelemetryStore::with_cores_and_capacity(1, 1024));

    let config = StdLogShipperConfig {
        target: "stdout".to_string(),
        format: "text".to_string(),
        flush_interval_ms: 50,
        max_batch_size: 128,
    };

    let plugin = Arc::new(
        StdLogShipperPlugin::with_store("test_shipper", config, Arc::clone(&telemetry_store))
            .expect("Plugin must construct"),
    );

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let runner_handle = tokio::spawn({
        let p = Arc::clone(&plugin);
        async move { p.run(shutdown_rx).await }
    });

    // Wait until background runner has registered its subscriber
    while telemetry_store
        .active_subscriber_count
        .load(Ordering::Acquire)
        == 0
    {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    // Emit 10 records across lanes
    for i in 0..10 {
        let mut rec = TelemetryRecord::zeroed();
        rec.request_id = i as u64;
        rec.ingress_time_nanos = 1_710_000_000_000_000_000 + i as u64;
        rec.duration_ns = 100 + i as u64;
        rec.peer_ip = velda_core::lut::ip::encode_ip_u128("127.0.0.1".parse().unwrap());
        rec.bytes_rx = 10;
        rec.bytes_tx = 20;
        rec.status_code = 200;
        rec.route_id = 1;
        rec.pipe = PipeKind::BufferedH1H1;
        rec.completion = CompletionReason::Complete;
        rec.method = HttpMethod::Get;
        rec.grpc_status = GrpcStatus::NonGrpc;
        let (fam, os, ver) = velda_core::lut::classify_user_agent(b"test-client");
        rec.ua_family = fam;
        rec.ua_os = os;
        rec.ua_major_version = ver;
        telemetry_store.emit_on_core(0, &rec);
    }
    telemetry_store.flush_all_lanes(128);

    // Wait for the background loop to flush and process
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Trigger shutdown
    let _ = shutdown_tx.send(true);
    let run_res = runner_handle.await.expect("Task must join cleanly");
    assert!(run_res.is_ok());

    // Shipped count must match emitted count
    assert_eq!(plugin.shipped_records.load(Ordering::Relaxed), 10);
}
