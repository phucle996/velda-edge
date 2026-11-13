//! Comprehensive Integration Test Suite for Standard Stream Log Shipper Plugin (`std_log_shipper`).

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::sync::watch;
use velda_observability::{
    AccessLogRecord, LogCompletionReason, LogGrpcStatus, LogHttpMethod, LogStore, PipeKind,
};
use velda_plugin::background::BackgroundPlugin;
use velda_plugin::background::std_log_shipper::config::TargetStream;
use velda_plugin::background::std_log_shipper::format::format_std_log_payload;
use velda_plugin::background::std_log_shipper::{StdLogShipperConfig, StdLogShipperPlugin};
use velda_sync::post_sync::plugin::PluginConfig;

#[test]
fn test_std_log_shipper_formatting_all_protocols() {
    let http1_rec = AccessLogRecord::from_scalars(
        0x1111_2222,
        1_710_000_000,
        150,
        "10.0.0.1:12345".parse().unwrap(),
        Some("192.168.1.1:8080".parse().unwrap()),
        120,
        1024,
        200,
        10,
        0,
        PipeKind::BufferedH1H1,
        LogCompletionReason::Complete,
        LogHttpMethod::Get,
        LogGrpcStatus::NonGrpc,
        b"Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    );

    let h2_rec = AccessLogRecord::from_scalars(
        0x3333_4444,
        1_710_000_001,
        220,
        "10.0.0.2:12346".parse().unwrap(),
        Some("192.168.1.2:8443".parse().unwrap()),
        200,
        4096,
        200,
        11,
        0,
        PipeKind::ServerStreamH2H2,
        LogCompletionReason::Complete,
        LogHttpMethod::Post,
        LogGrpcStatus::NonGrpc,
        b"curl/8.6.0",
    );

    let grpc_rec = AccessLogRecord::from_scalars(
        0x5555_6666,
        1_710_000_002,
        350,
        "10.0.0.3:12347".parse().unwrap(),
        Some("192.168.1.3:50051".parse().unwrap()),
        512,
        2048,
        200,
        12,
        10,
        PipeKind::DuplexGrpcTcpTcp,
        LogCompletionReason::Complete,
        LogHttpMethod::Post,
        LogGrpcStatus::Ok,
        b"grpc-go/1.64.0",
    );

    let raw_rec = AccessLogRecord::from_scalars(
        0x7777_8888,
        1_710_000_003,
        80,
        "10.0.0.4:12348".parse().unwrap(),
        Some("192.168.1.4:9000".parse().unwrap()),
        1024,
        2048,
        0,
        13,
        0,
        PipeKind::TcpTcp,
        LogCompletionReason::Complete,
        LogHttpMethod::Unknown,
        LogGrpcStatus::NonGrpc,
        b"",
    );

    let batches = vec![Arc::from(vec![http1_rec, h2_rec, grpc_rec, raw_rec])];

    // Verify Text format
    let mut text_buf = Vec::new();
    let count = format_std_log_payload(&batches, "text", &mut text_buf);
    assert_eq!(count, 4);
    let text = String::from_utf8(text_buf).unwrap();
    assert!(text.contains("10.0.0.1:12345"));
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
async fn test_std_log_shipper_background_subscription_lifecycle() {
    let log_store = Arc::new(LogStore::with_cores_and_capacity(1, 1024));

    let config = StdLogShipperConfig {
        target: "stdout".to_string(),
        format: "text".to_string(),
        flush_interval_ms: 50,
        max_batch_size: 128,
    };

    let plugin = Arc::new(
        StdLogShipperPlugin::with_store("test_shipper", config, Arc::clone(&log_store))
            .expect("Plugin must construct"),
    );

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let runner_handle = tokio::spawn({
        let p = Arc::clone(&plugin);
        async move { p.run(shutdown_rx).await }
    });

    // Emit 10 records across lanes
    for i in 0..10 {
        let rec = AccessLogRecord::from_scalars(
            i as u64,
            1_710_000_000 + i as u32,
            100 + i as u64,
            "127.0.0.1:8080".parse().unwrap(),
            None,
            10,
            20,
            200,
            1,
            0,
            PipeKind::BufferedH1H1,
            LogCompletionReason::Complete,
            LogHttpMethod::Get,
            LogGrpcStatus::NonGrpc,
            b"test-client",
        );
        log_store.emit_on_core(0, &rec);
    }

    // Wait for the background loop to flush and process
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Trigger shutdown
    let _ = shutdown_tx.send(true);
    let run_res = runner_handle.await.expect("Task must join cleanly");
    assert!(run_res.is_ok());

    // Shipped count must match emitted count
    assert_eq!(plugin.shipped_records.load(Ordering::Relaxed), 10);
}
