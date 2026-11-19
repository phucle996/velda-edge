//! Comprehensive Integration Test Suite for ClickHouse Log Shipper (`clickhouse_log_shipper`).
//!
//! Validates:
//! 1. End-to-end RowBinary batch export over persistent HTTP/1.1 to mock ClickHouse server.
//! 2. End-to-end JSONEachRow batch export over persistent HTTP/1.1.
//! 3. Auto-creation of table `velda_access_logs` on startup via `ensure_table()`.
//! 4. Factory registration and phase auto-inference (`canonical_phase_for_id`).
//! 5. Phase mismatch rejection when configured under wrong phase.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::watch;
use velda_core::{CompletionReason, GrpcStatus, HttpMethod, PipeKind, UaFamily, UaOs};
use velda_observability::{AccessLogRecord, LogStore};
use velda_plugin::background::BackgroundPlugin;
use velda_plugin::background::clickhouse_log_shipper::{
    ClickHouseLogShipperConfig, ClickHouseLogShipperPlugin,
};
use velda_plugin::factory::{LoadedPlugin, instantiate_plugin};
use velda_plugin::{PluginConfig, canonical_phase_for_id, validate_plugin_config};

fn make_sample_record(id: u64, ts: u32) -> AccessLogRecord {
    let mut rec = AccessLogRecord::zeroed();
    rec.ingress_time_nanos = (ts as u64) * 1_000_000_000;
    rec.request_id = id;
    rec.duration_ns = 150_000;
    // 192.168.1.100 -> 0xC0A80164
    rec.peer_ip = 0x0000_0000_0000_0000_0000_FFFF_C0A8_0164;
    // 10.0.0.1 -> 0x0A000001
    rec.upstream_ip = 0x0A000001;
    rec.upstream_port = 8080;
    rec.route_id = 1;
    rec.status_code = 200;
    rec.pipe = PipeKind::BufferedH1H1;
    rec.method = HttpMethod::Get;
    rec.completion = CompletionReason::Complete;
    rec.grpc_status = GrpcStatus::Ok;
    rec.ua_family = UaFamily::Curl;
    rec.ua_os = UaOs::Linux;
    rec.bytes_rx = 256;
    rec.bytes_tx = 1024;
    rec.set_url("/api/v1/health");
    rec
}

#[tokio::test]
async fn test_clickhouse_row_binary_e2e() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let server_task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut buf = vec![0u8; 16384];

        // 1. First request: ensure_table (CREATE TABLE IF NOT EXISTS)
        let n = socket.read(&mut buf).await.unwrap();
        let req1 = String::from_utf8_lossy(&buf[..n]);
        assert!(req1.contains("CREATE TABLE IF NOT EXISTS test_db.velda_access_logs"));
        socket
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n")
            .await
            .unwrap();

        // 2. Second request: batch insert (FORMAT RowBinary)
        let n = socket.read(&mut buf).await.unwrap();
        let req2 = String::from_utf8_lossy(&buf[..n]);
        assert!(req2.contains("INSERT%20INTO%20test_db.velda_access_logs%20FORMAT%20RowBinary"));
        assert!(req2.contains("Content-Type: application/octet-stream"));
        socket
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n")
            .await
            .unwrap();
    });

    let log_store = Arc::new(LogStore::with_cores_and_capacity(1, 1024));

    let config = ClickHouseLogShipperConfig {
        endpoint: format!("http://127.0.0.1:{port}"),
        transport_security: "plain".into(),
        database: "test_db".into(),
        format: "row_binary".into(),
        username: "default".into(),
        password: "".into(),
        flush_interval_ms: 100,
        max_batch_size: 500,
        connect_timeout_ms: 2000,
        request_timeout_ms: 3000,
        tls: Default::default(),
    };

    let plugin = Arc::new(
        ClickHouseLogShipperPlugin::with_store("ch_shipper", config, Arc::clone(&log_store))
            .unwrap(),
    );

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let p_clone = Arc::clone(&plugin);
    let runner = tokio::spawn(async move { p_clone.run(shutdown_rx).await });

    // Wait until subscriber is active
    while log_store.active_subscriber_count.load(Ordering::Acquire) == 0 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    // Emit 5 test records
    for i in 0..5 {
        let rec = make_sample_record(100 + i, 1732000000 + i as u32);
        log_store.emit_on_core(0, &rec);
    }
    log_store.flush_all_lanes(128);

    // Wait for shipper to flush batch
    for _ in 0..50 {
        if plugin.shipped_records() >= 5 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(15)).await;
    }

    assert_eq!(plugin.shipped_records(), 5);
    assert_eq!(plugin.failed_pushes(), 0);

    let _ = shutdown_tx.send(true);
    let res = runner.await.unwrap();
    assert!(res.is_ok());
    server_task.await.unwrap();
}

#[tokio::test]
async fn test_clickhouse_json_e2e() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let server_task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut buf = vec![0u8; 16384];

        // 1. First request: ensure_table
        let n = socket.read(&mut buf).await.unwrap();
        assert!(String::from_utf8_lossy(&buf[..n]).contains("CREATE TABLE IF NOT EXISTS"));
        socket
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n")
            .await
            .unwrap();

        // 2. Second request: batch insert (FORMAT JSONEachRow)
        let n = socket.read(&mut buf).await.unwrap();
        let req2 = String::from_utf8_lossy(&buf[..n]);
        assert!(req2.contains("INSERT%20INTO%20test_db.velda_access_logs%20FORMAT%20JSONEachRow"));
        assert!(req2.contains("Content-Type: application/x-ndjson"));
        socket
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n")
            .await
            .unwrap();
    });

    let log_store = Arc::new(LogStore::with_cores_and_capacity(1, 1024));

    let config = ClickHouseLogShipperConfig {
        endpoint: format!("http://127.0.0.1:{port}"),
        transport_security: "plain".into(),
        database: "test_db".into(),
        format: "json_each_row".into(),
        username: "default".into(),
        password: "".into(),
        flush_interval_ms: 100,
        max_batch_size: 500,
        connect_timeout_ms: 2000,
        request_timeout_ms: 3000,
        tls: Default::default(),
    };

    let plugin = Arc::new(
        ClickHouseLogShipperPlugin::with_store("ch_shipper_json", config, Arc::clone(&log_store))
            .unwrap(),
    );

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let p_clone = Arc::clone(&plugin);
    let runner = tokio::spawn(async move { p_clone.run(shutdown_rx).await });

    while log_store.active_subscriber_count.load(Ordering::Acquire) == 0 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    let rec = make_sample_record(999, 1732000000);
    log_store.emit_on_core(0, &rec);
    log_store.flush_all_lanes(128);

    for _ in 0..50 {
        if plugin.shipped_records() >= 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(15)).await;
    }

    assert_eq!(plugin.shipped_records(), 1);

    let _ = shutdown_tx.send(true);
    let res = runner.await.unwrap();
    assert!(res.is_ok());
    server_task.await.unwrap();
}

#[test]
fn test_clickhouse_phase_and_validation() {
    assert_eq!(
        canonical_phase_for_id("clickhouse_log_shipper"),
        Some("background")
    );

    // Auto-inference of empty phase
    let mut cfg = PluginConfig {
        id: "clickhouse_log_shipper".into(),
        phase: "".into(),
        config: serde_json::json!({
            "endpoint": "http://127.0.0.1:8123",
            "database": "velda_analytics",
            "format": "row_binary",
            "flush_interval_ms": 250
        }),
    };

    validate_plugin_config(&mut cfg).expect("validation should succeed and infer phase");
    assert_eq!(cfg.phase, "background");

    // Phase mismatch rejection
    let mut bad_cfg = PluginConfig {
        id: "clickhouse_log_shipper".into(),
        phase: "pre_route".into(),
        config: serde_json::json!({
            "endpoint": "http://127.0.0.1:8123",
            "database": "velda_analytics"
        }),
    };

    let err = validate_plugin_config(&mut bad_cfg).unwrap_err();
    assert!(err.to_string().contains("must specify phase 'background'"));
}

#[test]
fn test_clickhouse_factory() {
    let cfg = PluginConfig {
        id: "clickhouse_log_shipper".into(),
        phase: "background".into(),
        config: serde_json::json!({
            "endpoint": "http://127.0.0.1:8123",
            "database": "velda_analytics",
            "format": "row_binary",
            "flush_interval_ms": 100,
            "max_batch_size": 1000
        }),
    };

    let loaded = instantiate_plugin(&cfg).expect("factory instantiation must succeed");
    assert_eq!(loaded.id(), "clickhouse_log_shipper");
    assert!(matches!(loaded, LoadedPlugin::Background(_)));

    if let LoadedPlugin::Background(bg) = loaded {
        assert_eq!(bg.weight(), 75);
    }
}

#[tokio::test]
async fn test_clickhouse_tls_mtls_e2e() {
    let server_certified =
        rcgen::generate_simple_self_signed(vec!["localhost".into(), "127.0.0.1".into()]).unwrap();
    let server_cert_pem = server_certified.cert.pem();
    let server_key_pem = server_certified.signing_key.serialize_pem();

    let client_certified =
        rcgen::generate_simple_self_signed(vec!["clickhouse-client".into()]).unwrap();
    let client_cert_pem = client_certified.cert.pem();
    let client_key_pem = client_certified.signing_key.serialize_pem();

    let server_config = velda_tls::ServerTlsConfig {
        sni: vec!["localhost".into(), "127.0.0.1".into()],
        versions: vec!["tls1.3".into()],
        alpn: vec!["http/1.1".into()],
        cert_pem: server_cert_pem.clone(),
        key_pem: server_key_pem,
        client_ca_pem: Some(client_cert_pem.clone()), // Enforce mTLS: client cert must match this CA
    };

    let server_engine = velda_tls::TlsServerEngine::new(&[server_config]).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let server_task = tokio::spawn(async move {
        let (tcp_stream, _) = listener.accept().await.unwrap();
        let mut tls_stream = server_engine.accept(tcp_stream).await.unwrap();

        let mut buf = vec![0u8; 16384];

        // 1. First request: ensure_table over mTLS
        let n = tls_stream.read(&mut buf).await.unwrap();
        let req1 = String::from_utf8_lossy(&buf[..n]);
        assert!(req1.contains("CREATE TABLE IF NOT EXISTS mtls_db.velda_access_logs"));
        tls_stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n")
            .await
            .unwrap();
        tls_stream.flush().await.unwrap();

        // 2. Second request: batch insert over mTLS
        let n = tls_stream.read(&mut buf).await.unwrap();
        let req2 = String::from_utf8_lossy(&buf[..n]);
        assert!(req2.contains("INSERT%20INTO%20mtls_db.velda_access_logs%20FORMAT%20RowBinary"));
        tls_stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n")
            .await
            .unwrap();
        tls_stream.flush().await.unwrap();
    });

    let log_store = Arc::new(LogStore::with_cores_and_capacity(1, 1024));

    let config = ClickHouseLogShipperConfig {
        endpoint: format!("https://127.0.0.1:{port}"),
        transport_security: "mtls".into(),
        database: "mtls_db".into(),
        format: "row_binary".into(),
        username: "default".into(),
        password: "".into(),
        flush_interval_ms: 100,
        max_batch_size: 500,
        connect_timeout_ms: 2000,
        request_timeout_ms: 3000,
        tls: velda_plugin::OutboundTlsConfig {
            ca_pem: Some(server_cert_pem),
            client_cert_pem: Some(client_cert_pem),
            client_key_pem: Some(client_key_pem),
            insecure_skip_verify: false,
        },
    };

    let plugin = Arc::new(
        ClickHouseLogShipperPlugin::with_store("ch_mtls_shipper", config, Arc::clone(&log_store))
            .unwrap(),
    );

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let p_clone = Arc::clone(&plugin);
    let runner = tokio::spawn(async move { p_clone.run(shutdown_rx).await });

    while log_store.active_subscriber_count.load(Ordering::Acquire) == 0 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    log_store.emit_on_core(0, &make_sample_record(777, 1732000000));
    log_store.flush_all_lanes(128);

    for _ in 0..50 {
        if plugin.shipped_records() >= 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(15)).await;
    }

    assert_eq!(plugin.shipped_records(), 1);
    assert_eq!(plugin.failed_pushes(), 0);

    let _ = shutdown_tx.send(true);
    let res = runner.await.unwrap();
    assert!(res.is_ok());
    server_task.await.unwrap();
}

#[tokio::test]
async fn test_clickhouse_backoff_and_disconnect() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let server_task = tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            let mut buf = vec![0u8; 1024];
            if let Ok(n) = socket.read(&mut buf).await
                && n > 0
            {
                let _ = socket
                    .write_all(b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                    .await;
            }
        }
    });

    let log_store = Arc::new(LogStore::with_cores_and_capacity(1, 1024));

    let config = ClickHouseLogShipperConfig {
        endpoint: format!("http://127.0.0.1:{port}"),
        transport_security: "plain".into(),
        database: "test_db".into(),
        format: "row_binary".into(),
        username: "default".into(),
        password: "".into(),
        flush_interval_ms: 100,
        max_batch_size: 500,
        connect_timeout_ms: 500,
        request_timeout_ms: 500,
        tls: Default::default(),
    };

    let plugin = Arc::new(
        ClickHouseLogShipperPlugin::with_store(
            "ch_backoff_shipper",
            config,
            Arc::clone(&log_store),
        )
        .unwrap(),
    );

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let p_clone = Arc::clone(&plugin);
    let runner = tokio::spawn(async move { p_clone.run(shutdown_rx).await });

    while log_store.active_subscriber_count.load(Ordering::Acquire) == 0 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    log_store.emit_on_core(0, &make_sample_record(1, 1732000000));
    log_store.flush_all_lanes(128);

    for _ in 0..50 {
        if plugin.failed_pushes() >= 2 {
            break;
        }
        log_store.emit_on_core(0, &make_sample_record(1, 1732000000));
        log_store.flush_all_lanes(128);
        tokio::time::sleep(Duration::from_millis(30)).await;
    }

    assert!(plugin.failed_pushes() >= 2);
    assert_eq!(plugin.shipped_records(), 0);

    let _ = shutdown_tx.send(true);
    let _ = runner.await;
    server_task.abort();
}

#[test]
fn test_clickhouse_tls_validation() {
    use velda_plugin::background::clickhouse_log_shipper::validation::validate_clickhouse_log_shipper_config;

    // 1. Plain with http:// succeeds
    let valid_plain = serde_json::json!({
        "endpoint": "http://127.0.0.1:8123",
        "transport_security": "plain",
        "database": "velda"
    });
    let cfg = validate_clickhouse_log_shipper_config("ch_test", &valid_plain).unwrap();
    assert_eq!(cfg.transport_security, "plain");

    // 2. Plain with https:// fails fast
    let invalid_plain = serde_json::json!({
        "endpoint": "https://127.0.0.1:8443",
        "transport_security": "plain",
        "database": "velda"
    });
    assert!(validate_clickhouse_log_shipper_config("ch_test", &invalid_plain).is_err());

    // 3. TLS with http:// fails fast
    let invalid_tls = serde_json::json!({
        "endpoint": "http://127.0.0.1:8123",
        "transport_security": "tls",
        "database": "velda"
    });
    assert!(validate_clickhouse_log_shipper_config("ch_test", &invalid_tls).is_err());

    // 4. mTLS with http:// fails fast
    let invalid_mtls_http = serde_json::json!({
        "endpoint": "http://127.0.0.1:8123",
        "transport_security": "mtls",
        "database": "velda"
    });
    assert!(validate_clickhouse_log_shipper_config("ch_test", &invalid_mtls_http).is_err());

    // 5. mTLS missing client_key_pem fails fast
    let invalid_mtls_missing_key = serde_json::json!({
        "endpoint": "https://127.0.0.1:8443",
        "transport_security": "mtls",
        "database": "velda",
        "tls": {
            "client_cert_pem": "-----BEGIN CERTIFICATE-----\nMIIB...-----END CERTIFICATE-----\n"
        }
    });
    assert!(validate_clickhouse_log_shipper_config("ch_test", &invalid_mtls_missing_key).is_err());
}

#[tokio::test]
async fn test_clickhouse_full_logs_e2e() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let server_task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut buf = vec![0u8; 16384];

        // 1. First request: ensure_table
        let n = socket.read(&mut buf).await.unwrap();
        let req1 = String::from_utf8_lossy(&buf[..n]);
        assert!(req1.contains("CREATE TABLE IF NOT EXISTS test_db.velda_access_logs"));
        socket
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n")
            .await
            .unwrap();

        // 2. Second request: batch insert with 100% full records
        let n = socket.read(&mut buf).await.unwrap();
        let req2 = String::from_utf8_lossy(&buf[..n]);
        assert!(req2.contains("INSERT%20INTO%20test_db.velda_access_logs%20FORMAT%20RowBinary"));
        socket
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n")
            .await
            .unwrap();
    });

    let log_store = Arc::new(LogStore::with_cores_and_capacity(1, 1024));

    let config = ClickHouseLogShipperConfig {
        endpoint: format!("http://127.0.0.1:{port}"),
        transport_security: "plain".into(),
        database: "test_db".into(),
        format: "row_binary".into(),
        username: "default".into(),
        password: "".into(),
        flush_interval_ms: 100,
        max_batch_size: 500,
        connect_timeout_ms: 2000,
        request_timeout_ms: 3000,
        tls: Default::default(),
    };

    let plugin = Arc::new(
        ClickHouseLogShipperPlugin::with_store("ch_full_logger", config, Arc::clone(&log_store))
            .unwrap(),
    );

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let p_clone = Arc::clone(&plugin);
    let runner = tokio::spawn(async move { p_clone.run(shutdown_rx).await });

    while log_store.active_subscriber_count.load(Ordering::Acquire) == 0 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    // Emit 5 OK records (status 200) and 2 ERROR records (status 500)
    for i in 0..5 {
        let rec = make_sample_record(100 + i, 1732000000 + i as u32);
        log_store.emit_on_core(0, &rec);
    }
    for i in 0..2 {
        let mut err_rec = make_sample_record(200 + i, 1732000010 + i as u32);
        err_rec.status_code = 500;
        log_store.emit_on_core(0, &err_rec);
    }
    log_store.flush_all_lanes(128);

    // Give background shipper time to process
    tokio::time::sleep(Duration::from_millis(250)).await;

    // Verify: exactly 7 records shipped (100% full logs, zero sampling drop)
    assert_eq!(plugin.shipped_records(), 7);

    let _ = shutdown_tx.send(true);
    let _ = runner.await;
    let _ = server_task.await;
}
