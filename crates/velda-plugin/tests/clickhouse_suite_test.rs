//! Comprehensive Integration Test Suite for ClickHouse Log Shipper (`clickhouse_log_shipper`).
//!
//! Validates:
//! 1. End-to-end RowBinary batch export over pooled `ClickHouseEtp` HTTP/1.1 keep-alive leases.
//! 2. End-to-end JSONEachRow batch export over pooled `ClickHouseEtp` HTTP/1.1 leases.
//! 3. Auto-creation of table `velda_access_logs` on startup via `ensure_table()`.
//! 4. Compile-time factory registration, `EtpRegistry` wiring via `wire_plugin_etps()`, and mTLS.
//! 5. Automatic in-place `lease.reconnect().await` recovery when ClickHouse drops a keep-alive socket.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use rustc_hash::FxHashMap;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::watch;
use velda_core::{CompletionReason, GrpcStatus, HttpMethod, PipeKind, UaFamily, UaOs};
use velda_etp::clickhouse::ClickHouseEtp;
use velda_etp::registry::build_registry;
use velda_etp::{EtpEntryConfig, EtpKind, EtpPoolSettings, EtpTlsConfig, EtpTlsVersion};
use velda_observability::{TelemetryRecord, TelemetryStore};
use velda_plugin::background::BackgroundPlugin;
use velda_plugin::background::clickhouse_log_shipper::{
    ClickHouseLogShipperConfig, ClickHouseLogShipperPlugin,
};
use velda_plugin::factory::{LoadedPlugin, instantiate_plugin, wire_plugin_etps};
use velda_plugin::{PluginConfig, is_known_plugin_id, validate_plugin_config};

fn make_clickhouse_etp(
    id: &str,
    port: u16,
    database: &str,
    tls: Option<EtpTlsConfig>,
) -> Arc<ClickHouseEtp> {
    let entry = EtpEntryConfig {
        id: id.to_string(),
        kind: EtpKind::ClickHouse,
        pool: EtpPoolSettings {
            pool_size: 2,
            connect_timeout_ms: 2000,
            idle_timeout_ms: 60_000,
        },
        tls,
        settings: serde_json::json!({
            "host": "127.0.0.1",
            "port": port,
            "database": database,
            "user": "default",
            "password": ""
        }),
    };
    Arc::new(ClickHouseEtp::from_entry(&entry).unwrap())
}

fn make_sample_record(id: u64, ts: u32) -> TelemetryRecord {
    let mut rec = TelemetryRecord::zeroed();
    rec.ingress_time_nanos = (ts as u64) * 1_000_000_000;
    rec.request_id = id;
    rec.duration_ns = 150_000;
    rec.peer_ip = 0x0000_0000_0000_0000_0000_FFFF_C0A8_0164;
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

    let telemetry_store = Arc::new(TelemetryStore::with_cores_and_capacity(1, 1024));
    let etp = make_clickhouse_etp("ch-rb", port, "test_db", None);

    let config = ClickHouseLogShipperConfig {
        etp_id: "ch-rb".into(),
        database: "test_db".into(),
        format: "row_binary".into(),
        flush_interval_ms: 100,
        max_batch_size: 500,
        request_timeout_ms: 3000,
    };

    let plugin = Arc::new(
        ClickHouseLogShipperPlugin::with_etp(
            "ch_shipper",
            config,
            etp,
            Arc::clone(&telemetry_store),
        )
        .unwrap(),
    );

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let p_clone = Arc::clone(&plugin);
    let runner = tokio::spawn(async move { p_clone.run(shutdown_rx).await });

    while telemetry_store
        .active_subscriber_count
        .load(Ordering::Acquire)
        == 0
    {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    for i in 0..5 {
        let rec = make_sample_record(100 + i, 1732000000 + i as u32);
        telemetry_store.emit_on_core(0, &rec);
    }
    telemetry_store.flush_all_lanes(128);

    for _ in 0..50 {
        if plugin.shipped_records.load(Ordering::Relaxed) >= 5 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(15)).await;
    }

    assert_eq!(plugin.shipped_records.load(Ordering::Relaxed), 5);
    assert_eq!(plugin.failed_pushes.load(Ordering::Relaxed), 0);

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

    let telemetry_store = Arc::new(TelemetryStore::with_cores_and_capacity(1, 1024));
    let etp = make_clickhouse_etp("ch-json", port, "test_db", None);

    let config = ClickHouseLogShipperConfig {
        etp_id: "ch-json".into(),
        database: String::new(), // Inherit from ClickHouseEtp
        format: "json_each_row".into(),
        flush_interval_ms: 100,
        max_batch_size: 500,
        request_timeout_ms: 3000,
    };

    let plugin = Arc::new(
        ClickHouseLogShipperPlugin::with_etp(
            "ch_shipper_json",
            config,
            etp,
            Arc::clone(&telemetry_store),
        )
        .unwrap(),
    );

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let p_clone = Arc::clone(&plugin);
    let runner = tokio::spawn(async move { p_clone.run(shutdown_rx).await });

    while telemetry_store
        .active_subscriber_count
        .load(Ordering::Acquire)
        == 0
    {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    let rec = make_sample_record(999, 1732000000);
    telemetry_store.emit_on_core(0, &rec);
    telemetry_store.flush_all_lanes(128);

    for _ in 0..50 {
        if plugin.shipped_records.load(Ordering::Relaxed) >= 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(15)).await;
    }

    assert_eq!(plugin.shipped_records.load(Ordering::Relaxed), 1);

    let _ = shutdown_tx.send(true);
    let res = runner.await.unwrap();
    assert!(res.is_ok());
    server_task.await.unwrap();
}

#[test]
fn test_clickhouse_validation() {
    assert!(is_known_plugin_id("clickhouse_log_shipper"));
    assert!(velda_plugin::background::owns_id("clickhouse_log_shipper"));

    let mut cfg = PluginConfig {
        id: "clickhouse_log_shipper".into(),
        config: serde_json::json!({
            "etp_id": "clickhouse-logs",
            "database": "velda_analytics",
            "format": "row_binary",
            "flush_interval_ms": 250
        }),
    };

    validate_plugin_config(&mut cfg).expect("validation should succeed");

    let mut invalid_cfg = PluginConfig {
        id: "clickhouse_log_shipper".into(),
        config: serde_json::json!({
            "etp_id": "",
            "database": "velda_analytics"
        }),
    };
    assert!(validate_plugin_config(&mut invalid_cfg).is_err());
}

#[test]
fn test_clickhouse_factory() {
    let cfg = PluginConfig {
        id: "clickhouse_log_shipper".into(),
        config: serde_json::json!({
            "etp_id": "clickhouse-logs",
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
        assert_eq!(bg.pbww(), 75);
        assert_eq!(bg.etp_id(), Some("clickhouse-logs"));
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
        client_ca_pem: Some(client_cert_pem.clone()),
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

    let telemetry_store = Arc::new(TelemetryStore::with_cores_and_capacity(1, 1024));
    let etp = make_clickhouse_etp(
        "ch-mtls",
        port,
        "mtls_db",
        Some(EtpTlsConfig {
            ca_cert_pem: Some(server_cert_pem),
            client_cert_pem: Some(client_cert_pem),
            client_key_pem: Some(client_key_pem),
            server_name: Some("localhost".into()),
            insecure_skip_verify: false,
            tls_version: EtpTlsVersion::Tls13,
            pinned_cert_sha256: None,
        }),
    );

    let config = ClickHouseLogShipperConfig {
        etp_id: "ch-mtls".into(),
        database: "mtls_db".into(),
        format: "row_binary".into(),
        flush_interval_ms: 100,
        max_batch_size: 500,
        request_timeout_ms: 3000,
    };

    let plugin = Arc::new(
        ClickHouseLogShipperPlugin::with_etp(
            "ch_mtls_shipper",
            config,
            etp,
            Arc::clone(&telemetry_store),
        )
        .unwrap(),
    );

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let p_clone = Arc::clone(&plugin);
    let runner = tokio::spawn(async move { p_clone.run(shutdown_rx).await });

    while telemetry_store
        .active_subscriber_count
        .load(Ordering::Acquire)
        == 0
    {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    telemetry_store.emit_on_core(0, &make_sample_record(777, 1732000000));
    telemetry_store.flush_all_lanes(128);

    for _ in 0..50 {
        if plugin.shipped_records.load(Ordering::Relaxed) >= 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(15)).await;
    }

    assert_eq!(plugin.shipped_records.load(Ordering::Relaxed), 1);
    assert_eq!(plugin.failed_pushes.load(Ordering::Relaxed), 0);

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

    let telemetry_store = Arc::new(TelemetryStore::with_cores_and_capacity(1, 1024));
    let etp = make_clickhouse_etp("ch-backoff", port, "test_db", None);

    let config = ClickHouseLogShipperConfig {
        etp_id: "ch-backoff".into(),
        database: "test_db".into(),
        format: "row_binary".into(),
        flush_interval_ms: 100,
        max_batch_size: 500,
        request_timeout_ms: 500,
    };

    let plugin = Arc::new(
        ClickHouseLogShipperPlugin::with_etp(
            "ch_backoff_shipper",
            config,
            etp,
            Arc::clone(&telemetry_store),
        )
        .unwrap(),
    );

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let p_clone = Arc::clone(&plugin);
    let runner = tokio::spawn(async move { p_clone.run(shutdown_rx).await });

    while telemetry_store
        .active_subscriber_count
        .load(Ordering::Acquire)
        == 0
    {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    telemetry_store.emit_on_core(0, &make_sample_record(1, 1732000000));
    telemetry_store.flush_all_lanes(128);

    for _ in 0..50 {
        if plugin.failed_pushes.load(Ordering::Relaxed) >= 2 {
            break;
        }
        telemetry_store.emit_on_core(0, &make_sample_record(1, 1732000000));
        telemetry_store.flush_all_lanes(128);
        tokio::time::sleep(Duration::from_millis(30)).await;
    }

    assert!(plugin.failed_pushes.load(Ordering::Relaxed) >= 2);
    assert_eq!(plugin.shipped_records.load(Ordering::Relaxed), 0);

    let _ = shutdown_tx.send(true);
    let _ = runner.await;
    server_task.abort();
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

    let telemetry_store = Arc::new(TelemetryStore::with_cores_and_capacity(1, 1024));
    let etp = make_clickhouse_etp("ch-full", port, "test_db", None);

    let config = ClickHouseLogShipperConfig {
        etp_id: "ch-full".into(),
        database: "test_db".into(),
        format: "row_binary".into(),
        flush_interval_ms: 100,
        max_batch_size: 500,
        request_timeout_ms: 3000,
    };

    let plugin = Arc::new(
        ClickHouseLogShipperPlugin::with_etp(
            "ch_full_logger",
            config,
            etp,
            Arc::clone(&telemetry_store),
        )
        .unwrap(),
    );

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let p_clone = Arc::clone(&plugin);
    let runner = tokio::spawn(async move { p_clone.run(shutdown_rx).await });

    while telemetry_store
        .active_subscriber_count
        .load(Ordering::Acquire)
        == 0
    {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    for i in 0..5 {
        let rec = make_sample_record(100 + i, 1732000000 + i as u32);
        telemetry_store.emit_on_core(0, &rec);
    }
    for i in 0..2 {
        let mut err_rec = make_sample_record(200 + i, 1732000010 + i as u32);
        err_rec.status_code = 500;
        telemetry_store.emit_on_core(0, &err_rec);
    }
    telemetry_store.flush_all_lanes(128);

    tokio::time::sleep(Duration::from_millis(250)).await;

    assert_eq!(plugin.shipped_records.load(Ordering::Relaxed), 7);

    let _ = shutdown_tx.send(true);
    let _ = runner.await;
    let _ = server_task.await;
}

#[tokio::test]
async fn test_clickhouse_etp_registry_wiring_and_reconnect_recovery() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let server_task = tokio::spawn(async move {
        // Connection 1: handles ensure_table() -> 200 OK, then closes socket abruptly
        // when the first INSERT batch arrives so ClickHouseLogShipperPlugin triggers lease.reconnect().
        let (mut s1, _) = listener.accept().await.unwrap();
        let mut buf = vec![0u8; 16384];
        let n1 = s1.read(&mut buf).await.unwrap();
        assert!(String::from_utf8_lossy(&buf[..n1]).contains("CREATE TABLE IF NOT EXISTS"));
        s1.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n")
            .await
            .unwrap();
        s1.flush().await.unwrap();
        // Wait for client's first INSERT attempt, then drop s1 without replying!
        let _ = s1.read(&mut buf).await;
        drop(s1);

        // Connection 2: receives the in-place reconnected retry from Slot[0] and replies 200 OK.
        let (mut s2, _) = listener.accept().await.unwrap();
        let n2 = s2.read(&mut buf).await.unwrap();
        let req2 = String::from_utf8_lossy(&buf[..n2]);
        assert!(req2.contains("INSERT%20INTO%20wired_db.velda_access_logs%20FORMAT%20RowBinary"));
        s2.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n")
            .await
            .unwrap();
        s2.flush().await.unwrap();
    });

    // 1. Build EtpRegistry containing the clickhouse ETP
    let registry = build_registry(&[EtpEntryConfig {
        id: "ch-wired".into(),
        kind: EtpKind::ClickHouse,
        pool: EtpPoolSettings {
            pool_size: 1,
            connect_timeout_ms: 2000,
            idle_timeout_ms: 60_000,
        },
        tls: None,
        settings: serde_json::json!({
            "host": "127.0.0.1",
            "port": port,
            "database": "wired_db",
            "user": "default",
            "password": ""
        }),
    }])
    .unwrap();

    // 2. Instantiate plugin via factory and wire it via wire_plugin_etps()
    let plugin_cfg = PluginConfig {
        id: "clickhouse_log_shipper".into(),
        config: serde_json::json!({
            "etp_id": "ch-wired",
            "format": "row_binary",
            "flush_interval_ms": 100,
            "max_batch_size": 100,
            "request_timeout_ms": 2000
        }),
    };
    let loaded = instantiate_plugin(&plugin_cfg).unwrap();
    let mut available = FxHashMap::default();
    available.insert(loaded.id().to_string(), loaded.clone());
    wire_plugin_etps(&available, &registry).unwrap();

    // 3. Emit records onto global TelemetryStore and verify background task recovers transparently
    let store = velda_observability::global_telemetry_store();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let runner = tokio::spawn(loaded.run_background(shutdown_rx).unwrap());

    while store.active_subscriber_count.load(Ordering::Acquire) == 0 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    for i in 0..3 {
        store.emit_on_core(0, &make_sample_record(500 + i, 1732000000 + i as u32));
    }
    store.flush_all_lanes(128);

    // Wait for reconnect + retry push to complete
    server_task.await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let _ = shutdown_tx.send(true);
    let res = runner.await.unwrap();
    assert!(res.is_ok());

    if let LoadedPlugin::Background(bg) = loaded {
        // Also verify missing ETP produces an explicit PluginError
        let err_plugin = instantiate_plugin(&plugin_cfg).unwrap();
        let mut missing_map = FxHashMap::default();
        missing_map.insert(bg.id().to_string(), err_plugin);
        let empty_registry = velda_etp::EtpRegistry::default();
        assert!(wire_plugin_etps(&missing_map, &empty_registry).is_err());
    }
}

#[tokio::test]
async fn test_prebaked_headers_auth_database_override_and_chunked_drain() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let captured = Arc::new(tokio::sync::Mutex::new(Vec::<String>::new()));
    let captured_c = Arc::clone(&captured);

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut buf = vec![0u8; 8192];
        // 1. DDL request
        let n1 = sock.read(&mut buf).await.unwrap();
        captured_c
            .lock()
            .await
            .push(String::from_utf8_lossy(&buf[..n1]).into_owned());
        sock.write_all(
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: keep-alive\r\n\r\n2\r\nok\r\n0\r\n\r\n",
        )
        .await
        .unwrap();
        sock.flush().await.unwrap();

        // 2. Batch INSERT request over same kept-alive socket
        let n2 = sock.read(&mut buf).await.unwrap();
        captured_c
            .lock()
            .await
            .push(String::from_utf8_lossy(&buf[..n2]).into_owned());
        sock.write_all(
            b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: keep-alive\r\n\r\ndone",
        )
        .await
        .unwrap();
        sock.flush().await.unwrap();
    });

    let entry = EtpEntryConfig {
        id: "ch-auth".into(),
        kind: EtpKind::ClickHouse,
        pool: EtpPoolSettings {
            pool_size: 1,
            connect_timeout_ms: 2000,
            idle_timeout_ms: 60_000,
        },
        tls: None,
        settings: serde_json::json!({
            "host": "127.0.0.1",
            "port": port,
            "database": "etp_default_db",
            "user": "test_user",
            "password": "test_pass"
        }),
    };
    let etp = Arc::new(ClickHouseEtp::from_entry(&entry).unwrap());
    let store = Arc::new(TelemetryStore::with_cores_and_capacity(1, 128));

    let cfg = ClickHouseLogShipperConfig {
        etp_id: "ch-auth".into(),
        database: "override_db".into(),
        format: "json_each_row".into(),
        flush_interval_ms: 100,
        max_batch_size: 100,
        request_timeout_ms: 2000,
    };
    let plugin = Arc::new(
        ClickHouseLogShipperPlugin::with_etp("ch_unit", cfg, etp, Arc::clone(&store)).unwrap(),
    );

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let p = Arc::clone(&plugin);
    let runner = tokio::spawn(async move { p.run(shutdown_rx).await });

    while store.active_subscriber_count.load(Ordering::Acquire) == 0 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    store.emit_on_core(0, &make_sample_record(42, 1_732_000_000));
    store.flush_all_lanes(64);

    for _ in 0..50 {
        if plugin.shipped_records.load(Ordering::Relaxed) >= 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(15)).await;
    }
    assert_eq!(plugin.shipped_records.load(Ordering::Relaxed), 1);
    assert_eq!(plugin.failed_pushes.load(Ordering::Relaxed), 0);

    let _ = shutdown_tx.send(true);
    runner.await.unwrap().unwrap();
    server.await.unwrap();

    let reqs = captured.lock().await;
    assert_eq!(reqs.len(), 2);
    assert!(reqs[0].starts_with("POST / HTTP/1.1\r\n"));
    assert!(reqs[0].contains("CREATE TABLE IF NOT EXISTS override_db.velda_access_logs"));
    assert!(reqs[0].contains("X-ClickHouse-User: test_user\r\n"));
    assert!(reqs[0].contains("X-ClickHouse-Key: test_pass\r\n"));

    assert!(reqs[1].starts_with(
        "POST /?query=INSERT%20INTO%20override_db.velda_access_logs%20FORMAT%20JSONEachRow HTTP/1.1\r\n"
    ));
    assert!(reqs[1].contains(&format!("Host: 127.0.0.1:{port}\r\n")));
    assert!(reqs[1].contains("Content-Type: application/x-ndjson\r\n"));
    assert!(reqs[1].contains("X-ClickHouse-User: test_user\r\n"));
    assert!(reqs[1].contains("X-ClickHouse-Key: test_pass\r\n"));
}
