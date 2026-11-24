use std::io::Read;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use base64::Engine;
use flate2::read::GzDecoder;
use rustc_hash::FxHashMap;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::watch;
use velda_core::{CompletionReason, GrpcStatus, HttpMethod, PipeKind, UaFamily, UaOs};
use velda_etp::elasticsearch::ElasticsearchEtp;
use velda_etp::registry::build_registry;
use velda_etp::s3::S3Etp;
use velda_etp::{EtpEntryConfig, EtpKind, EtpPoolSettings};
use velda_observability::{TelemetryRecord, TelemetryStore};
use velda_plugin::background::BackgroundPlugin;
use velda_plugin::background::elasticsearch_log_shipper::{
    ElasticsearchLogShipperConfig, ElasticsearchLogShipperPlugin, EsBulkOpType, EsCompression,
};
use velda_plugin::background::s3_log_shipper::{
    S3Compression, S3LogShipperConfig, S3LogShipperPlugin,
};
use velda_plugin::config::PluginConfig;
use velda_plugin::factory::{instantiate_plugin, wire_plugin_etps};
use velda_plugin::validation::validate_plugin_config;

static GLOBAL_STORE_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn make_record(req_id: u64, url: &str) -> TelemetryRecord {
    let mut rec = TelemetryRecord::zeroed();
    rec.ingress_time_nanos = 1_732_015_800_123_456_789;
    rec.request_id = req_id;
    rec.duration_ns = 45_000;
    rec.peer_ip = 0x0000_0000_0000_0000_0000_FFFF_C0A8_0105;
    rec.upstream_ip = 0x0A00_0002;
    rec.upstream_port = 8081;
    rec.route_id = 3;
    rec.status_code = 200;
    rec.pipe = PipeKind::BufferedH1H1;
    rec.method = HttpMethod::Get;
    rec.completion = CompletionReason::Complete;
    rec.grpc_status = GrpcStatus::Ok;
    rec.ua_family = UaFamily::Curl;
    rec.ua_os = UaOs::Linux;
    rec.set_url(url);
    rec
}

async fn read_http_request(listener: &TcpListener) -> (String, Vec<u8>) {
    let (mut sock, _) = listener.accept().await.unwrap();
    let mut buf = Vec::with_capacity(4096);
    let mut tmp = [0u8; 2048];
    let header_end;
    loop {
        let n = sock.read(&mut tmp).await.unwrap();
        assert!(n > 0, "unexpected EOF reading request");
        buf.extend_from_slice(&tmp[..n]);
        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            header_end = pos + 4;
            break;
        }
    }
    let headers = String::from_utf8_lossy(&buf[..header_end]).to_string();
    let content_len = headers
        .lines()
        .find_map(|l| {
            let (k, v) = l.split_once(':')?;
            if k.trim().eq_ignore_ascii_case("content-length") {
                v.trim().parse::<usize>().ok()
            } else {
                None
            }
        })
        .unwrap_or(0);

    while buf.len() < header_end + content_len {
        let n = sock.read(&mut tmp).await.unwrap();
        assert!(n > 0, "unexpected EOF reading body");
        buf.extend_from_slice(&tmp[..n]);
    }
    let body = buf[header_end..header_end + content_len].to_vec();

    let resp = b"HTTP/1.1 200 OK\r\nContent-Length: 16\r\n\r\n{\"errors\":false}";
    sock.write_all(resp).await.unwrap();
    sock.flush().await.unwrap();

    (headers, body)
}

#[tokio::test]
async fn test_elasticsearch_shipper_end_to_end() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let etp = Arc::new(
        ElasticsearchEtp::from_entry(&EtpEntryConfig {
            id: "es-e2e".into(),
            kind: EtpKind::Elasticsearch,
            pool: EtpPoolSettings {
                pool_size: 2,
                connect_timeout_ms: 2000,
                idle_timeout_ms: 60_000,
            },
            tls: None,
            settings: serde_json::json!({
                "endpoint": format!("http://{addr}"),
                "api_key": "test-key"
            }),
        })
        .unwrap(),
    );

    let store = Arc::new(TelemetryStore::with_cores_and_capacity(1, 128));
    let cfg = ElasticsearchLogShipperConfig {
        etp_id: "es-e2e".to_string(),
        index: "velda-test".to_string(),
        op_type: EsBulkOpType::Create,
        compression: EsCompression::Gzip,
        flush_interval_ms: 25,
        ..Default::default()
    };

    let plugin = Arc::new(
        ElasticsearchLogShipperPlugin::with_etp("es_test", cfg, etp, Arc::clone(&store)).unwrap(),
    );
    let (tx, rx) = watch::channel(false);
    let handle = tokio::spawn(Arc::clone(&plugin).run(rx));

    while store.active_subscriber_count.load(Ordering::Acquire) == 0 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    store.emit_on_core(0, &make_record(1, "/es/one"));
    store.emit_on_core(0, &make_record(2, "/es/two"));
    store.flush_all_lanes(128);

    let (headers, body) =
        tokio::time::timeout(Duration::from_secs(3), read_http_request(&listener))
            .await
            .unwrap();

    assert!(headers.starts_with("POST /_bulk HTTP/1.1\r\n"));
    assert!(headers.contains("Content-Encoding: gzip\r\n"));
    assert!(headers.contains("Authorization: ApiKey test-key\r\n"));

    let mut dec = GzDecoder::new(body.as_slice());
    let mut raw = String::new();
    dec.read_to_string(&mut raw).unwrap();
    let lines: Vec<&str> = raw.lines().collect();
    assert_eq!(lines.len(), 4);
    assert_eq!(lines[0], r#"{"create":{"_index":"velda-test"}}"#);
    assert!(lines[1].contains("/es/one"));
    assert!(lines[3].contains("/es/two"));

    for _ in 0..50 {
        if plugin.shipped_records.load(Ordering::Relaxed) >= 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(plugin.shipped_records.load(Ordering::Relaxed), 2);
    assert_eq!(plugin.failed_pushes.load(Ordering::Relaxed), 0);

    let _ = tx.send(true);
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn test_elasticsearch_etp_registry_wiring_and_reconnect_recovery() {
    let _global_guard = GLOBAL_STORE_TEST_LOCK.lock().await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let server_task = tokio::spawn(async move {
        // 1. Accept pre-warmed/initial socket and drop abruptly on first push so
        //    ElasticsearchLogShipperPlugin executes `lease.reconnect().await` in-place.
        let (mut s1, _) = listener.accept().await.unwrap();
        let mut buf = vec![0u8; 8192];
        let _ = s1.read(&mut buf).await;
        drop(s1);

        // 2. Accept reconnected retry on the same slot and return HTTP/1.1 200 OK.
        let (headers, body) = read_http_request(&listener).await;
        let expected_basic = base64::engine::general_purpose::STANDARD.encode("elastic:pw");
        assert!(headers.starts_with("POST /_bulk HTTP/1.1\r\n"));
        assert!(headers.contains(&format!("Authorization: Basic {expected_basic}\r\n")));
        assert!(headers.contains("Content-Encoding: gzip\r\n"));

        let mut dec = GzDecoder::new(body.as_slice());
        let mut raw = String::new();
        dec.read_to_string(&mut raw).unwrap();
        assert!(raw.contains(r#"{"create":{"_index":"velda-logs"}}"#));
        assert!(raw.contains("/es/reconnect-check"));
    });

    let registry = build_registry(&[EtpEntryConfig {
        id: "es-wired".into(),
        kind: EtpKind::Elasticsearch,
        pool: EtpPoolSettings {
            pool_size: 1,
            connect_timeout_ms: 2000,
            idle_timeout_ms: 60_000,
        },
        tls: None,
        settings: serde_json::json!({
            "host": "127.0.0.1",
            "port": port,
            "username": "elastic",
            "password": "pw"
        }),
    }])
    .unwrap();

    let plugin_cfg = PluginConfig {
        id: "elasticsearch_log_shipper".into(),
        config: serde_json::json!({
            "etp_id": "es-wired",
            "compression": "gzip",
            "flush_interval_ms": 25,
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

    store.emit_on_core(0, &make_record(77, "/es/reconnect-check"));
    store.flush_all_lanes(128);

    tokio::time::timeout(Duration::from_secs(3), server_task)
        .await
        .unwrap()
        .unwrap();
    tokio::time::sleep(Duration::from_millis(40)).await;

    let _ = shutdown_tx.send(true);
    runner.await.unwrap().unwrap();

    // Missing ETP in registry produces an explicit PluginError
    let err_plugin = instantiate_plugin(&plugin_cfg, &[]).unwrap();
    let mut missing_map = FxHashMap::default();
    missing_map.insert(err_plugin.id().to_string(), err_plugin);
    let empty_registry = velda_etp::EtpRegistry::default();
    assert!(wire_plugin_etps(&missing_map, &empty_registry).is_err());
}

#[tokio::test]
async fn test_s3_shipper_end_to_end_sigv4_and_zstd() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let etp = Arc::new(
        S3Etp::from_entry(&EtpEntryConfig {
            id: "s3-e2e".into(),
            kind: EtpKind::S3,
            pool: EtpPoolSettings {
                pool_size: 2,
                connect_timeout_ms: 2000,
                idle_timeout_ms: 60_000,
            },
            tls: None,
            settings: serde_json::json!({
                "endpoint": format!("http://{addr}"),
                "bucket": "velda-bucket",
                "region": "us-east-1",
                "access_key_id": "AKIA_TEST",
                "secret_access_key": "SECRET_TEST",
                "force_path_style": true
            }),
        })
        .unwrap(),
    );

    let store = Arc::new(TelemetryStore::with_cores_and_capacity(1, 128));
    let cfg = S3LogShipperConfig {
        etp_id: "s3-e2e".to_string(),
        prefix: "edge-logs".to_string(),
        compression: S3Compression::Zstd,
        flush_interval_ms: 25,
        ..Default::default()
    };

    let plugin =
        Arc::new(S3LogShipperPlugin::with_etp("s3_test", cfg, etp, Arc::clone(&store)).unwrap());
    let (tx, rx) = watch::channel(false);
    let handle = tokio::spawn(Arc::clone(&plugin).run(rx));

    while store.active_subscriber_count.load(Ordering::Acquire) == 0 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    store.emit_on_core(0, &make_record(10, "/s3/alpha"));
    store.emit_on_core(0, &make_record(11, "/s3/beta"));
    store.flush_all_lanes(128);

    let (headers, body) =
        tokio::time::timeout(Duration::from_secs(3), read_http_request(&listener))
            .await
            .unwrap();

    assert!(headers.starts_with("PUT /velda-bucket/edge-logs/year="));
    assert!(headers.contains(".jsonl.zst HTTP/1.1\r\n"));
    assert!(headers.contains("Authorization: AWS4-HMAC-SHA256 Credential=AKIA_TEST/"));
    assert!(headers.contains("x-amz-content-sha256: "));

    let decompressed = zstd::bulk::decompress(&body, 64 * 1024).unwrap();
    let text = std::str::from_utf8(&decompressed).unwrap();
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(lines.len(), 2);
    assert!(lines[0].contains("/s3/alpha"));
    assert!(lines[1].contains("/s3/beta"));

    for _ in 0..50 {
        if plugin.shipped_records.load(Ordering::Relaxed) >= 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(plugin.shipped_records.load(Ordering::Relaxed), 2);
    assert_eq!(plugin.failed_pushes.load(Ordering::Relaxed), 0);

    let _ = tx.send(true);
    handle.await.unwrap().unwrap();
}

#[test]
fn test_validation_accepts_es_and_s3_background_plugins() {
    let mut es = PluginConfig {
        id: "elasticsearch_log_shipper".to_string(),
        config: serde_json::json!({
            "etp_id": "es-primary",
            "index": "velda-logs"
        }),
    };
    let mut s3 = PluginConfig {
        id: "s3_log_shipper".to_string(),
        config: serde_json::json!({
            "etp_id": "s3-primary",
            "prefix": "velda-logs"
        }),
    };
    validate_plugin_config(&mut es).unwrap();
    validate_plugin_config(&mut s3).unwrap();
}

#[tokio::test]
async fn test_s3_etp_registry_wiring_sts_token_and_reconnect_recovery() {
    let _global_guard = GLOBAL_STORE_TEST_LOCK.lock().await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let server_task = tokio::spawn(async move {
        // 1. Accept initial socket and drop abruptly so S3LogShipperPlugin
        //    executes `lease.reconnect().await` in-place.
        let (mut s1, _) = listener.accept().await.unwrap();
        let mut scratch = vec![0u8; 8192];
        let _ = s1.read(&mut scratch).await;
        drop(s1);

        // 2. Accept reconnected retry on same slot, verify STS token + SigV4 + chunked drain,
        //    then reply Connection: keep-alive.
        let (mut s2, _) = listener.accept().await.unwrap();
        let mut buf = Vec::with_capacity(4096);
        let mut tmp = [0u8; 2048];
        let hdr_end = loop {
            let n = s2.read(&mut tmp).await.unwrap();
            assert!(n > 0, "unexpected EOF on S3 reconnect socket");
            buf.extend_from_slice(&tmp[..n]);
            if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                break pos + 4;
            }
        };
        let headers = String::from_utf8_lossy(&buf[..hdr_end]).to_string();
        let content_len = headers
            .lines()
            .find_map(|l| {
                let (k, v) = l.split_once(':')?;
                if k.trim().eq_ignore_ascii_case("content-length") {
                    v.trim().parse::<usize>().ok()
                } else {
                    None
                }
            })
            .unwrap();
        while buf.len() < hdr_end + content_len {
            let n = s2.read(&mut tmp).await.unwrap();
            assert!(n > 0);
            buf.extend_from_slice(&tmp[..n]);
        }
        let body = &buf[hdr_end..hdr_end + content_len];
        let raw_text = std::str::from_utf8(body).unwrap();

        assert!(headers.starts_with("PUT /velda-sts/velda-logs/year="));
        assert!(headers.contains(".jsonl HTTP/1.1\r\n"));
        assert!(headers.contains("x-amz-security-token: STS-SESSION-TOKEN-999\r\n"));
        assert!(
            headers.contains(
                "SignedHeaders=host;x-amz-content-sha256;x-amz-date;x-amz-security-token"
            )
        );
        assert!(headers.contains(&format!("Host: 127.0.0.1:{port}\r\n")));
        assert!(raw_text.contains("/s3/sts-reconnect"));

        s2.write_all(
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: keep-alive\r\n\r\n0\r\n\r\n",
        )
        .await
        .unwrap();
        s2.flush().await.unwrap();

        // 3. Second PUT reuses kept-alive `s2`; server returns 500 with Content-Length body to verify
        //    body draining and background connection recreation on next attempt.
        buf.clear();
        let hdr_end2 = loop {
            let n = s2.read(&mut tmp).await.unwrap();
            assert!(n > 0, "expected second PUT reuse on keep-alive S3 socket");
            buf.extend_from_slice(&tmp[..n]);
            if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                break pos + 4;
            }
        };
        let headers2 = String::from_utf8_lossy(&buf[..hdr_end2]).to_string();
        assert!(headers2.starts_with("PUT /velda-sts/velda-logs/year="));
        s2.write_all(b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 9\r\n\r\nerr-retry")
            .await
            .unwrap();
        s2.flush().await.unwrap();
    });

    let registry = build_registry(&[EtpEntryConfig {
        id: "s3-wired".into(),
        kind: EtpKind::S3,
        pool: EtpPoolSettings {
            pool_size: 1,
            connect_timeout_ms: 2000,
            idle_timeout_ms: 60_000,
        },
        tls: None,
        settings: serde_json::json!({
            "endpoint": format!("http://127.0.0.1:{port}"),
            "bucket": "velda-sts",
            "region": "eu-west-1",
            "access_key": "AKIA_STS",
            "secret_key": "SECRET_STS",
            "session_token": "STS-SESSION-TOKEN-999",
            "addressing_style": "path"
        }),
    }])
    .unwrap();

    let plugin_cfg = PluginConfig {
        id: "s3_log_shipper".into(),
        config: serde_json::json!({
            "etp_id": "s3-wired",
            "prefix": "velda-logs",
            "compression": "none",
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

    store.emit_on_core(0, &make_record(88, "/s3/sts-reconnect"));
    store.flush_all_lanes(128);
    tokio::time::sleep(Duration::from_millis(180)).await;

    store.emit_on_core(0, &make_record(89, "/s3/keepalive-500"));
    store.flush_all_lanes(128);

    tokio::time::timeout(Duration::from_secs(3), server_task)
        .await
        .unwrap()
        .unwrap();
    tokio::time::sleep(Duration::from_millis(40)).await;

    let _ = shutdown_tx.send(true);
    runner.await.unwrap().unwrap();

    // Missing ETP produces an explicit PluginError
    let err_plugin = instantiate_plugin(&plugin_cfg, &[]).unwrap();
    let mut missing_map = FxHashMap::default();
    missing_map.insert(err_plugin.id().to_string(), err_plugin);
    assert!(wire_plugin_etps(&missing_map, &velda_etp::EtpRegistry::default()).is_err());

    // Wrong-kind ETP (Elasticsearch instead of S3) produces an explicit PluginError
    let wrong_registry = build_registry(&[EtpEntryConfig {
        id: "s3-wired".into(),
        kind: EtpKind::Elasticsearch,
        pool: EtpPoolSettings::default(),
        tls: None,
        settings: serde_json::json!({ "endpoint": "http://127.0.0.1:9200" }),
    }])
    .unwrap();
    let wrong_plugin = instantiate_plugin(&plugin_cfg, &[]).unwrap();
    let mut wrong_map = FxHashMap::default();
    wrong_map.insert(wrong_plugin.id().to_string(), wrong_plugin);
    assert!(wire_plugin_etps(&wrong_map, &wrong_registry).is_err());
}

#[tokio::test]
async fn test_prebaked_headers_bearer_base_path_index_fallback_and_chunked_drain() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let captured = Arc::new(tokio::sync::Mutex::new(Vec::<String>::new()));
    let captured_c = Arc::clone(&captured);

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut buf = vec![0u8; 8192];

        // 1. First _bulk push: returns chunked body with split "\"errors\":false"
        let n1 = sock.read(&mut buf).await.unwrap();
        captured_c
            .lock()
            .await
            .push(String::from_utf8_lossy(&buf[..n1]).into_owned());
        sock.write_all(
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: keep-alive\r\n\r\n10\r\n{\"errors\":false}\r\n0\r\n\r\n",
        )
        .await
        .unwrap();
        sock.flush().await.unwrap();

        // 2. Second _bulk push on same kept-alive socket: returns chunked "\"errors\":true" split across chunks
        let n2 = sock.read(&mut buf).await.unwrap();
        captured_c
            .lock()
            .await
            .push(String::from_utf8_lossy(&buf[..n2]).into_owned());
        sock.write_all(
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: keep-alive\r\n\r\na\r\n{\"errors\":\r\n5\r\ntrue}\r\n0\r\n\r\n",
        )
        .await
        .unwrap();
        sock.flush().await.unwrap();
    });

    let entry = EtpEntryConfig {
        id: "es-bearer".into(),
        kind: EtpKind::Elasticsearch,
        pool: EtpPoolSettings {
            pool_size: 1,
            connect_timeout_ms: 2000,
            idle_timeout_ms: 60_000,
        },
        tls: None,
        settings: serde_json::json!({
            "endpoint": format!("http://127.0.0.1:{port}/es-proxy"),
            "index": "etp-default-logs",
            "bearer_token": "tok-unit-secret"
        }),
    };
    let etp = Arc::new(ElasticsearchEtp::from_entry(&entry).unwrap());
    let store = Arc::new(TelemetryStore::with_cores_and_capacity(1, 128));

    let cfg = ElasticsearchLogShipperConfig {
        etp_id: "es-bearer".into(),
        index: String::new(),
        op_type: EsBulkOpType::Index,
        pipeline: Some("ingest-v1".into()),
        compression: EsCompression::None,
        max_drain_batches: 64,
        flush_interval_ms: 30,
        request_timeout_ms: 2000,
    };
    let plugin = Arc::new(
        ElasticsearchLogShipperPlugin::with_etp("es_unit", cfg, etp, Arc::clone(&store)).unwrap(),
    );

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let p = Arc::clone(&plugin);
    let runner = tokio::spawn(async move { p.run(shutdown_rx).await });

    while store.active_subscriber_count.load(Ordering::Acquire) == 0 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    store.emit_on_core(0, &make_record(1, "/first"));
    store.flush_all_lanes(64);
    for _ in 0..50 {
        if plugin.shipped_records.load(Ordering::Relaxed) >= 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(15)).await;
    }
    assert_eq!(plugin.shipped_records.load(Ordering::Relaxed), 1);

    store.emit_on_core(0, &make_record(2, "/second"));
    store.flush_all_lanes(64);
    for _ in 0..50 {
        if plugin.shipped_records.load(Ordering::Relaxed) >= 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(15)).await;
    }
    assert_eq!(plugin.shipped_records.load(Ordering::Relaxed), 2);
    assert_eq!(plugin.failed_pushes.load(Ordering::Relaxed), 0);

    let _ = shutdown_tx.send(true);
    runner.await.unwrap().unwrap();
    server.await.unwrap();

    let reqs = captured.lock().await;
    assert_eq!(reqs.len(), 2);
    assert!(reqs[0].starts_with("POST /es-proxy/_bulk?pipeline=ingest-v1 HTTP/1.1\r\n"));
    assert!(reqs[0].contains(&format!("Host: 127.0.0.1:{port}\r\n")));
    assert!(reqs[0].contains("Content-Type: application/x-ndjson\r\n"));
    assert!(reqs[0].contains("Authorization: Bearer tok-unit-secret\r\n"));
    assert!(reqs[0].contains(r#"{"index":{"_index":"etp-default-logs"}}"#));
    assert!(reqs[1].contains(r#"{"index":{"_index":"etp-default-logs"}}"#));
}
