//! Integration Test Suite for Kafka Log Shipper (`kafka_log_shipper`).
//!
//! Validates:
//! 1. Declarative ETP-backed schema parsing, semantic validation, and strict alias rejection.
//! 2. Compile-time factory instantiation (`LoadedPlugin::Background` with `etp_id`).
//! 3. End-to-end Kafka RecordBatch v2 produce over `KafkaEtp` + zstd compression to mock broker.
//! 4. `KafkaEtp` connect-time SASL/PLAIN `SaslHandshake` (17) + `SaslAuthenticate` (36) exchange.
//! 5. Partition leader failover metadata refresh (`NOT_LEADER_OR_FOLLOWER`, error code 6).
//! 6. ETP registry wiring (`wire_plugin_etps`), in-place `lease.reconnect().await` recovery, and error paths.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use rustc_hash::FxHashMap;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use velda_core::{CompletionReason, GrpcStatus, HttpMethod, PipeKind, UaFamily, UaOs};
use velda_etp::kafka::KafkaEtp;
use velda_etp::registry::build_registry;
use velda_etp::{EtpEntryConfig, EtpKind, EtpPoolSettings};
use velda_observability::{TelemetryRecord, TelemetryStore};
use velda_plugin::background::BackgroundPlugin;
use velda_plugin::background::kafka_log_shipper::{config, plugin, protocol, validation, wire};
use velda_plugin::factory::{LoadedPlugin, instantiate_plugin, wire_plugin_etps};
use velda_plugin::{PluginConfig, is_known_plugin_id, validate_plugin_config};

static GLOBAL_STORE_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn make_sample_record(id: u64, ts: u32) -> TelemetryRecord {
    let mut rec = TelemetryRecord::zeroed();
    rec.ingress_time_nanos = (ts as u64) * 1_000_000_000;
    rec.request_id = id;
    rec.duration_ns = 150_000;
    rec.peer_ip = 0x0000_0000_0000_0000_0000_FFFF_C0A8_0164; // 192.168.1.100
    rec.upstream_ip = 0x0A000001; // 10.0.0.1
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
    rec.set_url("/api/v1/orders?item=edge");
    rec
}

// ── Mock Kafka Framing Helpers ───────────────────────────────────────────────

fn mock_metadata_response(
    cid: i32,
    port: u16,
    topic: &str,
    node_id: i32,
    leader_id: i32,
) -> Vec<u8> {
    let mut resp = Vec::new();
    resp.extend_from_slice(&cid.to_be_bytes());
    resp.extend_from_slice(&0i32.to_be_bytes()); // throttle_time_ms = 0
    resp.extend_from_slice(&1i32.to_be_bytes()); // 1 broker
    resp.extend_from_slice(&node_id.to_be_bytes());
    protocol::write_string(&mut resp, "127.0.0.1");
    resp.extend_from_slice(&(port as i32).to_be_bytes());
    protocol::write_nullable_string(&mut resp, None); // rack
    protocol::write_nullable_string(&mut resp, Some("mock-cluster"));
    resp.extend_from_slice(&0i32.to_be_bytes()); // controller_id

    resp.extend_from_slice(&1i32.to_be_bytes()); // 1 topic
    resp.extend_from_slice(&0i16.to_be_bytes()); // error_code = 0
    protocol::write_string(&mut resp, topic);
    resp.push(0); // is_internal = false

    resp.extend_from_slice(&1i32.to_be_bytes()); // 1 partition
    resp.extend_from_slice(&0i16.to_be_bytes()); // error_code = 0
    resp.extend_from_slice(&0i32.to_be_bytes()); // partition_index = 0
    resp.extend_from_slice(&leader_id.to_be_bytes());
    resp.extend_from_slice(&1i32.to_be_bytes()); // replica_nodes: [node_id]
    resp.extend_from_slice(&node_id.to_be_bytes());
    resp.extend_from_slice(&1i32.to_be_bytes()); // isr_nodes: [node_id]
    resp.extend_from_slice(&node_id.to_be_bytes());
    resp
}

fn mock_produce_response(cid: i32, topic: &str, error_code: i16, base_offset: i64) -> Vec<u8> {
    let mut resp = Vec::new();
    resp.extend_from_slice(&cid.to_be_bytes());
    resp.extend_from_slice(&1i32.to_be_bytes()); // 1 topic
    protocol::write_string(&mut resp, topic);
    resp.extend_from_slice(&1i32.to_be_bytes()); // 1 partition
    resp.extend_from_slice(&0i32.to_be_bytes()); // partition_index = 0
    resp.extend_from_slice(&error_code.to_be_bytes());
    resp.extend_from_slice(&base_offset.to_be_bytes());
    resp.extend_from_slice(&0i64.to_be_bytes()); // log_append_time
    resp.extend_from_slice(&0i64.to_be_bytes()); // log_start_offset (Produce v5..=7)
    resp.extend_from_slice(&0i32.to_be_bytes()); // throttle_time_ms
    resp
}

async fn write_framed_response(socket: &mut TcpStream, body: &[u8]) {
    let len = body.len() as i32;
    socket.write_all(&len.to_be_bytes()).await.unwrap();
    socket.write_all(body).await.unwrap();
    socket.flush().await.unwrap();
}

async fn read_framed_request(socket: &mut TcpStream) -> Option<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    if socket.read_exact(&mut len_buf).await.is_err() {
        return None;
    }
    let frame_len = i32::from_be_bytes(len_buf) as usize;
    let mut body = vec![0u8; frame_len];
    if socket.read_exact(&mut body).await.is_err() {
        return None;
    }
    Some(body)
}

// ── 1. Configuration & Semantic Validation Tests ─────────────────────────────

#[test]
fn test_kafka_config_validation_success() {
    let json = serde_json::json!({
        "etp_id": "kafka-main",
        "topic": "access_logs_v1",
        "format": "json",
        "compression": "zstd",
        "acks": "leader",
        "partitioner": "round_robin",
        "flush_interval_ms": 500,
        "max_batch_size": 4000
    });

    let cfg = validation::validate_kafka_log_shipper_config("kafka_log_shipper", &json)
        .expect("Valid configuration must succeed");

    assert_eq!(cfg.etp_id, "kafka-main");
    assert_eq!(cfg.topic, "access_logs_v1");
    assert_eq!(cfg.parsed_acks().unwrap(), config::KafkaAckMode::Leader);
    assert_eq!(
        cfg.parsed_partitioner().unwrap(),
        config::PartitionStrategy::RoundRobin
    );
}

#[test]
fn test_kafka_config_validation_rules() {
    let validate = |v: serde_json::Value| {
        validation::validate_kafka_log_shipper_config("kafka_log_shipper", &v)
    };

    // 1. Empty / whitespace etp_id
    assert!(validate(serde_json::json!({ "etp_id": "", "topic": "test" })).is_err());
    assert!(validate(serde_json::json!({ "etp_id": "   ", "topic": "test" })).is_err());

    // 2. Invalid topics
    for bad_topic in ["", "invalid topic with space", "invalid/slash", ".", ".."] {
        assert!(
            validate(serde_json::json!({ "etp_id": "kafka-main", "topic": bad_topic })).is_err()
        );
    }

    // 3. Strict parsing: Reject loose aliases
    for (key, val) in [
        ("compression", "uncompressed"),
        ("compression", "zstandard"),
        ("partitioner", "rr"),
        ("partitioner", "roundrobin"),
        ("format", "clf"),
    ] {
        assert!(
            validate(serde_json::json!({
                "etp_id": "kafka-main",
                "topic": "t",
                key: val
            }))
            .is_err(),
            "Expected failure for loose alias '{key}: {val}'"
        );
    }
}

// ── 2. Factory Registration & Route Rejection ────────────────────────────────

#[test]
fn test_kafka_factory_registration() {
    assert!(is_known_plugin_id("kafka_log_shipper"));
    assert!(velda_plugin::background::owns_id("kafka_log_shipper"));

    let mut plugin_cfg = PluginConfig {
        id: "kafka_log_shipper".to_string(),
        config: serde_json::json!({
            "etp_id": "kafka-main",
            "topic": "velda_logs"
        }),
    };

    validate_plugin_config(&mut plugin_cfg).expect("Validation should pass");

    let loaded = instantiate_plugin(&plugin_cfg).expect("Instantiation should succeed");
    match loaded {
        LoadedPlugin::Background(bg) => {
            assert_eq!(bg.id(), "kafka_log_shipper");
            assert_eq!(bg.pbww(), 75);
            assert_eq!(bg.etp_id(), Some("kafka-main"));
        }
        _ => panic!("Expected LoadedPlugin::Background"),
    }
}

#[test]
fn test_kafka_attached_to_route_rejected() {
    let plugin_cfg = PluginConfig {
        id: "kafka_log_shipper".to_string(),
        config: serde_json::json!({
            "etp_id": "kafka-main",
            "topic": "velda_logs"
        }),
    };
    let loaded = instantiate_plugin(&plugin_cfg).expect("instantiation should succeed");
    let mut available = FxHashMap::default();
    available.insert("kafka_log_shipper".to_string(), loaded);

    let res = velda_plugin::build_route_plugin_plan(
        "route-1",
        &["kafka_log_shipper".to_string()],
        &available,
        velda_core::ProtocolFamily::Http,
        velda_core::StreamingMode::DISABLED,
    );
    assert!(matches!(
        res,
        Err(velda_plugin::PluginError::ProtocolMismatch { .. })
    ));
}

// ── 3. End-to-End Mock Kafka Server Produce (KafkaEtp + Zstd + Acks=Leader) ─

#[tokio::test]
async fn test_kafka_shipper_e2e_produce_success() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let server_task = tokio::spawn(async move {
        let mut handled_produce = false;

        while !handled_produce {
            let (mut socket, _) = match listener.accept().await {
                Ok(conn) => conn,
                Err(_) => break,
            };

            while let Some(body) = read_framed_request(&mut socket).await {
                let mut cur = body.as_slice();
                let api_key = protocol::read_i16(&mut cur).unwrap();
                let _api_ver = protocol::read_i16(&mut cur).unwrap();
                let cid = protocol::read_i32(&mut cur).unwrap();
                let client_id = protocol::read_string(&mut cur).unwrap();
                assert_eq!(client_id, "velda-edge-e2e");

                match api_key {
                    protocol::API_KEY_METADATA => {
                        let resp = mock_metadata_response(cid, port, "velda-access-logs", 0, 0);
                        write_framed_response(&mut socket, &resp).await;
                    }
                    protocol::API_KEY_PRODUCE => {
                        let resp = mock_produce_response(cid, "velda-access-logs", 0, 100);
                        write_framed_response(&mut socket, &resp).await;
                        handled_produce = true;
                        break;
                    }
                    _ => {}
                }
            }
        }
    });

    let etp = Arc::new(
        KafkaEtp::from_entry(&EtpEntryConfig {
            id: "kafka-e2e".into(),
            kind: EtpKind::Kafka,
            pool: EtpPoolSettings {
                pool_size: 1,
                connect_timeout_ms: 2000,
                idle_timeout_ms: 60_000,
            },
            tls: None,
            settings: serde_json::json!({
                "brokers": [format!("127.0.0.1:{port}")],
                "client_id": "velda-edge-e2e",
                "sasl_mechanism": "none"
            }),
        })
        .unwrap(),
    );

    let store = Arc::new(TelemetryStore::with_cores_and_capacity(1, 1024));
    let cfg = config::KafkaLogShipperConfig {
        etp_id: "kafka-e2e".into(),
        topic: "velda-access-logs".into(),
        format: "json".into(),
        compression: "zstd".into(),
        acks: "leader".into(),
        partitioner: "round_robin".into(),
        flush_interval_ms: 100,
        max_batch_size: 1000,
        request_timeout_ms: 2000,
        metadata_refresh_ms: 300_000,
    };

    let plugin = Arc::new(
        plugin::KafkaLogShipperPlugin::with_etp("kafka_log_shipper", cfg, etp, Arc::clone(&store))
            .unwrap(),
    );

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let runner = Arc::clone(&plugin);
    let run_task = tokio::spawn(async move {
        runner.run(shutdown_rx).await.unwrap();
    });

    while store.active_subscriber_count.load(Ordering::Acquire) == 0 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    for i in 1..=3 {
        let rec = make_sample_record(i, 1732011000 + i as u32);
        store.emit_on_core(0, &rec);
    }
    store.flush_all_lanes(128);

    let start = tokio::time::Instant::now();
    while plugin.shipped_records.load(Ordering::Relaxed) == 0
        && start.elapsed() < Duration::from_secs(3)
    {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    assert!(
        plugin.shipped_records.load(Ordering::Relaxed) >= 3,
        "Shipper must produce at least 3 records to Kafka broker"
    );

    let _ = shutdown_tx.send(true);
    let _ = run_task.await;
    let _ = server_task.await;
}

// ── 4. Connect-Time SASL/PLAIN Handshake in KafkaEtp + Metadata Wire ─────────

#[tokio::test]
async fn test_kafka_shipper_e2e_sasl_plain() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let server_task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();

        // 1. Read SaslHandshake
        let body = read_framed_request(&mut socket).await.unwrap();
        let mut cur = body.as_slice();
        let api_key = protocol::read_i16(&mut cur).unwrap();
        let _ = protocol::read_i16(&mut cur).unwrap();
        let cid = protocol::read_i32(&mut cur).unwrap();
        let client_id = protocol::read_string(&mut cur).unwrap();
        let mechanism = protocol::read_string(&mut cur).unwrap();
        assert_eq!(api_key, protocol::API_KEY_SASL_HANDSHAKE);
        assert_eq!(client_id, "velda-edge-sasl");
        assert_eq!(mechanism, "PLAIN");

        let mut resp = Vec::new();
        resp.extend_from_slice(&cid.to_be_bytes());
        resp.extend_from_slice(&0i16.to_be_bytes()); // error_code = 0
        resp.extend_from_slice(&1i32.to_be_bytes()); // ["PLAIN"]
        protocol::write_string(&mut resp, "PLAIN");
        write_framed_response(&mut socket, &resp).await;

        // 2. Read SaslAuthenticate
        let body = read_framed_request(&mut socket).await.unwrap();
        let mut cur = body.as_slice();
        let api_key = protocol::read_i16(&mut cur).unwrap();
        let _ = protocol::read_i16(&mut cur).unwrap();
        let cid = protocol::read_i32(&mut cur).unwrap();
        let _ = protocol::read_string(&mut cur).unwrap();
        let auth_len = protocol::read_i32(&mut cur).unwrap() as usize;
        let auth_payload = &cur[..auth_len];
        assert_eq!(api_key, protocol::API_KEY_SASL_AUTHENTICATE);
        assert_eq!(auth_payload, b"\0alice\0secret123");

        let mut auth_resp = Vec::new();
        auth_resp.extend_from_slice(&cid.to_be_bytes());
        auth_resp.extend_from_slice(&0i16.to_be_bytes()); // error_code = 0
        protocol::write_nullable_string(&mut auth_resp, None);
        auth_resp.extend_from_slice(&0i32.to_be_bytes()); // empty bytes
        write_framed_response(&mut socket, &auth_resp).await;

        // 3. Handle Metadata over the same pre-authenticated socket
        let body = read_framed_request(&mut socket).await.unwrap();
        let mut cur = body.as_slice();
        let api_key = protocol::read_i16(&mut cur).unwrap();
        let _ = protocol::read_i16(&mut cur).unwrap();
        let cid = protocol::read_i32(&mut cur).unwrap();
        assert_eq!(api_key, protocol::API_KEY_METADATA);

        let meta = mock_metadata_response(cid, port, "sasl-logs", 0, 0);
        write_framed_response(&mut socket, &meta).await;
    });

    let etp = Arc::new(
        KafkaEtp::from_entry(&EtpEntryConfig {
            id: "kafka-sasl".into(),
            kind: EtpKind::Kafka,
            pool: EtpPoolSettings {
                pool_size: 1,
                connect_timeout_ms: 2000,
                idle_timeout_ms: 60_000,
            },
            tls: None,
            settings: serde_json::json!({
                "brokers": [format!("127.0.0.1:{port}")],
                "client_id": "velda-edge-sasl",
                "sasl_mechanism": "plain",
                "sasl_username": "alice",
                "sasl_password": "secret123"
            }),
        })
        .unwrap(),
    );
    etp.warm().await.unwrap();

    let cfg = config::KafkaLogShipperConfig {
        etp_id: "kafka-sasl".into(),
        topic: "sasl-logs".into(),
        ..Default::default()
    };

    let mut wire_state = wire::KafkaWireState::new(etp, &cfg).unwrap();
    wire_state
        .refresh_metadata()
        .await
        .expect("SASL/PLAIN connection and metadata refresh must succeed");

    assert_eq!(wire_state.partitions, vec![0]);

    let _ = server_task.await;
}

// ── 5. Leader Rebalance / Failover Test (Error Code 6: NOT_LEADER_OR_FOLLOWER) ─

#[tokio::test]
async fn test_kafka_shipper_leader_failover_recovery() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let server_task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();

        // 1. Initial Metadata Request
        let body = read_framed_request(&mut socket).await.unwrap();
        let mut cur = body.as_slice();
        let _ = protocol::read_i16(&mut cur).unwrap();
        let _ = protocol::read_i16(&mut cur).unwrap();
        let cid = protocol::read_i32(&mut cur).unwrap();
        let meta = mock_metadata_response(cid, port, "failover-logs", 0, 0);
        write_framed_response(&mut socket, &meta).await;

        // 2. First Produce Request -> Return error_code = 6 (NOT_LEADER_OR_FOLLOWER)
        let body = read_framed_request(&mut socket).await.unwrap();
        let mut cur = body.as_slice();
        let _ = protocol::read_i16(&mut cur).unwrap();
        let _ = protocol::read_i16(&mut cur).unwrap();
        let cid = protocol::read_i32(&mut cur).unwrap();
        let fail = mock_produce_response(cid, "failover-logs", 6, -1);
        write_framed_response(&mut socket, &fail).await;

        // 3. Metadata Refresh triggered automatically by wire state
        let body = read_framed_request(&mut socket).await.unwrap();
        let mut cur = body.as_slice();
        let _ = protocol::read_i16(&mut cur).unwrap();
        let _ = protocol::read_i16(&mut cur).unwrap();
        let cid = protocol::read_i32(&mut cur).unwrap();
        let meta2 = mock_metadata_response(cid, port, "failover-logs", 0, 0);
        write_framed_response(&mut socket, &meta2).await;
    });

    let etp = Arc::new(
        KafkaEtp::from_entry(&EtpEntryConfig {
            id: "kafka-failover".into(),
            kind: EtpKind::Kafka,
            pool: EtpPoolSettings {
                pool_size: 1,
                connect_timeout_ms: 2000,
                idle_timeout_ms: 60_000,
            },
            tls: None,
            settings: serde_json::json!({
                "brokers": [format!("127.0.0.1:{port}")],
            }),
        })
        .unwrap(),
    );

    let cfg = config::KafkaLogShipperConfig {
        etp_id: "kafka-failover".into(),
        topic: "failover-logs".into(),
        compression: "none".into(),
        ..Default::default()
    };

    let mut wire_state = wire::KafkaWireState::new(etp, &cfg).unwrap();
    wire_state.refresh_metadata().await.unwrap();

    let rec = make_sample_record(999, 1732000000);
    assert!(wire_state.produce_records(&[rec]).await.is_err());

    assert_eq!(wire_state.partitions, vec![0]);

    server_task.await.unwrap();
}

// ── 6. Registry Wiring + In-Place lease.reconnect() Recovery ─────────────────

#[tokio::test]
async fn test_kafka_etp_registry_wiring_and_reconnect_recovery() {
    let _global_guard = GLOBAL_STORE_TEST_LOCK.lock().await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let server_task = tokio::spawn(async move {
        // 1. Initial Metadata handshake succeeds and keeps the socket warm in KafkaEtp arena.
        let (mut s1, _) = listener.accept().await.unwrap();
        let meta_req = read_framed_request(&mut s1).await.unwrap();
        let mut cur = meta_req.as_slice();
        let api_key = protocol::read_i16(&mut cur).unwrap();
        let _ = protocol::read_i16(&mut cur).unwrap();
        let cid = protocol::read_i32(&mut cur).unwrap();
        assert_eq!(api_key, protocol::API_KEY_METADATA);
        write_framed_response(
            &mut s1,
            &mock_metadata_response(cid, port, "wired-logs", 0, 0),
        )
        .await;

        // 2. When the first Produce arrives on stale `s1`, drop `s1` immediately so
        //    KafkaWireState hits UnexpectedEof and triggers in-place `lease.reconnect().await`.
        let _stale_produce = read_framed_request(&mut s1).await;
        drop(s1);

        // 3. Reconnected retry on the same slot delivers Produce and gets ACK 0.
        let (mut s2, _) = listener.accept().await.unwrap();
        let prod_req = read_framed_request(&mut s2).await.unwrap();
        let mut cur = prod_req.as_slice();
        let api_key = protocol::read_i16(&mut cur).unwrap();
        let _ = protocol::read_i16(&mut cur).unwrap();
        let cid = protocol::read_i32(&mut cur).unwrap();
        let client_id = protocol::read_string(&mut cur).unwrap();
        assert_eq!(api_key, protocol::API_KEY_PRODUCE);
        assert_eq!(client_id, "velda-wired-client");
        write_framed_response(&mut s2, &mock_produce_response(cid, "wired-logs", 0, 7)).await;
    });

    let registry = build_registry(&[EtpEntryConfig {
        id: "kafka-wired".into(),
        kind: EtpKind::Kafka,
        pool: EtpPoolSettings {
            pool_size: 1,
            connect_timeout_ms: 2000,
            idle_timeout_ms: 60_000,
        },
        tls: None,
        settings: serde_json::json!({
            "brokers": [format!("127.0.0.1:{port}")],
            "client_id": "velda-wired-client",
            "sasl_mechanism": "none"
        }),
    }])
    .unwrap();

    let plugin_cfg = PluginConfig {
        id: "kafka_log_shipper".into(),
        config: serde_json::json!({
            "etp_id": "kafka-wired",
            "topic": "wired-logs",
            "compression": "none",
            "flush_interval_ms": 100,
            "request_timeout_ms": 2000
        }),
    };
    let loaded = instantiate_plugin(&plugin_cfg).unwrap();
    let mut available = FxHashMap::default();
    available.insert(loaded.id().to_string(), loaded.clone());
    wire_plugin_etps(&available, &registry).unwrap();

    let store = velda_observability::global_telemetry_store();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let runner = tokio::spawn(loaded.run_background(shutdown_rx).unwrap());

    while store.active_subscriber_count.load(Ordering::Acquire) == 0 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    store.emit_on_core(0, &make_sample_record(501, 1_732_015_900));
    store.flush_all_lanes(128);

    tokio::time::timeout(Duration::from_secs(3), server_task)
        .await
        .unwrap()
        .unwrap();
    tokio::time::sleep(Duration::from_millis(40)).await;

    let _ = shutdown_tx.send(true);
    runner.await.unwrap().unwrap();

    // Missing ETP produces an explicit error
    let err_plugin = instantiate_plugin(&plugin_cfg).unwrap();
    let mut missing_map = FxHashMap::default();
    missing_map.insert(err_plugin.id().to_string(), err_plugin);
    assert!(wire_plugin_etps(&missing_map, &velda_etp::EtpRegistry::default()).is_err());

    // Wrong-kind ETP produces an explicit error
    let wrong_registry = build_registry(&[EtpEntryConfig {
        id: "kafka-wired".into(),
        kind: EtpKind::Statsd,
        pool: EtpPoolSettings::default(),
        tls: None,
        settings: serde_json::json!({ "endpoint": "127.0.0.1:8125" }),
    }])
    .unwrap();
    let wrong_plugin = instantiate_plugin(&plugin_cfg).unwrap();
    let mut wrong_map = FxHashMap::default();
    wrong_map.insert(wrong_plugin.id().to_string(), wrong_plugin);
    assert!(wire_plugin_etps(&wrong_map, &wrong_registry).is_err());
}
