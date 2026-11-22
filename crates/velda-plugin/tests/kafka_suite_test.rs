//! Integration Test Suite for Kafka Log Shipper (`kafka_log_shipper`).
//!
//! Validates:
//! 1. Declarative configuration schema, strict parsing, and semantic validation rules.
//! 2. Compile-time factory instantiation (`LoadedPlugin::Background`).
//! 3. End-to-end Kafka RecordBatch v2 produce over TCP with zstd compression to mock broker.
//! 4. SASL/PLAIN handshake and authentication wire exchange.
//! 5. Partition leader failover handling (`NOT_LEADER_OR_FOLLOWER`).
//! 6. Auto-generated per-node dynamic `client_id` isolation.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use velda_core::{CompletionReason, GrpcStatus, HttpMethod, PipeKind, UaFamily, UaOs};
use velda_observability::{TelemetryRecord, TelemetryStore};
use velda_plugin::background::BackgroundPlugin;
use velda_plugin::background::kafka_log_shipper::{client, config, plugin, protocol, validation};
use velda_plugin::factory::{LoadedPlugin, instantiate_plugin};
use velda_plugin::{PluginConfig, is_known_plugin_id, validate_plugin_config};

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
    resp.extend_from_slice(&0i64.to_be_bytes()); // log_start_offset
    resp.extend_from_slice(&0i32.to_be_bytes()); // record_errors
    protocol::write_nullable_string(&mut resp, None);
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
        "brokers": ["127.0.0.1:9092", "kafka.internal:9093"],
        "topic": "access_logs_v1",
        "format": "json",
        "transport_security": "plain",
        "compression": "zstd",
        "acks": "leader",
        "partitioner": "round_robin",
        "flush_interval_ms": 500,
        "max_batch_size": 4000
    });

    let cfg = validation::validate_kafka_log_shipper_config("kafka_log_shipper", &json)
        .expect("Valid configuration must succeed");

    assert_eq!(cfg.brokers.len(), 2);
    assert_eq!(cfg.topic, "access_logs_v1");
    assert_eq!(
        cfg.parsed_transport_security().unwrap(),
        config::KafkaTransportSecurity::Plain
    );
    assert_eq!(cfg.parsed_acks().unwrap(), config::KafkaAckMode::Leader);
}

#[test]
fn test_kafka_config_validation_rules() {
    let validate = |v: serde_json::Value| {
        validation::validate_kafka_log_shipper_config("kafka_log_shipper", &v)
    };

    // 1. Empty brokers
    assert!(validate(serde_json::json!({ "brokers": [], "topic": "test" })).is_err());

    // 2. Invalid topics
    for bad_topic in ["", "invalid topic with space", "invalid/slash", ".", ".."] {
        assert!(
            validate(serde_json::json!({ "brokers": ["127.0.0.1:9092"], "topic": bad_topic }))
                .is_err()
        );
    }

    // 3. Plain with TLS certificates rejected
    assert!(
        validate(serde_json::json!({
            "brokers": ["127.0.0.1:9092"], "topic": "t",
            "transport_security": "plain",
            "tls": { "client_cert_pem": "cert" }
        }))
        .is_err()
    );

    // 4. mTLS missing private key rejected
    assert!(
        validate(serde_json::json!({
            "brokers": ["127.0.0.1:9093"], "topic": "t",
            "transport_security": "mtls",
            "tls": { "client_cert_pem": "cert" }
        }))
        .is_err()
    );

    // 5. SASL credentials missing
    assert!(
        validate(serde_json::json!({
            "brokers": ["127.0.0.1:9092"], "topic": "t",
            "sasl_mechanism": "plain", "sasl_username": "", "sasl_password": ""
        }))
        .is_err()
    );

    // 6. Strict parsing: Reject backward-compatibility / loose aliases
    for (key, val) in [
        ("transport_security", "plaintext"),
        ("transport_security", "ssl"),
        ("compression", "uncompressed"),
        ("compression", "zstandard"),
        ("partitioner", "rr"),
        ("partitioner", "roundrobin"),
        ("format", "clf"),
        ("sasl_mechanism", "scram-sha-256"),
    ] {
        assert!(
            validate(serde_json::json!({
                "brokers": ["127.0.0.1:9092"], "topic": "t", key: val
            }))
            .is_err(),
            "Expected failure for loose alias '{key}: {val}'"
        );
    }
}

// ── 2. Factory Registration & Process ID Isolation ───────────────────────────

#[test]
fn test_kafka_factory_registration() {
    assert!(is_known_plugin_id("kafka_log_shipper"));
    assert!(velda_plugin::background::owns_id("kafka_log_shipper"));

    let mut plugin_cfg = PluginConfig {
        id: "kafka_log_shipper".to_string(),
        config: serde_json::json!({
            "brokers": ["127.0.0.1:9092"],
            "topic": "velda_logs"
        }),
    };

    validate_plugin_config(&mut plugin_cfg).expect("Validation should pass");

    let loaded = instantiate_plugin(&plugin_cfg).expect("Instantiation should succeed");
    match loaded {
        LoadedPlugin::Background(bg) => {
            assert_eq!(bg.id(), "kafka_log_shipper");
            assert_eq!(bg.pbww(), 75);
        }
        _ => panic!("Expected LoadedPlugin::Background"),
    }
}

#[test]
fn test_kafka_attached_to_route_rejected() {
    let plugin_cfg = PluginConfig {
        id: "kafka_log_shipper".to_string(),
        config: serde_json::json!({
            "brokers": ["127.0.0.1:9092"],
            "topic": "velda_logs"
        }),
    };
    let loaded = instantiate_plugin(&plugin_cfg).expect("instantiation should succeed");
    let mut available = rustc_hash::FxHashMap::default();
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

#[test]
fn test_dynamic_client_id_generation() {
    let id1 = config::generate_client_id();
    let id2 = config::generate_client_id();
    assert!(id1.starts_with("velda-edge-"));
    assert_eq!(
        id1, id2,
        "Process client_id should remain stable across invocations within the same process"
    );
}

// ── 3. End-to-End Mock Kafka Server Produce (TCP + Zstd + Acks=Leader) ────────

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

    let store = Arc::new(TelemetryStore::with_cores_and_capacity(1, 1024));
    let cfg = config::KafkaLogShipperConfig {
        brokers: vec![format!("127.0.0.1:{port}")],
        topic: "velda-access-logs".into(),
        format: "json".into(),
        transport_security: "plain".into(),
        tls: Default::default(),
        sasl_mechanism: "none".into(),
        sasl_username: "".into(),
        sasl_password: "".into(),
        compression: "zstd".into(),
        acks: "leader".into(),
        partitioner: "round_robin".into(),
        flush_interval_ms: 100,
        max_batch_size: 1000,
        connect_timeout_ms: 2000,
        request_timeout_ms: 2000,
        metadata_refresh_ms: 300000,
    };

    let plugin = Arc::new(
        plugin::KafkaLogShipperPlugin::with_store("kafka_log_shipper", cfg, Arc::clone(&store))
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

    // Push 3 records into store
    for i in 1..=3 {
        let rec = make_sample_record(i, 1732011000 + i as u32);
        store.emit_on_core(0, &rec);
    }
    store.flush_all_lanes(128);

    // Wait until records are shipped
    let start = tokio::time::Instant::now();
    while plugin.shipped_records() == 0 && start.elapsed() < Duration::from_secs(3) {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    assert!(
        plugin.shipped_records() >= 3,
        "Shipper must produce at least 3 records to Kafka broker"
    );

    let _ = shutdown_tx.send(true);
    let _ = run_task.await;
    let _ = server_task.await;
}

// ── 4. End-to-End Mock Kafka Server with SASL/PLAIN Authentication ───────────

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
        let _ = protocol::read_string(&mut cur).unwrap();
        let mechanism = protocol::read_string(&mut cur).unwrap();
        assert_eq!(api_key, protocol::API_KEY_SASL_HANDSHAKE);
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

        // 3. Handle Metadata
        let body = read_framed_request(&mut socket).await.unwrap();
        let mut cur = body.as_slice();
        let api_key = protocol::read_i16(&mut cur).unwrap();
        let _ = protocol::read_i16(&mut cur).unwrap();
        let cid = protocol::read_i32(&mut cur).unwrap();
        assert_eq!(api_key, protocol::API_KEY_METADATA);

        let meta = mock_metadata_response(cid, port, "sasl-logs", 0, 0);
        write_framed_response(&mut socket, &meta).await;
    });

    let cfg = config::KafkaLogShipperConfig {
        brokers: vec![format!("127.0.0.1:{port}")],
        topic: "sasl-logs".into(),
        format: "json".into(),
        transport_security: "plain".into(),
        tls: Default::default(),
        sasl_mechanism: "plain".into(),
        sasl_username: "alice".into(),
        sasl_password: "secret123".into(),
        compression: "none".into(),
        acks: "leader".into(),
        partitioner: "round_robin".into(),
        flush_interval_ms: 100,
        max_batch_size: 100,
        connect_timeout_ms: 2000,
        request_timeout_ms: 2000,
        metadata_refresh_ms: 300000,
    };

    let mut client = client::KafkaClusterClient::new(&cfg).unwrap();
    client
        .refresh_metadata()
        .await
        .expect("SASL/PLAIN connection and metadata refresh must succeed");

    assert_eq!(client.brokers.len(), 1);
    assert_eq!(client.partitions.len(), 1);

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

        // 3. Metadata Refresh triggered automatically by client
        let body = read_framed_request(&mut socket).await.unwrap();
        let mut cur = body.as_slice();
        let _ = protocol::read_i16(&mut cur).unwrap();
        let _ = protocol::read_i16(&mut cur).unwrap();
        let cid = protocol::read_i32(&mut cur).unwrap();
        let meta2 = mock_metadata_response(cid, port, "failover-logs", 0, 0);
        write_framed_response(&mut socket, &meta2).await;
    });

    let cfg = config::KafkaLogShipperConfig {
        brokers: vec![format!("127.0.0.1:{port}")],
        topic: "failover-logs".into(),
        format: "json".into(),
        transport_security: "plain".into(),
        tls: Default::default(),
        sasl_mechanism: "none".into(),
        sasl_username: "".into(),
        sasl_password: "".into(),
        compression: "none".into(),
        acks: "leader".into(),
        partitioner: "round_robin".into(),
        flush_interval_ms: 100,
        max_batch_size: 100,
        connect_timeout_ms: 2000,
        request_timeout_ms: 2000,
        metadata_refresh_ms: 300000,
    };

    let mut client = client::KafkaClusterClient::new(&cfg).unwrap();
    client.refresh_metadata().await.unwrap();

    let rec = make_sample_record(999, 1732000000);
    let _ = client.push_records(&[rec]).await;

    // After failover trigger, client should have automatically refreshed metadata
    assert_eq!(client.brokers.len(), 1);
    assert_eq!(
        client
            .partitions
            .iter()
            .find(|p| p.partition_id == 0)
            .map(|p| p.leader_node_id),
        Some(0)
    );

    let _ = server_task.await;
}
