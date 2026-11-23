//! Integration & End-to-End Test Suite for StatsD / DogStatsD Metrics Exporter.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use rustc_hash::FxHashMap;
use tokio::net::UdpSocket;
use tokio::sync::watch;

use velda_core::PipeKind;
use velda_etp::registry::build_registry;
use velda_etp::statsd::StatsdEtp;
use velda_etp::{EtpEntryConfig, EtpKind, EtpPoolSettings};
use velda_observability::{MetricsStore, StreamCompletion};
use velda_plugin::background::BackgroundPlugin;
use velda_plugin::background::statsd_exporter::dialect::{StatsdDialect, format_statsd_snapshot};
use velda_plugin::background::statsd_exporter::wire;
use velda_plugin::background::statsd_exporter::{StatsdExporterConfig, StatsdExporterPlugin};
use velda_plugin::config::PluginConfig;
use velda_plugin::factory::{instantiate_plugin, wire_plugin_etps};

#[test]
fn test_dogstatsd_formatting_and_tags() {
    let store = MetricsStore::new();

    // Record sample completions
    store.record_stream_start(PipeKind::BufferedH1H1);
    store.record_stream_completion(
        PipeKind::BufferedH1H1,
        StreamCompletion::new(200, 1024, 2048, 15_000_000, 0),
    );

    let snapshot = store.snapshot();
    let mut packet_buf = Vec::new();
    let mut collected_packets = Vec::new();

    let (packets, metrics) = format_statsd_snapshot(
        &snapshot,
        "velda",
        StatsdDialect::DogStatsd,
        &mut packet_buf,
        1432,
        &mut |pkt| {
            collected_packets.push(String::from_utf8(pkt.to_vec()).unwrap());
        },
    );

    assert!(packets > 0);
    assert!(metrics > 0);

    let combined = collected_packets.join("");
    // Check global metrics
    assert!(combined.contains("velda.connections.total:0|c\n"));
    assert!(combined.contains("velda.connections.active:0|g\n"));
    assert!(combined.contains("velda.pool.containers.total:0|g\n"));

    // Check dogstatsd tags
    assert!(combined.contains(
        "velda.requests.total:1|c|#downstream:http/1.1,upstream:http/1.1,pipe:buffered,status:2xx\n"
    ));
    assert!(
        combined.contains(
            "velda.bytes.rx:1024|c|#downstream:http/1.1,upstream:http/1.1,pipe:buffered\n"
        )
    );
    assert!(
        combined.contains(
            "velda.bytes.tx:2048|c|#downstream:http/1.1,upstream:http/1.1,pipe:buffered\n"
        )
    );
    assert!(combined.contains(
        "velda.duration_ms.bucket:1|c|#downstream:http/1.1,upstream:http/1.1,pipe:buffered,le:50\n"
    ));
}

#[test]
fn test_standard_statsd_format() {
    let store = MetricsStore::new();

    store.record_stream_start(PipeKind::ServerStreamH2H2);
    store.record_stream_completion(
        PipeKind::ServerStreamH2H2,
        StreamCompletion::new(503, 500, 1500, 2_000_000, 0),
    );

    let snapshot = store.snapshot();
    let mut packet_buf = Vec::new();
    let mut collected_packets = Vec::new();

    let (packets, metrics) = format_statsd_snapshot(
        &snapshot,
        "velda",
        StatsdDialect::Standard,
        &mut packet_buf,
        1432,
        &mut |pkt| {
            collected_packets.push(String::from_utf8(pkt.to_vec()).unwrap());
        },
    );

    assert!(packets > 0);
    assert!(metrics > 0);

    let combined = collected_packets.join("");
    // Check dot notation without tags
    assert!(combined.contains("velda.requests.http/2.http/2.server_stream.5xx:1|c\n"));
    assert!(combined.contains("velda.bytes.rx.http/2.http/2.server_stream:500|c\n"));
    assert!(combined.contains("velda.bytes.tx.http/2.http/2.server_stream:1500|c\n"));
    assert!(combined.contains("velda.duration_ms.http/2.http/2.server_stream.5:1|c\n"));
}

#[test]
fn test_mtu_safe_packet_chunking() {
    let store = MetricsStore::new();
    let snapshot = store.snapshot();
    let max_packet_size = 512; // Small packet size to force multiple chunks

    let mut packet_buf = Vec::new();
    let mut chunks = Vec::new();

    let (packets, _metrics) = format_statsd_snapshot(
        &snapshot,
        "edge_prod",
        StatsdDialect::DogStatsd,
        &mut packet_buf,
        max_packet_size,
        &mut |pkt| {
            assert!(
                pkt.len() <= max_packet_size,
                "Packet size {} exceeded max_packet_size {}",
                pkt.len(),
                max_packet_size
            );
            assert!(
                pkt.ends_with(b"\n"),
                "Packet must end on complete line delimiter"
            );
            chunks.push(pkt.to_vec());
        },
    );

    assert!(
        packets >= 5,
        "Expected at least 5 chunks for 512-byte MTU limit"
    );
    assert_eq!(chunks.len(), packets);
}

#[tokio::test]
async fn test_statsd_udp_push_e2e() {
    // 1. Bind mock UDP receiver socket on ephemeral port
    let receiver_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let receiver_addr: SocketAddr = receiver_socket.local_addr().unwrap();

    // 2. Setup MetricsStore with sample data
    let store = Arc::new(MetricsStore::new());
    store.record_stream_start(PipeKind::BufferedH1H1);
    store.record_stream_completion(
        PipeKind::BufferedH1H1,
        StreamCompletion::new(200, 4096, 8192, 5_000_000, 0),
    );

    // 3. Configure ETP + plugin and attach in-place
    let etp = Arc::new(
        StatsdEtp::from_entry(&EtpEntryConfig {
            id: "statsd-e2e".into(),
            kind: EtpKind::Statsd,
            pool: EtpPoolSettings {
                pool_size: 2,
                connect_timeout_ms: 2000,
                idle_timeout_ms: 60_000,
            },
            tls: None,
            settings: serde_json::json!({
                "endpoint": receiver_addr.to_string(),
                "flavor": "dogstatsd",
                "prefix": "velda_test"
            }),
        })
        .unwrap(),
    );

    let config = StatsdExporterConfig {
        etp_id: "statsd-e2e".into(),
        interval_secs: 1,
        prefix: "velda_test".into(),
        dialect: Some("dogstatsd".into()),
        max_packet_size: 1432,
    };

    let plugin = Arc::new(
        StatsdExporterPlugin::with_etp("test_statsd_exporter", config, etp, store.clone()).unwrap(),
    );

    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // 4. Run exporter on shadow background task
    let exporter_handle = tokio::spawn(async move {
        let _ = plugin.run(shutdown_rx).await;
    });

    // 5. Receive UDP datagram
    let mut recv_buf = [0u8; 2048];
    let recv_result = tokio::time::timeout(
        Duration::from_secs(3),
        receiver_socket.recv_from(&mut recv_buf),
    )
    .await;

    assert!(
        recv_result.is_ok(),
        "Timed out waiting for StatsD UDP datagram"
    );
    let (n, peer) = recv_result.unwrap().unwrap();
    assert!(n > 0);
    assert_eq!(peer.ip(), receiver_addr.ip());

    let payload = String::from_utf8_lossy(&recv_buf[..n]);
    assert!(
        payload.contains(
            "velda_test.requests.total:1|c|#downstream:h1,upstream:h1,pipe:buffered,status:2xx"
        ) || payload.contains("velda_test.connections.total:"),
        "Payload must contain valid DogStatsD lines, received: {payload}"
    );

    // 6. Signal graceful shutdown
    let _ = shutdown_tx.send(true);
    let join_res = tokio::time::timeout(Duration::from_secs(2), exporter_handle).await;
    assert!(join_res.is_ok(), "Exporter task shutdown timed out");
}

#[tokio::test]
async fn test_sendmmsg_batch_tx() {
    // 1. Mock UDP Receiver
    let receiver = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let receiver_addr = receiver.local_addr().unwrap();

    // 2. Establish ETP + borrow StatsdLease
    let etp = Arc::new(
        StatsdEtp::from_entry(&EtpEntryConfig {
            id: "statsd-batch".into(),
            kind: EtpKind::Statsd,
            pool: EtpPoolSettings {
                pool_size: 2,
                connect_timeout_ms: 2000,
                idle_timeout_ms: 60_000,
            },
            tls: None,
            settings: serde_json::json!({
                "endpoint": receiver_addr.to_string(),
            }),
        })
        .unwrap(),
    );
    let mut lease = etp.acquire().await.unwrap();

    // 3. Test empty batch
    let empty: Vec<Vec<u8>> = Vec::new();
    let sent_empty = wire::send_batch(&mut lease, &empty).await.unwrap();
    assert_eq!(sent_empty, 0);

    // 4. Test 20-packet batch (typical StatsD snapshot) via sendmmsg
    const BATCH_SIZE: usize = 20;
    let mut packets = Vec::with_capacity(BATCH_SIZE);
    for i in 0..BATCH_SIZE {
        packets.push(format!("velda.test.batch.metric_{i}:{i}|c\n").into_bytes());
    }

    let sent_count = wire::send_batch(&mut lease, &packets).await.unwrap();
    assert_eq!(sent_count, BATCH_SIZE);

    // 5. Receive all 20 packets and verify content
    let mut received = Vec::with_capacity(BATCH_SIZE);
    let mut buf = [0u8; 1024];
    for _ in 0..BATCH_SIZE {
        let (n, _) = tokio::time::timeout(Duration::from_secs(2), receiver.recv_from(&mut buf))
            .await
            .expect("Timed out waiting for batched UDP datagram")
            .unwrap();
        received.push(String::from_utf8_lossy(&buf[..n]).to_string());
    }

    assert_eq!(received.len(), BATCH_SIZE);
    for p in packets.iter() {
        let expected = String::from_utf8_lossy(p);
        assert!(
            received.contains(&expected.to_string()),
            "Missing expected packet: {expected}"
        );
    }

    // 6. Test large 80-packet batch (crossing 64-chunk boundary)
    const LARGE_BATCH: usize = 80;
    let mut large_packets = Vec::with_capacity(LARGE_BATCH);
    for i in 0..LARGE_BATCH {
        large_packets.push(format!("velda.test.large.chunk_{i}:{i}|g\n").into_bytes());
    }

    let sent_large = wire::send_batch(&mut lease, &large_packets).await.unwrap();
    assert_eq!(sent_large, LARGE_BATCH);

    for _ in 0..LARGE_BATCH {
        let (n, _) = tokio::time::timeout(Duration::from_secs(2), receiver.recv_from(&mut buf))
            .await
            .expect("Timed out waiting for large batch UDP datagram")
            .unwrap();
        assert!(n > 0);
    }
}

#[tokio::test]
async fn test_statsd_delta_no_idle_inflation() {
    let rx_sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let rx_addr = rx_sock.local_addr().unwrap();

    let store = Arc::new(MetricsStore::new());
    store.record_stream_start(PipeKind::BufferedH1H1);
    store.record_stream_completion(
        PipeKind::BufferedH1H1,
        StreamCompletion::new(200, 128, 256, 2_000_000, 0),
    );

    let etp = Arc::new(
        StatsdEtp::from_entry(&EtpEntryConfig {
            id: "statsd-delta".into(),
            kind: EtpKind::Statsd,
            pool: EtpPoolSettings {
                pool_size: 1,
                connect_timeout_ms: 2000,
                idle_timeout_ms: 60_000,
            },
            tls: None,
            settings: serde_json::json!({
                "endpoint": rx_addr.to_string(),
                "flavor": "dogstatsd",
                "prefix": "velda_delta"
            }),
        })
        .unwrap(),
    );

    let config = StatsdExporterConfig {
        etp_id: "statsd-delta".into(),
        interval_secs: 1,
        prefix: "velda_delta".into(),
        dialect: Some("dogstatsd".into()),
        max_packet_size: 4096,
    };
    let plugin =
        Arc::new(StatsdExporterPlugin::with_etp("delta_test", config, etp, store.clone()).unwrap());
    let (tx, rx) = watch::channel(false);
    let handle = tokio::spawn(async move { plugin.run_service(rx).await });

    // Wait for Tick 1 (emits immediate first tick)
    let mut tick1 = String::new();
    let mut buf = [0u8; 8192];
    let (n, _) = tokio::time::timeout(Duration::from_secs(2), rx_sock.recv_from(&mut buf))
        .await
        .unwrap()
        .unwrap();
    tick1.push_str(&String::from_utf8_lossy(&buf[..n]));
    while let Ok(Ok((extra, _))) =
        tokio::time::timeout(Duration::from_millis(120), rx_sock.recv_from(&mut buf)).await
    {
        tick1.push_str(&String::from_utf8_lossy(&buf[..extra]));
    }
    assert!(
        tick1.contains("velda_delta.requests.total:1|c|#downstream:http/1.1,upstream:http/1.1,pipe:buffered,status:2xx\n"),
        "Tick 1 must emit delta 1, got: {tick1}"
    );

    // Wait for Tick 2 (idle — zero new traffic, so counter delta MUST be 0|c)
    let (n2, _) = tokio::time::timeout(Duration::from_secs(2), rx_sock.recv_from(&mut buf))
        .await
        .unwrap()
        .unwrap();
    let mut tick2 = String::from_utf8_lossy(&buf[..n2]).to_string();
    while let Ok(Ok((extra, _))) =
        tokio::time::timeout(Duration::from_millis(120), rx_sock.recv_from(&mut buf)).await
    {
        tick2.push_str(&String::from_utf8_lossy(&buf[..extra]));
    }
    assert!(
        tick2.contains("velda_delta.requests.total:0|c|#downstream:http/1.1,upstream:http/1.1,pipe:buffered,status:2xx\n"),
        "Tick 2 at idle must emit delta 0 (not 1), got: {tick2}"
    );

    let _ = tx.send(true);
    let _ = handle.await;
}

#[tokio::test]
async fn test_statsd_etp_registry_wiring_and_prefix_flavor_fallback() {
    let rx_sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let rx_addr = rx_sock.local_addr().unwrap();

    let registry = build_registry(&[EtpEntryConfig {
        id: "statsd-wired".into(),
        kind: EtpKind::Statsd,
        pool: EtpPoolSettings {
            pool_size: 1,
            connect_timeout_ms: 2000,
            idle_timeout_ms: 60_000,
        },
        tls: None,
        settings: serde_json::json!({
            "endpoint": rx_addr.to_string(),
            "dialect": "dogstatsd",
            "prefix": "etp_ns"
        }),
    }])
    .unwrap();

    // Omit prefix and dialect in plugin config to verify fallback derivation from StatsdEtpConfig.
    let plugin_cfg = PluginConfig {
        id: "statsd_exporter".into(),
        config: serde_json::json!({
            "etp_id": "statsd-wired",
            "interval_secs": 1,
            "max_packet_size": 1432
        }),
    };
    let loaded = instantiate_plugin(&plugin_cfg).unwrap();
    let mut available = FxHashMap::default();
    available.insert(loaded.id().to_string(), loaded.clone());
    wire_plugin_etps(&available, &registry).unwrap();

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let runner = tokio::spawn(loaded.run_background(shutdown_rx).unwrap());

    let mut recv_buf = [0u8; 4096];
    let mut collected = String::new();
    let (n, _) = tokio::time::timeout(Duration::from_secs(3), rx_sock.recv_from(&mut recv_buf))
        .await
        .unwrap()
        .unwrap();
    collected.push_str(&String::from_utf8_lossy(&recv_buf[..n]));
    while let Ok(Ok((extra, _))) =
        tokio::time::timeout(Duration::from_millis(120), rx_sock.recv_from(&mut recv_buf)).await
    {
        collected.push_str(&String::from_utf8_lossy(&recv_buf[..extra]));
    }
    assert!(
        collected.contains("etp_ns.connections.total:")
            && collected.contains("etp_ns.requests.total:")
            && collected.contains("|#downstream:"),
        "Expected ETP-derived prefix 'etp_ns' and dogstatsd tags, got: {collected}"
    );

    let _ = shutdown_tx.send(true);
    runner.await.unwrap().unwrap();

    // Missing ETP produces an explicit error
    let err_plugin = instantiate_plugin(&plugin_cfg).unwrap();
    let mut missing_map = FxHashMap::default();
    missing_map.insert(err_plugin.id().to_string(), err_plugin);
    assert!(wire_plugin_etps(&missing_map, &velda_etp::EtpRegistry::default()).is_err());

    // Wrong-kind ETP produces an explicit error
    let wrong_registry = build_registry(&[EtpEntryConfig {
        id: "statsd-wired".into(),
        kind: EtpKind::S3,
        pool: EtpPoolSettings::default(),
        tls: None,
        settings: serde_json::json!({ "endpoint": "http://127.0.0.1:9000" }),
    }])
    .unwrap();
    let wrong_plugin = instantiate_plugin(&plugin_cfg).unwrap();
    let mut wrong_map = FxHashMap::default();
    wrong_map.insert(wrong_plugin.id().to_string(), wrong_plugin);
    assert!(wire_plugin_etps(&wrong_map, &wrong_registry).is_err());
}
