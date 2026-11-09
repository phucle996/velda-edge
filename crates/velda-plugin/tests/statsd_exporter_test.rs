//! Integration & End-to-End Test Suite for StatsD / DogStatsD Metrics Exporter.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::sync::watch;

use velda_observability::{
    DownstreamProto, MetricsStore, PipeStrategy, StreamCompletion, UpstreamProto,
};
use velda_plugin::background::BackgroundPlugin;
use velda_plugin::background::statsd_exporter::formatter::{
    StatsdDialect, fast_write_i64, fast_write_u64, format_statsd_snapshot,
};
use velda_plugin::background::statsd_exporter::{StatsdExporterConfig, StatsdExporterPlugin};

#[test]
fn test_fast_integer_ascii_serialization() {
    let mut buf = Vec::new();

    fast_write_u64(&mut buf, 0);
    assert_eq!(&buf, b"0");

    buf.clear();
    fast_write_u64(&mut buf, 9);
    assert_eq!(&buf, b"9");

    buf.clear();
    fast_write_u64(&mut buf, 42);
    assert_eq!(&buf, b"42");

    buf.clear();
    fast_write_u64(&mut buf, 1234567890);
    assert_eq!(&buf, b"1234567890");

    buf.clear();
    fast_write_u64(&mut buf, u64::MAX);
    assert_eq!(&buf, format!("{}", u64::MAX).as_bytes());

    buf.clear();
    fast_write_i64(&mut buf, -99);
    assert_eq!(&buf, b"-99");

    buf.clear();
    fast_write_i64(&mut buf, 0);
    assert_eq!(&buf, b"0");

    buf.clear();
    fast_write_i64(&mut buf, 100);
    assert_eq!(&buf, b"100");
}

#[test]
fn test_dogstatsd_formatting_and_tags() {
    let store = MetricsStore::new();

    // Record sample completions
    store.record_stream_start(
        DownstreamProto::H1,
        UpstreamProto::H1,
        PipeStrategy::Buffered,
    );
    store.record_stream_completion(
        DownstreamProto::H1,
        UpstreamProto::H1,
        PipeStrategy::Buffered,
        StreamCompletion::new(200, 1024, 2048, Duration::from_millis(15), None),
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
        |pkt| {
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
        "velda.requests.total:1|c|#downstream:h1,upstream:h1,pipe:buffered,status:2xx\n"
    ));
    assert!(combined.contains("velda.bytes.rx:1024|c|#downstream:h1,upstream:h1,pipe:buffered\n"));
    assert!(combined.contains("velda.bytes.tx:2048|c|#downstream:h1,upstream:h1,pipe:buffered\n"));
    assert!(
        combined.contains(
            "velda.duration_ms.bucket:1|c|#downstream:h1,upstream:h1,pipe:buffered,le:50\n"
        )
    );
}

#[test]
fn test_standard_statsd_formatting_dot_notation() {
    let store = MetricsStore::new();

    store.record_stream_start(
        DownstreamProto::H2,
        UpstreamProto::H2,
        PipeStrategy::ServerStream,
    );
    store.record_stream_completion(
        DownstreamProto::H2,
        UpstreamProto::H2,
        PipeStrategy::ServerStream,
        StreamCompletion::new(503, 500, 1500, Duration::from_millis(2), None),
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
        |pkt| {
            collected_packets.push(String::from_utf8(pkt.to_vec()).unwrap());
        },
    );

    assert!(packets > 0);
    assert!(metrics > 0);

    let combined = collected_packets.join("");
    // Check dot notation without tags
    assert!(combined.contains("velda.requests.h2.h2.server_stream.5xx:1|c\n"));
    assert!(combined.contains("velda.bytes.rx.h2.h2.server_stream:500|c\n"));
    assert!(combined.contains("velda.bytes.tx.h2.h2.server_stream:1500|c\n"));
    assert!(combined.contains("velda.duration_ms.h2.h2.server_stream.5:1|c\n"));
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
        |pkt| {
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
async fn test_end_to_end_statsd_exporter_push_to_udp_socket() {
    // 1. Bind mock UDP receiver socket on ephemeral port
    let receiver_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let receiver_addr: SocketAddr = receiver_socket.local_addr().unwrap();

    // 2. Setup MetricsStore with sample data
    let store = Arc::new(MetricsStore::new());
    store.record_stream_start(
        DownstreamProto::H1,
        UpstreamProto::H1,
        PipeStrategy::Buffered,
    );
    store.record_stream_completion(
        DownstreamProto::H1,
        UpstreamProto::H1,
        PipeStrategy::Buffered,
        StreamCompletion::new(200, 4096, 8192, Duration::from_millis(5), None),
    );

    // 3. Configure and instantiate plugin
    let config = StatsdExporterConfig {
        endpoint: receiver_addr.to_string(),
        interval_secs: 1, // Push every 1 second
        prefix: "velda_test".into(),
        dialect: "dogstatsd".into(),
        max_packet_size: 1432,
    };

    let plugin = Arc::new(StatsdExporterPlugin::new(
        "test_statsd_exporter",
        config,
        store.clone(),
    ));

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
async fn test_sendmmsg_vectorized_batch_transmission() {
    use velda_plugin::background::statsd_exporter::transport::StatsdTransport;

    // 1. Mock UDP Receiver
    let receiver = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let receiver_addr = receiver.local_addr().unwrap();

    // 2. Establish Transport
    let transport = StatsdTransport::connect(&receiver_addr.to_string(), "test_batch_transport")
        .await
        .unwrap();

    // 3. Test empty batch
    let empty: Vec<Vec<u8>> = Vec::new();
    let sent_empty = transport.send_batch(&empty).await.unwrap();
    assert_eq!(sent_empty, 0);

    // 4. Test 20-packet batch (typical StatsD snapshot) via sendmmsg
    const BATCH_SIZE: usize = 20;
    let mut packets = Vec::with_capacity(BATCH_SIZE);
    for i in 0..BATCH_SIZE {
        packets.push(format!("velda.test.batch.metric_{i}:{i}|c\n").into_bytes());
    }

    let sent_count = transport.send_batch(&packets).await.unwrap();
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

    let sent_large = transport.send_batch(&large_packets).await.unwrap();
    assert_eq!(sent_large, LARGE_BATCH);

    for _ in 0..LARGE_BATCH {
        let (n, _) = tokio::time::timeout(Duration::from_secs(2), receiver.recv_from(&mut buf))
            .await
            .expect("Timed out waiting for large batch UDP datagram")
            .unwrap();
        assert!(n > 0);
    }
}
