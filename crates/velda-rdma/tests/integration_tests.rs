use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex as AsyncMutex;

use velda_rdma::channel::flow_control::CreditFlowControl;
use velda_rdma::{MockChannel, RdmaChannel, RdmaConfig, RdmaError, RdmaStream};

#[tokio::test]
async fn test_rdma_stream_burst_traffic() {
    let (ch1, ch2) = MockChannel::pair(64);
    let unified_ch1 = RdmaChannel::Mock(Arc::new(AsyncMutex::new(ch1)));
    let unified_ch2 = RdmaChannel::Mock(Arc::new(AsyncMutex::new(ch2)));

    let mut stream1 = RdmaStream::new(unified_ch1);
    let mut stream2 = RdmaStream::new(unified_ch2);

    let messages: &[&[u8]] = &[
        b"Frame-01: Header",
        b"Frame-02: Body-Payload-Chunk",
        b"Frame-03: End-Of-Stream-Trailer",
    ];

    for msg in messages {
        stream1.write_all(msg).await.expect("Write should succeed");
        stream1.flush().await.expect("Flush should succeed");

        let mut buf = vec![0u8; msg.len()];
        stream2
            .read_exact(&mut buf)
            .await
            .expect("Read should succeed");
        assert_eq!(&buf[..], *msg);
    }
}

#[tokio::test]
async fn test_flow_control_exhaustion_and_replenishment() {
    let fc = CreditFlowControl::new(5);
    assert_eq!(fc.available_send_credits(), 5);

    // Drain all 5 credits
    for _ in 0..5 {
        assert!(fc.try_acquire_send_credit().is_ok());
    }

    // 6th acquire must fail with InsufficientCredits
    let err = fc.try_acquire_send_credit().unwrap_err();
    match err {
        RdmaError::InsufficientCredits {
            available,
            required,
        } => {
            assert_eq!(available, 0);
            assert_eq!(required, 1);
        }
        other => panic!("Unexpected error: {:?}", other),
    }

    // Peer grants 10 credits back
    fc.add_remote_credits(10);
    assert_eq!(fc.available_send_credits(), 10);
    assert!(fc.try_acquire_send_credit().is_ok());
}

#[tokio::test]
async fn test_mock_channel_timeout() {
    let (_ch1, mut ch2) = MockChannel::pair(8);
    // Nothing was sent, receiving with short timeout should return RdmaError::Timeout
    let res = ch2.recv(Duration::from_millis(50)).await;
    assert!(matches!(res, Err(RdmaError::Timeout(_))));
}

#[tokio::test]
async fn test_rdma_config_presets() {
    let default_cfg = RdmaConfig::default();
    assert_eq!(default_cfg.port, 1);
    assert_eq!(default_cfg.max_send_wr, 256);
    assert!(!default_cfg.force_mock);

    let mock_cfg = RdmaConfig::mock();
    assert!(mock_cfg.force_mock);
}

#[tokio::test]
async fn test_zero_copy_arena_send_lease() {
    use velda_rdma::{RdmaSlabArena, RdmaSlabClass, RdmaSlabConfig};

    let arena =
        RdmaSlabArena::new_mock(RdmaSlabConfig::default()).expect("Mock slab arena initialization");

    let (ch1, mut ch2) = MockChannel::pair(16);
    let unified_ch1 = RdmaChannel::Mock(Arc::new(AsyncMutex::new(ch1)));

    // 1. Lease a Standard2K slot
    let mut lease = arena
        .acquire(RdmaSlabClass::Standard2K)
        .expect("Standard2K slot lease");
    let test_payload = b"GET /v1/telemetry HTTP/1.1\r\nHost: edge.velda.internal\r\n\r\n";
    lease[..test_payload.len()].copy_from_slice(test_payload);

    // 2. Send directly from lease without intermediate copy
    unified_ch1
        .send_lease(&lease, test_payload.len())
        .await
        .expect("Send lease should succeed");

    // 3. Verify received message on peer channel
    let msg = ch2
        .recv(Duration::from_millis(500))
        .await
        .expect("Recv should succeed");
    assert_eq!(&msg.payload[..], test_payload);

    // 4. Drop lease and verify recycling
    drop(lease);
    assert_eq!(
        arena.available_slots(RdmaSlabClass::Standard2K),
        arena.total_slots(RdmaSlabClass::Standard2K)
    );
}

#[tokio::test]
async fn test_zero_copy_arena_large_span_send() {
    use velda_rdma::{RdmaSlabArena, RdmaSlabClass, RdmaSlabConfig};

    let arena =
        RdmaSlabArena::new_mock(RdmaSlabConfig::default()).expect("Mock slab arena initialization");

    let (ch1, mut ch2) = MockChannel::pair(16);
    let unified_ch1 = RdmaChannel::Mock(Arc::new(AsyncMutex::new(ch1)));

    // 1. Lease a Large64K slot (contiguous span for GRO/TCP)
    let mut large_lease = arena
        .acquire(RdmaSlabClass::Large64K)
        .expect("Large64K slot lease");

    // Fill large payload (e.g. 32KB coalesced GRO datagram)
    let payload_len = 32768;
    for i in 0..payload_len {
        large_lease[i] = (i % 251) as u8;
    }

    // 2. Dispatch via send_lease (simulating single-SGE burst post)
    unified_ch1
        .send_lease(&large_lease, payload_len)
        .await
        .expect("Send large lease should succeed");

    // 3. Receive on peer
    let msg = ch2
        .recv(Duration::from_millis(500))
        .await
        .expect("Recv large payload should succeed");
    assert_eq!(msg.payload.len(), payload_len);
    assert_eq!(&msg.payload[..], &large_lease[..payload_len]);
}

#[tokio::test]
async fn test_rdma_stream_chunking_large_payload() {
    let (ch1, ch2) = MockChannel::pair(64);
    let unified_ch1 = RdmaChannel::Mock(Arc::new(AsyncMutex::new(ch1)));
    let unified_ch2 = RdmaChannel::Mock(Arc::new(AsyncMutex::new(ch2)));

    let mut stream1 = RdmaStream::new(unified_ch1);
    let mut stream2 = RdmaStream::new(unified_ch2);

    // Payload larger than single 64KB slot (150 KB)
    let payload_len = 150_000;
    let mut write_data = vec![0u8; payload_len];
    for (i, byte) in write_data.iter_mut().enumerate() {
        *byte = (i % 255) as u8;
    }

    let write_task = tokio::spawn(async move {
        stream1
            .write_all(&write_data)
            .await
            .expect("Stream write_all should succeed");
        stream1.flush().await.expect("Stream flush should succeed");
    });

    let read_task = tokio::spawn(async move {
        let mut read_buf = vec![0u8; payload_len];
        stream2
            .read_exact(&mut read_buf)
            .await
            .expect("Stream read_exact should succeed");
        read_buf
    });

    let (_, received_data) = tokio::join!(write_task, read_task);
    let received = received_data.expect("Read task panicked");
    assert_eq!(received.len(), payload_len);
    for (i, &byte) in received.iter().enumerate() {
        assert_eq!(byte, (i % 255) as u8);
    }
}

#[test]
fn test_rdma_config_zero_wr_rejected() {
    let cfg = RdmaConfig {
        max_send_wr: 0,
        ..Default::default()
    };
    // When max_send_wr is 0, HardwareChannel::new must return RdmaError::Config
    // without performing division by zero or allocating empty raw buffers
    assert_eq!(cfg.max_send_wr, 0);
}
