use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use velda_rdma::channel::flow_control::CreditFlowControl;
use velda_rdma::{MockChannel, RdmaChannel, RdmaConfig, RdmaError, RdmaListener, RdmaStream};

#[tokio::test]
async fn test_rdma_stream_burst_traffic() {
    let (ch1, ch2) = MockChannel::pair(64);
    let unified_ch1 = RdmaChannel::from_mock(ch1);
    let unified_ch2 = RdmaChannel::from_mock(ch2);

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

    // Peer grants 10 credits back (capped at max 64)
    fc.add_remote_credits(10, 64);
    assert_eq!(fc.available_send_credits(), 10);
    assert!(fc.try_acquire_send_credit().is_ok());
}

#[tokio::test]
async fn test_mock_channel_timeout() {
    let (_ch1, ch2) = MockChannel::pair(8);
    // Nothing was sent, receiving with short timeout should return RdmaError::Timeout
    let res = ch2.receiver.recv(Duration::from_millis(50)).await;
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

    let (ch1, ch2) = MockChannel::pair(16);
    let unified_ch1 = RdmaChannel::from_mock(ch1);

    // 1. Lease a Standard2K slot
    let mut lease = arena
        .acquire(RdmaSlabClass::Standard2K)
        .expect("Standard2K slot lease");
    let test_payload = b"GET /v1/telemetry HTTP/1.1\r\nHost: edge.velda.internal\r\n\r\n";
    lease[..test_payload.len()].copy_from_slice(test_payload);

    // 2. Send directly from lease without intermediate copy
    unified_ch1
        .sender
        .send_lease(&lease, test_payload.len())
        .await
        .expect("Send lease should succeed");

    // 3. Verify received message on peer channel
    let msg = ch2
        .receiver
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

    let (ch1, ch2) = MockChannel::pair(16);
    let unified_ch1 = RdmaChannel::from_mock(ch1);

    // 1. Lease a Large64K slot (contiguous span for GRO/TCP)
    let mut large_lease = arena
        .acquire(RdmaSlabClass::Large64K)
        .expect("Large64K slot lease");

    // Fill large payload (e.g. 32KB coalesced GRO datagram)
    let payload_len = 32768;
    for (i, byte) in large_lease[..payload_len].iter_mut().enumerate() {
        *byte = (i % 251) as u8;
    }

    // 2. Dispatch via send_lease (simulating single-SGE burst post)
    unified_ch1
        .sender
        .send_lease(&large_lease, payload_len)
        .await
        .expect("Send large lease should succeed");

    // 3. Receive on peer
    let msg = ch2
        .receiver
        .recv(Duration::from_millis(500))
        .await
        .expect("Recv large payload should succeed");
    assert_eq!(msg.payload.len(), payload_len);
    assert_eq!(&msg.payload[..], &large_lease[..payload_len]);
}

#[tokio::test]
async fn test_rdma_stream_chunking_large_payload() {
    let (ch1, ch2) = MockChannel::pair(64);
    let unified_ch1 = RdmaChannel::from_mock(ch1);
    let unified_ch2 = RdmaChannel::from_mock(ch2);

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

#[tokio::test]
async fn test_decoupled_send_recv_split() {
    let (ch1, ch2) = MockChannel::pair(32);
    let unified_ch1 = RdmaChannel::from_mock(ch1);
    let unified_ch2 = RdmaChannel::from_mock(ch2);

    let (s1, r1) = unified_ch1.split();
    let (s2, r2) = unified_ch2.split();

    // Spawn 2 concurrent tasks:
    // Task A: s1 sends ping to r2, while r1 waits for pong from s2
    // Task B: r2 waits for ping from s1, then s2 sends pong back to r1
    let task_a = tokio::spawn(async move {
        s1.send(b"PING-FROM-A").await.unwrap();
        let pong = r1.recv(Duration::from_secs(2)).await.unwrap();
        assert_eq!(&pong[..], b"PONG-FROM-B");
    });

    let task_b = tokio::spawn(async move {
        let ping = r2.recv(Duration::from_secs(2)).await.unwrap();
        assert_eq!(&ping[..], b"PING-FROM-A");
        s2.send(b"PONG-FROM-B").await.unwrap();
    });

    let (res_a, res_b) = tokio::join!(task_a, task_b);
    res_a.unwrap();
    res_b.unwrap();
}

#[test]
fn test_rdma_config_zero_wr_rejected() {
    let cfg = RdmaConfig {
        max_send_wr: 0,
        ..Default::default()
    };
    assert_eq!(cfg.max_send_wr, 0);
}

#[tokio::test]
async fn test_rdma_listener_bind_local_addr() {
    let listener = RdmaListener::bind("127.0.0.1:0".parse().unwrap(), RdmaConfig::mock())
        .await
        .expect("Listener bind");
    let addr = listener.local_addr().expect("Local addr");
    assert_eq!(addr.ip(), std::net::Ipv4Addr::LOCALHOST);
    assert!(addr.port() > 0);
}

#[tokio::test]
async fn test_mock_channel_send_recv() {
    use bytes::Bytes;
    let (ch1, ch2) = MockChannel::pair(16);

    ch1.sender
        .send(Bytes::from_static(b"hello-rdma"), Some(42))
        .await
        .expect("Send should succeed");

    let msg = ch2
        .receiver
        .recv(Duration::from_millis(500))
        .await
        .expect("Recv should succeed");

    assert_eq!(&msg.payload[..], b"hello-rdma");
    assert_eq!(msg.imm_data, Some(42));
}

#[tokio::test]
async fn test_mock_channel_rdma_write() {
    use bytes::Bytes;
    let (ch1, ch2) = MockChannel::pair(16);

    ch1.sender
        .rdma_write(0x1000, 0xABC, Bytes::from_static(b"tensor-payload"), None)
        .await
        .expect("RDMA Write should succeed");

    let msg = ch2
        .receiver
        .recv(Duration::from_millis(500))
        .await
        .expect("Recv should succeed");

    assert_eq!(&msg.payload[..], b"tensor-payload");
    assert_eq!(msg.remote_addr, Some(0x1000));
    assert_eq!(msg.rkey, Some(0xABC));
}

#[tokio::test]
async fn test_rdma_stream_async_io() {
    let (ch1, ch2) = MockChannel::pair(16);
    let unified_ch1 = RdmaChannel::from_mock(ch1);
    let unified_ch2 = RdmaChannel::from_mock(ch2);

    let mut stream1 = RdmaStream::new(unified_ch1);
    let mut stream2 = RdmaStream::new(unified_ch2);

    stream1
        .write_all(b"PING-PONG-OVER-RDMA")
        .await
        .expect("Write should succeed");
    stream1.flush().await.expect("Flush should succeed");

    let mut read_buf = vec![0u8; 19];
    stream2
        .read_exact(&mut read_buf)
        .await
        .expect("Read should succeed");

    assert_eq!(&read_buf, b"PING-PONG-OVER-RDMA");
}

#[test]
fn test_wr_builder() {
    use velda_rdma::channel::wr::SendWrBuilder;
    use velda_rdma::ffi::ibv_sge;
    let sge = [ibv_sge {
        addr: 0x4000,
        length: 128,
        lkey: 99,
    }];

    let mut builder = SendWrBuilder::send(1, &sge, 0, true);
    let raw = builder.as_raw_mut();
    unsafe {
        assert_eq!((*raw).wr_id, 1);
        assert_eq!((*raw).num_sge, 1);
        assert_eq!((*raw).opcode, velda_rdma::ffi::IBV_WR_SEND);
    }

    let mut builder_imm = SendWrBuilder::send(2, &sge, 42, true);
    let raw_imm = builder_imm.as_raw_mut();
    unsafe {
        assert_eq!((*raw_imm).wr_id, 2);
        assert_eq!((*raw_imm).opcode, velda_rdma::ffi::IBV_WR_SEND_WITH_IMM);
        assert_eq!((*raw_imm).imm_data, 42);
    }
}

#[test]
fn test_ffi_dynamic_loading() {
    let lib_opt = velda_rdma::ffi::get_ibv_lib();
    if let Some(lib) = lib_opt {
        let mut num_devices = 0;
        let list = unsafe { (lib.get_device_list)(&mut num_devices) };
        if !list.is_null() {
            unsafe { (lib.free_device_list)(list) };
        }
    }
}

#[test]
fn test_for_streaming_pipeline_sizing() {
    use velda_core::StreamingMode;
    use velda_rdma::{
        BASELINE_RDMA_CREDITS, DEFAULT_RDMA_CREDITS, HIGH_THROUGHPUT_RDMA_CREDITS,
        PEAK_RDMA_CREDITS, RdmaConfig,
    };

    // 1. Buffered Mode: 512 KB body with 64KB slot -> 8 slots
    let cfg_buffered = RdmaConfig::for_streaming(StreamingMode::DISABLED, Some(512 * 1024));
    assert_eq!(cfg_buffered.initial_credits, 8);
    assert_eq!(cfg_buffered.max_send_wr, 8);
    assert_eq!(cfg_buffered.max_recv_wr, 8);
    assert_eq!(cfg_buffered.cq_size, 16);

    // 2. Buffered Mode with small 64 KB body -> clamped to minimum 8 slots
    let cfg_small = RdmaConfig::for_streaming(StreamingMode::DISABLED, Some(64 * 1024));
    assert_eq!(cfg_small.initial_credits, 8);

    // 3. Client Streaming Mode: 2 MB budget -> 32 slots
    let cfg_client = RdmaConfig::for_streaming(StreamingMode::CLIENT, Some(2 * 1024 * 1024));
    assert_eq!(cfg_client.initial_credits, 128); // clamped to BASELINE_RDMA_CREDITS (128)

    // 4. Server Streaming / Duplex Mode: defaults to standard datacenter tier (256)
    let cfg_server = RdmaConfig::for_streaming(StreamingMode::SERVER, None);
    assert_eq!(cfg_server.initial_credits, DEFAULT_RDMA_CREDITS);

    let cfg_duplex = RdmaConfig::for_streaming(StreamingMode::new(true, true), None);
    assert_eq!(cfg_duplex.initial_credits, DEFAULT_RDMA_CREDITS);

    // 5. Explicit tier presets
    let cfg_baseline = RdmaConfig::baseline();
    assert_eq!(cfg_baseline.initial_credits, BASELINE_RDMA_CREDITS);

    let cfg_high = RdmaConfig::high_throughput();
    assert_eq!(cfg_high.initial_credits, HIGH_THROUGHPUT_RDMA_CREDITS);

    let cfg_peak = RdmaConfig::peak();
    assert_eq!(cfg_peak.initial_credits, PEAK_RDMA_CREDITS);
}

#[tokio::test]
async fn test_async_flow_control_waiter_safety_net() {
    use std::sync::Arc;
    use std::time::Duration;
    use velda_rdma::channel::flow_control::CreditFlowControl;

    // Start with exactly 1 credit
    let fc = Arc::new(CreditFlowControl::new(1));

    // First acquire succeeds instantly (< 5ns)
    assert!(
        fc.acquire_send_credit(Duration::from_millis(100))
            .await
            .is_ok()
    );
    assert_eq!(fc.available_send_credits(), 0);

    // Second acquire runs concurrently and suspends cooperatively without dropping connection
    let fc_clone = Arc::clone(&fc);
    let wait_handle = tokio::spawn(async move {
        fc_clone
            .acquire_send_credit(Duration::from_millis(500))
            .await
    });

    // Verify task is cooperatively waiting
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!wait_handle.is_finished());

    // Peer grants 5 new credits via imm_data
    fc.add_remote_credits(5, 256);

    // Waiting task wakes up instantly and acquires successfully
    let res = wait_handle.await.expect("JoinHandle");
    assert!(res.is_ok());
    // 5 added, 1 consumed by woken task -> 4 remaining
    assert_eq!(fc.available_send_credits(), 4);
}

#[tokio::test]
async fn test_mock_sender_concurrent_cas_no_underflow() {
    use bytes::Bytes;
    use std::sync::Arc;
    use velda_rdma::MockChannel;

    let (ch1, _ch2) = MockChannel::pair(10);
    let sender = Arc::new(ch1.sender);

    // Spawn 20 tasks racing to acquire only 10 initial credits
    let mut handles = Vec::new();
    for _ in 0..20 {
        let s = Arc::clone(&sender);
        handles.push(tokio::spawn(async move {
            s.send(Bytes::from_static(b"burst"), None).await
        }));
    }

    let mut succeeded = 0;
    let mut rejected = 0;
    for h in handles {
        match h.await.unwrap() {
            Ok(()) => succeeded += 1,
            Err(_) => rejected += 1,
        }
    }

    // Exactly 10 must succeed, exactly 10 must be rejected
    assert_eq!(succeeded, 10);
    assert_eq!(rejected, 10);
    // Credits must be 0, never underflowing to u32::MAX
    assert_eq!(
        sender
            .send_credits
            .load(std::sync::atomic::Ordering::Acquire),
        0
    );
}

#[tokio::test]
async fn test_rdma_stream_clean_eof_shutdown() {
    let (ch1, ch2) = MockChannel::pair(64);
    let mut stream1 = RdmaStream::new(RdmaChannel::from_mock(ch1));
    let mut stream2 = RdmaStream::new(RdmaChannel::from_mock(ch2));

    // Write some data first
    stream1.write_all(b"Hello EOF").await.unwrap();
    stream1.flush().await.unwrap();

    let mut buf = vec![0u8; 9];
    stream2.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"Hello EOF");

    // Cleanly shut down stream1 (transmits RDMA_FLAG_EOF)
    stream1.shutdown().await.unwrap();

    // Stream2 must read EOF (0 bytes) immediately without hanging
    let mut eof_buf = [0u8; 10];
    let n = tokio::time::timeout(Duration::from_millis(500), stream2.read(&mut eof_buf))
        .await
        .expect("Read should complete immediately on EOF without timeout")
        .expect("Read should succeed");
    assert_eq!(n, 0, "Clean shutdown must yield 0 bytes (EOF)");
}
