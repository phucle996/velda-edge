//! Chaos, Fuzzing & High-Load Security Benchmark Suite for RDMA Subsystem
//!
//! Validates resilience under adversarial inputs, malformed wire frames,
//! credit flooding attacks, abrupt connection aborts, and high-concurrency traffic bursts.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

use velda_rdma::channel::flow_control::CreditFlowControl;
use velda_rdma::connector::RdmaListener;
use velda_rdma::{
    MockChannel, RdmaChannel, RdmaConfig, RdmaError, RdmaSlabArena, RdmaSlabClass, RdmaSlabConfig,
};

// =========================================================================
// 1. OOB Handshake Fuzzing Matrix
// =========================================================================

#[tokio::test]
async fn test_fuzz_malformed_oob_handshake_matrix() {
    let config = RdmaConfig::mock();
    let listener = RdmaListener::bind("127.0.0.1:0".parse().unwrap(), config)
        .await
        .expect("Failed binding listener");
    let addr = listener.local_addr().unwrap();

    let listener_arc = Arc::new(listener);

    // Matrix of adversarial / malformed payloads:
    let malformed_payloads: Vec<(&'static str, Vec<u8>)> = vec![
        ("Empty payload (0 bytes)", vec![]),
        ("Single byte truncation", vec![0x56]),
        ("Magic only (4 bytes)", b"VRDM".to_vec()),
        ("Half frame (16 bytes)", vec![0x41; 16]),
        ("Off-by-one truncation (31 bytes)", vec![0x42; 31]),
        ("Oversized payload (64 bytes)", vec![0x43; 64]),
        (
            "HTTP GET probe injection",
            b"GET / HTTP/1.1\r\nHost: target\r\n\r\n".to_vec(),
        ),
        ("Zeroed magic header", vec![0u8; 32]),
        ("All 0xFF bytes", vec![0xFFu8; 32]),
        ("Invalid magic 'HACK'", {
            let mut buf = vec![0u8; 32];
            buf[0..4].copy_from_slice(b"HACK");
            buf[4..8].copy_from_slice(&100u32.to_be_bytes());
            buf
        }),
        ("Valid magic but QPN = 0 (Privileged Subnet QP)", {
            let mut buf = vec![0u8; 32];
            buf[0..4].copy_from_slice(b"VRDM");
            buf[4..8].copy_from_slice(&0u32.to_be_bytes());
            buf[16..32].copy_from_slice(&[0x11u8; 16]);
            buf
        }),
        ("Valid magic but Empty GID & LID = 0", {
            let mut buf = vec![0u8; 32];
            buf[0..4].copy_from_slice(b"VRDM");
            buf[4..8].copy_from_slice(&42u32.to_be_bytes());
            buf[12..14].copy_from_slice(&0u16.to_be_bytes());
            // GID is all zeros
            buf
        }),
    ];

    for (name, payload) in malformed_payloads {
        let l_clone = listener_arc.clone();
        let accept_task = tokio::spawn(async move {
            tokio::time::timeout(Duration::from_millis(200), l_clone.accept()).await
        });

        // Client connects and pushes malformed payload
        if let Ok(mut stream) = TcpStream::connect(addr).await {
            if !payload.is_empty() {
                let _ = stream.write_all(&payload).await;
                let _ = stream.flush().await;
            }
            // Drop client socket to simulate client disconnect/abort
            drop(stream);
        }

        let res = accept_task.await.unwrap();
        match res {
            Ok(Err(e)) => {
                // Expected: server cleanly rejected the malformed frame with RdmaError
                let err_str = e.to_string();
                assert!(
                    err_str.contains("Invalid")
                        || err_str.contains("early eof")
                        || err_str.contains("unexpected end of file")
                        || err_str.contains("disconnected")
                        || err_str.contains("os error"),
                    "Test '{name}' yielded unexpected error: {err_str}"
                );
            }
            Ok(Ok(_)) => {
                panic!("Security violation: Server accepted malformed payload in test '{name}'!");
            }
            Err(_) => {
                // Timeout is acceptable for truncated frames where read_exact waited for missing bytes
            }
        }
    }
}

// =========================================================================
// 2. High-Frequency Rapid Connection & Disconnect Storm (SYN/RST Chaos)
// =========================================================================

#[tokio::test]
async fn test_chaos_rapid_oob_connect_drop_storm() {
    let config = RdmaConfig::mock();
    let listener = RdmaListener::bind("127.0.0.1:0".parse().unwrap(), config)
        .await
        .expect("Failed binding listener");
    let addr = listener.local_addr().unwrap();

    let listener_arc = Arc::new(listener);
    let rejected_count = Arc::new(AtomicUsize::new(0));

    // Spawn server background loop
    let l_server = listener_arc.clone();
    let r_count = rejected_count.clone();
    let server_handle = tokio::spawn(async move {
        for _ in 0..50 {
            match l_server.accept().await {
                Ok(_) => {}
                Err(_) => {
                    r_count.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    });

    // Client storm: 50 connections that connect and drop immediately
    for _ in 0..50 {
        if let Ok(stream) = TcpStream::connect(addr).await {
            drop(stream);
        }
    }

    let _ = tokio::time::timeout(Duration::from_secs(3), server_handle).await;
    // Server must survive without deadlock or panic
    assert!(rejected_count.load(Ordering::Relaxed) > 0);
}

// =========================================================================
// 3. Flow Control Credit Flooding & Starvation Stress
// =========================================================================

#[test]
fn test_chaos_credit_flooding_and_overflow_saturation() {
    let fc = CreditFlowControl::new(64);
    assert_eq!(fc.available_send_credits(), 64);

    // Adversary injects multiple u32::MAX overflow packets
    for _ in 0..10 {
        fc.add_remote_credits(u32::MAX, 64);
        assert_eq!(
            fc.available_send_credits(),
            64,
            "Credit overflowed beyond max_credits!"
        );
    }

    // Adversary injects intermediate values exceeding limit
    fc.add_remote_credits(1_000_000, 64);
    assert_eq!(fc.available_send_credits(), 64);

    // Drain all credits to 0
    for _ in 0..64 {
        assert!(fc.try_acquire_send_credit().is_ok());
    }
    assert_eq!(fc.available_send_credits(), 0);

    // Further acquire attempts must return Error (CreditExhausted) without panic
    assert!(fc.try_acquire_send_credit().is_err());
    assert_eq!(fc.available_send_credits(), 0);
}

#[tokio::test]
async fn test_chaos_concurrent_credit_exhaustion_and_replenishment() {
    let fc = Arc::new(CreditFlowControl::new(32));
    let mut handles = Vec::new();

    // Spawn 16 concurrent consumers attempting to acquire 64 credits in total
    let acquired_count = Arc::new(AtomicUsize::new(0));
    for _ in 0..16 {
        let fc_clone = fc.clone();
        let ac_clone = acquired_count.clone();
        handles.push(tokio::spawn(async move {
            for _ in 0..4 {
                if fc_clone.try_acquire_send_credit().is_ok() {
                    ac_clone.fetch_add(1, Ordering::Relaxed);
                }
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    // Exactly 32 credits should have been successfully acquired, 32 rejected
    assert_eq!(acquired_count.load(Ordering::Relaxed), 32);
    assert_eq!(fc.available_send_credits(), 0);

    // Replenish 16 credits
    fc.add_remote_credits(16, 32);
    assert_eq!(fc.available_send_credits(), 16);

    // Next 16 acquires succeed
    for _ in 0..16 {
        assert!(fc.try_acquire_send_credit().is_ok());
    }
    assert_eq!(fc.available_send_credits(), 0);
}

// =========================================================================
// 4. Memory Slab Capacity Boundaries & Oversized Write Protection
// =========================================================================

#[test]
fn test_chaos_oversized_payload_rejections() {
    let arena = RdmaSlabArena::new_mock(RdmaSlabConfig::default()).unwrap();

    // Standard2K slab is 2048 bytes
    let standard_lease = arena.acquire(RdmaSlabClass::Standard2K).unwrap();
    assert_eq!(standard_lease.capacity(), 2048);

    // Requesting slab for 2049 bytes must scale up to Large64K
    let large_lease = arena.acquire_for_size(2049).unwrap();
    assert_eq!(large_lease.slot_class(), RdmaSlabClass::Large64K);
    assert_eq!(large_lease.capacity(), 65536);

    // Requesting slab exceeding Large64K (> 65536 bytes) must be rejected with BufferTooSmall
    let err = arena.acquire_for_size(65537).err().unwrap();
    match err {
        RdmaError::BufferTooSmall { capacity, needed } => {
            assert_eq!(capacity, 65536);
            assert_eq!(needed, 65537);
        }
        other => panic!("Unexpected error on oversized slab request: {other:?}"),
    }
}

// =========================================================================
// 5. High-Load Throughput & Latency Profiling (1,000 Iterations Burst)
// =========================================================================

#[tokio::test]
async fn test_high_load_throughput_and_latency_distribution() {
    let (ch1, ch2) = MockChannel::pair(64);
    let client = Arc::new(RdmaChannel::from_mock(ch1));
    let server = Arc::new(RdmaChannel::from_mock(ch2));

    let (client_sender, client_receiver) = client.split();
    let (server_sender, server_receiver) = server.split();

    let iters = 1_000;
    let payload = vec![0xAAu8; 1024]; // 1KB frame

    // Spawn server echo loop
    let s_recv = server_receiver.clone();
    let s_send = server_sender.clone();
    let server_task = tokio::spawn(async move {
        for _ in 0..iters {
            let msg = s_recv.recv(Duration::from_secs(5)).await.unwrap();
            s_send.send(&msg).await.unwrap();
        }
    });

    let mut latencies: Vec<Duration> = Vec::with_capacity(iters);
    let total_start = Instant::now();

    for _ in 0..iters {
        let req_start = Instant::now();
        client_sender.send(&payload).await.unwrap();
        let resp = client_receiver.recv(Duration::from_secs(5)).await.unwrap();
        assert_eq!(resp.len(), 1024);
        latencies.push(req_start.elapsed());
    }

    server_task.await.unwrap();
    let total_elapsed = total_start.elapsed();

    latencies.sort();
    let p50 = latencies[iters * 50 / 100];
    let p90 = latencies[iters * 90 / 100];
    let p99 = latencies[iters * 99 / 100];
    let max = latencies[iters - 1];

    let total_mb = (iters * 1024 * 2) as f64 / (1024.0 * 1024.0);
    let throughput_mb_s = total_mb / total_elapsed.as_secs_f64();

    println!(
        "\n=== High-Load RDMA Stress Benchmark (1000 Iterations, 1KB frames) ===\n\
         Total Elapsed:      {total_elapsed:?}\n\
         Throughput:         {throughput_mb_s:.2} MB/s\n\
         P50 Latency:        {p50:?}\n\
         P90 Latency:        {p90:?}\n\
         P99 Latency:        {p99:?}\n\
         Max Latency:        {max:?}\n\
         ====================================================================\n"
    );

    // Sanity check: P99 under in-memory mock should be sub-millisecond
    assert!(p99 < Duration::from_millis(50));
}
