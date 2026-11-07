//! Security & Hardening Integration Tests for RDMA Subsystem
//!
//! Validates resilience against protocol-level vulnerabilities, resource exhaustion,
//! flow-control integer tampering, and invalid OOB handshake injections.

use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use velda_rdma::channel::flow_control::CreditFlowControl;
use velda_rdma::connector::{RdmaListener, exchange_oob};
use velda_rdma::verbs::QpAddress;
use velda_rdma::{
    MockChannel, RdmaChannel, RdmaConfig, RdmaSlabArena, RdmaSlabClass, RdmaSlabConfig,
};

#[test]
fn test_credit_tampering_overflow_clamping() {
    let fc = CreditFlowControl::new(64);
    assert_eq!(fc.available_send_credits(), 64);

    // Consume all credits
    for _ in 0..64 {
        assert!(fc.try_acquire_send_credit().is_ok());
    }
    assert_eq!(fc.available_send_credits(), 0);

    // Malicious peer injects integer overflow attempt (u32::MAX)
    // The saturating clamp must limit credits to max_credits (64) rather than wrapping around to 0 or panicking
    fc.add_remote_credits(u32::MAX, 64);
    assert_eq!(fc.available_send_credits(), 64);

    // Normal grant exceeding max_credits should clamp to 64
    fc.add_remote_credits(100, 64);
    assert_eq!(fc.available_send_credits(), 64);
}

#[test]
fn test_credit_replenishment_persistence_without_vanishing() {
    let fc = CreditFlowControl::new(64);

    // Consume 100 receive buffers across multiple batches
    for _ in 0..100 {
        let _ = fc.on_recv_consumed();
    }

    // take_pending_replenishment must retain the exact accumulated count without losing credits
    let pending = fc.take_pending_replenishment();
    assert_eq!(pending, 100);

    // After swapping, pending must be 0
    assert_eq!(fc.take_pending_replenishment(), 0);

    // Re-queuing pending credits in case transmission was deferred
    fc.add_pending_credits(100);
    assert_eq!(fc.take_pending_replenishment(), 100);
}

#[tokio::test]
async fn test_channel_health_and_mark_broken() {
    let (ch1, ch2) = MockChannel::pair(16);
    let channel1 = RdmaChannel::from_mock(ch1);
    let channel2 = RdmaChannel::from_mock(ch2);

    assert!(channel1.is_healthy());
    assert!(channel2.is_healthy());

    // Mark broken on sender propagates to channel
    channel1.mark_broken();
    assert!(!channel1.is_healthy());
}

#[tokio::test]
async fn test_oob_handshake_rejects_invalid_magic() {
    let config = RdmaConfig::mock();
    let listener = RdmaListener::bind("127.0.0.1:0".parse().unwrap(), config)
        .await
        .expect("Failed binding listener");
    let addr = listener.local_addr().unwrap();

    let accept_task = tokio::spawn(async move { listener.accept().await });

    // Client connects and injects invalid magic
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let mut malicious_frame = [0u8; 32];
    malicious_frame[0..4].copy_from_slice(b"HACK");
    stream.write_all(&malicious_frame).await.unwrap();
    stream.flush().await.unwrap();

    let res = accept_task.await.unwrap();
    assert!(res.is_err(), "Accept should have rejected invalid magic");
    let err = res.err().unwrap().to_string();
    assert!(
        err.contains("Invalid OOB handshake magic header"),
        "Unexpected error: {err}"
    );
}

#[tokio::test]
async fn test_oob_handshake_rejects_zero_qpn() {
    let config = RdmaConfig::mock();
    let listener = RdmaListener::bind("127.0.0.1:0".parse().unwrap(), config)
        .await
        .expect("Failed binding listener");
    let addr = listener.local_addr().unwrap();

    let accept_task = tokio::spawn(async move { listener.accept().await });

    let mut stream = TcpStream::connect(addr).await.unwrap();
    let mut frame = [0u8; 32];
    frame[0..4].copy_from_slice(b"VRDM");
    // qpn = 0 (invalid)
    frame[4..8].copy_from_slice(&0u32.to_be_bytes());
    frame[16..32].copy_from_slice(&[0x12u8; 16]); // GID not empty
    stream.write_all(&frame).await.unwrap();
    stream.flush().await.unwrap();

    let res = accept_task.await.unwrap();
    assert!(res.is_err(), "Accept should have rejected zero QPN");
    let err = res.err().unwrap().to_string();
    assert!(
        err.contains("Invalid remote QPN (0)"),
        "Unexpected error: {err}"
    );
}

#[tokio::test]
async fn test_oob_handshake_rejects_empty_gid_and_lid() {
    let config = RdmaConfig::mock();
    let listener = RdmaListener::bind("127.0.0.1:0".parse().unwrap(), config)
        .await
        .expect("Failed binding listener");
    let addr = listener.local_addr().unwrap();

    let accept_task = tokio::spawn(async move { listener.accept().await });

    let mut stream = TcpStream::connect(addr).await.unwrap();
    let mut frame = [0u8; 32];
    frame[0..4].copy_from_slice(b"VRDM");
    frame[4..8].copy_from_slice(&1234u32.to_be_bytes()); // valid qpn
    frame[8..12].copy_from_slice(&100u32.to_be_bytes());
    // lid = 0, gid = [0; 16] (empty)
    stream.write_all(&frame).await.unwrap();
    stream.flush().await.unwrap();

    let res = accept_task.await.unwrap();
    assert!(
        res.is_err(),
        "Accept should have rejected empty GID and LID"
    );
    let err = res.err().unwrap().to_string();
    assert!(
        err.contains("Invalid empty remote GID and LID"),
        "Unexpected error: {err}"
    );
}

#[test]
fn test_protection_domain_sharing_with_arena() {
    // In mock mode, pd() returns None gracefully
    let arena = RdmaSlabArena::new_mock(RdmaSlabConfig::default()).unwrap();
    assert!(arena.pd().is_none());
}

#[test]
fn test_slab_memory_zeroed_upon_drop() {
    let arena = RdmaSlabArena::new_mock(RdmaSlabConfig::default()).unwrap();
    let raw_ptr: *const u8;
    let len: usize;

    {
        let mut lease = arena.acquire(RdmaSlabClass::Standard2K).unwrap();
        raw_ptr = lease.as_slice().as_ptr();
        len = lease.len();
        lease.as_mut_slice().fill(0xAA);
        assert_eq!(lease.as_slice()[0], 0xAA);
        assert_eq!(lease.as_slice()[len - 1], 0xAA);
        // lease is dropped here, triggering RdmaSlabLease::drop() which zeroes memory
    }

    // Direct inspect to verify memory zeroing (VULN-02 fix)
    unsafe {
        let slice = std::slice::from_raw_parts(raw_ptr, len);
        assert!(
            slice.iter().all(|&b| b == 0),
            "Slab buffer was not zeroed on drop! Memory bleed vulnerability present."
        );
    }

    // Reacquired lease from same slot must be cleanly zeroed
    let reacquired = arena.acquire(RdmaSlabClass::Standard2K).unwrap();
    assert!(
        reacquired.as_slice().iter().all(|&b| b == 0),
        "Reacquired slab contains residual tenant data!"
    );
}

#[tokio::test]
async fn test_client_oob_handshake_rejects_zero_qpn() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server_task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut rx = [0u8; 32];
        socket.read_exact(&mut rx).await.unwrap();

        // Send back frame with QPN = 0
        let mut tx = [0u8; 32];
        tx[0..4].copy_from_slice(b"VRDM");
        tx[4..8].copy_from_slice(&0u32.to_be_bytes()); // QPN 0
        tx[8..12].copy_from_slice(&100u32.to_be_bytes());
        tx[12..14].copy_from_slice(&1u16.to_be_bytes());
        tx[16..32].copy_from_slice(&[0x11u8; 16]);
        socket.write_all(&tx).await.unwrap();
        socket.flush().await.unwrap();
    });

    let local_addr = QpAddress {
        qpn: 42,
        psn: 1000,
        lid: 1,
        gid: velda_rdma::ffi::ibv_gid { raw: [1; 16] },
    };

    let res = exchange_oob(addr, &local_addr, Duration::from_secs(1)).await;
    server_task.await.unwrap();

    assert!(res.is_err(), "Client should reject QPN 0");
    let err = res.err().unwrap().to_string();
    assert!(
        err.contains("Invalid remote QPN (0)"),
        "Unexpected error: {err}"
    );
}

#[tokio::test]
async fn test_client_oob_handshake_rejects_empty_gid_and_lid() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server_task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut rx = [0u8; 32];
        socket.read_exact(&mut rx).await.unwrap();

        // Send back frame with valid QPN but empty GID and LID = 0
        let mut tx = [0u8; 32];
        tx[0..4].copy_from_slice(b"VRDM");
        tx[4..8].copy_from_slice(&42u32.to_be_bytes()); // valid QPN
        tx[8..12].copy_from_slice(&100u32.to_be_bytes());
        tx[12..14].copy_from_slice(&0u16.to_be_bytes()); // LID 0
        // GID all 0s
        socket.write_all(&tx).await.unwrap();
        socket.flush().await.unwrap();
    });

    let local_addr = QpAddress {
        qpn: 42,
        psn: 1000,
        lid: 1,
        gid: velda_rdma::ffi::ibv_gid { raw: [1; 16] },
    };

    let res = exchange_oob(addr, &local_addr, Duration::from_secs(1)).await;
    server_task.await.unwrap();

    assert!(res.is_err(), "Client should reject empty GID and LID");
    let err = res.err().unwrap().to_string();
    assert!(
        err.contains("Invalid empty remote GID and LID"),
        "Unexpected error: {err}"
    );
}
