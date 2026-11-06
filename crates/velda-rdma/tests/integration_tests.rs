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
