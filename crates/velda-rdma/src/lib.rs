//! # Velda RDMA — Kernel-Bypass Transport Engine
//!
//! Provides zero-copy, kernel-bypass remote direct memory access (RoCEv2 & InfiniBand)
//! transport capabilities for the Velda Edge Data Plane.
//!
//! ## Subsystem Boundary (`AGENTS.md` Section 2.8)
//! - `velda-upstream` owns connection lifecycle, pool state, and endpoint selection ("WHEN & WHO").
//! - `velda-rdma` owns queue pairs, memory registration, hardware verbs, and wire execution ("HOW").

pub mod channel;
pub mod config;
pub mod connector;
pub mod error;
pub mod ffi;
pub mod mock;
pub mod verbs;

pub use channel::{HardwareChannel, RdmaChannel, RdmaStream};
pub use config::RdmaConfig;
pub use connector::RdmaConnector;
pub use error::RdmaError;
pub use mock::{MockChannel, MockMessage};
pub use verbs::{
    CompletionQueue, MemoryRegion, ProtectionDomain, QpAddress, QueuePair, RdmaContext, RdmaDevice,
};

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn test_mock_channel_send_recv() {
        let (ch1, mut ch2) = MockChannel::pair(16);

        ch1.send(Bytes::from_static(b"hello-rdma"), Some(42))
            .await
            .expect("Send should succeed");

        let msg = ch2
            .recv(Duration::from_millis(500))
            .await
            .expect("Recv should succeed");

        assert_eq!(&msg.payload[..], b"hello-rdma");
        assert_eq!(msg.imm_data, Some(42));
    }

    #[tokio::test]
    async fn test_mock_channel_rdma_write() {
        let (ch1, mut ch2) = MockChannel::pair(16);

        ch1.rdma_write(0x1000, 0xABC, Bytes::from_static(b"tensor-payload"), None)
            .await
            .expect("RDMA Write should succeed");

        let msg = ch2
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
        let unified_ch1 = RdmaChannel::Mock(std::sync::Arc::new(tokio::sync::Mutex::new(ch1)));
        let unified_ch2 = RdmaChannel::Mock(std::sync::Arc::new(tokio::sync::Mutex::new(ch2)));

        let mut stream1 = RdmaStream::new(unified_ch1);
        let mut stream2 = RdmaStream::new(unified_ch2);

        // Write on stream1
        stream1
            .write_all(b"PING-PONG-OVER-RDMA")
            .await
            .expect("Write should succeed");
        stream1.flush().await.expect("Flush should succeed");

        // Read on stream2
        let mut read_buf = vec![0u8; 19];
        stream2
            .read_exact(&mut read_buf)
            .await
            .expect("Read should succeed");

        assert_eq!(&read_buf, b"PING-PONG-OVER-RDMA");
    }

    #[test]
    fn test_flow_control_credits() {
        let fc = channel::flow_control::CreditFlowControl::new(10);
        assert_eq!(fc.available_send_credits(), 10);

        for _ in 0..10 {
            assert!(fc.try_acquire_send_credit().is_ok());
        }

        // Exhausted
        assert!(fc.try_acquire_send_credit().is_err());

        // Replenish
        fc.add_remote_credits(5);
        assert_eq!(fc.available_send_credits(), 5);
        assert!(fc.try_acquire_send_credit().is_ok());
    }

    #[test]
    fn test_wr_builder() {
        use crate::ffi::ibv_sge;
        let sge = [ibv_sge {
            addr: 0x4000,
            length: 128,
            lkey: 99,
        }];

        let mut builder = channel::wr::SendWrBuilder::send(1, &sge, true);
        let raw = builder.as_raw_mut();
        unsafe {
            assert_eq!((*raw).wr_id, 1);
            assert_eq!((*raw).num_sge, 1);
            assert_eq!((*raw).opcode, crate::ffi::IBV_WR_SEND);
        }
    }

    #[test]
    fn test_ffi_dynamic_loading() {
        // Test that dynamic loading executes without crashing
        let lib_opt = ffi::get_ibv_lib();
        if let Some(lib) = lib_opt {
            let mut num_devices = 0;
            let list = unsafe { (lib.get_device_list)(&mut num_devices) };
            if !list.is_null() {
                unsafe { (lib.free_device_list)(list) };
            }
        }
    }
}
