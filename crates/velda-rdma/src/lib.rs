//! # Velda RDMA — Kernel-Bypass Transport Engine
//!
//! Provides an ultra-low latency, zero-copy, kernel-bypass remote direct memory access
//! (RoCEv2 & InfiniBand) transport engine for the Velda Edge Data Plane.
//!
//! ### Hardware Kernel-Bypass Mechanism
//! 1. **Direct Memory Access (DMA)**: Data is transferred directly between host user-space RAM
//!    and backend memory by the RDMA Host Channel Adapter (HCA / rNIC) over RoCEv2 or InfiniBand fabric.
//!    Completely bypasses the Linux kernel network stack: zero syscalls, zero kernel socket buffers (`sk_buff`),
//!    and zero CPU data copy overhead (`memcpy`).
//! 2. **Protection Domain & Memory Registration**: Pre-allocated memory slabs (`PayloadSlab`) are pinned
//!    into physical RAM (`ibv_reg_mr`) and mapped into the NIC's onboard Memory Translation Table (MTT)
//!    with hardware access keys (`lkey`/`rkey`), enabling wire-speed DMA validation.
//! 3. **Queue Pair & Credit-Based Flow Control**: Bidirectional communication executes over Reliable Connected
//!    Queue Pairs (RC QP). Atomic credit tracking strictly prevents Receiver Not Ready (RNR) hardware drops.
//! 4. **Adaptive Hybrid Polling**: Direct fast-path busy polling during active traffic bursts
//!    coupled with slow-path hardware MSI-X interrupt sleep (`tokio::io::unix::AsyncFd` via `ibv_comp_channel`)
//!    during idle windows, eliminating idle CPU burn.
//!
//! ### Subsystem Boundary (`AGENTS.md` Section 2.8)
//! - `velda-upstream`: Owns connection lifecycle, sharded pool state, and endpoint selection ("WHEN & WHO").
//! - `velda-rdma`: Owns verbs, queue pairs, memory registration, and wire protocol mechanics ("HOW").
//!
//! ### Deterministic Thread Topology (`AGENTS.md` Section 2.9)
//! - **Frontline Traffic Worker (`velda-worker-{id}`)**: Exclusively serves hot-path traffic by leasing
//!   pre-warmed Queue Pairs from its core-local lane shard without cross-thread coordination. Never spawns threads or blocks on the hot path.
//! - **Shadow Logistics Worker (`velda-shadow-{id}`)**: Autonomous background companion thread pinned to the
//!   same CPU core running under `SCHED_IDLE`, exclusively responsible for Out-of-Band (OOB) handshakes,
//!   hardware QP initialization, and replenishing warm connections into the lane shard.

pub mod channel;
pub mod config;
pub mod connector;
pub mod error;
pub mod ffi;
pub mod mock;
pub mod pool;
pub mod verbs;

pub use channel::{HardwareChannel, RdmaChannel, RdmaStream};
pub use config::RdmaConfig;
pub use connector::RdmaConnector;
pub use error::RdmaError;
pub use mock::{MockChannel, MockMessage};
pub use pool::{RdmaSlabArena, RdmaSlabClass, RdmaSlabConfig, RdmaSlabLease};
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
