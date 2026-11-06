//! RDMA Transport Channels, Stream Adapters, and Flow Control.

pub mod flow_control;
pub mod poller;
pub mod stream;
pub mod wr;

use bytes::Bytes;
use parking_lot::Mutex as SyncMutex;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex as AsyncMutex;

use crate::channel::flow_control::CreditFlowControl;
use crate::channel::poller::AdaptiveCqPoller;
use crate::channel::wr::{SendWrBuilder, build_recv_wr};
use crate::config::RdmaConfig;
use crate::error::RdmaError;
use crate::ffi::ibv_sge;
use crate::mock::MockChannel;
use crate::verbs::cq::CompletionQueue;
use crate::verbs::mr::MemoryRegion;
use crate::verbs::pd::ProtectionDomain;
use crate::verbs::qp::QueuePair;

pub use flow_control::CreditFlowControl as FlowControl;
pub use poller::AdaptiveCqPoller as CqPoller;
pub use stream::RdmaStream;

/// Hardware-backed RDMA channel holding registered memory buffers and QP.
pub struct HardwareChannel {
    qp: Arc<QueuePair>,
    _cq: Arc<CompletionQueue>,
    poller: AdaptiveCqPoller,
    flow_control: CreditFlowControl,
    mr: MemoryRegion,
    raw_buf: *mut u8,
    buf_len: usize,
    send_slot_size: usize,
    recv_slot_size: usize,
    timeout: Duration,
    _guard: SyncMutex<()>,
}

unsafe impl Send for HardwareChannel {}
unsafe impl Sync for HardwareChannel {}

impl HardwareChannel {
    /// Constructs a new hardware channel, pinning the internal memory buffer and pre-posting receive buffers.
    pub fn new(
        qp: Arc<QueuePair>,
        cq: Arc<CompletionQueue>,
        pd: Arc<ProtectionDomain>,
        config: &RdmaConfig,
    ) -> Result<Self, RdmaError> {
        let poller = AdaptiveCqPoller::new(cq.clone(), config.poll_spin_count)?;
        let flow_control = CreditFlowControl::new(config.initial_credits);

        // Pre-allocate contiguous pinned buffer for send/recv slots (64KB slots default)
        let slot_size = 65536;
        let total_slots = (config.max_send_wr + config.max_recv_wr) as usize;
        let total_bytes = total_slots * slot_size;

        let mut host_buf = vec![0u8; total_bytes];
        let raw_buf = host_buf.as_mut_ptr();
        let buf_len = host_buf.len();

        let mr = MemoryRegion::register_slice(pd, &mut host_buf)?;
        std::mem::forget(host_buf); // Memory is now managed by MemoryRegion and pinned
        let lkey = mr.lkey();

        let channel = Self {
            qp,
            _cq: cq,
            poller,
            flow_control,
            mr,
            raw_buf,
            buf_len,
            send_slot_size: slot_size,
            recv_slot_size: slot_size,
            timeout: Duration::from_millis(config.timeout_ms),
            _guard: SyncMutex::new(()),
        };

        // Pre-post initial Receive Work Requests into RQ to prevent RNR
        for i in 0..config.initial_credits as usize {
            let offset = (config.max_send_wr as usize + i) * slot_size;
            let recv_ptr = unsafe { raw_buf.add(offset) };
            let sge = [ibv_sge {
                addr: recv_ptr as u64,
                length: slot_size as u32,
                lkey,
            }];

            let mut rwr = build_recv_wr(i as u64, &sge);
            unsafe { channel.qp.post_recv(&mut rwr) }?;
        }

        Ok(channel)
    }

    /// Sends data over the RDMA Queue Pair using Send WR.
    pub async fn send(&self, data: &[u8]) -> Result<(), RdmaError> {
        if data.len() > self.send_slot_size {
            return Err(RdmaError::BufferTooSmall {
                capacity: self.send_slot_size,
                needed: data.len(),
            });
        }

        self.flow_control.try_acquire_send_credit()?;

        // Copy into pinned send slot 0 and post WR
        {
            unsafe {
                std::ptr::copy_nonoverlapping(data.as_ptr(), self.raw_buf, data.len());
            }

            let sge = [ibv_sge {
                addr: self.raw_buf as u64,
                length: data.len() as u32,
                lkey: self.mr.lkey(),
            }];

            let mut builder = SendWrBuilder::send(100, &sge, true);
            unsafe { self.qp.post_send(builder.as_raw_mut()) }?;
        }

        // Await send completion
        self.poller.poll_one(self.timeout).await?;
        Ok(())
    }

    /// Receives data from the RDMA Queue Pair.
    pub async fn recv(&self) -> Result<Bytes, RdmaError> {
        let wc = self.poller.poll_one(self.timeout).await?;
        let byte_len = wc.byte_len as usize;

        // The received payload is in the receive slot
        let rx_offset = (self.buf_len / 2).min(self.buf_len - byte_len);
        let rx_slice = unsafe { std::slice::from_raw_parts(self.raw_buf.add(rx_offset), byte_len) };
        let payload = Bytes::copy_from_slice(rx_slice);

        // Re-post receive buffer to replenish RQ
        let sge = [ibv_sge {
            addr: unsafe { self.raw_buf.add(rx_offset) } as u64,
            length: self.recv_slot_size as u32,
            lkey: self.mr.lkey(),
        }];
        let mut rwr = build_recv_wr(wc.wr_id, &sge);
        let _ = unsafe { self.qp.post_recv(&mut rwr) };

        // Track credits
        if let Some(credits_to_grant) = self.flow_control.on_recv_consumed() {
            tracing::trace!(
                credits = credits_to_grant,
                "Replenished RDMA receive credits"
            );
        }

        Ok(payload)
    }

    /// Performs a One-Sided RDMA Write into remote memory.
    pub async fn rdma_write(
        &self,
        remote_addr: u64,
        rkey: u32,
        data: &[u8],
    ) -> Result<(), RdmaError> {
        if data.len() > self.send_slot_size {
            return Err(RdmaError::BufferTooSmall {
                capacity: self.send_slot_size,
                needed: data.len(),
            });
        }

        {
            unsafe {
                std::ptr::copy_nonoverlapping(data.as_ptr(), self.raw_buf, data.len());
            }

            let sge = [ibv_sge {
                addr: self.raw_buf as u64,
                length: data.len() as u32,
                lkey: self.mr.lkey(),
            }];

            let mut builder = SendWrBuilder::rdma_write(101, &sge, remote_addr, rkey, true);
            unsafe { self.qp.post_send(builder.as_raw_mut()) }?;
        }

        self.poller.poll_one(self.timeout).await?;
        Ok(())
    }
}

impl Drop for HardwareChannel {
    fn drop(&mut self) {
        if !self.raw_buf.is_null() {
            unsafe {
                // Reconstruct host vector so its memory is cleanly deallocated after dereg_mr
                let _ = Vec::from_raw_parts(self.raw_buf, self.buf_len, self.buf_len);
            }
        }
    }
}

/// Unified RDMA Channel enum representing either hardware verbs or pure-Rust mock channel.
#[derive(Clone)]
pub enum RdmaChannel {
    /// Active hardware RoCEv2 / InfiniBand channel.
    Hardware(Arc<HardwareChannel>),
    /// In-memory simulated mock channel for tests and environments without RDMA NICs.
    Mock(Arc<AsyncMutex<MockChannel>>),
}

impl RdmaChannel {
    /// Sends a payload over the RDMA channel.
    pub async fn send(&self, data: &[u8]) -> Result<(), RdmaError> {
        match self {
            Self::Hardware(hw) => hw.send(data).await,
            Self::Mock(mock) => {
                let guard = mock.lock().await;
                guard.send(Bytes::copy_from_slice(data), None).await
            }
        }
    }

    /// Receives a payload from the RDMA channel.
    pub async fn recv(&self, timeout: Duration) -> Result<Bytes, RdmaError> {
        match self {
            Self::Hardware(hw) => hw.recv().await,
            Self::Mock(mock) => {
                let mut guard = mock.lock().await;
                let msg = guard.recv(timeout).await?;
                Ok(msg.payload)
            }
        }
    }

    /// Performs a One-Sided RDMA Write.
    pub async fn rdma_write(
        &self,
        remote_addr: u64,
        rkey: u32,
        data: &[u8],
    ) -> Result<(), RdmaError> {
        match self {
            Self::Hardware(hw) => hw.rdma_write(remote_addr, rkey, data).await,
            Self::Mock(mock) => {
                let guard = mock.lock().await;
                guard
                    .rdma_write(remote_addr, rkey, Bytes::copy_from_slice(data), None)
                    .await
            }
        }
    }
}
