//! # RDMA Transport Channels, Stream Adapters, and Flow Control
//!
//! ## Architectural Invariants & Subsystem Structure
//!
//! The channel subsystem coordinates the hardware resources required to conduct bidirectional,
//! zero-copy communication over InfiniBand or RoCEv2 networks. It ties together the underlying
//! Verbs primitives (Protection Domain, Memory Region, Queue Pair, Completion Queue) into an active,
//! credit-governed transport channel.
//!
//! ### Hardware Channel Architecture (`HardwareChannel`)
//! A `HardwareChannel` manages the lifecycle of a dedicated connection to an upstream node:
//! 1. **Contiguous Pinned Buffer Ring**: Pre-allocates and registers a contiguous chunk of memory
//!    partitioned into discrete Send and Receive slots (default 64 KB per slot). The memory is pinned
//!    into physical RAM (`mlock`), eliminating MMU page faults during PCIe DMA transactions.
//! 2. **Pre-Posted Receive Ring**: At initialization, receive slots are registered as `ibv_recv_wr`
//!    work requests on the QP's Receive Queue. This ensures the NIC hardware always has memory destinations
//!    available when incoming packets arrive from the network wire.
//! 3. **Credit-Based Flow Control**: Integrates `CreditFlowControl` to prevent buffer overflow and
//!    eliminate hardware Receiver Not Ready (RNR) state transitions.
//! 4. **Adaptive Polling**: Uses `AdaptiveCqPoller` to achieve minimal completion harvesting latency
//!    on hot bursts without context-switch overhead, while dropping CPU utilization to 0% during idle traffic periods.
//!
//! ### Dual Transport Abstraction (`RdmaChannel`)
//! `RdmaChannel` is an explicit enum representing either:
//! - `Hardware`: Real hardware HCA channel backed by Mellanox/NVIDIA OFED or InfiniBand drivers.
//! - `Mock`: High-fidelity, pure-Rust in-memory simulation channel allowing end-to-end tests to pass
//!   on standard development machines and CI/CD pipelines without physical RDMA hardware.

pub mod flow_control;
pub mod poller;
pub mod stream;
pub mod wr;

use bytes::Bytes;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::Mutex as AsyncMutex;

pub use flow_control::CreditFlowControl;
pub use poller::AdaptiveCqPoller;
pub use stream::RdmaStream;

use crate::channel::wr::{SendWrBuilder, build_recv_wr};
use crate::config::RdmaConfig;
use crate::error::RdmaError;
use crate::ffi::ibv_sge;
use crate::mock::MockChannel;
use crate::verbs::cq::CompletionQueue;
use crate::verbs::mr::MemoryRegion;
use crate::verbs::pd::ProtectionDomain;
use crate::verbs::qp::QueuePair;

/// Default send/recv slot size (64KB) for pre-allocated pinned hardware channel buffers.
pub const DEFAULT_CHANNEL_SLOT_SIZE: usize = 65536;
/// Work Request ID for standard Send WR operations.
const WR_ID_SEND: u64 = 100;

/// Hardware-backed RDMA channel holding registered memory buffers and QP.
pub struct HardwareChannel {
    qp: Arc<QueuePair>,
    _send_cq: Arc<CompletionQueue>,
    _recv_cq: Arc<CompletionQueue>,
    send_poller: AdaptiveCqPoller,
    recv_poller: AdaptiveCqPoller,
    flow_control: CreditFlowControl,
    mr: Option<MemoryRegion>,
    raw_buf: *mut u8,
    buf_len: usize,
    max_send_wr: u32,
    max_recv_wr: u32,
    send_slot_size: usize,
    recv_slot_size: usize,
    timeout: Duration,
    send_slot_seq: AtomicU64,
}

unsafe impl Send for HardwareChannel {}
unsafe impl Sync for HardwareChannel {}

impl HardwareChannel {
    /// Constructs a new hardware channel with dedicated Send and Receive Completion Queues,
    /// pinning internal memory buffers and pre-posting initial receive work requests.
    pub fn new(
        qp: Arc<QueuePair>,
        send_cq: Arc<CompletionQueue>,
        recv_cq: Arc<CompletionQueue>,
        pd: Arc<ProtectionDomain>,
        config: &RdmaConfig,
    ) -> Result<Self, RdmaError> {
        if config.max_send_wr == 0 || config.max_recv_wr == 0 {
            return Err(RdmaError::Config(
                "RDMA max_send_wr and max_recv_wr must be greater than zero".into(),
            ));
        }

        let send_poller = AdaptiveCqPoller::new(send_cq.clone(), config.poll_spin_count)?;
        let recv_poller = AdaptiveCqPoller::new(recv_cq.clone(), config.poll_spin_count)?;
        let flow_control = CreditFlowControl::new(config.initial_credits);

        // 1. Pre-allocate contiguous pinned buffer for send/recv slots (64KB slots default).
        // Sending and receiving operate out of symmetric slots within this single registered region.
        let slot_size = DEFAULT_CHANNEL_SLOT_SIZE;
        let total_slots = (config.max_send_wr + config.max_recv_wr) as usize;
        let total_bytes = total_slots * slot_size;

        let mut host_buf = vec![0u8; total_bytes];
        let raw_buf = host_buf.as_mut_ptr();
        let buf_len = host_buf.len();

        // 2. Pin memory via kernel mlock and register translation table (MTT) in HCA hardware.
        let mr = MemoryRegion::register_slice(pd, &mut host_buf)?;
        std::mem::forget(host_buf); // Memory is now managed by MemoryRegion and pinned
        let lkey = mr.lkey();

        let channel = Self {
            qp,
            _send_cq: send_cq,
            _recv_cq: recv_cq,
            send_poller,
            recv_poller,
            flow_control,
            mr: Some(mr),
            raw_buf,
            buf_len,
            max_send_wr: config.max_send_wr,
            max_recv_wr: config.max_recv_wr,
            send_slot_size: slot_size,
            recv_slot_size: slot_size,
            timeout: Duration::from_millis(config.timeout_ms),
            send_slot_seq: AtomicU64::new(0),
        };

        // 3. Pre-post initial Receive Work Requests into RQ before traffic arrives.
        // Each receive slot occupies its own distinct, non-overlapping memory offset
        // keyed by `wr_id = i` to eliminate receive buffer aliasing under burst traffic.
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
        // Step 1: Validate payload does not exceed the pinned send slot size.
        if data.len() > self.send_slot_size {
            return Err(RdmaError::BufferTooSmall {
                capacity: self.send_slot_size,
                needed: data.len(),
            });
        }

        // Step 2: Acquire flow-control credit; blocks or errors if remote RQ buffer is exhausted.
        self.flow_control.try_acquire_send_credit()?;

        // Step 3: Copy data into a dedicated pinned send slot and dispatch Two-Sided Send WR to HCA.
        let slot_idx = (self.send_slot_seq.fetch_add(1, Ordering::Relaxed) as usize)
            % (self.max_send_wr as usize);
        let send_offset = slot_idx * self.send_slot_size;
        let lkey = self.mr.as_ref().map(|m| m.lkey()).unwrap_or(0);

        {
            let send_ptr = unsafe { self.raw_buf.add(send_offset) };
            unsafe {
                std::ptr::copy_nonoverlapping(data.as_ptr(), send_ptr, data.len());
            }

            let sge = [ibv_sge {
                addr: send_ptr as u64,
                length: data.len() as u32,
                lkey,
            }];

            let mut builder = SendWrBuilder::send(slot_idx as u64, &sge, true);
            if let Err(e) = unsafe { self.qp.post_send(builder.as_raw_mut()) } {
                self.flow_control.add_remote_credits(1);
                return Err(e);
            }
        }

        // Step 4: Await send completion exclusively from dedicated hardware Send CQ.
        let poll_res = self.send_poller.poll_one(self.timeout).await;
        // Inflight Send WR completed; return credit to flow control quota
        self.flow_control.add_remote_credits(1);
        poll_res?;
        Ok(())
    }

    /// Posts a Send WR directly referencing a pre-registered virtual memory address and lkey
    /// with a single SGE (`num_sge = 1`), eliminating intermediate user-space `memcpy`.
    pub async fn send_registered(
        &self,
        addr: u64,
        length: u32,
        lkey: u32,
    ) -> Result<(), RdmaError> {
        // Step 1: Acquire flow-control credit to prevent remote RNR drops.
        self.flow_control.try_acquire_send_credit()?;

        // Step 2: Dispatch Two-Sided Send WR referencing the pre-registered memory address.
        {
            let sge = [ibv_sge { addr, length, lkey }];
            let mut builder = SendWrBuilder::send(WR_ID_SEND, &sge, true);
            if let Err(e) = unsafe { self.qp.post_send(builder.as_raw_mut()) } {
                self.flow_control.add_remote_credits(1);
                return Err(e);
            }
        }

        // Step 3: Await send completion exclusively from dedicated hardware Send CQ.
        let poll_res = self.send_poller.poll_one(self.timeout).await;
        // Inflight Send WR completed; return credit to flow control quota
        self.flow_control.add_remote_credits(1);
        poll_res?;
        Ok(())
    }

    /// Posts a Send WR directly from an `RdmaSlabLease` zero-copy buffer.
    pub async fn send_lease(
        &self,
        lease: &crate::pool::RdmaSlabLease,
        length: usize,
    ) -> Result<(), RdmaError> {
        if length > lease.capacity() {
            return Err(RdmaError::BufferTooSmall {
                capacity: lease.capacity(),
                needed: length,
            });
        }
        self.send_registered(lease.addr(), length as u32, lease.lkey())
            .await
    }

    /// Receives data from the RDMA Queue Pair using caller-specified timeout.
    pub async fn recv(&self, timeout: Duration) -> Result<Bytes, RdmaError> {
        // Step 1: Await inbound Work Completion exclusively from dedicated hardware Receive CQ.
        let wc = self.recv_poller.poll_one(timeout).await?;
        let byte_len = wc.byte_len as usize;

        // Step 2: Identify designated receive slot by `wc.wr_id` to prevent buffer aliasing.
        let slot_idx = (wc.wr_id as usize) % (self.max_recv_wr as usize);
        let rx_offset = (self.max_send_wr as usize + slot_idx) * self.recv_slot_size;
        let safe_len = byte_len.min(self.recv_slot_size);

        let rx_slice = unsafe { std::slice::from_raw_parts(self.raw_buf.add(rx_offset), safe_len) };
        let payload = Bytes::copy_from_slice(rx_slice);

        // Step 3: Immediately re-post receive buffer to replenish RQ capacity for this specific slot.
        let lkey = self.mr.as_ref().map(|m| m.lkey()).unwrap_or(0);
        let sge = [ibv_sge {
            addr: unsafe { self.raw_buf.add(rx_offset) } as u64,
            length: self.recv_slot_size as u32,
            lkey,
        }];
        let mut rwr = build_recv_wr(slot_idx as u64, &sge);
        if let Err(e) = unsafe { self.qp.post_recv(&mut rwr) } {
            tracing::warn!(
                slot = slot_idx,
                error = %e,
                "Failed to re-post receive buffer to RDMA RQ"
            );
        }

        // Step 4: Track consumed receive slot and determine if credit update should be granted.
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
        // Step 1: Validate payload bounds.
        if data.len() > self.send_slot_size {
            return Err(RdmaError::BufferTooSmall {
                capacity: self.send_slot_size,
                needed: data.len(),
            });
        }

        // Step 2: Copy payload into designated pinned send slot and dispatch One-Sided RDMA Write with rkey.
        let slot_idx = (self.send_slot_seq.fetch_add(1, Ordering::Relaxed) as usize)
            % (self.max_send_wr as usize);
        let send_offset = slot_idx * self.send_slot_size;
        let lkey = self.mr.as_ref().map(|m| m.lkey()).unwrap_or(0);

        unsafe {
            let send_ptr = self.raw_buf.add(send_offset);
            std::ptr::copy_nonoverlapping(data.as_ptr(), send_ptr, data.len());

            let sge = [ibv_sge {
                addr: send_ptr as u64,
                length: data.len() as u32,
                lkey,
            }];

            let mut builder =
                SendWrBuilder::rdma_write(slot_idx as u64, &sge, remote_addr, rkey, true);
            self.qp.post_send(builder.as_raw_mut())?;
        }

        // Step 3: Await local write completion from dedicated Send CQ.
        self.send_poller.poll_one(self.timeout).await?;
        Ok(())
    }
}

impl Drop for HardwareChannel {
    fn drop(&mut self) {
        // Step 1: Explicitly drop MemoryRegion first to deregister from HCA before freeing memory
        self.mr.take();

        // Step 2: Return pinned host memory buffer to OS allocator
        if !self.raw_buf.is_null() {
            unsafe {
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

    /// Sends registered memory directly over the RDMA channel without intermediate memcpy.
    pub async fn send_registered(
        &self,
        addr: u64,
        length: u32,
        lkey: u32,
    ) -> Result<(), RdmaError> {
        match self {
            Self::Hardware(hw) => hw.send_registered(addr, length, lkey).await,
            Self::Mock(mock) => {
                let guard = mock.lock().await;
                guard.send_registered(addr, length, lkey).await
            }
        }
    }

    /// Sends directly from an `RdmaSlabLease` zero-copy buffer.
    pub async fn send_lease(
        &self,
        lease: &crate::pool::RdmaSlabLease,
        length: usize,
    ) -> Result<(), RdmaError> {
        match self {
            Self::Hardware(hw) => hw.send_lease(lease, length).await,
            Self::Mock(mock) => {
                let guard = mock.lock().await;
                guard.send_lease(lease, length).await
            }
        }
    }

    /// Receives a payload from the RDMA channel.
    pub async fn recv(&self, timeout: Duration) -> Result<Bytes, RdmaError> {
        match self {
            Self::Hardware(hw) => hw.recv(timeout).await,
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
