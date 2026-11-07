//! # Dedicated Hardware RoCEv2 / InfiniBand Queue Pair Channels
//!
//! Owns the physical hardware resources (Queue Pair, Completion Queues, Pinned Memory Region)
//! and provides decoupled, independent Send and Receive handles (`HardwareSender`, `HardwareReceiver`).

use bytes::Bytes;
use parking_lot::RwLock;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;
use tokio::sync::Mutex as AsyncMutex;

use crate::channel::flow_control::CreditFlowControl;
use crate::channel::poller::AdaptiveCqPoller;
use crate::channel::wr::{SendWrBuilder, build_recv_wr};
use crate::config::RdmaConfig;
use crate::error::RdmaError;
use crate::ffi::ibv_sge;
use crate::verbs::cq::CompletionQueue;
use crate::verbs::mr::MemoryRegion;
use crate::verbs::pd::ProtectionDomain;
use crate::verbs::qp::QueuePair;

/// Default send/recv slot size (64KB) for pre-allocated pinned hardware channel buffers.
pub const DEFAULT_CHANNEL_SLOT_SIZE: usize = 65536;
/// Work Request ID for standard Send WR operations.
pub const WR_ID_SEND: u64 = 100;
/// Most significant bit in 32-bit immediate data denoting a clean connection EOF shutdown.
pub const RDMA_FLAG_EOF: u32 = 0x8000_0000;
/// Bitmask for extracting credit quantity from immediate data.
pub const RDMA_CREDIT_MASK: u32 = 0x7FFF_FFFF;

/// Shared underlying hardware resources and pinned memory buffer.
///
/// Guaranteed to stay alive until BOTH `HardwareSender` and `HardwareReceiver`
/// are dropped, at which point QP transitions to RESET and memory is unpinned.
pub struct HardwareChannelShared {
    pub(crate) qp: Arc<QueuePair>,
    pub(crate) _send_cq: Arc<CompletionQueue>,
    pub(crate) _recv_cq: Arc<CompletionQueue>,
    pub(crate) mr: Option<MemoryRegion>,
    pub(crate) raw_buf: *mut u8,
    pub(crate) buf_len: usize,
    pub(crate) lkey: u32,
    pub(crate) healthy: AtomicBool,
}

unsafe impl Send for HardwareChannelShared {}
unsafe impl Sync for HardwareChannelShared {}

/// RAII guard ensuring in-flight hardware DMA Work Requests are immediately aborted
/// if the task future is cancelled or times out, preventing DMA use-after-free and cross-tenant bleeds.
struct InFlightGuard<'a> {
    qp: &'a QueuePair,
    healthy: &'a AtomicBool,
    completed: bool,
}

impl Drop for InFlightGuard<'_> {
    fn drop(&mut self) {
        if !self.completed {
            // Task future was cancelled, dropped, or timed out while WR was in flight on HCA!
            // Abort immediately by transitioning QP to RESET to halt hardware DMA into host buffers.
            let _ = self.qp.to_reset();
            self.healthy.store(false, Ordering::Release);
        }
    }
}

impl Drop for HardwareChannelShared {
    fn drop(&mut self) {
        // Step 1: Transition QP to RESET state immediately to cancel and flush all
        // queued and in-flight Receive and Send Work Requests. This prevents the HCA
        // from executing PCIe DMA into host memory after it has been freed.
        let _ = self.qp.to_reset();

        // Step 2: Explicitly drop MemoryRegion first to deregister from HCA before freeing memory
        self.mr.take();

        // Step 3: Return pinned host memory buffer to OS allocator
        if !self.raw_buf.is_null() {
            unsafe {
                let _ = Vec::from_raw_parts(self.raw_buf, self.buf_len, self.buf_len);
            }
        }
    }
}

/// Independent send flow handle for hardware RDMA Queue Pair.
pub struct HardwareSender {
    pub(crate) shared: Arc<HardwareChannelShared>,
    pub(crate) send_poller: AdaptiveCqPoller,
    pub(crate) flow_control: Arc<CreditFlowControl>,
    pub(crate) send_lock: AsyncMutex<()>,
    pub(crate) max_send_wr: u32,
    pub(crate) send_wr_mask: usize,
    pub(crate) send_slot_size: usize,
    pub(crate) timeout: Duration,
    pub(crate) send_slot_seq: AtomicU64,
}

unsafe impl Send for HardwareSender {}
unsafe impl Sync for HardwareSender {}

impl HardwareSender {
    /// Sends data over the RDMA Queue Pair using Send WR.
    pub async fn send(&self, data: &[u8]) -> Result<(), RdmaError> {
        if data.len() > self.send_slot_size {
            return Err(RdmaError::BufferTooSmall {
                capacity: self.send_slot_size,
                needed: data.len(),
            });
        }

        let _guard = self.send_lock.lock().await;
        self.flow_control.acquire_send_credit(self.timeout).await?;

        let seq = self.send_slot_seq.fetch_add(1, Ordering::Relaxed) as usize;
        let slot_idx = seq & self.send_wr_mask;
        let send_offset = slot_idx * self.send_slot_size;
        let lkey = self.shared.lkey;

        {
            let send_ptr = unsafe { self.shared.raw_buf.add(send_offset) };

            // Non-temporal prefetch on source payload + non-temporal streaming copy into pinned DMA buffer.
            // Bypasses L1D/L2 cache allocation so 64KB RDMA chunks never evict the L1D router table
            // and never dirty core L1D/L2 lines (eliminating PCIe DMA snoop stalls on the HCA).
            velda_core::prefetch_nta_range(data.as_ptr(), data.len().min(256));
            let dst_slice = unsafe { std::slice::from_raw_parts_mut(send_ptr, data.len()) };
            (*velda_core::COPY_NON_TEMPORAL_FN)(data, dst_slice);

            let sge = [ibv_sge {
                addr: send_ptr as u64,
                length: data.len() as u32,
                lkey,
            }];

            let pending_credits = self.flow_control.take_pending_replenishment();
            let mut builder = SendWrBuilder::send(slot_idx as u64, &sge, pending_credits, true);

            if let Err(e) = unsafe { self.shared.qp.post_send(builder.as_raw_mut()) } {
                self.flow_control.add_remote_credits(1, self.max_send_wr);
                self.shared.healthy.store(false, Ordering::Release);
                return Err(e);
            }
        }

        let mut in_flight = InFlightGuard {
            qp: &self.shared.qp,
            healthy: &self.shared.healthy,
            completed: false,
        };

        match self.send_poller.poll_one(self.timeout).await {
            Ok(wc) => {
                in_flight.completed = true;
                if wc.wr_id != slot_idx as u64 {
                    tracing::warn!(
                        expected = slot_idx,
                        got = wc.wr_id,
                        "Mismatched WR ID on Send CQ completion"
                    );
                }
                // Post-Send Drain Check:
                // If receiver returned credits while send_lock was held, flush them immediately
                // before dropping send_lock so peer does not starve (Fixes Scenario 3).
                let leftover_credits = self.flow_control.take_pending_replenishment();
                if leftover_credits > 0 {
                    let post_slot_idx = (self.send_slot_seq.fetch_add(1, Ordering::Relaxed)
                        as usize)
                        & self.send_wr_mask;
                    let post_send_offset = post_slot_idx * self.send_slot_size;
                    let posted = {
                        let post_send_ptr = unsafe { self.shared.raw_buf.add(post_send_offset) };
                        let sge = [ibv_sge {
                            addr: post_send_ptr as u64,
                            length: 0,
                            lkey,
                        }];
                        let mut builder =
                            SendWrBuilder::send(post_slot_idx as u64, &sge, leftover_credits, true);
                        unsafe { self.shared.qp.post_send(builder.as_raw_mut()) }.is_ok()
                    };
                    if posted {
                        let mut drain_guard = InFlightGuard {
                            qp: &self.shared.qp,
                            healthy: &self.shared.healthy,
                            completed: false,
                        };
                        let _ = self.send_poller.poll_one(self.timeout).await;
                        drain_guard.completed = true;
                    }
                }
                Ok(())
            }
            Err(e) => {
                self.flow_control.add_remote_credits(1, self.max_send_wr);
                self.shared.healthy.store(false, Ordering::Release);
                Err(e)
            }
        }
    }

    /// Posts a 0-byte flow control credit grant packet to peer without transmitting user payload.
    /// Operates as a non-blocking pipelined flush to return credits continuously ("vừa nhận vừa xả")
    /// without blocking the caller's receive pipeline on CQ completion.
    pub async fn try_send_credits(&self) -> Result<(), RdmaError> {
        let pending_credits = self.flow_control.take_pending_replenishment();
        if pending_credits == 0 {
            return Ok(());
        }

        let _guard = match self.send_lock.try_lock() {
            Ok(guard) => guard,
            Err(_) => {
                // If pending credits accumulate beyond emergency threshold (2x),
                // escalate to short cooperative lock to guarantee peer is not starved (Fixes Scenario 3).
                if pending_credits >= self.flow_control.replenish_threshold * 2 {
                    match tokio::time::timeout(Duration::from_millis(50), self.send_lock.lock())
                        .await
                    {
                        Ok(guard) => guard,
                        Err(_) => {
                            self.flow_control.add_pending_credits(pending_credits);
                            return Ok(());
                        }
                    }
                } else {
                    // Send lock is currently held by active payload transmitter;
                    // put back pending credits so payload transmitter piggybacks them immediately.
                    self.flow_control.add_pending_credits(pending_credits);
                    return Ok(());
                }
            }
        };

        // Opportunistically drain completed Send CQ entries non-blocking
        // preventing CQ overflow during unidirectional streaming without task blocking.
        let mut wc_buf = [crate::ffi::ibv_wc::default(); 8];
        while let Ok(count) = self.shared._send_cq.poll(&mut wc_buf) {
            if count == 0 {
                break;
            }
        }

        let slot_idx =
            (self.send_slot_seq.fetch_add(1, Ordering::Relaxed) as usize) & self.send_wr_mask;
        let send_offset = slot_idx * self.send_slot_size;
        let lkey = self.shared.lkey;

        let send_ptr = unsafe { self.shared.raw_buf.add(send_offset) };
        let sge = [ibv_sge {
            addr: send_ptr as u64,
            length: 0,
            lkey,
        }];

        let mut builder = SendWrBuilder::send(slot_idx as u64, &sge, pending_credits, true);
        if let Err(e) = unsafe { self.shared.qp.post_send(builder.as_raw_mut()) } {
            self.flow_control.add_pending_credits(pending_credits);
            self.shared.healthy.store(false, Ordering::Release);
            return Err(e);
        }

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
        let _guard = self.send_lock.lock().await;
        self.flow_control.acquire_send_credit(self.timeout).await?;

        {
            let sge = [ibv_sge { addr, length, lkey }];
            let pending_credits = self.flow_control.take_pending_replenishment();
            let mut builder = SendWrBuilder::send(WR_ID_SEND, &sge, pending_credits, true);

            if let Err(e) = unsafe { self.shared.qp.post_send(builder.as_raw_mut()) } {
                self.flow_control.add_remote_credits(1, self.max_send_wr);
                self.shared.healthy.store(false, Ordering::Release);
                return Err(e);
            }
        }

        let mut in_flight = InFlightGuard {
            qp: &self.shared.qp,
            healthy: &self.shared.healthy,
            completed: false,
        };

        match self.send_poller.poll_one(self.timeout).await {
            Ok(_) => {
                in_flight.completed = true;
                // Post-Send Drain Check:
                // If receiver returned credits while send_lock was held, flush them immediately
                // before dropping send_lock so peer does not starve (Fixes Scenario 3).
                let leftover_credits = self.flow_control.take_pending_replenishment();
                if leftover_credits > 0 {
                    let post_slot_idx = (self.send_slot_seq.fetch_add(1, Ordering::Relaxed)
                        as usize)
                        & self.send_wr_mask;
                    let post_send_offset = post_slot_idx * self.send_slot_size;
                    let posted = {
                        let post_send_ptr = unsafe { self.shared.raw_buf.add(post_send_offset) };
                        let sge = [ibv_sge {
                            addr: post_send_ptr as u64,
                            length: 0,
                            lkey: self.shared.lkey,
                        }];
                        let mut builder =
                            SendWrBuilder::send(post_slot_idx as u64, &sge, leftover_credits, true);
                        unsafe { self.shared.qp.post_send(builder.as_raw_mut()) }.is_ok()
                    };
                    if posted {
                        let mut drain_guard = InFlightGuard {
                            qp: &self.shared.qp,
                            healthy: &self.shared.healthy,
                            completed: false,
                        };
                        let _ = self.send_poller.poll_one(self.timeout).await;
                        drain_guard.completed = true;
                    }
                }
                Ok(())
            }
            Err(e) => {
                self.flow_control.add_remote_credits(1, self.max_send_wr);
                self.shared.healthy.store(false, Ordering::Release);
                Err(e)
            }
        }
    }

    /// Sends an explicit EOF shutdown frame to peer, piggybacking any remaining flow-control credits.
    /// Informs the remote receiver to terminate gracefully (yielding `Ok(Bytes::new())`)
    /// without waiting for CQ poll timeout (Fixes Scenario 5).
    pub async fn shutdown(&self) -> Result<(), RdmaError> {
        let _guard = self.send_lock.lock().await;
        let pending_credits = self.flow_control.take_pending_replenishment();
        let imm_data = RDMA_FLAG_EOF | (pending_credits & RDMA_CREDIT_MASK);

        let slot_idx =
            (self.send_slot_seq.fetch_add(1, Ordering::Relaxed) as usize) & self.send_wr_mask;
        let send_offset = slot_idx * self.send_slot_size;
        let lkey = self.shared.lkey;

        let post_res = {
            let send_ptr = unsafe { self.shared.raw_buf.add(send_offset) };
            let sge = [ibv_sge {
                addr: send_ptr as u64,
                length: 0,
                lkey,
            }];
            let mut builder = SendWrBuilder::send(slot_idx as u64, &sge, imm_data, true);
            unsafe { self.shared.qp.post_send(builder.as_raw_mut()) }
        };
        if let Err(e) = post_res {
            self.shared.healthy.store(false, Ordering::Release);
            return Err(e);
        }

        let mut in_flight = InFlightGuard {
            qp: &self.shared.qp,
            healthy: &self.shared.healthy,
            completed: false,
        };
        let res = self.send_poller.poll_one(self.timeout).await;
        in_flight.completed = true;
        res.map(|_| ())
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

        let _guard = self.send_lock.lock().await;

        let slot_idx = (self.send_slot_seq.fetch_add(1, Ordering::Relaxed) as usize)
            % (self.max_send_wr as usize);
        let send_offset = slot_idx * self.send_slot_size;
        let lkey = self.shared.lkey;

        unsafe {
            let send_ptr = self.shared.raw_buf.add(send_offset);
            velda_core::prefetch_nta_range(data.as_ptr(), data.len().min(256));
            let dst_slice = std::slice::from_raw_parts_mut(send_ptr, data.len());
            (*velda_core::COPY_NON_TEMPORAL_FN)(data, dst_slice);

            let sge = [ibv_sge {
                addr: send_ptr as u64,
                length: data.len() as u32,
                lkey,
            }];

            let mut builder =
                SendWrBuilder::rdma_write(slot_idx as u64, &sge, remote_addr, rkey, true);
            if let Err(e) = self.shared.qp.post_send(builder.as_raw_mut()) {
                self.shared.healthy.store(false, Ordering::Release);
                return Err(e);
            }
        }

        let mut in_flight = InFlightGuard {
            qp: &self.shared.qp,
            healthy: &self.shared.healthy,
            completed: false,
        };

        match self.send_poller.poll_one(self.timeout).await {
            Ok(_) => {
                in_flight.completed = true;
                Ok(())
            }
            Err(e) => {
                self.shared.healthy.store(false, Ordering::Release);
                Err(e)
            }
        }
    }

    /// Checks whether the hardware channel is healthy.
    #[inline(always)]
    pub fn is_healthy(&self) -> bool {
        self.shared.healthy.load(Ordering::Acquire)
    }

    /// Marks the channel as broken, preventing reuse in connection pools.
    #[inline(always)]
    pub fn mark_broken(&self) {
        self.shared.healthy.store(false, Ordering::Release);
    }
}

/// Independent receive flow handle for hardware RDMA Queue Pair.
pub struct HardwareReceiver {
    pub(crate) shared: Arc<HardwareChannelShared>,
    pub(crate) recv_poller: AdaptiveCqPoller,
    pub(crate) flow_control: Arc<CreditFlowControl>,
    pub(crate) recv_lock: AsyncMutex<()>,
    pub(crate) max_send_wr: u32,
    pub(crate) max_recv_wr: u32,
    pub(crate) recv_wr_mask: usize,
    pub(crate) recv_slot_size: usize,
    pub(crate) credit_sender: RwLock<Option<Weak<HardwareSender>>>,
}

unsafe impl Send for HardwareReceiver {}
unsafe impl Sync for HardwareReceiver {}

impl HardwareReceiver {
    /// Receives data from the RDMA Queue Pair using caller-specified timeout.
    pub async fn recv(&self, timeout: Duration) -> Result<Bytes, RdmaError> {
        let _guard = self.recv_lock.lock().await;

        loop {
            let wc = match self.recv_poller.poll_one(timeout).await {
                Ok(wc) => wc,
                Err(e) => {
                    self.shared.healthy.store(false, Ordering::Release);
                    return Err(e);
                }
            };

            let slot_idx = (wc.wr_id as usize) & self.recv_wr_mask;
            let rx_offset = (self.max_send_wr as usize + slot_idx) * self.recv_slot_size;
            let rx_ptr = unsafe { self.shared.raw_buf.add(rx_offset) };

            let byte_len = wc.byte_len as usize;
            let safe_len = byte_len.min(self.recv_slot_size);

            // Prefetch inbound DMA buffer at L3 LLC (_MM_HINT_T2 / pldl3keep) where PCIe DDIO
            // deposits frames, preventing 64KB RX payloads from evicting L1D router or L2 headers.
            velda_core::prefetch_l3_range(rx_ptr, safe_len.min(256));

            // Only read imm_data if IBV_WC_WITH_IMM flag is set, and saturate at max_recv_wr.
            // Also inspect RDMA_FLAG_EOF bit to detect clean connection shutdown (Fixes Scenario 5).
            let is_eof = if (wc.wc_flags & crate::ffi::IBV_WC_WITH_IMM) != 0 {
                let is_eof_frame = (wc.imm_data & RDMA_FLAG_EOF) != 0;
                let credits = wc.imm_data & RDMA_CREDIT_MASK;
                if credits > 0 {
                    self.flow_control
                        .add_remote_credits(credits, self.max_recv_wr);
                    tracing::trace!(credits, "Replenished remote send credits via imm_data");
                }
                is_eof_frame
            } else {
                false
            };

            let payload = if safe_len > 0 {
                let rx_slice = unsafe { std::slice::from_raw_parts(rx_ptr, safe_len) };
                Bytes::copy_from_slice(rx_slice)
            } else {
                Bytes::new()
            };

            // Re-post receive buffer to RQ to maintain receiver window in an isolated scope
            // so non-Send ibv_recv_wr is dropped before any downstream .await points
            {
                let sge = [ibv_sge {
                    addr: unsafe { self.shared.raw_buf.add(rx_offset) } as u64,
                    length: self.recv_slot_size as u32,
                    lkey: self.shared.lkey,
                }];
                let mut rwr = build_recv_wr(slot_idx as u64, &sge);
                if let Err(e) = unsafe { self.shared.qp.post_recv(&mut rwr) } {
                    self.shared.healthy.store(false, Ordering::Release);
                    tracing::error!(
                        slot = slot_idx,
                        error = %e,
                        "Failed to re-post receive buffer to RDMA RQ"
                    );
                }
            }

            // Fix Issue 2: Transparent credit replenishment, clean EOF, and pure flow-control handling
            if safe_len > 0 {
                if self.flow_control.on_recv_consumed() {
                    let maybe_sender = self.credit_sender.read().as_ref().and_then(|w| w.upgrade());
                    if let Some(sender) = maybe_sender {
                        let _ = sender.try_send_credits().await;
                    }
                }
                return Ok(payload);
            } else if is_eof {
                // Peer cleanly closed connection via RDMA_FLAG_EOF frame (Fixes Scenario 5)
                tracing::debug!("Received clean RDMA EOF shutdown frame from peer");
                return Ok(Bytes::new());
            } else {
                // Pure flow-control frame (0 payload bytes): credits already replenished above.
                // Continue loop to await actual user payload frame.
                tracing::trace!("Received pure RDMA flow-control credit update frame");
            }
        }
    }

    /// Checks whether the hardware channel is healthy.
    #[inline(always)]
    pub fn is_healthy(&self) -> bool {
        self.shared.healthy.load(Ordering::Acquire)
    }

    /// Marks the channel as broken, preventing reuse in connection pools.
    #[inline(always)]
    pub fn mark_broken(&self) {
        self.shared.healthy.store(false, Ordering::Release);
    }
}

/// Hardware-backed RDMA channel holding registered memory buffers and QP.
#[derive(Clone)]
pub struct HardwareChannel {
    pub sender: Arc<HardwareSender>,
    pub receiver: Arc<HardwareReceiver>,
}

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
        if config.initial_credits > config.max_recv_wr {
            return Err(RdmaError::Config(
                "initial_credits cannot exceed max_recv_wr".into(),
            ));
        }

        let send_poller = AdaptiveCqPoller::new(send_cq.clone(), config.poll_spin_count)?;
        let recv_poller = AdaptiveCqPoller::new(recv_cq.clone(), config.poll_spin_count)?;
        let flow_control = Arc::new(CreditFlowControl::new(config.initial_credits));

        let slot_size = DEFAULT_CHANNEL_SLOT_SIZE;
        let total_slots = (config.max_send_wr as usize)
            .checked_add(config.max_recv_wr as usize)
            .ok_or_else(|| RdmaError::Config("Total WR slot count overflow".into()))?;
        let total_bytes = total_slots
            .checked_mul(slot_size)
            .ok_or_else(|| RdmaError::Config("Buffer size overflow".into()))?;

        let mut host_buf = vec![0u8; total_bytes];
        let raw_buf = host_buf.as_mut_ptr();
        let buf_len = host_buf.len();

        let mr = MemoryRegion::register_slice(pd, &mut host_buf)?;
        std::mem::forget(host_buf);
        let lkey = mr.lkey();

        let shared = Arc::new(HardwareChannelShared {
            qp: qp.clone(),
            _send_cq: send_cq,
            _recv_cq: recv_cq,
            mr: Some(mr),
            raw_buf,
            buf_len,
            lkey,
            healthy: AtomicBool::new(true),
        });

        for i in 0..config.initial_credits as usize {
            let offset = (config.max_send_wr as usize + i) * slot_size;
            let recv_ptr = unsafe { raw_buf.add(offset) };
            let sge = [ibv_sge {
                addr: recv_ptr as u64,
                length: slot_size as u32,
                lkey,
            }];

            let mut rwr = build_recv_wr(i as u64, &sge);
            unsafe { qp.post_recv(&mut rwr) }?;
        }

        let send_wr_mask = (config.max_send_wr.max(1).next_power_of_two() - 1) as usize;
        let recv_wr_mask = (config.max_recv_wr.max(1).next_power_of_two() - 1) as usize;

        let sender = Arc::new(HardwareSender {
            shared: shared.clone(),
            send_poller,
            flow_control: flow_control.clone(),
            send_lock: AsyncMutex::new(()),
            max_send_wr: config.max_send_wr,
            send_wr_mask,
            send_slot_size: slot_size,
            timeout: Duration::from_millis(config.timeout_ms),
            send_slot_seq: AtomicU64::new(0),
        });

        let receiver = Arc::new(HardwareReceiver {
            shared,
            recv_poller,
            flow_control,
            recv_lock: AsyncMutex::new(()),
            max_send_wr: config.max_send_wr,
            max_recv_wr: config.max_recv_wr,
            recv_wr_mask,
            recv_slot_size: slot_size,
            credit_sender: RwLock::new(Some(Arc::downgrade(&sender))),
        });

        Ok(Self { sender, receiver })
    }

    /// Splits the hardware channel into independent sender and receiver handles.
    pub fn split(&self) -> (Arc<HardwareSender>, Arc<HardwareReceiver>) {
        (self.sender.clone(), self.receiver.clone())
    }

    /// Consumes the channel, yielding independent sender and receiver handles.
    pub fn into_split(self) -> (Arc<HardwareSender>, Arc<HardwareReceiver>) {
        (self.sender, self.receiver)
    }

    /// Checks whether the hardware channel is healthy.
    #[inline(always)]
    pub fn is_healthy(&self) -> bool {
        self.sender.is_healthy() && self.receiver.is_healthy()
    }

    /// Marks the channel as broken, preventing reuse in connection pools.
    #[inline(always)]
    pub fn mark_broken(&self) {
        self.sender.mark_broken();
        self.receiver.mark_broken();
    }
}
