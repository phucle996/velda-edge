//! Credit-Based Flow Control — Eliminating Receiver Not Ready (RNR) Hardware Drops.
//!
//! ### The Hardware RNR Problem in RDMA
//! Unlike TCP which buffers incoming packets in arbitrary OS kernel queues when user-space is slow,
//! RDMA two-sided Send/Receive semantics require that a matching Receive Buffer (`ibv_recv_wr`) is
//! **ALREADY posted into the receiver's hardware Receive Queue (RQ) BEFORE the packet arrives on the wire**.
//!
//! If an inbound packet hits an HCA with an empty RQ, the receiver's NIC generates a hardware NACK:
//! **Receiver Not Ready (RNR)**. After exhausting `rnr_retry` attempts, the hardware permanently aborts
//! the Queue Pair (`IBV_WC_RNR_RETRY_EXC_ERR`), breaking the connection.
//!
//! ### Credit-Based Flow Control Mechanism
//! 1. **Initial Credit Grant**: Both peers agree on an initial credit allotment (e.g. 64 credits).
//!    The local receiver pre-posts 64 Receive Buffers into its hardware RQ via `ibv_post_recv`.
//! 2. **Sender Credit Check**: Before submitting any Send WR, the local sender acquires a credit via
//!    `remote_send_credits.fetch_sub(1)`. If credits reach 0, transmission blocks cooperatively rather
//!    than risking an RNR hardware drop.
//! 3. **Batch Credit Replenishment**: As the local receiver consumes incoming messages and reaches the
//!    replenishment threshold (`credits / 2`), it posts fresh Receive Buffers back into the RQ and
//!    piggybacks a credit update grant to the remote peer (via `imm_data` in SendWithImm).
//! 4. **Lock-Free Atomic Tracking**: All credit operations execute via `AtomicU32` with relaxed/acquire/release
//!    semantics, ensuring zero mutex contention on the hot path.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;
use tokio::sync::Notify;

use crate::error::RdmaError;

/// Manages send and receive credits for an active RDMA connection.
///
/// In RDMA Reliable Connected (RC) mode, posting a Send WR when the receiver has not
/// posted a corresponding Receive WR causes an immediate hardware RNR error.
/// CreditFlowControl guarantees that the local sender never posts more Work Requests
/// than the remote receiver has pre-posted Receive Buffers.
pub struct CreditFlowControl {
    /// Send credits granted by the remote endpoint (decremented on each Send WR).
    remote_send_credits: AtomicU32,

    /// Number of pre-posted receive buffers currently active in our local Receive Queue.
    local_posted_recvs: AtomicU32,

    /// Number of received buffers consumed by the application since the last credit replenishment sent to peer.
    unreplenished_recvs: AtomicU32,

    /// Threshold of consumed receives that triggers sending a credit update to peer.
    pub replenish_threshold: u32,

    /// Cooperative notification handle waking sender tasks when remote peer grants credits.
    notify: Arc<Notify>,
}

impl CreditFlowControl {
    /// Initializes flow control with an initial credit allotment.
    ///
    /// Configures early low-watermark replenishment (`replenish_threshold = 16..32`)
    /// so credits are continuously returned while the in-flight window remains 75-90% full.
    pub fn new(initial_credits: u32) -> Self {
        let threshold = (initial_credits / 8)
            .clamp(8, 32)
            .min((initial_credits / 2).max(1));
        Self {
            remote_send_credits: AtomicU32::new(initial_credits),
            local_posted_recvs: AtomicU32::new(initial_credits),
            unreplenished_recvs: AtomicU32::new(0),
            replenish_threshold: threshold,
            notify: Arc::new(Notify::new()),
        }
    }

    /// Acquires 1 send credit, waiting cooperatively if credits are temporarily exhausted.
    ///
    /// - **Fast-path**: Atomic CAS decrement. If credits > 0, returns immediately without yielding.
    /// - **Cold-path (Safety Net)**: If credits reach 0 during high-load bursts, cooperatively awaits
    ///   replenishment notification from peer via `tokio::sync::Notify` instead of dropping the connection.
    pub async fn acquire_send_credit(&self, timeout: Duration) -> Result<(), RdmaError> {
        // Fast-path: Instant lock-free CAS attempt
        if self.try_acquire_send_credit().is_ok() {
            return Ok(());
        }

        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let notified = self.notify.notified();
            // Re-check after registering listener to eliminate race condition
            if self.try_acquire_send_credit().is_ok() {
                return Ok(());
            }

            let now = tokio::time::Instant::now();
            if now >= deadline {
                return Err(RdmaError::InsufficientCredits {
                    available: 0,
                    required: 1,
                });
            }

            let remaining = deadline - now;
            match tokio::time::timeout(remaining, notified).await {
                Ok(()) => {
                    if self.try_acquire_send_credit().is_ok() {
                        return Ok(());
                    }
                }
                Err(_) => {
                    return Err(RdmaError::Timeout(
                        "Timed out waiting for remote RDMA send credits".into(),
                    ));
                }
            }
        }
    }

    /// Non-blocking check attempting to acquire 1 send credit immediately.
    ///
    /// Fails immediately if the remote peer has no available receive buffers.
    #[inline(always)]
    pub fn try_acquire_send_credit(&self) -> Result<(), RdmaError> {
        let mut current = self.remote_send_credits.load(Ordering::Acquire);
        loop {
            if current == 0 {
                return Err(RdmaError::InsufficientCredits {
                    available: 0,
                    required: 1,
                });
            }
            match self.remote_send_credits.compare_exchange_weak(
                current,
                current - 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(()),
                Err(actual) => current = actual,
            }
        }
    }

    /// Replenishes send credits when remote peer informs us of newly posted receive buffers.
    ///
    /// Uses saturating addition and clamps to `max_credits` to protect against integer
    /// overflow and malicious imm_data tampering. Immediately wakes waiting senders.
    #[inline(always)]
    pub fn add_remote_credits(&self, credits: u32, max_credits: u32) {
        if credits == 0 {
            return;
        }
        let mut current = self.remote_send_credits.load(Ordering::Acquire);
        loop {
            let next = current.saturating_add(credits).min(max_credits);
            match self.remote_send_credits.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.notify.notify_waiters();
                    break;
                }
                Err(actual) => current = actual,
            }
        }
    }

    /// Records that the local application consumed a received buffer.
    ///
    /// Returns true if unreplenished recvs has reached or exceeded replenish_threshold.
    /// Does NOT reset the count; only `take_pending_replenishment` resets it, preventing
    /// credit vanishing on high-throughput streaming.
    #[inline(always)]
    pub fn on_recv_consumed(&self) -> bool {
        let count = self.unreplenished_recvs.fetch_add(1, Ordering::AcqRel) + 1;
        count >= self.replenish_threshold
    }

    /// Extracts any accumulated receive buffer completions that have not yet been signaled
    /// to the remote peer, resetting the unreplenished counter to 0.
    #[inline(always)]
    pub fn take_pending_replenishment(&self) -> u32 {
        self.unreplenished_recvs.swap(0, Ordering::AcqRel)
    }

    /// Re-queues pending credits if an attempt to send them was deferred.
    #[inline(always)]
    pub fn add_pending_credits(&self, credits: u32) {
        self.unreplenished_recvs
            .fetch_add(credits, Ordering::Release);
    }

    /// Returns the currently available send credits.
    #[inline(always)]
    pub fn available_send_credits(&self) -> u32 {
        self.remote_send_credits.load(Ordering::Relaxed)
    }

    /// Returns the count of receive buffers currently posted locally.
    #[inline(always)]
    pub fn local_posted_recvs(&self) -> u32 {
        self.local_posted_recvs.load(Ordering::Relaxed)
    }
}
