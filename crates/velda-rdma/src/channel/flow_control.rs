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

use std::sync::atomic::{AtomicU32, Ordering};

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
    replenish_threshold: u32,
}

impl CreditFlowControl {
    /// Initializes flow control with an initial credit allotment.
    pub fn new(initial_credits: u32) -> Self {
        let threshold = (initial_credits / 2).max(1);
        Self {
            remote_send_credits: AtomicU32::new(initial_credits),
            local_posted_recvs: AtomicU32::new(initial_credits),
            unreplenished_recvs: AtomicU32::new(0),
            replenish_threshold: threshold,
        }
    }

    /// Acquires 1 send credit before posting a Send WR.
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
    #[inline(always)]
    pub fn add_remote_credits(&self, credits: u32) {
        self.remote_send_credits
            .fetch_add(credits, Ordering::Release);
    }

    /// Records that the local application consumed a received buffer.
    ///
    /// If the unreplenished count reaches `replenish_threshold`, returns `Some(credits_to_grant)`
    /// so the caller can piggyback the credit grant to the remote peer.
    #[inline(always)]
    pub fn on_recv_consumed(&self) -> Option<u32> {
        let count = self.unreplenished_recvs.fetch_add(1, Ordering::AcqRel) + 1;
        if count >= self.replenish_threshold {
            // Reset unreplenished count and return batch to replenish
            self.unreplenished_recvs.store(0, Ordering::Release);
            Some(count)
        } else {
            None
        }
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
