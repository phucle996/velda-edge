//! Credit-Based Flow Control to eliminate Receiver Not Ready (RNR) hardware drops.

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
