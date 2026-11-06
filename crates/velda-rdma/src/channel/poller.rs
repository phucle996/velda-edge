//! # Adaptive Hybrid Completion Queue Poller (Fast-Path Busy-Spin + Slow-Path AsyncFd Sleep)
//!
//! ## Architectural Invariants & Hardware Mechanism
//!
//! Completion Queue (CQ) polling is the single most critical performance junction in kernel-bypass
//! RDMA architectures. Traditional network stacks incur substantial kernel interrupt processing,
//! context switching, and socket buffer copies. While direct RDMA polling interrogates the HCA completion
//! ring buffer without kernel context switches, a pure busy-poll loop burns 100% CPU on worker cores even during
//! idle traffic periods.
//!
//! To resolve this latency-versus-efficiency tension, `AdaptiveCqPoller` implements a two-stage
//! hybrid completion harvesting mechanism:
//!
//! 1. **Fast-Path Busy-Spin (Direct HCA Interrogation)**:
//!    - Upon dispatch, the poller immediately interrogates the hardware CQ ring buffer up to
//!      `spin_count` iterations using `ibv_poll_cq`.
//!    - Under sustained or bursty edge traffic, completions are harvested instantly on the first
//!      few spins without yielding the CPU thread or invoking kernel syscalls.
//!
//! 2. **Slow-Path Event-Driven Asynchronous Sleep (0% idle CPU burn)**:
//!    - When the spin count is exhausted without harvesting a completion, the poller arms the
//!      hardware completion channel via `ibv_req_notify_cq(cq, 0)`.
//!    - It then registers the completion channel's file descriptor (`ibv_comp_channel.fd`) with
//!      Tokio's reactor via `tokio::io::unix::AsyncFd`.
//!    - The worker task yields execution back to the Tokio runtime. When incoming packets arrive,
//!      the HCA fires an MSI-X hardware interrupt, waking Tokio's epoll driver.
//!    - The poller acknowledges the event via `ibv_get_cq_event` and `ibv_ack_cq_events`, re-arms
//!      the channel, and retrieves the Work Completion (`ibv_wc`).
//!
//! ## Core Sharding & Thread Affinity
//! In accordance with `AGENTS.md` Section 2.9, Completion Queues are sharded 1:1 per worker core
//! (`lane = worker_id`). Because each worker thread polls its own dedicated hardware CQ, there is
//! zero cross-core cache line bouncing or lock contention.

use std::sync::Arc;
use std::time::Duration;
use tokio::io::unix::AsyncFd;

use crate::error::RdmaError;
use crate::ffi::{IBV_WC_SUCCESS, ibv_wc};
use crate::verbs::cq::CompletionQueue;

/// Adaptive hybrid poller that balances direct completion harvesting under load against zero CPU burn when idle.
pub struct AdaptiveCqPoller {
    cq: Arc<CompletionQueue>,
    spin_count: u32,
    async_fd: Option<AsyncFd<std::os::fd::RawFd>>,
}

impl AdaptiveCqPoller {
    /// Creates an adaptive poller for the given Completion Queue.
    pub fn new(cq: Arc<CompletionQueue>, spin_count: u32) -> Result<Self, RdmaError> {
        let async_fd = match cq.channel_fd() {
            Some(fd) => match AsyncFd::new(fd) {
                Ok(afd) => Some(afd),
                Err(e) => {
                    tracing::warn!(error = %e, "Failed to register CQ event channel with Tokio AsyncFd; falling back to busy-poll only");
                    None
                }
            },
            None => None,
        };

        Ok(Self {
            cq,
            spin_count,
            async_fd,
        })
    }

    /// Polls for at least one completed Work Completion matching the given timeout.
    ///
    /// 1. Fast-Path: Spins up to `spin_count` iterations directly querying hardware CQ.
    ///    If a completion is found, returns immediately without entering kernel sleep or yielding the thread.
    /// 2. Slow-Path: Arms CQ notification (`ibv_req_notify_cq`) and awaits Tokio `AsyncFd`.
    ///    Wakes up on kernel hardware interrupt, acknowledges the event, and retrieves the completion.
    pub async fn poll_one(&self, timeout: Duration) -> Result<ibv_wc, RdmaError> {
        let mut wc_buf = [ibv_wc::default(); 1];

        // 1. Fast-Path: Direct hardware poll without syscalls
        for _ in 0..self.spin_count {
            let count = self.cq.poll(&mut wc_buf)?;
            if count > 0 {
                let wc = wc_buf[0];
                if wc.status != IBV_WC_SUCCESS {
                    return Err(RdmaError::Verbs(
                        format!("Work completion failed with status {}", wc.status),
                        wc.status as i32,
                    ));
                }
                return Ok(wc);
            }
            std::hint::spin_loop();
        }

        // If no AsyncFd was configured, fallback to polling loop with short yield
        let async_fd = match &self.async_fd {
            Some(fd) => fd,
            None => {
                let deadline = tokio::time::Instant::now() + timeout;
                while tokio::time::Instant::now() < deadline {
                    let count = self.cq.poll(&mut wc_buf)?;
                    if count > 0 {
                        let wc = wc_buf[0];
                        if wc.status != IBV_WC_SUCCESS {
                            return Err(RdmaError::Verbs(
                                format!("Work completion failed with status {}", wc.status),
                                wc.status as i32,
                            ));
                        }
                        return Ok(wc);
                    }
                    tokio::task::yield_now().await;
                }
                return Err(RdmaError::Timeout("CQ polling timed out".into()));
            }
        };

        // 2. Slow-Path: Arm notification and await hardware interrupt on Tokio event loop
        let timeout_fut = tokio::time::sleep(timeout);
        tokio::pin!(timeout_fut);

        loop {
            // Arm notification before sleeping
            self.cq.req_notify(false)?;

            // Double check CQ in case an event completed between poll and notify
            let count = self.cq.poll(&mut wc_buf)?;
            if count > 0 {
                let wc = wc_buf[0];
                if wc.status != IBV_WC_SUCCESS {
                    return Err(RdmaError::Verbs(
                        format!("Work completion failed with status {}", wc.status),
                        wc.status as i32,
                    ));
                }
                return Ok(wc);
            }

            // Await notification event from kernel without burning CPU
            tokio::select! {
                biased;
                _ = &mut timeout_fut => {
                    return Err(RdmaError::Timeout("CQ event notification timed out".into()));
                }
                guard = async_fd.readable() => {
                    match guard {
                        Ok(mut g) => {
                            // Acknowledge event
                            let _ = self.cq.get_and_ack_event();
                            g.clear_ready();

                            // Poll completed entry
                            let count = self.cq.poll(&mut wc_buf)?;
                            if count > 0 {
                                let wc = wc_buf[0];
                                if wc.status != IBV_WC_SUCCESS {
                                    return Err(RdmaError::Verbs(
                                        format!("Work completion failed with status {}", wc.status),
                                        wc.status as i32,
                                    ));
                                }
                                return Ok(wc);
                            }
                        }
                        Err(e) => {
                            return Err(RdmaError::Io(e));
                        }
                    }
                }
            }
        }
    }
}
