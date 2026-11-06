//! # Work Request (WR) Construction and Scatter-Gather Vectoring
//!
//! ## Architectural Invariants & Hardware Mechanism
//!
//! In RDMA programming, communication operations are dispatched to the HCA through Work Requests (WRs)
//! placed onto the Queue Pair's Send Queue (SQ) or Receive Queue (RQ). Each WR references one or more
//! Scatter-Gather Elements (`ibv_sge`), which describe memory segments by their virtual address, byte
//! length, and local memory key (`lkey`).
//!
//! ### Zero-Allocation SGE Vectoring
//! Heap allocations on the hot request forwarding path introduce latency jitter and memory fragmentation.
//! `SendWrBuilder` and `RecvWr` constructors are designed to operate strictly with stack-allocated SGE
//! slices. The HCA hardware reads these SGE descriptors via DMA from host memory, performing scatter
//! (assembly from multiple buffers) or gather (disassembly into destination buffers) without CPU intervention.
//!
//! ### Two-Sided vs. One-Sided Operations
//! This module supports both RDMA communication paradigms:
//! - **Two-Sided Operations (`IBV_WR_SEND`, `IBV_WR_SEND_WITH_IMM`)**:
//!   The sender posts a Send WR; the receiver must have a matching Receive WR pre-posted on its RQ.
//!   The immediate data variant (`IBV_WR_SEND_WITH_IMM`) transmits an out-of-band 32-bit integer directly
//!   within the hardware completion packet, which Velda Edge uses to synchronize flow-control credits
//!   without writing to remote host RAM.
//! - **One-Sided Operations (`IBV_WR_RDMA_WRITE`, `IBV_WR_RDMA_READ`)**:
//!   The sender directly manipulates the remote host's registered memory addresses using the remote key (`rkey`).
//!   The remote CPU and operating system are completely bypassed and remain unaware of the transfer.
//!
//! ### Receive Queue Pre-Posting
//! To prevent hardware Receiver Not Ready (RNR) NAKs, the Receive Queue must continuously contain posted
//! Receive WRs (`ibv_recv_wr`). This module provides helpers to bulk-post receive buffers to ensure the
//! HCA can immediately deposit incoming frames upon physical wire arrival.

use std::os::raw::c_int;

use crate::ffi::{
    IBV_SEND_SIGNALED, IBV_WR_RDMA_READ, IBV_WR_RDMA_WRITE, IBV_WR_RDMA_WRITE_WITH_IMM,
    IBV_WR_SEND, IBV_WR_SEND_WITH_IMM, ibv_recv_wr, ibv_send_wr, ibv_send_wr_rdma, ibv_sge,
};

/// Builder for constructing zero-allocation RDMA Send Work Requests.
pub struct SendWrBuilder<'a> {
    wr: ibv_send_wr,
    _sge: &'a [ibv_sge],
}

impl<'a> SendWrBuilder<'a> {
    /// Constructs a basic RDMA Send WR.
    pub fn send(wr_id: u64, sge: &'a [ibv_sge], signaled: bool) -> Self {
        let flags = if signaled { IBV_SEND_SIGNALED } else { 0 };
        Self {
            wr: ibv_send_wr {
                wr_id,
                next: std::ptr::null_mut(),
                sg_list: sge.as_ptr() as *mut ibv_sge,
                num_sge: sge.len() as c_int,
                opcode: IBV_WR_SEND,
                send_flags: flags,
                imm_data: 0,
                wr: ibv_send_wr_rdma::default(),
            },
            _sge: sge,
        }
    }

    /// Constructs an RDMA Send with Immediate Data (used to piggyback credit updates).
    pub fn send_with_imm(wr_id: u64, sge: &'a [ibv_sge], imm_data: u32, signaled: bool) -> Self {
        let flags = if signaled { IBV_SEND_SIGNALED } else { 0 };
        Self {
            wr: ibv_send_wr {
                wr_id,
                next: std::ptr::null_mut(),
                sg_list: sge.as_ptr() as *mut ibv_sge,
                num_sge: sge.len() as c_int,
                opcode: IBV_WR_SEND_WITH_IMM,
                send_flags: flags,
                imm_data,
                wr: ibv_send_wr_rdma::default(),
            },
            _sge: sge,
        }
    }

    /// Constructs a One-Sided RDMA Write WR.
    pub fn rdma_write(
        wr_id: u64,
        sge: &'a [ibv_sge],
        remote_addr: u64,
        rkey: u32,
        signaled: bool,
    ) -> Self {
        let flags = if signaled { IBV_SEND_SIGNALED } else { 0 };
        Self {
            wr: ibv_send_wr {
                wr_id,
                next: std::ptr::null_mut(),
                sg_list: sge.as_ptr() as *mut ibv_sge,
                num_sge: sge.len() as c_int,
                opcode: IBV_WR_RDMA_WRITE,
                send_flags: flags,
                imm_data: 0,
                wr: ibv_send_wr_rdma { remote_addr, rkey },
            },
            _sge: sge,
        }
    }

    /// Constructs a One-Sided RDMA Write with Immediate (used for AI tensor notifications).
    pub fn rdma_write_with_imm(
        wr_id: u64,
        sge: &'a [ibv_sge],
        remote_addr: u64,
        rkey: u32,
        imm_data: u32,
        signaled: bool,
    ) -> Self {
        let flags = if signaled { IBV_SEND_SIGNALED } else { 0 };
        Self {
            wr: ibv_send_wr {
                wr_id,
                next: std::ptr::null_mut(),
                sg_list: sge.as_ptr() as *mut ibv_sge,
                num_sge: sge.len() as c_int,
                opcode: IBV_WR_RDMA_WRITE_WITH_IMM,
                send_flags: flags,
                imm_data,
                wr: ibv_send_wr_rdma { remote_addr, rkey },
            },
            _sge: sge,
        }
    }

    /// Constructs a One-Sided RDMA Read WR.
    pub fn rdma_read(
        wr_id: u64,
        sge: &'a [ibv_sge],
        remote_addr: u64,
        rkey: u32,
        signaled: bool,
    ) -> Self {
        let flags = if signaled { IBV_SEND_SIGNALED } else { 0 };
        Self {
            wr: ibv_send_wr {
                wr_id,
                next: std::ptr::null_mut(),
                sg_list: sge.as_ptr() as *mut ibv_sge,
                num_sge: sge.len() as c_int,
                opcode: IBV_WR_RDMA_READ,
                send_flags: flags,
                imm_data: 0,
                wr: ibv_send_wr_rdma { remote_addr, rkey },
            },
            _sge: sge,
        }
    }

    /// Returns a raw pointer to the inner `ibv_send_wr`.
    #[inline(always)]
    pub fn as_raw_mut(&mut self) -> *mut ibv_send_wr {
        &mut self.wr
    }
}

/// Helper to build an RDMA Receive Work Request.
#[inline(always)]
pub fn build_recv_wr(wr_id: u64, sge: &[ibv_sge]) -> ibv_recv_wr {
    ibv_recv_wr {
        wr_id,
        next: std::ptr::null_mut(),
        sg_list: sge.as_ptr() as *mut ibv_sge,
        num_sge: sge.len() as c_int,
    }
}
