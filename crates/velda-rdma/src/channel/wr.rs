//! Work Request (WR) construction and Scatter-Gather vectoring.

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
