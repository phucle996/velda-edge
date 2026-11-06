//! RDMA Queue Pair (QP) — Reliable Connected (RC) State Machine & Work Request Engine.
//!
//! ### Hardware Queue Pair Architecture & State Machine
//! 1. **Dual Queue Mechanics**: A Queue Pair is the fundamental bi-directional communication endpoint
//!    in RDMA, consisting of two hardware-managed circular rings:
//!    - **Send Queue (SQ)**: Houses outbound Work Requests (Send, SendWithImm, RDMA Write, RDMA Read).
//!    - **Receive Queue (RQ)**: Houses pre-posted Receive Buffers (`ibv_post_recv`) waiting to catch inbound DMA.
//! 2. **Hardware State Transitions**:
//!    - `RESET`: Uninitialized hardware state.
//!    - `INIT`: Configures local port, PKey index, and access flags (`IBV_ACCESS_LOCAL_WRITE`). RQ is now ready.
//!    - `RTR` (Ready to Receive): Binds to remote endpoint parameters (`remote_qpn`, `rq_psn`, MTU, GID/LID).
//!      The NIC can now accept inbound packets on the wire.
//!    - `RTS` (Ready to Send): Sets local Send Queue packet sequence number (`sq_psn`), retry counters (`RETRY_CNT`),
//!      timeout, and Receiver-Not-Ready retry limits (`RNR_RETRY`). Both sending and receiving are fully active.
//! 3. **Out-of-Band (OOB) Addressing ([`QpAddress`])**:
//!    Queue Pairs cannot discover each other autonomously. Endpoints exchange their 32-byte `QpAddress`
//!    (QPN, PSN, LID, GID) over an initial TCP channel to execute the state transition before cutting over
//!    to direct hardware communication.
//! 4. **Strict RAII Lifecycle**: On drop, `ibv_destroy_qp` flushes all pending work queues and frees
//!    hardware descriptors in the HCA driver.

use std::os::raw::c_int;
use std::sync::Arc;

use crate::error::RdmaError;
use crate::ffi::{
    IBV_ACCESS_LOCAL_WRITE, IBV_ACCESS_REMOTE_READ, IBV_ACCESS_REMOTE_WRITE, IBV_MTU_1024,
    IBV_QP_ACCESS_FLAGS, IBV_QP_AV, IBV_QP_DEST_QPN, IBV_QP_MAX_DEST_RD_ATOMIC,
    IBV_QP_MAX_QP_RD_ATOMIC, IBV_QP_MIN_RNR_TIMER, IBV_QP_PATH_MTU, IBV_QP_PKEY_INDEX, IBV_QP_PORT,
    IBV_QP_RETRY_CNT, IBV_QP_RNR_RETRY, IBV_QP_RQ_PSN, IBV_QP_SQ_PSN, IBV_QP_STATE, IBV_QP_TIMEOUT,
    IBV_QPS_INIT, IBV_QPS_RTR, IBV_QPS_RTS, IBV_QPT_RC, IbvLib, get_ibv_lib, ibv_gid, ibv_qp,
    ibv_qp_attr, ibv_qp_cap, ibv_qp_init_attr, ibv_recv_wr, ibv_send_wr,
};
use crate::verbs::cq::CompletionQueue;
use crate::verbs::pd::ProtectionDomain;

/// Addressing parameters exchanged out-of-band between RDMA endpoints to bring QP to RTS.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QpAddress {
    /// Local or Remote Queue Pair Number.
    pub qpn: u32,
    /// Packet Sequence Number (starting random 24-bit integer).
    pub psn: u32,
    /// Local Identifier (LID) for InfiniBand subnet (0 if pure RoCEv2).
    pub lid: u16,
    /// Global Identifier (GID) for RoCEv2 / IB routing.
    pub gid: ibv_gid,
}

/// RAII wrapper for an RDMA Reliable Connected (RC) Queue Pair.
pub struct QueuePair {
    raw: *mut ibv_qp,
    qp_num: u32,
    _pd: Arc<ProtectionDomain>,
    _cq: Arc<CompletionQueue>,
    lib: &'static IbvLib,
}

unsafe impl Send for QueuePair {}
unsafe impl Sync for QueuePair {}

impl QueuePair {
    /// Creates a new Reliable Connected (RC) Queue Pair tied to the given Protection Domain and CQ.
    pub fn new(
        pd: Arc<ProtectionDomain>,
        cq: Arc<CompletionQueue>,
        max_send_wr: u32,
        max_recv_wr: u32,
        max_sge: u32,
    ) -> Result<Arc<Self>, RdmaError> {
        let lib =
            get_ibv_lib().ok_or_else(|| RdmaError::Library("libibverbs not loaded".into()))?;

        let mut init_attr = ibv_qp_init_attr {
            qp_context: std::ptr::null_mut(),
            send_cq: cq.raw(),
            recv_cq: cq.raw(),
            srq: std::ptr::null_mut(),
            cap: ibv_qp_cap {
                max_send_wr,
                max_recv_wr,
                max_send_sge: max_sge,
                max_recv_sge: max_sge,
                max_inline_data: 64, // allow inline for short control messages
            },
            qp_type: IBV_QPT_RC,
            sq_sig_all: 1, // signal all completions
        };

        let qp_ptr = unsafe { (lib.create_qp)(pd.raw(), &mut init_attr) };
        if qp_ptr.is_null() {
            return Err(RdmaError::Verbs(
                "Failed to create Queue Pair (ibv_create_qp)".into(),
                std::io::Error::last_os_error().raw_os_error().unwrap_or(0),
            ));
        }

        let qp_num = unsafe { (*qp_ptr).qp_num };

        Ok(Arc::new(Self {
            raw: qp_ptr,
            qp_num,
            _pd: pd,
            _cq: cq,
            lib,
        }))
    }

    /// Queue Pair Number assigned by the local HCA.
    #[inline(always)]
    pub fn qp_num(&self) -> u32 {
        self.qp_num
    }

    /// Step 1: Transitions QP from RESET to INIT state.
    pub fn to_init(&self, port: u8) -> Result<(), RdmaError> {
        let mut attr = ibv_qp_attr {
            qp_state: IBV_QPS_INIT,
            pkey_index: 0,
            port_num: port,
            qp_access_flags: IBV_ACCESS_LOCAL_WRITE
                | IBV_ACCESS_REMOTE_WRITE
                | IBV_ACCESS_REMOTE_READ,
            ..unsafe { std::mem::zeroed() }
        };

        let mask = (IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS) as c_int;
        let ret = unsafe { (self.lib.modify_qp)(self.raw, &mut attr, mask) };

        if ret != 0 {
            return Err(RdmaError::StateTransition {
                from: "RESET",
                to: "INIT",
                reason: format!("ibv_modify_qp returned {ret}"),
            });
        }
        Ok(())
    }

    /// Step 2: Transitions QP from INIT to RTR (Ready to Receive).
    pub fn to_rtr(
        &self,
        dest: &QpAddress,
        port: u8,
        gid_index: u8,
        mtu: Option<u32>,
    ) -> Result<(), RdmaError> {
        let mut attr: ibv_qp_attr = unsafe { std::mem::zeroed() };
        attr.qp_state = IBV_QPS_RTR;
        attr.path_mtu = mtu.unwrap_or(IBV_MTU_1024);
        attr.dest_qp_num = dest.qpn;
        attr.rq_psn = dest.psn;
        attr.max_dest_rd_atomic = 1;
        attr.min_rnr_timer = 12; // 0.64 ms RNR timer

        attr.ah_attr.dlid = dest.lid;
        attr.ah_attr.sl = 0;
        attr.ah_attr.src_path_bits = 0;
        attr.ah_attr.static_rate = 0;
        attr.ah_attr.is_global = 1;
        attr.ah_attr.port_num = port;

        attr.ah_attr.grh.dgid = dest.gid;
        attr.ah_attr.grh.flow_label = 0;
        attr.ah_attr.grh.sgid_index = gid_index;
        attr.ah_attr.grh.hop_limit = 255;
        attr.ah_attr.grh.traffic_class = 0;

        let mask = (IBV_QP_STATE
            | IBV_QP_AV
            | IBV_QP_PATH_MTU
            | IBV_QP_DEST_QPN
            | IBV_QP_RQ_PSN
            | IBV_QP_MAX_DEST_RD_ATOMIC
            | IBV_QP_MIN_RNR_TIMER) as c_int;

        let ret = unsafe { (self.lib.modify_qp)(self.raw, &mut attr, mask) };
        if ret != 0 {
            return Err(RdmaError::StateTransition {
                from: "INIT",
                to: "RTR",
                reason: format!("ibv_modify_qp returned {ret}"),
            });
        }
        Ok(())
    }

    /// Step 3: Transitions QP from RTR to RTS (Ready to Send).
    pub fn to_rts(
        &self,
        sq_psn: u32,
        timeout: u8,
        retry_cnt: u8,
        rnr_retry: u8,
    ) -> Result<(), RdmaError> {
        let mut attr: ibv_qp_attr = unsafe { std::mem::zeroed() };
        attr.qp_state = IBV_QPS_RTS;
        attr.sq_psn = sq_psn;
        attr.timeout = timeout; // e.g. 14 (~4ms * 4096)
        attr.retry_cnt = retry_cnt; // e.g. 7
        attr.rnr_retry = rnr_retry; // 7 = infinite retry
        attr.max_rd_atomic = 1;

        let mask = (IBV_QP_STATE
            | IBV_QP_TIMEOUT
            | IBV_QP_RETRY_CNT
            | IBV_QP_RNR_RETRY
            | IBV_QP_SQ_PSN
            | IBV_QP_MAX_QP_RD_ATOMIC) as c_int;

        let ret = unsafe { (self.lib.modify_qp)(self.raw, &mut attr, mask) };
        if ret != 0 {
            return Err(RdmaError::StateTransition {
                from: "RTR",
                to: "RTS",
                reason: format!("ibv_modify_qp returned {ret}"),
            });
        }
        Ok(())
    }

    /// Posts a Send Work Request directly to the HCA Send Queue via PCIe Doorbell Register.
    ///
    /// # Safety
    ///
    /// The caller must guarantee that `wr` points to a valid, initialized `ibv_send_wr`
    /// chain whose memory regions remain valid and pinned for the duration of the transfer.
    #[inline(always)]
    pub unsafe fn post_send(&self, wr: *mut ibv_send_wr) -> Result<(), RdmaError> {
        let mut bad_wr = std::ptr::null_mut();
        let ret = unsafe { (self.lib.post_send)(self.raw, wr, &mut bad_wr) };
        if ret != 0 {
            Err(RdmaError::Verbs(
                "Failed to post Send WR (ibv_post_send)".into(),
                ret,
            ))
        } else {
            Ok(())
        }
    }

    /// Posts a Receive Work Request directly to the HCA Receive Queue.
    ///
    /// # Safety
    ///
    /// The caller must guarantee that `wr` points to a valid, initialized `ibv_recv_wr`
    /// chain whose memory buffers remain pinned until completed.
    #[inline(always)]
    pub unsafe fn post_recv(&self, wr: *mut ibv_recv_wr) -> Result<(), RdmaError> {
        let mut bad_wr = std::ptr::null_mut();
        let ret = unsafe { (self.lib.post_recv)(self.raw, wr, &mut bad_wr) };
        if ret != 0 {
            Err(RdmaError::Verbs(
                "Failed to post Recv WR (ibv_post_recv)".into(),
                ret,
            ))
        } else {
            Ok(())
        }
    }
}

impl Drop for QueuePair {
    fn drop(&mut self) {
        if !self.raw.is_null() {
            unsafe { (self.lib.destroy_qp)(self.raw) };
        }
    }
}
