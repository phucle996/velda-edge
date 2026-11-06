//! Foreign Function Interface (FFI) bindings for Linux `libibverbs.so.1`.
//!
//! Provides dynamic runtime loading of `libibverbs.so.1` or `libibverbs.so` via `dlopen`.
//! This avoids build-time linking failures on machines without `libibverbs-dev` installed
//! and enables seamless fallback to pure-Rust software mock implementations.

use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_void};
use std::sync::OnceLock;

// -----------------------------------------------------------------------------
// Constants
// -----------------------------------------------------------------------------

pub const IBV_ACCESS_LOCAL_WRITE: u32 = 1;
pub const IBV_ACCESS_REMOTE_WRITE: u32 = 1 << 1;
pub const IBV_ACCESS_REMOTE_READ: u32 = 1 << 2;
pub const IBV_ACCESS_REMOTE_ATOMIC: u32 = 1 << 3;
pub const IBV_ACCESS_MW_BIND: u32 = 1 << 4;
pub const IBV_ACCESS_ZERO_BASED: u32 = 1 << 5;

pub const IBV_QPT_RC: u32 = 2; // Reliable Connected
pub const IBV_QPT_UC: u32 = 3; // Unreliable Connected
pub const IBV_QPT_UD: u32 = 4; // Unreliable Datagram

pub const IBV_QPS_RESET: u32 = 0;
pub const IBV_QPS_INIT: u32 = 1;
pub const IBV_QPS_RTR: u32 = 2;
pub const IBV_QPS_RTS: u32 = 3;
pub const IBV_QPS_SQD: u32 = 4;
pub const IBV_QPS_SQE: u32 = 5;
pub const IBV_QPS_ERR: u32 = 6;

pub const IBV_QP_STATE: u32 = 1 << 0;
pub const IBV_QP_PKEY_INDEX: u32 = 1 << 4;
pub const IBV_QP_PORT: u32 = 1 << 5;
pub const IBV_QP_ACCESS_FLAGS: u32 = 1 << 3;
pub const IBV_QP_AV: u32 = 1 << 7;
pub const IBV_QP_PATH_MTU: u32 = 1 << 8;
pub const IBV_QP_TIMEOUT: u32 = 1 << 9;
pub const IBV_QP_RETRY_CNT: u32 = 1 << 10;
pub const IBV_QP_RNR_RETRY: u32 = 1 << 11;
pub const IBV_QP_RQ_PSN: u32 = 1 << 12;
pub const IBV_QP_MAX_QP_RD_ATOMIC: u32 = 1 << 13;
pub const IBV_QP_MIN_RNR_TIMER: u32 = 1 << 15;
pub const IBV_QP_SQ_PSN: u32 = 1 << 16;
pub const IBV_QP_MAX_DEST_RD_ATOMIC: u32 = 1 << 17;
pub const IBV_QP_CAP: u32 = 1 << 19;
pub const IBV_QP_DEST_QPN: u32 = 1 << 20;

pub const IBV_WR_RDMA_WRITE: u32 = 0;
pub const IBV_WR_RDMA_WRITE_WITH_IMM: u32 = 1;
pub const IBV_WR_SEND: u32 = 2;
pub const IBV_WR_SEND_WITH_IMM: u32 = 3;
pub const IBV_WR_RDMA_READ: u32 = 4;

pub const IBV_SEND_FENCE: u32 = 1 << 0;
pub const IBV_SEND_SIGNALED: u32 = 1 << 1;
pub const IBV_SEND_SOLICITED: u32 = 1 << 2;
pub const IBV_SEND_INLINE: u32 = 1 << 3;

pub const IBV_WC_SUCCESS: u32 = 0;
pub const IBV_WC_LOC_LEN_ERR: u32 = 1;
pub const IBV_WC_LOC_QP_OP_ERR: u32 = 2;
pub const IBV_WC_LOC_PROT_ERR: u32 = 4;
pub const IBV_WC_WR_FLUSH_ERR: u32 = 5;
pub const IBV_WC_RETRY_EXC_ERR: u32 = 12;
pub const IBV_WC_RNR_RETRY_EXC_ERR: u32 = 13;
pub const IBV_WC_RESP_TIMEOUT_ERR: u32 = 20;

pub const IBV_MTU_1024: u32 = 3;
pub const IBV_MTU_2048: u32 = 4;
pub const IBV_MTU_4096: u32 = 5;

// -----------------------------------------------------------------------------
// C Struct Layouts
// -----------------------------------------------------------------------------

#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct ibv_sge {
    pub addr: u64,
    pub length: u32,
    pub lkey: u32,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ibv_gid {
    pub raw: [u8; 16],
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct ibv_global_route {
    pub dgid: ibv_gid,
    pub flow_label: u32,
    pub sgid_index: u8,
    pub hop_limit: u8,
    pub traffic_class: u8,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct ibv_ah_attr {
    pub grh: ibv_global_route,
    pub dlid: u16,
    pub sl: u8,
    pub src_path_bits: u8,
    pub static_rate: u8,
    pub is_global: u8,
    pub port_num: u8,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct ibv_qp_cap {
    pub max_send_wr: u32,
    pub max_recv_wr: u32,
    pub max_send_sge: u32,
    pub max_recv_sge: u32,
    pub max_inline_data: u32,
}

#[repr(C)]
pub struct ibv_device {
    pub _ops: *mut c_void,
    pub node_type: u32,
    pub transport_type: u32,
    pub name: [c_char; 64],
    pub dev_name: [c_char; 64],
    pub dev_path: [c_char; 256],
    pub ibdev_path: [c_char; 256],
}

#[repr(C)]
pub struct ibv_context {
    pub device: *mut ibv_device,
    pub ops: [usize; 32],
    pub cmd_fd: c_int,
    pub async_fd: c_int,
    pub num_comp_vectors: c_int,
}

#[repr(C)]
pub struct ibv_comp_channel {
    pub context: *mut ibv_context,
    pub fd: c_int,
    pub refcnt: c_int,
}

#[repr(C)]
pub struct ibv_pd {
    pub context: *mut ibv_context,
    pub handle: u32,
}

#[repr(C)]
pub struct ibv_mr {
    pub context: *mut ibv_context,
    pub pd: *mut ibv_pd,
    pub addr: *mut c_void,
    pub length: usize,
    pub handle: u32,
    pub lkey: u32,
    pub rkey: u32,
}

#[repr(C)]
pub struct ibv_cq {
    pub context: *mut ibv_context,
    pub channel: *mut ibv_comp_channel,
    pub cq_context: *mut c_void,
    pub handle: u32,
    pub cqe: c_int,
}

#[repr(C)]
pub struct ibv_qp_init_attr {
    pub qp_context: *mut c_void,
    pub send_cq: *mut ibv_cq,
    pub recv_cq: *mut ibv_cq,
    pub srq: *mut c_void,
    pub cap: ibv_qp_cap,
    pub qp_type: u32,
    pub sq_sig_all: c_int,
}

#[repr(C)]
pub struct ibv_qp_attr {
    pub qp_state: u32,
    pub cur_qp_state: u32,
    pub path_mtu: u32,
    pub path_mig_state: u32,
    pub qkey: u32,
    pub rq_psn: u32,
    pub sq_psn: u32,
    pub dest_qp_num: u32,
    pub qp_access_flags: u32,
    pub cap: ibv_qp_cap,
    pub ah_attr: ibv_ah_attr,
    pub alt_ah_attr: ibv_ah_attr,
    pub pkey_index: u16,
    pub alt_pkey_index: u16,
    pub en_sqd_async_notify: u8,
    pub sq_draining: u8,
    pub max_rd_atomic: u8,
    pub max_dest_rd_atomic: u8,
    pub min_rnr_timer: u8,
    pub port_num: u8,
    pub timeout: u8,
    pub retry_cnt: u8,
    pub rnr_retry: u8,
    pub alt_port_num: u8,
    pub alt_timeout: u8,
}

#[repr(C)]
pub struct ibv_qp {
    pub context: *mut ibv_context,
    pub qp_context: *mut c_void,
    pub pd: *mut ibv_pd,
    pub send_cq: *mut ibv_cq,
    pub recv_cq: *mut ibv_cq,
    pub srq: *mut c_void,
    pub handle: u32,
    pub qp_num: u32,
    pub state: u32,
    pub qp_type: u32,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct ibv_send_wr_rdma {
    pub remote_addr: u64,
    pub rkey: u32,
}

#[repr(C)]
pub struct ibv_send_wr {
    pub wr_id: u64,
    pub next: *mut ibv_send_wr,
    pub sg_list: *mut ibv_sge,
    pub num_sge: c_int,
    pub opcode: u32,
    pub send_flags: u32,
    pub imm_data: u32,
    pub wr: ibv_send_wr_rdma,
}

#[repr(C)]
pub struct ibv_recv_wr {
    pub wr_id: u64,
    pub next: *mut ibv_recv_wr,
    pub sg_list: *mut ibv_sge,
    pub num_sge: c_int,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct ibv_wc {
    pub wr_id: u64,
    pub status: u32,
    pub opcode: u32,
    pub vendor_err: u32,
    pub byte_len: u32,
    pub imm_data: u32,
    pub qp_num: u32,
    pub src_qp: u32,
    pub wc_flags: u32,
    pub pkey_index: u16,
    pub slid: u16,
    pub sl: u8,
    pub dlid_path_bits: u8,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct ibv_port_attr {
    pub state: u32,
    pub max_mtu: u32,
    pub active_mtu: u32,
    pub gid_tbl_len: c_int,
    pub port_cap_flags: u32,
    pub max_msg_sz: u32,
    pub bad_pkey_cntr: u32,
    pub qkey_viol_cntr: u32,
    pub pkey_tbl_len: u16,
    pub lid: u16,
    pub sm_lid: u16,
    pub lmc: u8,
    pub max_vl_num: u8,
    pub sm_sl: u8,
    pub subnet_timeout: u8,
    pub init_type_reply: u8,
    pub active_width: u8,
    pub active_speed: u8,
    pub phys_state: u8,
    pub link_layer: u8,
}

// -----------------------------------------------------------------------------
// Dynamic Loader Table
// -----------------------------------------------------------------------------

pub struct IbvLib {
    _handle: *mut c_void,
    pub get_device_list: unsafe extern "C" fn(num_devices: *mut c_int) -> *mut *mut ibv_device,
    pub free_device_list: unsafe extern "C" fn(list: *mut *mut ibv_device),
    pub get_device_name: unsafe extern "C" fn(device: *mut ibv_device) -> *const c_char,
    pub open_device: unsafe extern "C" fn(device: *mut ibv_device) -> *mut ibv_context,
    pub close_device: unsafe extern "C" fn(context: *mut ibv_context) -> c_int,
    pub alloc_pd: unsafe extern "C" fn(context: *mut ibv_context) -> *mut ibv_pd,
    pub dealloc_pd: unsafe extern "C" fn(pd: *mut ibv_pd) -> c_int,
    pub reg_mr: unsafe extern "C" fn(
        pd: *mut ibv_pd,
        addr: *mut c_void,
        length: usize,
        access: c_int,
    ) -> *mut ibv_mr,
    pub dereg_mr: unsafe extern "C" fn(mr: *mut ibv_mr) -> c_int,
    pub create_comp_channel:
        unsafe extern "C" fn(context: *mut ibv_context) -> *mut ibv_comp_channel,
    pub destroy_comp_channel: unsafe extern "C" fn(channel: *mut ibv_comp_channel) -> c_int,
    pub create_cq: unsafe extern "C" fn(
        context: *mut ibv_context,
        cqe: c_int,
        cq_context: *mut c_void,
        channel: *mut ibv_comp_channel,
        comp_vector: c_int,
    ) -> *mut ibv_cq,
    pub destroy_cq: unsafe extern "C" fn(cq: *mut ibv_cq) -> c_int,
    pub create_qp:
        unsafe extern "C" fn(pd: *mut ibv_pd, qp_init_attr: *mut ibv_qp_init_attr) -> *mut ibv_qp,
    pub destroy_qp: unsafe extern "C" fn(qp: *mut ibv_qp) -> c_int,
    pub modify_qp:
        unsafe extern "C" fn(qp: *mut ibv_qp, attr: *mut ibv_qp_attr, attr_mask: c_int) -> c_int,
    pub post_send: unsafe extern "C" fn(
        qp: *mut ibv_qp,
        wr: *mut ibv_send_wr,
        bad_wr: *mut *mut ibv_send_wr,
    ) -> c_int,
    pub post_recv: unsafe extern "C" fn(
        qp: *mut ibv_qp,
        wr: *mut ibv_recv_wr,
        bad_wr: *mut *mut ibv_recv_wr,
    ) -> c_int,
    pub poll_cq:
        unsafe extern "C" fn(cq: *mut ibv_cq, num_entries: c_int, wc: *mut ibv_wc) -> c_int,
    pub req_notify_cq: unsafe extern "C" fn(cq: *mut ibv_cq, solicited_only: c_int) -> c_int,
    pub get_cq_event: unsafe extern "C" fn(
        channel: *mut ibv_comp_channel,
        cq: *mut *mut ibv_cq,
        cq_context: *mut *mut c_void,
    ) -> c_int,
    pub ack_cq_events: unsafe extern "C" fn(cq: *mut ibv_cq, nevents: c_int),
    pub query_port: unsafe extern "C" fn(
        context: *mut ibv_context,
        port_num: u8,
        port_attr: *mut ibv_port_attr,
    ) -> c_int,
    pub query_gid: unsafe extern "C" fn(
        context: *mut ibv_context,
        port_num: u8,
        index: c_int,
        gid: *mut ibv_gid,
    ) -> c_int,
}

// Safety: IbvLib holds immutable C function pointers that are safe to share across threads.
unsafe impl Send for IbvLib {}
unsafe impl Sync for IbvLib {}

static IBV_INSTANCE: OnceLock<Option<IbvLib>> = OnceLock::new();

/// Attempts to dynamically load `libibverbs.so.1` or `libibverbs.so` and resolve all required symbols.
pub fn get_ibv_lib() -> Option<&'static IbvLib> {
    IBV_INSTANCE
        .get_or_init(|| {
            let lib_names = ["libibverbs.so.1", "libibverbs.so"];
            let mut handle = std::ptr::null_mut();

            for name in lib_names {
                let c_name = CString::new(name).ok()?;
                handle =
                    unsafe { libc::dlopen(c_name.as_ptr(), libc::RTLD_NOW | libc::RTLD_LOCAL) };
                if !handle.is_null() {
                    tracing::debug!(lib = name, "Successfully loaded RDMA verbs library");
                    break;
                }
            }

            if handle.is_null() {
                tracing::debug!("libibverbs not available on system, hardware RDMA disabled");
                return None;
            }

            unsafe {
                macro_rules! sym {
                    ($name:expr, $ty:ty) => {{
                        let c_sym = CString::new($name).ok()?;
                        let ptr = libc::dlsym(handle, c_sym.as_ptr());
                        if ptr.is_null() {
                            tracing::warn!(symbol = $name, "Missing symbol in libibverbs");
                            libc::dlclose(handle);
                            return None;
                        }
                        std::mem::transmute::<*mut c_void, $ty>(ptr)
                    }};
                }

                Some(IbvLib {
                    _handle: handle,
                    get_device_list: sym!(
                        "ibv_get_device_list",
                        unsafe extern "C" fn(*mut c_int) -> *mut *mut ibv_device
                    ),
                    free_device_list: sym!(
                        "ibv_free_device_list",
                        unsafe extern "C" fn(*mut *mut ibv_device)
                    ),
                    get_device_name: sym!(
                        "ibv_get_device_name",
                        unsafe extern "C" fn(*mut ibv_device) -> *const c_char
                    ),
                    open_device: sym!(
                        "ibv_open_device",
                        unsafe extern "C" fn(*mut ibv_device) -> *mut ibv_context
                    ),
                    close_device: sym!(
                        "ibv_close_device",
                        unsafe extern "C" fn(*mut ibv_context) -> c_int
                    ),
                    alloc_pd: sym!(
                        "ibv_alloc_pd",
                        unsafe extern "C" fn(*mut ibv_context) -> *mut ibv_pd
                    ),
                    dealloc_pd: sym!("ibv_dealloc_pd", unsafe extern "C" fn(*mut ibv_pd) -> c_int),
                    reg_mr: sym!(
                        "ibv_reg_mr",
                        unsafe extern "C" fn(*mut ibv_pd, *mut c_void, usize, c_int) -> *mut ibv_mr
                    ),
                    dereg_mr: sym!("ibv_dereg_mr", unsafe extern "C" fn(*mut ibv_mr) -> c_int),
                    create_comp_channel: sym!(
                        "ibv_create_comp_channel",
                        unsafe extern "C" fn(*mut ibv_context) -> *mut ibv_comp_channel
                    ),
                    destroy_comp_channel: sym!(
                        "ibv_destroy_comp_channel",
                        unsafe extern "C" fn(*mut ibv_comp_channel) -> c_int
                    ),
                    create_cq: sym!(
                        "ibv_create_cq",
                        unsafe extern "C" fn(
                            *mut ibv_context,
                            c_int,
                            *mut c_void,
                            *mut ibv_comp_channel,
                            c_int,
                        ) -> *mut ibv_cq
                    ),
                    destroy_cq: sym!("ibv_destroy_cq", unsafe extern "C" fn(*mut ibv_cq) -> c_int),
                    create_qp: sym!(
                        "ibv_create_qp",
                        unsafe extern "C" fn(*mut ibv_pd, *mut ibv_qp_init_attr) -> *mut ibv_qp
                    ),
                    destroy_qp: sym!("ibv_destroy_qp", unsafe extern "C" fn(*mut ibv_qp) -> c_int),
                    modify_qp: sym!(
                        "ibv_modify_qp",
                        unsafe extern "C" fn(*mut ibv_qp, *mut ibv_qp_attr, c_int) -> c_int
                    ),
                    post_send: sym!(
                        "ibv_post_send",
                        unsafe extern "C" fn(
                            *mut ibv_qp,
                            *mut ibv_send_wr,
                            *mut *mut ibv_send_wr,
                        ) -> c_int
                    ),
                    post_recv: sym!(
                        "ibv_post_recv",
                        unsafe extern "C" fn(
                            *mut ibv_qp,
                            *mut ibv_recv_wr,
                            *mut *mut ibv_recv_wr,
                        ) -> c_int
                    ),
                    poll_cq: sym!(
                        "ibv_poll_cq",
                        unsafe extern "C" fn(*mut ibv_cq, c_int, *mut ibv_wc) -> c_int
                    ),
                    req_notify_cq: sym!(
                        "ibv_req_notify_cq",
                        unsafe extern "C" fn(*mut ibv_cq, c_int) -> c_int
                    ),
                    get_cq_event: sym!(
                        "ibv_get_cq_event",
                        unsafe extern "C" fn(
                            *mut ibv_comp_channel,
                            *mut *mut ibv_cq,
                            *mut *mut c_void,
                        ) -> c_int
                    ),
                    ack_cq_events: sym!(
                        "ibv_ack_cq_events",
                        unsafe extern "C" fn(*mut ibv_cq, c_int)
                    ),
                    query_port: sym!(
                        "ibv_query_port",
                        unsafe extern "C" fn(*mut ibv_context, u8, *mut ibv_port_attr) -> c_int
                    ),
                    query_gid: sym!(
                        "ibv_query_gid",
                        unsafe extern "C" fn(*mut ibv_context, u8, c_int, *mut ibv_gid) -> c_int
                    ),
                })
            }
        })
        .as_ref()
}

/// Helper to convert a raw C string to a Rust String.
///
/// # Safety
///
/// Caller must ensure that `ptr` is either null or points to a valid, null-terminated C string.
pub unsafe fn c_str_to_string(ptr: *const c_char) -> String {
    if ptr.is_null() {
        return String::new();
    }
    unsafe { CStr::from_ptr(ptr).to_string_lossy().into_owned() }
}
