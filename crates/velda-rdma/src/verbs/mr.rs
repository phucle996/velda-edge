//! RDMA Memory Region (MR) — Pinned RAM Registration & Zero-Copy DMA.
//!
//! ### Pinned Memory Registration & Direct Memory Access Mechanism
//! 1. **Why Standard OS Sockets Incur Copies**: Under normal OS networking, user-space buffers can be
//!    swapped, migrated, or paged out by the Linux kernel MMU at any moment. The kernel is therefore
//!    forced to copy data into intermediate kernel socket buffers (`sk_buff`) before wire transmission.
//! 2. **Memory Registration Mechanism (`ibv_reg_mr`)**:
//!    - **Physical Page Pinning (`mlock`)**: Pins virtual memory pages into physical RAM frames,
//!      completely preventing OS paging and page faults while hardware DMA transfers are in flight.
//!    - **Memory Translation Table (MTT)**: Programs the virtual-to-physical address mapping directly into
//!      the NIC's internal translation hardware, enabling the NIC DMA engine to navigate host RAM.
//!    - **Access Key Pair**:
//!      - `lkey` (Local Key): Authorizes the local CPU to submit Work Requests referencing this buffer.
//!      - `rkey` (Remote Key): Shared out-of-band with the remote backend, allowing one-sided RDMA Read/Write
//!        operations directly into this host memory slice without remote CPU involvement.
//! 3. **Synergy with `velda-core::PayloadSlab`**: Pre-allocated 4KB RAM slabs are registered once during
//!    warmup. Downstream request payloads are populated directly into these slabs, and the NIC DMAs them
//!    straight into backend memory with zero intermediate `memcpy` operations.

use std::os::raw::c_void;
use std::sync::Arc;

use crate::error::RdmaError;
use crate::ffi::{
    IBV_ACCESS_LOCAL_WRITE, IBV_ACCESS_REMOTE_READ, IBV_ACCESS_REMOTE_WRITE, IbvLib, get_ibv_lib,
    ibv_mr,
};
use crate::verbs::pd::ProtectionDomain;

/// Pinned, zero-copy memory region registered with the HCA.
pub struct MemoryRegion {
    raw: *mut ibv_mr,
    addr: u64,
    length: usize,
    lkey: u32,
    rkey: u32,
    _pd: Arc<ProtectionDomain>,
    lib: &'static IbvLib,
}

unsafe impl Send for MemoryRegion {}
unsafe impl Sync for MemoryRegion {}

impl MemoryRegion {
    /// Registers a contiguous virtual memory block into the given Protection Domain.
    ///
    /// The memory pages are pinned in physical RAM so the HCA can perform direct DMA.
    pub fn register(
        pd: Arc<ProtectionDomain>,
        ptr: *mut u8,
        len: usize,
        access_flags: u32,
    ) -> Result<Self, RdmaError> {
        let lib =
            get_ibv_lib().ok_or_else(|| RdmaError::Library("libibverbs not loaded".into()))?;
        let mr_ptr =
            unsafe { (lib.reg_mr)(pd.raw(), ptr as *mut c_void, len, access_flags as i32) };

        if mr_ptr.is_null() {
            return Err(RdmaError::Verbs(
                format!("Failed to register Memory Region of {len} bytes (ibv_reg_mr)"),
                std::io::Error::last_os_error().raw_os_error().unwrap_or(0),
            ));
        }

        let lkey = unsafe { (*mr_ptr).lkey };
        let rkey = unsafe { (*mr_ptr).rkey };

        Ok(Self {
            raw: mr_ptr,
            addr: ptr as u64,
            length: len,
            lkey,
            rkey,
            _pd: pd,
            lib,
        })
    }

    /// Helper to register a mutable slice with standard read/write permissions.
    pub fn register_slice(pd: Arc<ProtectionDomain>, slice: &mut [u8]) -> Result<Self, RdmaError> {
        let flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ;
        Self::register(pd, slice.as_mut_ptr(), slice.len(), flags)
    }

    /// Local memory key used by the local HCA to access memory in Work Requests.
    #[inline(always)]
    pub fn lkey(&self) -> u32 {
        self.lkey
    }

    /// Remote memory key shared with peers for One-Sided RDMA Write / Read.
    #[inline(always)]
    pub fn rkey(&self) -> u32 {
        self.rkey
    }

    /// Virtual memory address of the registered buffer.
    #[inline(always)]
    pub fn addr(&self) -> u64 {
        self.addr
    }

    /// Byte capacity of the registered buffer.
    #[inline(always)]
    pub fn length(&self) -> usize {
        self.length
    }
}

impl Drop for MemoryRegion {
    fn drop(&mut self) {
        if !self.raw.is_null() {
            unsafe { (self.lib.dereg_mr)(self.raw) };
        }
    }
}
