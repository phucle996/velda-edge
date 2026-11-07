//! RDMA Protection Domain (PD) — Hardware Memory Isolation Boundary.
//!
//! ### Hardware Memory Isolation Mechanism
//! 1. **Hardware Security Container**: In RoCEv2/InfiniBand architecture, a Protection Domain (PD)
//!    allocated via `ibv_alloc_pd` acts as an in-hardware anchor binding **Memory Regions (MR)**
//!    and **Queue Pairs (QP)** together into an isolated security namespace.
//! 2. **Wire-Speed Access Enforcement**: A Queue Pair associated with PD `A` cannot read, write,
//!    or DMA into/out of a Memory Region registered under PD `B`. The hardware NIC enforces this
//!    boundary at line rate without consulting host OS page tables or burning CPU cycles.
//! 3. **Velda Edge Integration**: A single Protection Domain is allocated per device context, anchoring
//!    all pre-allocated slab buffers (`PayloadSlab`) and per-core Queue Pairs, guaranteeing that rogue
//!    or corrupt remote keys (`rkey`) are blocked directly by the NIC's onboard MMU.
//! 4. **Strict RAII Lifecycle**: On drop, `ibv_dealloc_pd` safely frees the domain context only after all
//!    referencing QPs and MRs have been deallocated, preventing kernel memory leaks.

use std::sync::Arc;

use crate::error::RdmaError;
use crate::ffi::{IbvLib, get_ibv_lib, ibv_pd};
use crate::verbs::device::RdmaContext;

/// RAII wrapper around `ibv_pd`.
///
/// An RDMA Protection Domain isolates Memory Regions and Queue Pairs from other
/// domains, providing hardware-enforced memory safety boundaries.
pub struct ProtectionDomain {
    raw: *mut ibv_pd,
    _context: Arc<RdmaContext>,
    lib: &'static IbvLib,
}

unsafe impl Send for ProtectionDomain {}
unsafe impl Sync for ProtectionDomain {}

impl ProtectionDomain {
    /// Allocates a new Protection Domain for the given device context.
    pub fn new(context: Arc<RdmaContext>) -> Result<Arc<Self>, RdmaError> {
        let lib =
            get_ibv_lib().ok_or_else(|| RdmaError::Library("libibverbs not loaded".into()))?;
        let pd_ptr = unsafe { (lib.alloc_pd)(context.raw()) };
        if pd_ptr.is_null() {
            return Err(RdmaError::Verbs(
                "Failed to allocate Protection Domain (ibv_alloc_pd)".into(),
                std::io::Error::last_os_error().raw_os_error().unwrap_or(0),
            ));
        }

        Ok(Arc::new(Self {
            raw: pd_ptr,
            _context: context,
            lib,
        }))
    }

    /// Returns the raw pointer to `ibv_pd`.
    pub fn raw(&self) -> *mut ibv_pd {
        self.raw
    }

    /// Returns a reference to the parent RDMA context.
    #[inline(always)]
    pub fn context(&self) -> &Arc<RdmaContext> {
        &self._context
    }
}

impl Drop for ProtectionDomain {
    fn drop(&mut self) {
        if !self.raw.is_null() {
            unsafe { (self.lib.dealloc_pd)(self.raw) };
        }
    }
}
