//! RDMA Protection Domain (PD) memory safety boundary.

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
}

impl Drop for ProtectionDomain {
    fn drop(&mut self) {
        if !self.raw.is_null() {
            unsafe { (self.lib.dealloc_pd)(self.raw) };
        }
    }
}
