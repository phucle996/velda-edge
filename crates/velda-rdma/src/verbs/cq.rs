//! RDMA Completion Queue (CQ) — Hardware Event Queue & Per-Core CQ Sharding.
//!
//! ### Hardware Work Completion Mechanism
//! 1. **Completion Ring Buffer**: When an asynchronous Send, Receive, or RDMA Write/Read operation
//!    finishes traversing the wire, the HCA hardware automatically writes a Work Completion (`ibv_wc`)
//!    entry into the associated Completion Queue (CQ).
//! 2. **Dual-Mode Completion Harvesting**:
//!    - **Fast-path Busy-Polling (`ibv_poll_cq`)**: Directly inspects the mapped memory ring buffer
//!      without entering the kernel or incurring context switch overhead.
//!    - **Slow-path Event Notification (`ibv_req_notify_cq` + `ibv_comp_channel`)**: When idle,
//!      the channel arms hardware MSI-X interrupts on an event file descriptor (`RawFd`), waking up
//!      the Tokio event loop via `AsyncFd` and saving 100% CPU when no traffic is present.
//! 3. **Per-Core CQ Sharding Invariant**:
//!    Each Frontline Worker Core (`lane = worker_id`) creates and owns its dedicated Completion Queue
//!    (`ibv_create_cq`). CQs are never shared across cores, eliminating cross-core cache invalidation
//!    (false sharing) and avoiding all mutex contention during hot-path polling.

use std::os::fd::RawFd;
use std::os::raw::c_int;
use std::sync::Arc;

use crate::error::RdmaError;
use crate::ffi::{IbvLib, get_ibv_lib, ibv_comp_channel, ibv_cq, ibv_wc};
use crate::verbs::device::RdmaContext;

/// Completion Queue (CQ) handle for receiving completion notifications from the HCA.
pub struct CompletionQueue {
    raw: *mut ibv_cq,
    channel: *mut ibv_comp_channel,
    _context: Arc<RdmaContext>,
    lib: &'static IbvLib,
}

unsafe impl Send for CompletionQueue {}
unsafe impl Sync for CompletionQueue {}

impl CompletionQueue {
    /// Creates a new Completion Queue tied to the specified context.
    ///
    /// If `with_channel` is true, allocates a completion event channel whose `fd` can be
    /// integrated with Tokio event loop via `AsyncFd`.
    pub fn new(
        context: Arc<RdmaContext>,
        cqe: u32,
        with_channel: bool,
    ) -> Result<Arc<Self>, RdmaError> {
        let lib =
            get_ibv_lib().ok_or_else(|| RdmaError::Library("libibverbs not loaded".into()))?;

        let mut channel_ptr = std::ptr::null_mut();
        if with_channel {
            channel_ptr = unsafe { (lib.create_comp_channel)(context.raw()) };
            if channel_ptr.is_null() {
                return Err(RdmaError::Verbs(
                    "Failed to create completion channel (ibv_create_comp_channel)".into(),
                    std::io::Error::last_os_error().raw_os_error().unwrap_or(0),
                ));
            }
        }

        let cq_ptr = unsafe {
            (lib.create_cq)(
                context.raw(),
                cqe as c_int,
                std::ptr::null_mut(),
                channel_ptr,
                0, // comp_vector (typically 0 or pinned per core)
            )
        };

        if cq_ptr.is_null() {
            if !channel_ptr.is_null() {
                unsafe { (lib.destroy_comp_channel)(channel_ptr) };
            }
            return Err(RdmaError::Verbs(
                "Failed to create completion queue (ibv_create_cq)".into(),
                std::io::Error::last_os_error().raw_os_error().unwrap_or(0),
            ));
        }

        Ok(Arc::new(Self {
            raw: cq_ptr,
            channel: channel_ptr,
            _context: context,
            lib,
        }))
    }

    /// Returns the raw pointer to `ibv_cq`.
    #[inline(always)]
    pub fn raw(&self) -> *mut ibv_cq {
        self.raw
    }

    /// Returns the completion event channel file descriptor if event-driven notification is enabled.
    #[inline(always)]
    pub fn channel_fd(&self) -> Option<RawFd> {
        if self.channel.is_null() {
            None
        } else {
            Some(unsafe { (*self.channel).fd })
        }
    }

    /// Polls the completion queue for completed work requests (hot-path non-blocking).
    ///
    /// Returns the number of Work Completions placed into `wc`.
    #[inline(always)]
    pub fn poll(&self, wc: &mut [ibv_wc]) -> Result<usize, RdmaError> {
        let ret = unsafe { (self.lib.poll_cq)(self.raw, wc.len() as c_int, wc.as_mut_ptr()) };

        if ret < 0 {
            Err(RdmaError::Verbs(
                "Failed to poll CQ (ibv_poll_cq)".into(),
                ret,
            ))
        } else {
            Ok(ret as usize)
        }
    }

    /// Arms the completion queue to trigger a hardware notification on the next completion event.
    #[inline(always)]
    pub fn req_notify(&self, solicited_only: bool) -> Result<(), RdmaError> {
        let ret = unsafe { (self.lib.req_notify_cq)(self.raw, solicited_only as c_int) };
        if ret != 0 {
            Err(RdmaError::Verbs(
                "Failed to arm CQ notification (ibv_req_notify_cq)".into(),
                ret,
            ))
        } else {
            Ok(())
        }
    }

    /// Reads and acknowledges a pending completion event from the channel.
    pub fn get_and_ack_event(&self) -> Result<(), RdmaError> {
        if self.channel.is_null() {
            return Ok(());
        }

        let mut cq_ptr = std::ptr::null_mut();
        let mut cq_context = std::ptr::null_mut();

        let ret = unsafe { (self.lib.get_cq_event)(self.channel, &mut cq_ptr, &mut cq_context) };
        if ret != 0 {
            return Err(RdmaError::Verbs(
                "Failed to get CQ event (ibv_get_cq_event)".into(),
                ret,
            ));
        }

        unsafe { (self.lib.ack_cq_events)(self.raw, 1) };
        Ok(())
    }
}

impl Drop for CompletionQueue {
    fn drop(&mut self) {
        if !self.raw.is_null() {
            unsafe { (self.lib.destroy_cq)(self.raw) };
        }
        if !self.channel.is_null() {
            unsafe { (self.lib.destroy_comp_channel)(self.channel) };
        }
    }
}
