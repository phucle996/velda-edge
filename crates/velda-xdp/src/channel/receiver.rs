//! # Inbound Packet Reception Channel (Rx Channel)
//!
//! ## Zero-Copy Ingress Processing
//!
//! When the NIC receives an inbound frame, it writes bytes directly into a UMEM slab (previously
//! populated via the Fill Ring) via DMA and writes an [`xdp_desc`] descriptor into the Rx Ring.
//!
//! [`XdpReceiver`] executes hot-path frame drainage:
//! 1. Dequeues batches of 32 descriptors from the Rx Ring.
//! 2. Divides `desc.addr` by the 4KB frame size to derive the exact `frame_idx` in user space.
//! 3. Instantiates an [`XdpSlabLease`] referencing the allocated slab without memory copies.

use parking_lot::Mutex;
use std::sync::Arc;

use crate::error::Result;
use crate::ffi::xdp_desc;
use crate::pool::{XdpSlabArena, XdpSlabLease};
use crate::rings::RxRing;

/// Ingress packet receiver driving the AF_XDP Rx Ring.
pub struct XdpReceiver {
    rx: RxRing,
    arena: XdpSlabArena,
    desc_buffer: [xdp_desc; 32],
}

impl XdpReceiver {
    /// Constructs a receiver bound to an Rx Ring and UMEM arena.
    pub fn new(rx: RxRing, arena: XdpSlabArena) -> Self {
        Self {
            rx,
            arena,
            desc_buffer: [xdp_desc::default(); 32],
        }
    }

    /// Drains up to 32 packet descriptors from the Rx Ring into leased slabs.
    ///
    /// Returns the number of frames successfully retrieved.
    pub fn recv_batch(&mut self, out: &mut Vec<XdpSlabLease>) -> Result<usize> {
        let count = self.rx.dequeue_batch(&mut self.desc_buffer);
        if count == 0 {
            return Ok(0);
        }

        let frame_size = self.arena.frame_size() as u64;
        for desc in &self.desc_buffer[..count] {
            let frame_idx = (desc.addr / frame_size) as u32;
            let lease = self.arena.lease_by_index(frame_idx, desc.len as usize)?;
            out.push(lease);
        }

        Ok(count)
    }

    /// Checks if the driver NAPI loop went to sleep and requires an RX wake-up syscall.
    #[inline]
    pub fn needs_wakeup(&self) -> bool {
        self.rx.needs_wakeup()
    }
}

/// Independent Read Half of an [`XdpStream`], movable into dedicated ingress async tasks.
pub struct XdpReadHalf {
    inner: Arc<Mutex<XdpReceiver>>,
}

impl XdpReadHalf {
    pub fn new(receiver: XdpReceiver) -> Self {
        Self {
            inner: Arc::new(Mutex::new(receiver)),
        }
    }

    /// Drains available packet slabs from the Rx Ring.
    pub fn recv_batch(&self, out: &mut Vec<XdpSlabLease>) -> Result<usize> {
        self.inner.lock().recv_batch(out)
    }
}
