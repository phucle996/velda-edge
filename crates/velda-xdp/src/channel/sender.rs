//! # Outbound Packet Transmission Channel (Tx & Completion Channel)
//!
//! ## Closed-Loop Memory Recycling
//!
//! Outbound frame delivery operates as a closed allocation loop:
//! 1. Responses are written directly into leased [`XdpSlabLease`] buffers.
//! 2. [`XdpSender`] builds [`xdp_desc`] descriptors referencing UMEM payload addresses and enqueues them to the Tx Ring.
//! 3. The NIC DMA engine reads bytes from UMEM and transmits them onto the wire.
//! 4. Following wire completion, the NIC places transmitted UMEM addresses onto the Completion Ring.
//! 5. [`XdpSender::reclaim_and_refill`] retrieves these addresses and transfers them directly back into the Fill Ring.
//!    This cycle completely eliminates dynamic memory reallocations and garbage collection on the egress path.

use parking_lot::Mutex;
use std::sync::Arc;

use crate::ffi::xdp_desc;
use crate::pool::XdpSlabLease;
use crate::rings::{CompletionRing, FillRing, TxRing};

/// Egress transmitter driving the AF_XDP Tx Ring, Completion Ring, and Fill Ring replenishment.
pub struct XdpSender {
    tx: TxRing,
    comp: CompletionRing,
    fill: FillRing,
    desc_buffer: [xdp_desc; 32],
    addr_buffer: [u64; 32],
}

impl XdpSender {
    /// Constructs a sender wrapping Tx, Completion, and Fill rings.
    pub fn new(tx: TxRing, comp: CompletionRing, fill: FillRing) -> Self {
        Self {
            tx,
            comp,
            fill,
            desc_buffer: [xdp_desc::default(); 32],
            addr_buffer: [0u64; 32],
        }
    }

    /// Enqueues a batch of outbound slab frames into the Tx Ring for transmission.
    pub fn send_batch(&mut self, slabs: &[XdpSlabLease]) -> usize {
        let count = slabs.len().min(self.desc_buffer.len());
        if count == 0 {
            return 0;
        }

        for (i, slab) in slabs.iter().take(count).enumerate() {
            self.desc_buffer[i] = xdp_desc {
                addr: slab.payload_umem_addr(),
                len: slab.payload_len() as u32,
                options: 0,
            };
        }

        self.tx.enqueue_batch(&self.desc_buffer[..count])
    }

    /// Reclaims transmitted UMEM addresses from the Completion Ring and returns them to the Fill Ring.
    pub fn reclaim_and_refill(&mut self) -> usize {
        let count = self.comp.dequeue_batch(&mut self.addr_buffer);
        if count > 0 {
            self.fill.enqueue_batch(&self.addr_buffer[..count]);
        }
        count
    }

    /// Checks if the driver requires a `sendto()` wake-up syscall to flush pending Tx descriptors.
    #[inline]
    pub fn needs_wakeup(&self) -> bool {
        self.tx.needs_wakeup()
    }
}

/// Independent Write Half of an [`XdpStream`], movable into dedicated egress async tasks.
pub struct XdpWriteHalf {
    inner: Arc<Mutex<XdpSender>>,
}

impl XdpWriteHalf {
    pub fn new(sender: XdpSender) -> Self {
        Self {
            inner: Arc::new(Mutex::new(sender)),
        }
    }

    /// Transmits a batch of outbound packet slabs.
    pub fn send_batch(&self, slabs: &[XdpSlabLease]) -> usize {
        self.inner.lock().send_batch(slabs)
    }

    /// Reclaims completed frames and replenishes the Fill Ring.
    pub fn reclaim_and_refill(&self) -> usize {
        self.inner.lock().reclaim_and_refill()
    }
}
