//! # Decoupled Full-Duplex Bidirectional XDP Stream
//!
//! ## Full-Duplex Stream Splitting via `into_split`
//!
//! In high-performance edge proxies (specifically HTTP/2, HTTP/3, and gRPC):
//! - Communication is full-duplex: an async task reads inbound frames from downstream clients
//!   while a companion task concurrently transmits responses or `WINDOW_UPDATE` frames.
//! - Managing both operations under a shared `Mutex<XdpStream>` introduces cross-task lock contention
//!   across `.await` points, capping request throughput.
//! - In the AF_XDP architecture, the **Rx Ring** (inbound) and **Tx Ring** (outbound) are physically
//!   independent circular buffers. [`XdpStream::into_split`] separates the stream into an [`XdpReadHalf`]
//!   and an [`XdpWriteHalf`], allowing independent tasks to drive ingress and egress simultaneously
//!   with zero lock contention.

use crate::channel::receiver::{XdpReadHalf, XdpReceiver};
use crate::channel::sender::{XdpSender, XdpWriteHalf};
use crate::error::Result;
use crate::pool::XdpSlabLease;

/// Bidirectional XDP communication stream combining ingress and egress channels.
pub struct XdpStream {
    receiver: XdpReceiver,
    sender: XdpSender,
}

impl XdpStream {
    /// Constructs a bidirectional stream from independent receiver and sender instances.
    pub fn new(receiver: XdpReceiver, sender: XdpSender) -> Self {
        Self { receiver, sender }
    }

    /// Drains inbound packet frames from the Rx Ring into leased slabs.
    #[inline]
    pub fn recv_batch(&mut self, out: &mut Vec<XdpSlabLease>) -> Result<usize> {
        self.receiver.recv_batch(out)
    }

    /// Enqueues response packet slabs into the Tx Ring for transmission by the NIC.
    #[inline]
    pub fn send_batch(&mut self, slabs: &[XdpSlabLease]) -> usize {
        self.sender.send_batch(slabs)
    }

    /// Reclaims transmitted UMEM frame addresses from the Completion Ring to replenish the Fill Ring.
    #[inline]
    pub fn reclaim_and_refill(&mut self) -> usize {
        self.sender.reclaim_and_refill()
    }

    /// Splits the bidirectional stream into independent Read and Write halves, eliminating mutex contention.
    pub fn into_split(self) -> (XdpReadHalf, XdpWriteHalf) {
        (
            XdpReadHalf::new(self.receiver),
            XdpWriteHalf::new(self.sender),
        )
    }
}
