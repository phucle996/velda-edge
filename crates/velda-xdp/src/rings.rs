//! # SPSC Lock-Free AF_XDP Memory-Mapped Ring Buffers
//!
//! ## Memory Model & Driver Synchronization
//!
//! AF_XDP coordinates high-throughput packet transfers between kernel drivers and user-space
//! workers via four circular ring buffers mapped directly into process memory using a
//! **Single-Producer Single-Consumer (SPSC)** access pattern.
//!
//! ### Memory Barrier Ordering (`Acquire` and `Release`)
//! Modern superscalar CPUs execute instructions out of order. Without explicit memory barriers,
//! the CPU can publish an updated producer index before descriptor writes reach cache or RAM,
//! causing the NIC DMA controller to fetch stale or incomplete descriptors.
//! - `fence(Ordering::Release)` flushes all descriptor writes to memory before publishing the updated
//!   producer counter to the kernel.
//! - `fence(Ordering::Acquire)` prevents the consumer from reading descriptor payloads until the
//!   producer index is observed as updated.
//!
//! ### Local Index Caching & Inter-Core Cache Bouncing
//! Direct reads and writes to shared mmap pointers across CPU sockets trigger cache line bouncing
//! over the inter-core MESI coherency bus. Maintaining thread-local cached indexes (`cached_prod`,
//! `cached_cons`) confines hot-path loop checks to local L1 cache, refreshing against shared kernel
//! memory only when the ring is full or exhausted.
//!
//! ### Wakeup Suppression via `XDP_RING_NEED_WAKEUP`
//! During multi-gigabit bursts, the kernel driver NAPI polling loop runs continuously without sleeping.
//! Issuing `poll()` or `sendto()` syscalls for every batch burns substantial CPU time in context switches.
//! The kernel raises `XDP_RING_NEED_WAKEUP` only when the NAPI loop goes idle, signaling user-space
//! to issue a wake-up syscall. During active traffic bursts, both sides exchange frames with zero syscalls.

use std::sync::atomic::{Ordering, fence};

use crate::error::{Result, XdpError};
use crate::ffi::{XDP_RING_NEED_WAKEUP, xdp_desc, xdp_mmap_offsets};

/// Fill Ring: Populated by user-space with free UMEM frame addresses for the NIC to receive packets.
pub struct FillRing {
    producer: *mut u32,
    consumer: *const u32,
    ring: *mut u64,
    flags: *const u32,
    size: u32,
    mask: u32,
    cached_prod: u32,
    cached_cons: u32,
}

unsafe impl Send for FillRing {}
unsafe impl Sync for FillRing {}

impl FillRing {
    /// Instantiates FillRing from raw kernel mmap pointers.
    ///
    /// # Safety
    /// Pointers must originate from a valid `mmap` call with offset `XDP_UMEM_PGOFF_FILL_RING`.
    pub unsafe fn from_raw(
        producer: *mut u32,
        consumer: *const u32,
        ring: *mut u64,
        flags: *const u32,
        size: u32,
    ) -> Self {
        let mask = size.saturating_sub(1);
        let cached_prod = unsafe { *producer };
        let cached_cons = unsafe { *consumer };
        Self {
            producer,
            consumer,
            ring,
            flags,
            size,
            mask,
            cached_prod,
            cached_cons,
        }
    }

    /// Evaluates available free slots in the Fill Ring.
    ///
    /// Checks the thread-local cached consumer index first, refreshing against shared
    /// kernel memory only when the local count indicates full capacity.
    #[inline]
    pub fn available(&mut self) -> u32 {
        let entries = self.cached_prod.wrapping_sub(self.cached_cons);
        let free = self.size.saturating_sub(entries);
        if free == 0 {
            self.cached_cons = unsafe { std::ptr::read_volatile(self.consumer) };
            let entries = self.cached_prod.wrapping_sub(self.cached_cons);
            self.size.saturating_sub(entries)
        } else {
            free
        }
    }

    /// Enqueues a batch of UMEM addresses into the Fill Ring.
    ///
    /// Returns the number of addresses successfully queued.
    pub fn enqueue_batch(&mut self, addrs: &[u64]) -> usize {
        let count = (addrs.len() as u32).min(self.available()) as usize;
        if count == 0 {
            return 0;
        }

        for (i, &addr) in addrs.iter().take(count).enumerate() {
            let idx = (self.cached_prod.wrapping_add(i as u32) & self.mask) as usize;
            unsafe {
                std::ptr::write_volatile(self.ring.add(idx), addr);
            }
        }

        // Release barrier: guarantees all address writes reach memory before publishing producer index
        fence(Ordering::Release);
        self.cached_prod = self.cached_prod.wrapping_add(count as u32);
        unsafe {
            std::ptr::write_volatile(self.producer, self.cached_prod);
        }

        count
    }

    /// Checks if the driver NAPI loop went to sleep and requires a wake-up syscall.
    #[inline]
    pub fn needs_wakeup(&self) -> bool {
        if self.flags.is_null() {
            return false;
        }
        (unsafe { std::ptr::read_volatile(self.flags) } & XDP_RING_NEED_WAKEUP) != 0
    }
}

/// Rx Ring: Drained by user-space as the NIC places received packet descriptors onto the ring.
pub struct RxRing {
    producer: *const u32,
    consumer: *mut u32,
    ring: *const xdp_desc,
    flags: *const u32,
    size: u32,
    mask: u32,
    cached_prod: u32,
    cached_cons: u32,
}

unsafe impl Send for RxRing {}
unsafe impl Sync for RxRing {}

impl RxRing {
    /// Instantiates RxRing from raw kernel mmap pointers.
    ///
    /// # Safety
    /// Pointers must originate from a valid `mmap` call with offset `XDP_PGOFF_RX_RING`.
    pub unsafe fn from_raw(
        producer: *const u32,
        consumer: *mut u32,
        ring: *const xdp_desc,
        flags: *const u32,
        size: u32,
    ) -> Self {
        let mask = size.saturating_sub(1);
        let cached_prod = unsafe { *producer };
        let cached_cons = unsafe { *consumer };
        Self {
            producer,
            consumer,
            ring,
            flags,
            size,
            mask,
            cached_prod,
            cached_cons,
        }
    }

    /// Total descriptor capacity of the ring.
    #[inline]
    pub fn size(&self) -> u32 {
        self.size
    }

    /// Counts pending received packet descriptors ready for consumption.
    #[inline]
    pub fn ready_count(&mut self) -> u32 {
        let entries = self.cached_prod.wrapping_sub(self.cached_cons);
        if entries == 0 {
            self.cached_prod = unsafe { std::ptr::read_volatile(self.producer) };
            self.cached_prod.wrapping_sub(self.cached_cons)
        } else {
            entries
        }
    }

    /// Dequeues a batch of packet descriptors from the Rx Ring.
    pub fn dequeue_batch(&mut self, out: &mut [xdp_desc]) -> usize {
        let ready = self.ready_count();
        let count = (out.len() as u32).min(ready) as usize;
        if count == 0 {
            return 0;
        }

        // Acquire barrier: ensures CPU does not read descriptor payloads before confirming producer index
        fence(Ordering::Acquire);
        for (i, slot) in out.iter_mut().take(count).enumerate() {
            let idx = (self.cached_cons.wrapping_add(i as u32) & self.mask) as usize;
            *slot = unsafe { std::ptr::read_volatile(self.ring.add(idx)) };
        }

        fence(Ordering::Release);
        self.cached_cons = self.cached_cons.wrapping_add(count as u32);
        unsafe {
            std::ptr::write_volatile(self.consumer, self.cached_cons);
        }

        count
    }

    /// Checks if the kernel requires an RX wake-up syscall.
    #[inline]
    pub fn needs_wakeup(&self) -> bool {
        if self.flags.is_null() {
            return false;
        }
        (unsafe { std::ptr::read_volatile(self.flags) } & XDP_RING_NEED_WAKEUP) != 0
    }
}

/// Tx Ring: Populated by user-space with outbound packet descriptors to transmit.
pub struct TxRing {
    producer: *mut u32,
    consumer: *const u32,
    ring: *mut xdp_desc,
    flags: *const u32,
    size: u32,
    mask: u32,
    cached_prod: u32,
    cached_cons: u32,
}

unsafe impl Send for TxRing {}
unsafe impl Sync for TxRing {}

impl TxRing {
    /// Instantiates TxRing from raw kernel mmap pointers.
    ///
    /// # Safety
    /// Pointers must originate from a valid `mmap` call with offset `XDP_PGOFF_TX_RING`.
    pub unsafe fn from_raw(
        producer: *mut u32,
        consumer: *const u32,
        ring: *mut xdp_desc,
        flags: *const u32,
        size: u32,
    ) -> Self {
        let mask = size.saturating_sub(1);
        let cached_prod = unsafe { *producer };
        let cached_cons = unsafe { *consumer };
        Self {
            producer,
            consumer,
            ring,
            flags,
            size,
            mask,
            cached_prod,
            cached_cons,
        }
    }

    /// Number of free slots available for enqueuing outbound frames.
    #[inline]
    pub fn available(&mut self) -> u32 {
        let entries = self.cached_prod.wrapping_sub(self.cached_cons);
        let free = self.size.saturating_sub(entries);
        if free == 0 {
            self.cached_cons = unsafe { std::ptr::read_volatile(self.consumer) };
            let entries = self.cached_prod.wrapping_sub(self.cached_cons);
            self.size.saturating_sub(entries)
        } else {
            free
        }
    }

    /// Enqueues a batch of outbound packet descriptors into the Tx Ring.
    pub fn enqueue_batch(&mut self, descs: &[xdp_desc]) -> usize {
        let count = (descs.len() as u32).min(self.available()) as usize;
        if count == 0 {
            return 0;
        }

        for (i, desc) in descs.iter().take(count).enumerate() {
            let idx = (self.cached_prod.wrapping_add(i as u32) & self.mask) as usize;
            unsafe {
                std::ptr::write_volatile(self.ring.add(idx), *desc);
            }
        }

        fence(Ordering::Release);
        self.cached_prod = self.cached_prod.wrapping_add(count as u32);
        unsafe {
            std::ptr::write_volatile(self.producer, self.cached_prod);
        }

        count
    }

    /// Checks if the driver requires a `sendto()` syscall to trigger transmission.
    #[inline]
    pub fn needs_wakeup(&self) -> bool {
        if self.flags.is_null() {
            return false;
        }
        (unsafe { std::ptr::read_volatile(self.flags) } & XDP_RING_NEED_WAKEUP) != 0
    }
}

/// Completion Ring: Drained by user-space to reclaim UMEM addresses of completed transmissions.
pub struct CompletionRing {
    producer: *const u32,
    consumer: *mut u32,
    ring: *const u64,
    #[allow(dead_code)]
    flags: *const u32,
    size: u32,
    mask: u32,
    cached_prod: u32,
    cached_cons: u32,
}

unsafe impl Send for CompletionRing {}
unsafe impl Sync for CompletionRing {}

impl CompletionRing {
    /// Instantiates CompletionRing from raw kernel mmap pointers.
    ///
    /// # Safety
    /// Pointers must originate from a valid `mmap` call with offset `XDP_UMEM_PGOFF_COMPLETION_RING`.
    pub unsafe fn from_raw(
        producer: *const u32,
        consumer: *mut u32,
        ring: *const u64,
        flags: *const u32,
        size: u32,
    ) -> Self {
        let mask = size.saturating_sub(1);
        let cached_prod = unsafe { *producer };
        let cached_cons = unsafe { *consumer };
        Self {
            producer,
            consumer,
            ring,
            flags,
            size,
            mask,
            cached_prod,
            cached_cons,
        }
    }

    /// Total capacity of the Completion Ring.
    #[inline]
    pub fn size(&self) -> u32 {
        self.size
    }

    /// Counts completed transmission addresses ready to be recycled.
    #[inline]
    pub fn ready_count(&mut self) -> u32 {
        let entries = self.cached_prod.wrapping_sub(self.cached_cons);
        if entries == 0 {
            self.cached_prod = unsafe { std::ptr::read_volatile(self.producer) };
            self.cached_prod.wrapping_sub(self.cached_cons)
        } else {
            entries
        }
    }

    /// Dequeues a batch of transmitted UMEM addresses to return to the Fill Ring.
    pub fn dequeue_batch(&mut self, out: &mut [u64]) -> usize {
        let ready = self.ready_count();
        let count = (out.len() as u32).min(ready) as usize;
        if count == 0 {
            return 0;
        }

        fence(Ordering::Acquire);
        for (i, slot) in out.iter_mut().take(count).enumerate() {
            let idx = (self.cached_cons.wrapping_add(i as u32) & self.mask) as usize;
            *slot = unsafe { std::ptr::read_volatile(self.ring.add(idx)) };
        }

        fence(Ordering::Release);
        self.cached_cons = self.cached_cons.wrapping_add(count as u32);
        unsafe {
            std::ptr::write_volatile(self.consumer, self.cached_cons);
        }

        count
    }
}

/// Facade grouping all 4 memory-mapped rings for a dedicated worker core shard.
pub struct XdpRings {
    pub fill: FillRing,
    pub rx: RxRing,
    pub tx: TxRing,
    pub comp: CompletionRing,
}

impl XdpRings {
    /// Maps and binds the 4 rings using kernel offsets retrieved via `getsockopt(..., XDP_MMAP_OFFSETS, ...)`.
    ///
    /// # Safety
    /// Pointers `rx_tx_map` and `umem_map` must reference active memory mappings created from the AF_XDP socket fd.
    pub unsafe fn new(
        rx_tx_map: *mut u8,
        umem_map: *mut u8,
        offsets: &xdp_mmap_offsets,
        rx_size: u32,
        tx_size: u32,
        fill_size: u32,
        comp_size: u32,
    ) -> Result<Self> {
        if rx_tx_map.is_null() || umem_map.is_null() {
            return Err(XdpError::UmemAllocation(
                "Kernel returned null mmap pointers, AF_XDP socket initialization incomplete"
                    .to_string(),
            ));
        }

        let fill = unsafe {
            FillRing::from_raw(
                umem_map.add(offsets.fr.producer as usize) as *mut u32,
                umem_map.add(offsets.fr.consumer as usize) as *const u32,
                umem_map.add(offsets.fr.desc as usize) as *mut u64,
                if offsets.fr.flags != 0 {
                    umem_map.add(offsets.fr.flags as usize) as *const u32
                } else {
                    std::ptr::null()
                },
                fill_size,
            )
        };

        let rx = unsafe {
            RxRing::from_raw(
                rx_tx_map.add(offsets.rx.producer as usize) as *const u32,
                rx_tx_map.add(offsets.rx.consumer as usize) as *mut u32,
                rx_tx_map.add(offsets.rx.desc as usize) as *const xdp_desc,
                if offsets.rx.flags != 0 {
                    rx_tx_map.add(offsets.rx.flags as usize) as *const u32
                } else {
                    std::ptr::null()
                },
                rx_size,
            )
        };

        let tx = unsafe {
            TxRing::from_raw(
                rx_tx_map.add(offsets.tx.producer as usize) as *mut u32,
                rx_tx_map.add(offsets.tx.consumer as usize) as *const u32,
                rx_tx_map.add(offsets.tx.desc as usize) as *mut xdp_desc,
                if offsets.tx.flags != 0 {
                    rx_tx_map.add(offsets.tx.flags as usize) as *const u32
                } else {
                    std::ptr::null()
                },
                tx_size,
            )
        };

        let comp = unsafe {
            CompletionRing::from_raw(
                umem_map.add(offsets.cr.producer as usize) as *const u32,
                umem_map.add(offsets.cr.consumer as usize) as *mut u32,
                umem_map.add(offsets.cr.desc as usize) as *const u64,
                if offsets.cr.flags != 0 {
                    umem_map.add(offsets.cr.flags as usize) as *const u32
                } else {
                    std::ptr::null()
                },
                comp_size,
            )
        };

        Ok(Self { fill, rx, tx, comp })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fill_ring_enqueue_and_dequeue_simulation() {
        let size = 16u32;
        let mut producer = 0u32;
        let consumer = 0u32;
        let mut storage = vec![0u64; size as usize];

        let mut fill = unsafe {
            FillRing::from_raw(
                &mut producer as *mut u32,
                &consumer as *const u32,
                storage.as_mut_ptr(),
                std::ptr::null(),
                size,
            )
        };

        assert_eq!(fill.available(), 16);

        // Enqueue batch of 4 addresses
        let addrs = [4096u64, 8192, 12288, 16384];
        let enqueued = fill.enqueue_batch(&addrs);
        assert_eq!(enqueued, 4);
        assert_eq!(producer, 4);
        assert_eq!(fill.available(), 12);

        // Verify storage memory layout
        assert_eq!(storage[0], 4096);
        assert_eq!(storage[1], 8192);
        assert_eq!(storage[2], 12288);
        assert_eq!(storage[3], 16384);
    }

    #[test]
    fn test_rx_ring_dequeue_simulation() {
        let size = 8u32;
        let producer = 3u32;
        let mut consumer = 0u32;
        let descs = [
            xdp_desc {
                addr: 100,
                len: 64,
                options: 0,
            },
            xdp_desc {
                addr: 200,
                len: 128,
                options: 0,
            },
            xdp_desc {
                addr: 300,
                len: 256,
                options: 0,
            },
            xdp_desc::default(),
            xdp_desc::default(),
            xdp_desc::default(),
            xdp_desc::default(),
            xdp_desc::default(),
        ];

        let mut rx = unsafe {
            RxRing::from_raw(
                &producer as *const u32,
                &mut consumer as *mut u32,
                descs.as_ptr(),
                std::ptr::null(),
                size,
            )
        };

        assert_eq!(rx.ready_count(), 3);

        let mut out = [xdp_desc::default(); 2];
        let dequeued = rx.dequeue_batch(&mut out);
        assert_eq!(dequeued, 2);
        assert_eq!(consumer, 2);
        assert_eq!(out[0].addr, 100);
        assert_eq!(out[0].len, 64);
        assert_eq!(out[1].addr, 200);
        assert_eq!(out[1].len, 128);

        // Dequeue remaining 1 descriptor
        let mut out_remaining = [xdp_desc::default(); 2];
        let dequeued2 = rx.dequeue_batch(&mut out_remaining);
        assert_eq!(dequeued2, 1);
        assert_eq!(consumer, 3);
        assert_eq!(out_remaining[0].addr, 300);
    }
}
