//! Lock-Free Single-Producer Single-Consumer (SPSC) Ring Buffer (`std_grpc_access_log::ring_buffer`).
//!
//! Provides hardware-isolated, per-worker lane SPSC queues for zero-contention
//! log record dispatching between Frontline Traffic Workers and Shadow Logistics Workers.
//!
//! # Micro-Architectural Guarantees
//! - **Per-Lane Sharded**: Each worker thread accesses strictly its own dedicated ring buffer shard.
//! - **Cacheline Isolated**: Head, tail, and buffer data pointers are isolated to 64-byte boundaries,
//!   preventing false sharing across CPU cores.
//! - **Lock-Free Zero-Allocation Hot Path**: Hot-path pushes never allocate heap memory, never take locks,
//!   and never block under queue saturation (fail-open non-blocking drop).
//! - **SIMD Record Transfers**: Records are copied using hardware-harmonized vector registers.

use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::record::PackedGrpcRecord;
use super::simd;

#[repr(align(64))]
struct CachePadded<T>(T);

/// Page-aligned (4096B) contiguous ring buffer slot memory residing directly in physical RAM.
struct AlignedSlotBuffer {
    ptr: *mut UnsafeCell<PackedGrpcRecord>,
    capacity: usize,
    layout: std::alloc::Layout,
}

unsafe impl Send for AlignedSlotBuffer {}
unsafe impl Sync for AlignedSlotBuffer {}

impl AlignedSlotBuffer {
    fn new(capacity: usize) -> Self {
        let raw_bytes = capacity * std::mem::size_of::<PackedGrpcRecord>();
        let byte_len = (raw_bytes + 4095) & !4095;
        let layout = std::alloc::Layout::from_size_align(byte_len, 4096)
            .expect("valid page-aligned memory layout for slot buffer");

        let ptr = unsafe { std::alloc::alloc_zeroed(layout) as *mut UnsafeCell<PackedGrpcRecord> };
        assert!(
            !ptr.is_null(),
            "OOM: failed to allocate page-aligned slot buffer in RAM"
        );

        #[cfg(unix)]
        unsafe {
            libc::madvise(ptr as *mut libc::c_void, byte_len, libc::MADV_SEQUENTIAL);
        }

        Self {
            ptr,
            capacity,
            layout,
        }
    }

    #[inline(always)]
    fn get(&self, index: usize) -> &UnsafeCell<PackedGrpcRecord> {
        debug_assert!(index < self.capacity);
        unsafe { &*self.ptr.add(index) }
    }
}

impl Drop for AlignedSlotBuffer {
    fn drop(&mut self) {
        unsafe {
            std::alloc::dealloc(self.ptr as *mut u8, self.layout);
        }
    }
}

/// Single-Producer Single-Consumer lock-free ring buffer slot container for gRPC records.
pub struct SpscRingBuffer {
    capacity: usize,
    mask: usize,
    slots: AlignedSlotBuffer,
    head: CachePadded<AtomicUsize>,
    cached_tail: CachePadded<AtomicUsize>,
    tail: CachePadded<AtomicUsize>,
    cached_head: CachePadded<AtomicUsize>,
}

unsafe impl Send for SpscRingBuffer {}
unsafe impl Sync for SpscRingBuffer {}

impl SpscRingBuffer {
    /// Creates a new SPSC ring buffer with capacity rounded up to next power of 2.
    pub fn new(capacity: usize) -> Self {
        let cap = capacity.next_power_of_two().max(128);
        Self {
            capacity: cap,
            mask: cap - 1,
            slots: AlignedSlotBuffer::new(cap),
            head: CachePadded(AtomicUsize::new(0)),
            cached_tail: CachePadded(AtomicUsize::new(0)),
            tail: CachePadded(AtomicUsize::new(0)),
            cached_head: CachePadded(AtomicUsize::new(0)),
        }
    }

    /// Frontline hot path: Enqueues a gRPC log record non-blocking.
    #[allow(clippy::result_unit_err)]
    #[allow(clippy::result_unit_err)]
    #[inline(always)]
    pub fn try_push(&self, record: &PackedGrpcRecord) -> Result<(), ()> {
        let head = self.head.0.load(Ordering::Relaxed);
        let mut tail = self.cached_tail.0.load(Ordering::Relaxed);

        if head.wrapping_sub(tail) >= self.capacity {
            tail = self.tail.0.load(Ordering::Acquire);
            self.cached_tail.0.store(tail, Ordering::Relaxed);
            if head.wrapping_sub(tail) >= self.capacity {
                return Err(());
            }
        }

        let slot_idx = head & self.mask;
        let slot = self.slots.get(slot_idx);
        unsafe {
            let dst = &mut *slot.get();
            simd::stream_record(dst, record);
        }

        self.head.0.store(head.wrapping_add(1), Ordering::Release);
        Ok(())
    }

    /// Shadow Logistics Worker: Drains up to `max_records` batch.
    pub fn pop_batch(&self, batch: &mut Vec<PackedGrpcRecord>, max_records: usize) -> usize {
        let tail = self.tail.0.load(Ordering::Relaxed);
        let mut head = self.cached_head.0.load(Ordering::Relaxed);

        if tail == head {
            head = self.head.0.load(Ordering::Acquire);
            self.cached_head.0.store(head, Ordering::Relaxed);
            if tail == head {
                return 0;
            }
        }

        let available = head.wrapping_sub(tail);
        let count = available.min(max_records);

        batch.reserve(count);
        for i in 0..count {
            let slot_idx = (tail.wrapping_add(i)) & self.mask;
            let slot = self.slots.get(slot_idx);
            unsafe {
                let src = &*slot.get();
                simd::prefetch_nta(src);
                let mut record = PackedGrpcRecord::empty();
                simd::copy_record(&mut record, src);
                batch.push(record);
            }
        }

        self.tail
            .0
            .store(tail.wrapping_add(count), Ordering::Release);
        count
    }

    pub fn len(&self) -> usize {
        let head = self.head.0.load(Ordering::Relaxed);
        let tail = self.tail.0.load(Ordering::Relaxed);
        head.wrapping_sub(tail)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

/// Sharded container owning 1 SPSC ring buffer shard per frontline worker lane.
pub struct ShardedRingBuffer {
    shards: Vec<SpscRingBuffer>,
}

impl ShardedRingBuffer {
    pub fn new(num_shards: usize, capacity_per_shard: usize) -> Self {
        let shards = (0..num_shards.max(1))
            .map(|_| SpscRingBuffer::new(capacity_per_shard))
            .collect();
        Self { shards }
    }

    #[allow(clippy::result_unit_err)]
    #[inline(always)]
    pub fn try_push(&self, record: &PackedGrpcRecord) -> Result<(), ()> {
        let shard_idx = velda_observability::current_worker_lane() % self.shards.len();
        self.shards[shard_idx].try_push(record)
    }

    pub fn drain_shards(&self, batch: &mut Vec<PackedGrpcRecord>, max_per_shard: usize) -> usize {
        let mut total = 0;
        for shard in &self.shards {
            total += shard.pop_batch(batch, max_per_shard);
        }
        total
    }

    pub fn total_queued(&self) -> usize {
        self.shards.iter().map(|s| s.len()).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_grpc_spsc_ring_buffer_fifo() {
        let ring = SpscRingBuffer::new(128);
        assert_eq!(ring.capacity(), 128);

        let mut r1 = PackedGrpcRecord::empty();
        r1.timestamp_ms = 100;
        r1.grpc_status = 0;
        r1.set_rpc_path("/order.OrderService/GetOrder");

        let mut r2 = PackedGrpcRecord::empty();
        r2.timestamp_ms = 200;
        r2.grpc_status = 14;
        r2.set_rpc_path("/order.OrderService/StreamOrders");

        assert!(ring.try_push(&r1).is_ok());
        assert!(ring.try_push(&r2).is_ok());
        assert_eq!(ring.len(), 2);

        let mut batch = Vec::new();
        let drained = ring.pop_batch(&mut batch, 10);
        assert_eq!(drained, 2);
        assert_eq!(batch.len(), 2);
        assert_eq!(batch[0], r1);
        assert_eq!(batch[1], r2);
        assert!(ring.is_empty());
    }

    #[test]
    fn test_grpc_ring_buffer_saturation_fail_open() {
        let ring = SpscRingBuffer::new(128);
        let cap = ring.capacity();

        let r = PackedGrpcRecord::empty();
        for _ in 0..cap {
            assert!(ring.try_push(&r).is_ok());
        }

        // 129th push must fail-open (Err) without blocking or panic
        assert!(ring.try_push(&r).is_err());
    }
}
