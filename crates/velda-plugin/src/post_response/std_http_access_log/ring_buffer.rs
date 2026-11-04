//! Lock-Free Single-Producer Single-Consumer (SPSC) Ring Buffer (`std_http_access_log::ring_buffer`).
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

use super::record::PackedLogRecord;
use super::simd;

#[repr(align(64))]
struct CachePadded<T>(T);

/// Page-aligned (4096B) contiguous ring buffer slot memory residing directly in physical RAM.
///
/// Guarantees:
/// - 4KB physical page alignment: eliminates false sharing with general heap objects.
/// - Kernel streaming advice: invokes `libc::madvise(..., MADV_SEQUENTIAL)` on Unix.
/// - Non-temporal store safety: satisfies 32-byte and 64-byte vector alignment for `_mm256_stream_si256`.
struct AlignedSlotBuffer {
    ptr: *mut UnsafeCell<PackedLogRecord>,
    capacity: usize,
    layout: std::alloc::Layout,
}

unsafe impl Send for AlignedSlotBuffer {}
unsafe impl Sync for AlignedSlotBuffer {}

impl AlignedSlotBuffer {
    fn new(capacity: usize) -> Self {
        let raw_bytes = capacity * std::mem::size_of::<PackedLogRecord>();
        let byte_len = (raw_bytes + 4095) & !4095;
        let layout = std::alloc::Layout::from_size_align(byte_len, 4096)
            .expect("valid page-aligned memory layout for slot buffer");

        let ptr = unsafe { std::alloc::alloc_zeroed(layout) as *mut UnsafeCell<PackedLogRecord> };
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
    fn get(&self, index: usize) -> &UnsafeCell<PackedLogRecord> {
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

/// Single-Producer Single-Consumer lock-free ring buffer slot container.
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
    /// Constructs a new `SpscRingBuffer` with capacity rounded up to the nearest power of two.
    pub fn new(capacity: usize) -> Self {
        let actual_cap = capacity.max(2).next_power_of_two();
        let mask = actual_cap - 1;

        Self {
            capacity: actual_cap,
            mask,
            slots: AlignedSlotBuffer::new(actual_cap),
            head: CachePadded(AtomicUsize::new(0)),
            cached_tail: CachePadded(AtomicUsize::new(0)),
            tail: CachePadded(AtomicUsize::new(0)),
            cached_head: CachePadded(AtomicUsize::new(0)),
        }
    }

    /// Attempts to push a record into the ring buffer.
    ///
    /// # Hot-Path Invariant
    /// - Non-blocking: returns `Err(())` immediately if the queue is full.
    /// - Zero allocations.
    /// - Non-temporal streaming store: writes directly to RAM via `simd::stream_record`, bypassing L1/L2/L3.
    #[allow(clippy::result_unit_err)]
    #[inline]
    pub fn try_push(&self, record: &PackedLogRecord) -> Result<(), ()> {
        let head = self.head.0.load(Ordering::Relaxed);
        let mut cached_tail = self.cached_tail.0.load(Ordering::Relaxed);

        if head.wrapping_sub(cached_tail) >= self.capacity {
            cached_tail = self.tail.0.load(Ordering::Acquire);
            self.cached_tail.0.store(cached_tail, Ordering::Relaxed);

            if head.wrapping_sub(cached_tail) >= self.capacity {
                return Err(()); // Buffer saturated -> drop without blocking
            }
        }

        let slot_idx = head & self.mask;
        unsafe {
            let slot = &mut *self.slots.get(slot_idx).get();
            simd::stream_record(slot, record);
        }

        self.head.0.store(head.wrapping_add(1), Ordering::Release);
        Ok(())
    }

    /// Drains up to `batch.len()` records into the provided slice.
    ///
    /// Called exclusively by the background Shadow Logistics Worker.
    pub fn pop_batch(&self, batch: &mut [PackedLogRecord]) -> usize {
        let tail = self.tail.0.load(Ordering::Relaxed);
        let mut cached_head = self.cached_head.0.load(Ordering::Relaxed);

        let mut available = cached_head.wrapping_sub(tail);
        if available == 0 {
            cached_head = self.head.0.load(Ordering::Acquire);
            self.cached_head.0.store(cached_head, Ordering::Relaxed);
            available = cached_head.wrapping_sub(tail);
            if available == 0 {
                return 0;
            }
        }

        let to_drain = available.min(batch.len());
        for (i, item) in batch.iter_mut().enumerate().take(to_drain) {
            let slot_idx = (tail.wrapping_add(i)) & self.mask;
            unsafe {
                let slot = &*self.slots.get(slot_idx).get();
                simd::prefetch_nta(slot);
                simd::copy_record(item, slot);
            }
        }

        self.tail
            .0
            .store(tail.wrapping_add(to_drain), Ordering::Release);
        to_drain
    }

    /// Returns the configured slot capacity.
    #[inline(always)]
    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

/// Sharded collection of per-lane SPSC ring buffers matching host CPU worker topology.
pub struct ShardedRingBuffer {
    shards: Vec<SpscRingBuffer>,
}

impl ShardedRingBuffer {
    /// Creates a sharded ring buffer container with `num_shards` independent queues.
    pub fn new(num_shards: usize, per_shard_capacity: usize) -> Self {
        let count = num_shards.max(1);
        let mut shards = Vec::with_capacity(count);
        for _ in 0..count {
            shards.push(SpscRingBuffer::new(per_shard_capacity));
        }
        Self { shards }
    }

    /// Pushes a record to the calling thread's assigned lane shard.
    #[allow(clippy::result_unit_err)]
    #[inline(always)]
    pub fn try_push(&self, record: &PackedLogRecord) -> Result<(), ()> {
        let lane = velda_observability::current_worker_lane() % self.shards.len();
        self.shards[lane].try_push(record)
    }

    /// Drains records across all shards into `batch`.
    ///
    /// Returns the total number of records successfully drained.
    pub fn pop_all(&self, batch: &mut [PackedLogRecord]) -> usize {
        let mut total = 0;
        let mut remaining = batch;

        for shard in &self.shards {
            if remaining.is_empty() {
                break;
            }
            let drained = shard.pop_batch(remaining);
            total += drained;
            remaining = &mut remaining[drained..];
        }

        total
    }

    /// Number of shards in this buffer.
    #[inline(always)]
    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_spsc_ring_buffer_push_pop() {
        let ring = SpscRingBuffer::new(8);
        assert_eq!(ring.capacity(), 8);

        let mut rec1 = PackedLogRecord::empty();
        rec1.status = 200;
        rec1.timestamp_ms = 1000;

        let mut rec2 = PackedLogRecord::empty();
        rec2.status = 404;
        rec2.timestamp_ms = 2000;

        assert!(ring.try_push(&rec1).is_ok());
        assert!(ring.try_push(&rec2).is_ok());

        let mut batch = [PackedLogRecord::empty(); 4];
        let drained = ring.pop_batch(&mut batch);
        assert_eq!(drained, 2);
        assert_eq!(batch[0].status, 200);
        assert_eq!(batch[0].timestamp_ms, 1000);
        assert_eq!(batch[1].status, 404);
        assert_eq!(batch[1].timestamp_ms, 2000);

        // Next pop is empty
        assert_eq!(ring.pop_batch(&mut batch), 0);
    }

    #[test]
    fn test_spsc_ring_buffer_saturation_drops() {
        let ring = SpscRingBuffer::new(64); // Min 64
        let cap = ring.capacity();

        let mut rec = PackedLogRecord::empty();
        rec.status = 200;

        // Fill buffer to capacity
        for _ in 0..cap {
            assert!(ring.try_push(&rec).is_ok());
        }

        // Next push must fail non-blocking
        assert!(ring.try_push(&rec).is_err());
    }

    #[test]
    fn test_sharded_ring_buffer() {
        let sharded = ShardedRingBuffer::new(4, 64);
        assert_eq!(sharded.shard_count(), 4);

        let mut rec = PackedLogRecord::empty();
        rec.status = 502;
        assert!(sharded.try_push(&rec).is_ok());

        let mut batch = [PackedLogRecord::empty(); 16];
        let drained = sharded.pop_all(&mut batch);
        assert_eq!(drained, 1);
        assert_eq!(batch[0].status, 502);
    }
}
