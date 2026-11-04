//! Lock-Free SPSC Ring Buffer in Physical RAM (`opentelemetry_logger::ring_buffer`).
//!
//! Provides hardware-isolated, per-worker lane SPSC queues for zero-contention
//! log record dispatching between Frontline Traffic Workers and Shadow Logistics Workers.
//!
//! # Micro-Architectural Guarantees
//! - **Per-Lane Sharded**: Each worker thread accesses strictly its own dedicated ring buffer shard.
//! - **Cacheline Isolated**: Head, tail, and buffer data pointers are isolated to 64-byte boundaries.
//! - **Non-Temporal Store**: Hot-path pushes write directly to RAM via `simd::stream_record`, bypassing CPU cache.
//! - **Handoff Only**: Worker only updates an atomic tail pointer index in ~1ns.

use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::record::PackedOtlpRecord;
use super::simd;

#[repr(align(64))]
struct CachePadded<T>(T);

struct AlignedSlotBuffer {
    ptr: *mut UnsafeCell<PackedOtlpRecord>,
    capacity: usize,
    layout: std::alloc::Layout,
}

unsafe impl Send for AlignedSlotBuffer {}
unsafe impl Sync for AlignedSlotBuffer {}

impl AlignedSlotBuffer {
    fn new(capacity: usize) -> Self {
        let raw_bytes = capacity * std::mem::size_of::<PackedOtlpRecord>();
        let byte_len = (raw_bytes + 4095) & !4095;
        let layout = std::alloc::Layout::from_size_align(byte_len, 4096)
            .expect("valid page-aligned memory layout for slot buffer");

        let ptr = unsafe { std::alloc::alloc_zeroed(layout) as *mut UnsafeCell<PackedOtlpRecord> };
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
    fn get(&self, index: usize) -> &UnsafeCell<PackedOtlpRecord> {
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

    #[allow(clippy::result_unit_err)]
    #[inline]
    pub fn try_push(&self, record: &PackedOtlpRecord) -> Result<(), ()> {
        let head = self.head.0.load(Ordering::Relaxed);
        let mut cached_tail = self.cached_tail.0.load(Ordering::Relaxed);

        if head.wrapping_sub(cached_tail) >= self.capacity {
            cached_tail = self.tail.0.load(Ordering::Acquire);
            self.cached_tail.0.store(cached_tail, Ordering::Relaxed);

            if head.wrapping_sub(cached_tail) >= self.capacity {
                return Err(());
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

    pub fn pop_batch(&self, batch: &mut [PackedOtlpRecord]) -> usize {
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
    pub fn new(num_shards: usize, per_shard_capacity: usize) -> Self {
        let count = num_shards.max(1);
        let mut shards = Vec::with_capacity(count);
        for _ in 0..count {
            shards.push(SpscRingBuffer::new(per_shard_capacity));
        }
        Self { shards }
    }

    #[allow(clippy::result_unit_err)]
    #[inline(always)]
    pub fn try_push(&self, record: &PackedOtlpRecord) -> Result<(), ()> {
        let lane = velda_observability::current_worker_lane() % self.shards.len();
        self.shards[lane].try_push(record)
    }

    pub fn pop_all(&self, batch: &mut [PackedOtlpRecord]) -> usize {
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

    #[inline(always)]
    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }
}
