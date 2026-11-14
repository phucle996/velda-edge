//! Pre-Allocated 64-Byte Aligned SPSC Ring Buffer (`velda_observability::logs::ring`).
//!
//! # Hardware Mechanism & L1 Cache Protection ("L1 is Gold")
//! 1. **Pre-Allocated 256-Byte Stride Buffer (4 CPU Cache Lines)**:
//!    `Box<[UnsafeCell<AccessLogRecord>]>` is allocated once at worker initialization. Because
//!    [`AccessLogRecord`] is `#[repr(C, align(64))]` with size 256, the slice base address and every
//!    slot offset are strictly 64-byte aligned (each record spans exactly 4 cache lines).
//! 2. **Non-Temporal Streaming Store on Push (`_mm256_stream_si256` + `_mm_sfence()`)**:
//!    `velda-worker-{id}` writes the 256-byte record through CPU Write-Combining buffers directly
//!    to L3/RAM without allocating a cache line in L1d, protecting hot routing and socket state.
//!    `_mm_sfence()` enforces global visibility of the Write-Combining buffer before the
//!    `Ordering::Release` store on `head`.
//! 3. **Non-Temporal Streaming Load on Drain (`_mm256_stream_load_si256` / `VMOVNTDQA`)**:
//!    `velda-shadow-{id}` drains ring slots directly using Non-Temporal Streaming Loads (`VMOVNTDQA`),
//!    bypassing L1d cache allocation for dead ring buffer slots so background log draining
//!    never evicts hot routing tables, connection pools, or metadata of its sibling Frontline thread.
//!    Upcoming slots are prefetched ahead into L2/L3 via `_MM_HINT_T1` to eliminate DRAM wait latency.

use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use super::record::AccessLogRecord;

/// Default per-core SPSC ring capacity (4096 records × 256B = 1 MB per core).
pub const DEFAULT_CORE_RING_CAPACITY: usize = 4096;

#[repr(align(64))]
struct CachePadded<T>(T);

/// Pre-allocated per-core Single-Producer Single-Consumer (SPSC) 64-byte aligned ring buffer.
pub struct SpscLogRing {
    capacity: usize,
    mask: usize,
    buffer: Box<[UnsafeCell<AccessLogRecord>]>,
    head: CachePadded<AtomicUsize>,
    cached_tail: CachePadded<AtomicUsize>,
    tail: CachePadded<AtomicUsize>,
    cached_head: CachePadded<AtomicUsize>,
    pub dropped: AtomicU64,
}

unsafe impl Send for SpscLogRing {}
unsafe impl Sync for SpscLogRing {}

impl SpscLogRing {
    pub fn new(capacity: usize) -> Self {
        let cap = capacity.max(64).next_power_of_two();
        let mask = cap - 1;
        let mut vec = Vec::with_capacity(cap);
        for _ in 0..cap {
            vec.push(UnsafeCell::new(AccessLogRecord::zeroed()));
        }

        Self {
            capacity: cap,
            mask,
            buffer: vec.into_boxed_slice(),
            head: CachePadded(AtomicUsize::new(0)),
            cached_tail: CachePadded(AtomicUsize::new(0)),
            tail: CachePadded(AtomicUsize::new(0)),
            cached_head: CachePadded(AtomicUsize::new(0)),
            dropped: AtomicU64::new(0),
        }
    }

    /// Writes a 64-byte [`AccessLogRecord`] into the SPSC ring using Non-Temporal Streaming Stores
    /// (`VMOVNTDQ` / `MOVNTDQ` + `_mm_sfence()`) to bypass L1d cache allocation ("L1 is gold").
    #[inline]
    pub fn push(&self, record: &AccessLogRecord) -> bool {
        let head = self.head.0.load(Ordering::Relaxed);
        let mut tail = self.cached_tail.0.load(Ordering::Relaxed);

        if head.wrapping_sub(tail) >= self.capacity {
            tail = self.tail.0.load(Ordering::Acquire);
            self.cached_tail.0.store(tail, Ordering::Relaxed);
            if head.wrapping_sub(tail) >= self.capacity {
                self.dropped.fetch_add(1, Ordering::Relaxed);
                return false;
            }
        }

        let idx = head & self.mask;
        unsafe {
            let dst = self.buffer.get_unchecked(idx).get();
            debug_assert!((dst as usize) & 63 == 0);

            #[cfg(target_arch = "x86_64")]
            {
                use core::arch::x86_64::*;
                if std::is_x86_feature_detected!("avx2") {
                    let s = record as *const AccessLogRecord as *const __m256i;
                    let d = dst as *mut __m256i;
                    let v0 = _mm256_load_si256(s);
                    let v1 = _mm256_load_si256(s.add(1));
                    let v2 = _mm256_load_si256(s.add(2));
                    let v3 = _mm256_load_si256(s.add(3));
                    let v4 = _mm256_load_si256(s.add(4));
                    let v5 = _mm256_load_si256(s.add(5));
                    let v6 = _mm256_load_si256(s.add(6));
                    let v7 = _mm256_load_si256(s.add(7));
                    _mm256_stream_si256(d, v0);
                    _mm256_stream_si256(d.add(1), v1);
                    _mm256_stream_si256(d.add(2), v2);
                    _mm256_stream_si256(d.add(3), v3);
                    _mm256_stream_si256(d.add(4), v4);
                    _mm256_stream_si256(d.add(5), v5);
                    _mm256_stream_si256(d.add(6), v6);
                    _mm256_stream_si256(d.add(7), v7);
                    _mm_sfence();
                } else {
                    let s = record as *const AccessLogRecord as *const __m128i;
                    let d = dst as *mut __m128i;
                    for i in 0..16 {
                        let v = _mm_load_si128(s.add(i));
                        _mm_stream_si128(d.add(i), v);
                    }
                    _mm_sfence();
                }
            }

            #[cfg(not(target_arch = "x86_64"))]
            {
                std::ptr::copy_nonoverlapping(record as *const AccessLogRecord, dst, 1);
                std::sync::atomic::fence(Ordering::Release);
            }
        }

        self.head.0.store(head.wrapping_add(1), Ordering::Release);
        true
    }

    /// Discards all queued records in $\mathcal{O}(1)$ when no exporter plugins are active.
    #[inline]
    pub fn discard_all(&self) -> usize {
        let tail = self.tail.0.load(Ordering::Relaxed);
        let head = self.head.0.load(Ordering::Acquire);
        let count = head.wrapping_sub(tail);
        if count > 0 {
            self.cached_head.0.store(head, Ordering::Relaxed);
            self.tail.0.store(head, Ordering::Release);
        }
        count
    }

    /// Drains up to `max_records` from the ring using Non-Temporal Streaming Loads (`VMOVNTDQA` / `MOVNTDQA`)
    /// and `_MM_HINT_T1` (L2/L3 cache prefetch lookahead).
    ///
    /// # Microarchitectural Rationale ("L1 is Gold")
    /// Shadow Workers drain up to 4,096 records (1 MB of memory) in a single burst.
    /// Because each log record is read exactly once and then discarded from the ring buffer,
    /// regular memory loads (`std::ptr::read`) would pull 1 MB of dead ring slots through L1d,
    /// evicting 100% of L1d (32–48 KB) and thrashing L2, which stalls sibling frontline serving threads.
    ///
    /// By using `_mm256_stream_load_si256` (`VMOVNTDQA`) on AVX2 or `_mm_stream_load_si128` (`MOVNTDQA`) on SSE4.1:
    /// - Data streams directly from L3/RAM through CPU Line Fill Buffers (LFB) into vector registers,
    ///   completely bypassing L1d cache allocation for the consumed ring slots.
    /// - Data is stored into the newly allocated output batch slice using regular vector stores,
    ///   ensuring the batch is placed into cache for active exporter plugins to serialize.
    pub fn drain_batch(&self, max_records: usize) -> Vec<AccessLogRecord> {
        let tail = self.tail.0.load(Ordering::Relaxed);
        let mut head = self.cached_head.0.load(Ordering::Relaxed);

        let mut avail = head.wrapping_sub(tail);
        if avail == 0 {
            head = self.head.0.load(Ordering::Acquire);
            self.cached_head.0.store(head, Ordering::Relaxed);
            avail = head.wrapping_sub(tail);
            if avail == 0 {
                return Vec::new();
            }
        }

        let to_take = avail.min(max_records);
        let mut out = Vec::with_capacity(to_take);
        let spare = out.spare_capacity_mut();

        #[cfg(target_arch = "x86_64")]
        let has_avx2 = std::is_x86_feature_detected!("avx2");

        for (i, slot_uninit) in spare[..to_take].iter_mut().enumerate() {
            let idx = tail.wrapping_add(i) & self.mask;

            #[cfg(target_arch = "x86_64")]
            if i + 4 < to_take {
                let prefetch_idx = tail.wrapping_add(i + 4) & self.mask;
                unsafe {
                    use core::arch::x86_64::*;
                    let slot = self.buffer.get_unchecked(prefetch_idx).get() as *const i8;
                    _mm_prefetch(slot, _MM_HINT_T1);
                    _mm_prefetch(slot.add(64), _MM_HINT_T1);
                    _mm_prefetch(slot.add(128), _MM_HINT_T1);
                    _mm_prefetch(slot.add(192), _MM_HINT_T1);
                }
            }

            let src = unsafe { self.buffer.get_unchecked(idx).get() };
            let dst = slot_uninit.as_mut_ptr();

            // Both src (slot in Box<[UnsafeCell<AccessLogRecord>]>) and dst (element in Vec<AccessLogRecord>)
            // are strictly 64-byte aligned due to #[repr(C, align(64))].
            debug_assert!((src as usize) & 63 == 0);
            debug_assert!((dst as usize) & 63 == 0);

            #[cfg(target_arch = "x86_64")]
            unsafe {
                use core::arch::x86_64::*;
                if has_avx2 {
                    let s = src as *const __m256i;
                    let d = dst as *mut __m256i;

                    // Non-Temporal Streaming Loads (VMOVNTDQA): loads 8 x 32-byte chunks directly into registers,
                    // bypassing L1d cache allocation for the consumed ring buffer slot ("L1 is gold").
                    let v0 = _mm256_stream_load_si256(s);
                    let v1 = _mm256_stream_load_si256(s.add(1));
                    let v2 = _mm256_stream_load_si256(s.add(2));
                    let v3 = _mm256_stream_load_si256(s.add(3));
                    let v4 = _mm256_stream_load_si256(s.add(4));
                    let v5 = _mm256_stream_load_si256(s.add(5));
                    let v6 = _mm256_stream_load_si256(s.add(6));
                    let v7 = _mm256_stream_load_si256(s.add(7));

                    // Store into exporter destination batch slice (regular store to keep batch in cache for serialization)
                    _mm256_store_si256(d, v0);
                    _mm256_store_si256(d.add(1), v1);
                    _mm256_store_si256(d.add(2), v2);
                    _mm256_store_si256(d.add(3), v3);
                    _mm256_store_si256(d.add(4), v4);
                    _mm256_store_si256(d.add(5), v5);
                    _mm256_store_si256(d.add(6), v6);
                    _mm256_store_si256(d.add(7), v7);
                } else {
                    // SSE4.1 fallback: 16 x 16-byte chunks using _mm_stream_load_si128
                    let s = src as *const __m128i;
                    let d = dst as *mut __m128i;
                    for k in 0..16 {
                        let v = _mm_stream_load_si128(s.add(k));
                        _mm_store_si128(d.add(k), v);
                    }
                }
            }

            #[cfg(not(target_arch = "x86_64"))]
            unsafe {
                std::ptr::copy_nonoverlapping(src, dst, 1);
            }
        }

        // SAFETY: All `to_take` elements in `out` were initialized above via direct SIMD/pointer copy.
        unsafe {
            out.set_len(to_take);
        }

        self.tail
            .0
            .store(tail.wrapping_add(to_take), Ordering::Release);
        out
    }
}
