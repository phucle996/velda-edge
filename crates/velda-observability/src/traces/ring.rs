//! Pre-Allocated 64-Byte Aligned SPSC Ring Buffer (`velda_observability::traces::ring`).
//!
//! ```text
//!   FRONTLINE TRAFFIC WORKER (Core-Local Hot Path)     SHADOW LOGISTICS WORKER (SCHED_IDLE)
//!  ┌──────────────────────────────────────────────┐   ┌─────────────────────────────────────────────┐
//!  │ SpanRecord (64B, on Stack)                   │   │ 1. Non-Temporal Load (_mm256_stream_load)   │
//!  │   │                                          │   │    Bypass L1d for consumed ring slots       │
//!  │   ▼                                          │   │      │                                      │
//!  │ Non-Temporal Store (_mm256_stream) + sfence  │   │      ▼                                      │
//!  │ Bypass L1d directly to RAM (L1 is gold)      │   │ 2. Non-Temporal Store (_mm256_stream)       │
//!  └──────────────────────┬───────────────────────┘   │    Bypass L1d directly to L3/RAM + sfence   │
//!                         │                           └──────────────────────┬──────────────────────┘
//!                         ▼                                                  │
//!          ┌────────────────────────── SPSC Ring Buffer ─────────────────────┴────────┐
//!          │ Slot 0 │ Slot 1 │ Slot 2 │ ... │ Slot N-1 (Pre-allocated in RAM / 256 KB) │
//!          └──────────────────────────────────────────────────────────────────────────┘
//!                                                                            │
//!                                                                            ▼
//!                                                           Stage into L2 via _MM_HINT_T1 (Skip L1d)
//!                                                           Process 64B Record Window in Registers
//! ```

use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use super::record::SpanRecord;

/// Default per-core SPSC ring capacity (4096 records × 64B = 256 KB per core).
pub const DEFAULT_TRACE_CORE_RING_CAPACITY: usize = 4096;

#[repr(align(64))]
struct CachePadded<T>(T);

/// Pre-allocated per-core Single-Producer Single-Consumer (SPSC) 64-byte aligned ring buffer for spans.
pub struct SpscSpanRing {
    capacity: usize,
    mask: usize,
    buffer: Box<[UnsafeCell<SpanRecord>]>,
    head: CachePadded<AtomicUsize>,
    cached_tail: CachePadded<AtomicUsize>,
    tail: CachePadded<AtomicUsize>,
    cached_head: CachePadded<AtomicUsize>,
    pub dropped: AtomicU64,
}

unsafe impl Send for SpscSpanRing {}
unsafe impl Sync for SpscSpanRing {}

impl SpscSpanRing {
    pub fn new(capacity: usize) -> Self {
        let cap = capacity.max(64).next_power_of_two();
        let mask = cap - 1;
        let mut vec = Vec::with_capacity(cap);
        for _ in 0..cap {
            vec.push(UnsafeCell::new(SpanRecord::empty()));
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

    /// Writes a 64-byte [`SpanRecord`] into the SPSC ring using Non-Temporal Streaming Stores
    /// (`VMOVNTDQ` / `MOVNTDQ` + `_mm_sfence()`) to bypass L1d cache allocation ("L1 is gold").
    #[inline]
    pub fn push(&self, record: &SpanRecord) -> bool {
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
                    let s = record as *const SpanRecord as *const __m256i;
                    let d = dst as *mut __m256i;
                    let v0 = _mm256_load_si256(s);
                    let v1 = _mm256_load_si256(s.add(1));
                    _mm256_stream_si256(d, v0);
                    _mm256_stream_si256(d.add(1), v1);
                    _mm_sfence();
                } else {
                    let s = record as *const SpanRecord as *const __m128i;
                    let d = dst as *mut __m128i;
                    for i in 0..4 {
                        let v = _mm_load_si128(s.add(i));
                        _mm_stream_si128(d.add(i), v);
                    }
                    _mm_sfence();
                }
            }

            #[cfg(not(target_arch = "x86_64"))]
            {
                std::ptr::copy_nonoverlapping(record as *const SpanRecord, dst, 1);
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
            self.tail.0.store(head, Ordering::Release);
            self.cached_head.0.store(head, Ordering::Relaxed);
        }
        count
    }

    /// Drains up to `max_records` from the ring buffer into an owned `Vec<SpanRecord>`.
    ///
    /// Microarchitectural Cache Staging:
    /// - Non-Temporal Streaming Loads (`_mm256_stream_load_si256` / `VMOVNTDQA`) on source ring slots
    ///   bypass L1d cache allocation for consumed slots.
    /// - Non-Temporal Streaming Stores (`_mm256_stream_si256` / `VMOVNTDQ` + `_mm_sfence()`) on destination batch
    ///   write directly to L3 (LLC) or RAM via Write-Combining buffers, bypassing L1d cache allocation.
    /// - Lookahead prefetch with `_MM_HINT_T1` stages upcoming lines into L2/L3 while explicitly skipping L1d.
    pub fn drain_batch(&self, max_records: usize) -> Vec<SpanRecord> {
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
                }
            }

            let src = unsafe { self.buffer.get_unchecked(idx).get() };
            let dst = slot_uninit.as_mut_ptr();

            debug_assert!((src as usize) & 63 == 0);
            debug_assert!((dst as usize) & 63 == 0);

            #[cfg(target_arch = "x86_64")]
            unsafe {
                use core::arch::x86_64::*;
                if has_avx2 {
                    let s = src as *const __m256i;
                    let d = dst as *mut __m256i;

                    // 1. Non-Temporal Streaming Loads (VMOVNTDQA): loads 2 x 32-byte chunks directly into registers,
                    // bypassing L1d cache allocation for the consumed ring buffer slot.
                    let v0 = _mm256_stream_load_si256(s);
                    let v1 = _mm256_stream_load_si256(s.add(1));

                    // 2. Non-Temporal Streaming Stores (VMOVNTDQ): writes destination batch directly to L3 (LLC)
                    // through Write-Combining buffers, keeping L1d completely untouched ("L1 is gold").
                    _mm256_stream_si256(d, v0);
                    _mm256_stream_si256(d.add(1), v1);
                } else {
                    let s = src as *const __m128i;
                    let d = dst as *mut __m128i;
                    for j in 0..4 {
                        let v = _mm_load_si128(s.add(j));
                        _mm_stream_si128(d.add(j), v);
                    }
                }
            }

            #[cfg(not(target_arch = "x86_64"))]
            unsafe {
                std::ptr::copy_nonoverlapping(src, dst, 1);
            }
        }

        #[cfg(target_arch = "x86_64")]
        unsafe {
            use core::arch::x86_64::_mm_sfence;
            _mm_sfence();
        }

        unsafe {
            out.set_len(to_take);
        }

        self.tail
            .0
            .store(tail.wrapping_add(to_take), Ordering::Release);
        out
    }
}
