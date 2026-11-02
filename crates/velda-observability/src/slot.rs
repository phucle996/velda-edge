//! Cacheline-Aligned In-Memory Pipeline Metrics Slot.
//!
//! Provides thread-safe, lock-free atomic counters and histogram buckets
//! dedicated to a single `(DownstreamProto, UpstreamProto, PipeStrategy)` tuple.
//!
//! # Hardware Invariants
//! - `#[repr(align(64))]`: Aligned strictly to a 64-byte boundary (L1 CPU cacheline length
//!   on x86_64 AMD Zen 3/Zen 4 and Intel Skylake/Sapphire Rapids).
//! - Eliminates False Sharing: Multiple worker threads updating distinct pipeline slots
//!   will never invalidate each other's L1/L2 cache lines (MESI cache protocol isolation).
//! - Zero Locks: All fields are updated using `Ordering::Relaxed` atomics, avoiding
//!   expensive memory fences (`mfence`), bus locks, or CPU pipeline flushes.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

use crate::types::{StatusClass, StreamCompletion};
use crate::vector::PIPELINE_COUNTERS_LEN;

// Canonical Counter Indices in the 20-counter array:
pub const IDX_REQ_2XX: usize = 0;
pub const IDX_REQ_3XX: usize = 1;
pub const IDX_REQ_4XX: usize = 2;
pub const IDX_REQ_5XX: usize = 3;
pub const IDX_REQ_OTHER: usize = 4;
pub const IDX_BYTES_RX: usize = 5;
pub const IDX_BYTES_TX: usize = 6;
pub const IDX_DUR_1MS: usize = 7;
pub const IDX_DUR_5MS: usize = 8;
pub const IDX_DUR_10MS: usize = 9;
pub const IDX_DUR_50MS: usize = 10;
pub const IDX_DUR_250MS: usize = 11;
pub const IDX_DUR_1000MS: usize = 12;
pub const IDX_DUR_INF: usize = 13;
pub const IDX_TTFB_1MS: usize = 14;
pub const IDX_TTFB_5MS: usize = 15;
pub const IDX_TTFB_10MS: usize = 16;
pub const IDX_TTFB_50MS: usize = 17;
pub const IDX_TTFB_250MS: usize = 18;
pub const IDX_TTFB_INF: usize = 19;

/// Cacheline-aligned atomic metrics slot representing a unique pipeline combination.
#[repr(align(64))]
pub struct PipelineSlot {
    /// 20 contiguous atomic 64-bit counters matching `PIPELINE_COUNTERS_LEN`.
    pub counters: [AtomicU64; PIPELINE_COUNTERS_LEN],
    /// Instantaneous number of in-flight active streams currently being served.
    pub active_streams: AtomicI64,
}

impl Default for PipelineSlot {
    fn default() -> Self {
        Self::new()
    }
}

impl PipelineSlot {
    /// Creates a new zeroed pipeline metrics slot.
    pub const fn new() -> Self {
        Self {
            counters: [const { AtomicU64::new(0) }; PIPELINE_COUNTERS_LEN],
            active_streams: AtomicI64::new(0),
        }
    }

    /// Increments the in-flight stream gauge when a request begins.
    #[inline(always)]
    pub fn stream_started(&self) {
        self.active_streams.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrements the in-flight stream gauge when a request ends.
    #[inline(always)]
    pub fn stream_ended(&self) {
        self.active_streams.fetch_sub(1, Ordering::Relaxed);
    }

    /// Records stream completion metrics in a single lock-free atomic sequence.
    ///
    /// # Performance
    /// Executes purely with register operations and cacheline-local atomic adds.
    /// Zero heap allocations, zero system calls, zero mutex contention.
    ///
    /// Uses disjoint exclusive buckets to guarantee EXACTLY ONE atomic addition
    /// for duration and EXACTLY ONE atomic addition for TTFB, cutting atomic cacheline
    /// invalidations (MESI protocol thrashing) by up to 83.3%.
    #[inline(always)]
    pub fn record_completion(&self, completion: &StreamCompletion) {
        // 1. Status Counter
        let status_idx = match StatusClass::from_status_code(completion.status_code) {
            StatusClass::Success2xx => IDX_REQ_2XX,
            StatusClass::Redirect3xx => IDX_REQ_3XX,
            StatusClass::ClientError4xx => IDX_REQ_4XX,
            StatusClass::ServerError5xx => IDX_REQ_5XX,
            StatusClass::Other => IDX_REQ_OTHER,
        };
        self.counters[status_idx].fetch_add(1, Ordering::Relaxed);

        // 2. Transferred Bytes
        if completion.bytes_rx > 0 {
            self.counters[IDX_BYTES_RX].fetch_add(completion.bytes_rx, Ordering::Relaxed);
        }
        if completion.bytes_tx > 0 {
            self.counters[IDX_BYTES_TX].fetch_add(completion.bytes_tx, Ordering::Relaxed);
        }

        // 3. E2E Duration Histogram Buckets (Single atomic add via disjoint branches)
        let dur_ms = completion.duration.as_millis();
        let dur_idx = if dur_ms <= 1 {
            IDX_DUR_1MS
        } else if dur_ms <= 5 {
            IDX_DUR_5MS
        } else if dur_ms <= 10 {
            IDX_DUR_10MS
        } else if dur_ms <= 50 {
            IDX_DUR_50MS
        } else if dur_ms <= 250 {
            IDX_DUR_250MS
        } else if dur_ms <= 1000 {
            IDX_DUR_1000MS
        } else {
            IDX_DUR_INF
        };
        self.counters[dur_idx].fetch_add(1, Ordering::Relaxed);

        // 4. TTFB Histogram Buckets (if captured - single atomic add via disjoint branches)
        if let Some(t) = completion.ttfb {
            let ttfb_ms = t.as_millis();
            let ttfb_idx = if ttfb_ms <= 1 {
                IDX_TTFB_1MS
            } else if ttfb_ms <= 5 {
                IDX_TTFB_5MS
            } else if ttfb_ms <= 10 {
                IDX_TTFB_10MS
            } else if ttfb_ms <= 50 {
                IDX_TTFB_50MS
            } else if ttfb_ms <= 250 {
                IDX_TTFB_250MS
            } else {
                IDX_TTFB_INF
            };
            self.counters[ttfb_idx].fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Returns a direct relaxed snapshot of the raw 20 counters.
    #[inline(always)]
    pub fn load_counters(&self, dst: &mut [u64; PIPELINE_COUNTERS_LEN]) {
        for (i, c) in self.counters.iter().enumerate() {
            dst[i] = c.load(Ordering::Relaxed);
        }
    }
}
