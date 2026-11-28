//! Cacheline-Aligned In-Memory Pipeline Metrics Slot (`velda_observability::metrics::slot`).
//!
//! ```text
//! ┌────────────────────────────────────────────────────────────────────────────────────────┐
//! │               PIPELINESLOT BITMASK + L1D LAYOUT (64-BYTE ALIGNED)                      │
//! ├────────────────────────────────────────────────────────────────────────────────────────┤
//! │ CACHE LINE 0 (Bytes 0..64):                                                            │
//! │ 0..8   : status_mask    (AtomicU64 bit i set iff status_counts[i] > 0)                 │
//! │ 8..16  : active_streams (AtomicI64 in-flight gauge)                                    │
//! │ 16..64 : status_counts[0..6] (Completed, ClientCanceled, UpstreamError, ...)           │
//! ├────────────────────────────────────────────────────────────────────────────────────────┤
//! │ CACHE LINES 1..6:                                                                      │
//! │ Remaining status_counts[6..InternalStatus::COUNT] (cold fault counters untouched       │
//! │ on normal traffic)                                                                     │
//! ├────────────────────────────────────────────────────────────────────────────────────────┤
//! │ TRANSPORT & LATENCY BLOCK (`counters[0..16]`, 128B SIMD Aligned):                      │
//! │ 0: BYTES_RX | 1: BYTES_TX | 2..9: DUR_1MS..DUR_INF | 9..15: TTFB_1MS..TTFB_INF | 15:pad│
//! └────────────────────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Hardware Invariants
//! - Load-Before-Or Hot Path: `record_completion` checks `(status_mask.load & bit) == 0`
//!   before executing `fetch_or`, eliminating atomic RMW on `status_mask` for 99.9999% of traffic.
//! - Zero Locks & Core Isolation: Per-core shard writes use `Ordering::Relaxed` only.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

use velda_core::InternalStatus;

use super::counter::PIPELINE_COUNTERS_LEN;
use super::types::StreamCompletion;

// Compile-time guard: 64-bit `status_mask` accommodates up to 64 states.
const _: () = assert!(InternalStatus::COUNT <= 64);

// Canonical Counter Indices in the 16-counter transport & latency array:
pub const IDX_BYTES_RX: usize = 0;
pub const IDX_BYTES_TX: usize = 1;
pub const IDX_DUR_1MS: usize = 2;
pub const IDX_DUR_5MS: usize = 3;
pub const IDX_DUR_10MS: usize = 4;
pub const IDX_DUR_50MS: usize = 5;
pub const IDX_DUR_250MS: usize = 6;
pub const IDX_DUR_1000MS: usize = 7;
pub const IDX_DUR_INF: usize = 8;
pub const IDX_TTFB_1MS: usize = 9;
pub const IDX_TTFB_5MS: usize = 10;
pub const IDX_TTFB_10MS: usize = 11;
pub const IDX_TTFB_50MS: usize = 12;
pub const IDX_TTFB_250MS: usize = 13;
pub const IDX_TTFB_INF: usize = 14;

/// Cacheline-aligned atomic metrics slot representing a unique pipeline combination.
#[repr(align(64))]
pub struct PipelineSlot {
    /// Bitmask where bit `i` is set once `status_counts[i]` has been incremented at least once.
    pub status_mask: AtomicU64,
    /// Instantaneous number of in-flight active streams currently being served.
    pub active_streams: AtomicI64,
    /// Per-state monotonic request counters indexed by `InternalStatus as usize`.
    pub status_counts: [AtomicU64; InternalStatus::COUNT],
    /// Contiguous transport & latency histogram counters matching `PIPELINE_COUNTERS_LEN`.
    pub counters: [AtomicU64; PIPELINE_COUNTERS_LEN],
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
            status_mask: AtomicU64::new(0),
            active_streams: AtomicI64::new(0),
            status_counts: [const { AtomicU64::new(0) }; InternalStatus::COUNT],
            counters: [const { AtomicU64::new(0) }; PIPELINE_COUNTERS_LEN],
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

    /// Records stream completion metrics in a lock-free sequence.
    #[inline(always)]
    pub fn record_completion(&self, completion: &StreamCompletion) {
        // 1. Increment status counter first, then flip its bit in status_mask once (load-before-or).
        let idx = completion.internal_status as usize;
        self.status_counts[idx].fetch_add(1, Ordering::Relaxed);
        let bit = 1u64 << idx;
        if (self.status_mask.load(Ordering::Relaxed) & bit) == 0 {
            self.status_mask.fetch_or(bit, Ordering::Relaxed);
        }

        // 2. Transferred Bytes
        if completion.bytes_rx > 0 {
            self.counters[IDX_BYTES_RX].fetch_add(completion.bytes_rx, Ordering::Relaxed);
        }
        if completion.bytes_tx > 0 {
            self.counters[IDX_BYTES_TX].fetch_add(completion.bytes_tx, Ordering::Relaxed);
        }

        // 3. E2E Duration Histogram Buckets (Single atomic add via disjoint branches)
        let dur_idx = if completion.duration_ns <= 1_000_000 {
            IDX_DUR_1MS
        } else if completion.duration_ns <= 5_000_000 {
            IDX_DUR_5MS
        } else if completion.duration_ns <= 10_000_000 {
            IDX_DUR_10MS
        } else if completion.duration_ns <= 50_000_000 {
            IDX_DUR_50MS
        } else if completion.duration_ns <= 250_000_000 {
            IDX_DUR_250MS
        } else if completion.duration_ns <= 1_000_000_000 {
            IDX_DUR_1000MS
        } else {
            IDX_DUR_INF
        };
        self.counters[dur_idx].fetch_add(1, Ordering::Relaxed);

        // 4. TTFB Histogram Buckets (if captured - single atomic add via disjoint branches)
        if completion.ttfb_ns > 0 {
            let ttfb_idx = if completion.ttfb_ns <= 1_000_000 {
                IDX_TTFB_1MS
            } else if completion.ttfb_ns <= 5_000_000 {
                IDX_TTFB_5MS
            } else if completion.ttfb_ns <= 10_000_000 {
                IDX_TTFB_10MS
            } else if completion.ttfb_ns <= 50_000_000 {
                IDX_TTFB_50MS
            } else if completion.ttfb_ns <= 250_000_000 {
                IDX_TTFB_250MS
            } else {
                IDX_TTFB_INF
            };
            self.counters[ttfb_idx].fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Accumulates active status counters indicated by `mask` directly into `dst`.
    #[inline(always)]
    pub fn accumulate_status_counts(&self, mut mask: u64, dst: &mut [u64; InternalStatus::COUNT]) {
        while mask != 0 {
            let idx = mask.trailing_zeros() as usize;
            mask &= mask - 1;
            if idx < InternalStatus::COUNT {
                dst[idx] = dst[idx].saturating_add(self.status_counts[idx].load(Ordering::Relaxed));
            }
        }
    }

    /// Returns a direct relaxed snapshot of the raw 16 transport & histogram counters.
    #[inline(always)]
    pub fn load_counters(&self, dst: &mut [u64; PIPELINE_COUNTERS_LEN]) {
        let mut i = 0;
        while i < PIPELINE_COUNTERS_LEN {
            dst[i] = self.counters[i].load(Ordering::Relaxed);
            dst[i + 1] = self.counters[i + 1].load(Ordering::Relaxed);
            dst[i + 2] = self.counters[i + 2].load(Ordering::Relaxed);
            dst[i + 3] = self.counters[i + 3].load(Ordering::Relaxed);
            i += 4;
        }
    }
}
