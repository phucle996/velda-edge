//! Hardware-Accelerated Metric Counter Accumulation & Bitmask Reduction (`velda_observability::metrics::counter`).
//!
//! ```text
//! ┌────────────────────────────────────────────────────────────────────────────────────────┐
//! │         16-COUNTER TRANSPORT/HISTOGRAM SIMD + BITMASK-GATED STATUS REDUCTION           │
//! ├────────────────────────────────────────────────────────────────────────────────────────┤
//! │ 1. Transport & Histogram Slice (`PIPELINE_COUNTERS_LEN = 16`, 128B):                   │
//! │    [ 0..2  : Bytes RX, Bytes TX ]                                                      │
//! │    [ 2..9  : Duration Buckets (<=1ms, 5ms, 10ms, 50ms, 250ms, 1000ms, >1000ms) ]       │
//! │    [ 9..15 : TTFB Buckets (<=1ms, 5ms, 10ms, 50ms, 250ms, >250ms) ]                    │
//! │    [ 15    : Alignment Pad (exact 4x 256-bit AVX2 / 8x 128-bit NEON registers) ]       │
//! ├────────────────────────────────────────────────────────────────────────────────────────┤
//! │ 2. InternalStatus Bitmask Scan (`status_mask: u64`, TZCNT + BLSR):                     │
//! │    while mask != 0 {                                                                   │
//! │        idx = mask.trailing_zeros();   // Hardware jump directly to next seen status    │
//! │        mask &= mask - 1;              // Clear lowest set bit in 1 CPU cycle           │
//! │        dst[idx] += src[idx];                                                           │
//! │    }                                                                                   │
//! └────────────────────────────────────────────────────────────────────────────────────────┘
//! ```

use velda_core::InternalStatus;

/// Number of contiguous transport & latency histogram 64-bit counters per pipeline slot.
///
/// Padded to 16 quadwords (128 bytes = 2 cache lines = 4x 256-bit AVX2 registers).
pub const PIPELINE_COUNTERS_LEN: usize = 16;

/// Adds 16 contiguous transport/latency 64-bit counters from `src` into `dst`.
#[inline]
pub fn add_pipeline_counters(
    dst: &mut [u64; PIPELINE_COUNTERS_LEN],
    src: &[u64; PIPELINE_COUNTERS_LEN],
) {
    for i in 0..PIPELINE_COUNTERS_LEN {
        dst[i] = dst[i].saturating_add(src[i]);
    }
}

/// Accumulates only active [`InternalStatus`] counters selected by `mask` via hardware bit-scan.
#[inline]
pub fn add_status_counts(
    dst: &mut [u64; InternalStatus::COUNT],
    src: &[u64; InternalStatus::COUNT],
    mut mask: u64,
) {
    while mask != 0 {
        let idx = mask.trailing_zeros() as usize;
        mask &= mask - 1;
        if idx < InternalStatus::COUNT {
            dst[idx] = dst[idx].saturating_add(src[idx]);
        }
    }
}

/// Aggregates counter slices of arbitrary length using the optimal path.
#[inline]
pub fn add_counters(dst: &mut [u64], src: &[u64]) {
    let len = dst.len().min(src.len());
    if len >= PIPELINE_COUNTERS_LEN {
        let dst_arr = unsafe { &mut *(dst.as_mut_ptr() as *mut [u64; PIPELINE_COUNTERS_LEN]) };
        let src_arr = unsafe { &*(src.as_ptr() as *const [u64; PIPELINE_COUNTERS_LEN]) };
        add_pipeline_counters(dst_arr, src_arr);
        for i in PIPELINE_COUNTERS_LEN..len {
            dst[i] = dst[i].saturating_add(src[i]);
        }
    } else {
        for i in 0..len {
            dst[i] = dst[i].saturating_add(src[i]);
        }
    }
}

/// Aggregated metrics totals across multiple pipelines.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PipelineMetricsTotals {
    pub status_mask: u64,
    pub status_counts: [u64; InternalStatus::COUNT],
    pub bytes_rx: u64,
    pub bytes_tx: u64,
    pub duration_le_1ms: u64,
    pub duration_le_5ms: u64,
    pub duration_le_10ms: u64,
    pub duration_le_50ms: u64,
    pub duration_le_250ms: u64,
    pub duration_le_1000ms: u64,
    pub duration_gt_1000ms: u64,
    pub ttfb_le_1ms: u64,
    pub ttfb_le_5ms: u64,
    pub ttfb_le_10ms: u64,
    pub ttfb_le_50ms: u64,
    pub ttfb_le_250ms: u64,
    pub ttfb_gt_250ms: u64,
}

impl Default for PipelineMetricsTotals {
    fn default() -> Self {
        Self {
            status_mask: 0,
            status_counts: [0; InternalStatus::COUNT],
            bytes_rx: 0,
            bytes_tx: 0,
            duration_le_1ms: 0,
            duration_le_5ms: 0,
            duration_le_10ms: 0,
            duration_le_50ms: 0,
            duration_le_250ms: 0,
            duration_le_1000ms: 0,
            duration_gt_1000ms: 0,
            ttfb_le_1ms: 0,
            ttfb_le_5ms: 0,
            ttfb_le_10ms: 0,
            ttfb_le_50ms: 0,
            ttfb_le_250ms: 0,
            ttfb_gt_250ms: 0,
        }
    }
}

impl PipelineMetricsTotals {
    /// Returns the total request count recorded for a specific [`InternalStatus`].
    #[inline(always)]
    pub const fn count(&self, status: InternalStatus) -> u64 {
        self.status_counts[status as usize]
    }

    /// Computes total requests across all observed states using hardware bit-scan over `status_mask`.
    #[inline]
    pub fn total_requests(&self) -> u64 {
        let mut sum = 0u64;
        let mut mask = self.status_mask;
        while mask != 0 {
            let idx = mask.trailing_zeros() as usize;
            mask &= mask - 1;
            if idx < InternalStatus::COUNT {
                sum = sum.saturating_add(self.status_counts[idx]);
            }
        }
        sum
    }

    /// Computes success rate in basis points (1 bp = 0.01%, 10,000 = 100.00%).
    #[inline]
    pub fn success_rate_basis_points(&self) -> u32 {
        let total = self.total_requests();
        if total == 0 {
            10_000
        } else {
            let completed = self.status_counts[InternalStatus::Completed as usize];
            ((completed as u128 * 10_000) / total as u128) as u32
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_counter_simd_equivalence() {
        let mut base_dst = [0u64; PIPELINE_COUNTERS_LEN];
        let mut src = [0u64; PIPELINE_COUNTERS_LEN];
        for i in 0..PIPELINE_COUNTERS_LEN {
            base_dst[i] = (i as u64 + 1) * 1_000_000;
            src[i] = (i as u64 + 1) * 2_500_000;
        }

        let mut dst = base_dst;
        add_pipeline_counters(&mut dst, &src);

        for i in 0..PIPELINE_COUNTERS_LEN {
            assert_eq!(dst[i], base_dst[i].saturating_add(src[i]));
        }
    }

    #[test]
    fn test_bitmask_status_aggregation_and_totals() {
        let mut a = [0u64; InternalStatus::COUNT];
        let mut b = [0u64; InternalStatus::COUNT];
        a[InternalStatus::Completed as usize] = 100;
        a[InternalStatus::ClientCanceled as usize] = 20;
        b[InternalStatus::RouteNotFound as usize] = 5;
        b[InternalStatus::UpstreamStale as usize] = 2;

        let mut totals = PipelineMetricsTotals::default();
        let mask_a = (1u64 << InternalStatus::Completed as u64)
            | (1u64 << InternalStatus::ClientCanceled as u64);
        let mask_b = (1u64 << InternalStatus::RouteNotFound as u64)
            | (1u64 << InternalStatus::UpstreamStale as u64);

        add_status_counts(&mut totals.status_counts, &a, mask_a);
        add_status_counts(&mut totals.status_counts, &b, mask_b);
        totals.status_mask = mask_a | mask_b;

        assert_eq!(totals.count(InternalStatus::Completed), 100);
        assert_eq!(totals.count(InternalStatus::ClientCanceled), 20);
        assert_eq!(totals.count(InternalStatus::RouteNotFound), 5);
        assert_eq!(totals.count(InternalStatus::UpstreamStale), 2);
        assert_eq!(totals.total_requests(), 127);
    }
}
