//! Hardware-Accelerated Metric Counter Accumulation & Totals (`velda_observability::metrics::counter`).
//!
//! ```text
//! ┌────────────────────────────────────────────────────────────────────────────────────────┐
//! │                 20-COUNTER PIPELINE SLOT SIMD ACCUMULATION ARCHITECTURE                 │
//! ├────────────────────────────────────────────────────────────────────────────────────────┤
//! │ 20 Contiguous 64-Bit Integers (160 Bytes Total)                                        │
//! │ [ 0..4  : Requests 2xx, 3xx, 4xx, 5xx, Other ]                                         │
//! │ [ 5..6  : Bytes RX, Bytes TX ]                                                         │
//! │ [ 7..13 : Duration Buckets (<=1ms, 5ms, 10ms, 50ms, 250ms, 1000ms, >1000ms) ]        │
//! │ [ 14..19: TTFB Buckets (<=1ms, 5ms, 10ms, 50ms, 250ms, >250ms) ]                      │
//! ├────────────────────────────────────────────────────────────────────────────────────────┤
//! │ HARDWARE VECTOR EXECUTION:                                                             │
//! │                                                                                        │
//! │ 1. AVX2 PATH (x86_64, 256-bit Vector Registers):                                       │
//! │    Chunk 0: [c0,  c1,  c2,  c3]  ──► _mm256_loadu_si256 ──► _mm256_add_epi64 ──► YMM0 │
//! │    Chunk 1: [c4,  c5,  c6,  c7]  ──► _mm256_loadu_si256 ──► _mm256_add_epi64 ──► YMM1 │
//! │    Chunk 2: [c8,  c9,  c10, c11] ──► _mm256_loadu_si256 ──► _mm256_add_epi64 ──► YMM2 │
//! │    Chunk 3: [c12, c13, c14, c15] ──► _mm256_loadu_si256 ──► _mm256_add_epi64 ──► YMM3 │
//! │    Chunk 4: [c16, c17, c18, c19] ──► _mm256_loadu_si256 ──► _mm256_add_epi64 ──► YMM4 │
//! │    Total: Exactly 5 unrolled 256-bit vector operations (0 unaligned loop tails)        │
//! │                                                                                        │
//! │ 2. SSE2 / NEON PATH (x86_64 / aarch64, 128-bit Vector Registers):                      │
//! │    10 contiguous chunks of 2x u64 (_mm_add_epi64 / vaddq_u64)                          │
//! │                                                                                        │
//! │ 3. SCALAR FALLBACK:                                                                    │
//! │    Single loop 0..20 saturating_add (Zero float, pure ALU arithmetic)                  │
//! └────────────────────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! Provides direct vector register accelerated integer addition across fixed 20-counter pipeline slots,
//! with hardware SIMD acceleration (AVX2 for x86_64, NEON for aarch64, scalar fallback).
//!
//! # Architectural Invariants
//! - Zero Float on Metrics Hot Path: All accumulations and ratios operate strictly
//!   in pure integer arithmetic (basis points and fixed-point scaling), bypassing FPU conversions.
//! - Direct Execution: Zero dynamic function pointer dispatch. Direct hardware acceleration
//!   branching without indirect call overhead.
//! - Fixed 20-Counter Geometry: Exactly 5x 256-bit (AVX2) or 10x 128-bit (SSE2/NEON) vector registers,
//!   eliminating unaligned tails and runtime loop setup costs.

/// Number of canonical 64-bit counters per pipeline slot.
pub const PIPELINE_COUNTERS_LEN: usize = 20;

/// Adds 20 contiguous 64-bit pipeline counters from `src` into `dst` using SIMD acceleration.
#[inline]
pub fn add_pipeline_counters(
    dst: &mut [u64; PIPELINE_COUNTERS_LEN],
    src: &[u64; PIPELINE_COUNTERS_LEN],
) {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") {
            unsafe {
                add_pipeline_counters_avx2(dst, src);
            }
            return;
        }
        if is_x86_feature_detected!("sse2") {
            unsafe {
                add_pipeline_counters_sse2(dst, src);
            }
            return;
        }
    }
    #[cfg(target_arch = "aarch64")]
    {
        if std::arch::is_aarch64_feature_detected!("neon") {
            unsafe {
                add_pipeline_counters_neon(dst, src);
            }
            return;
        }
    }
    add_pipeline_counters_scalar(dst, src);
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

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
pub unsafe fn add_pipeline_counters_avx2(
    dst: &mut [u64; PIPELINE_COUNTERS_LEN],
    src: &[u64; PIPELINE_COUNTERS_LEN],
) {
    use std::arch::x86_64::*;
    // 20 u64 counters = exactly 5 chunks of 4 u64 (256-bit each)
    unsafe {
        for chunk in 0..5 {
            let offset = chunk * 4;
            let d = _mm256_loadu_si256(dst.as_ptr().add(offset) as *const __m256i);
            let s = _mm256_loadu_si256(src.as_ptr().add(offset) as *const __m256i);
            let sum = _mm256_add_epi64(d, s);
            _mm256_storeu_si256(dst.as_mut_ptr().add(offset) as *mut __m256i, sum);
        }
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse2")]
pub unsafe fn add_pipeline_counters_sse2(
    dst: &mut [u64; PIPELINE_COUNTERS_LEN],
    src: &[u64; PIPELINE_COUNTERS_LEN],
) {
    use std::arch::x86_64::*;
    // 20 u64 counters = exactly 10 chunks of 2 u64 (128-bit each)
    unsafe {
        for chunk in 0..10 {
            let offset = chunk * 2;
            let d = _mm_loadu_si128(dst.as_ptr().add(offset) as *const __m128i);
            let s = _mm_loadu_si128(src.as_ptr().add(offset) as *const __m128i);
            let sum = _mm_add_epi64(d, s);
            _mm_storeu_si128(dst.as_mut_ptr().add(offset) as *mut __m128i, sum);
        }
    }
}

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
pub unsafe fn add_pipeline_counters_neon(
    dst: &mut [u64; PIPELINE_COUNTERS_LEN],
    src: &[u64; PIPELINE_COUNTERS_LEN],
) {
    use std::arch::aarch64::*;
    // 20 u64 counters = exactly 10 chunks of 2 u64 (128-bit each)
    unsafe {
        for chunk in 0..10 {
            let offset = chunk * 2;
            let d = vld1q_u64(dst.as_ptr().add(offset));
            let s = vld1q_u64(src.as_ptr().add(offset));
            let sum = vaddq_u64(d, s);
            vst1q_u64(dst.as_mut_ptr().add(offset), sum);
        }
    }
}

/// Fallback scalar accumulation for 20 pipeline counters.
#[inline]
pub fn add_pipeline_counters_scalar(
    dst: &mut [u64; PIPELINE_COUNTERS_LEN],
    src: &[u64; PIPELINE_COUNTERS_LEN],
) {
    for i in 0..PIPELINE_COUNTERS_LEN {
        dst[i] = dst[i].saturating_add(src[i]);
    }
}

/// Aggregated metrics totals across multiple pipelines.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PipelineMetricsTotals {
    pub requests_2xx: u64,
    pub requests_3xx: u64,
    pub requests_4xx: u64,
    pub requests_5xx: u64,
    pub requests_other: u64,
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

impl PipelineMetricsTotals {
    /// Constructs totals from a raw counter slice.
    #[inline]
    pub fn from_counters(counters: &[u64]) -> Self {
        let get = |idx: usize| -> u64 {
            if idx < counters.len() {
                counters[idx]
            } else {
                0
            }
        };

        Self {
            requests_2xx: get(0),
            requests_3xx: get(1),
            requests_4xx: get(2),
            requests_5xx: get(3),
            requests_other: get(4),
            bytes_rx: get(5),
            bytes_tx: get(6),
            duration_le_1ms: get(7),
            duration_le_5ms: get(8),
            duration_le_10ms: get(9),
            duration_le_50ms: get(10),
            duration_le_250ms: get(11),
            duration_le_1000ms: get(12),
            duration_gt_1000ms: get(13),
            ttfb_le_1ms: get(14),
            ttfb_le_5ms: get(15),
            ttfb_le_10ms: get(16),
            ttfb_le_50ms: get(17),
            ttfb_le_250ms: get(18),
            ttfb_gt_250ms: get(19),
        }
    }

    /// Exports totals into a raw 20-counter array.
    #[inline]
    pub fn to_counters(&self) -> [u64; PIPELINE_COUNTERS_LEN] {
        [
            self.requests_2xx,
            self.requests_3xx,
            self.requests_4xx,
            self.requests_5xx,
            self.requests_other,
            self.bytes_rx,
            self.bytes_tx,
            self.duration_le_1ms,
            self.duration_le_5ms,
            self.duration_le_10ms,
            self.duration_le_50ms,
            self.duration_le_250ms,
            self.duration_le_1000ms,
            self.duration_gt_1000ms,
            self.ttfb_le_1ms,
            self.ttfb_le_5ms,
            self.ttfb_le_10ms,
            self.ttfb_le_50ms,
            self.ttfb_le_250ms,
            self.ttfb_gt_250ms,
        ]
    }

    /// Computes total requests across all status classes.
    #[inline]
    pub fn total_requests(&self) -> u64 {
        self.requests_2xx
            .saturating_add(self.requests_3xx)
            .saturating_add(self.requests_4xx)
            .saturating_add(self.requests_5xx)
            .saturating_add(self.requests_other)
    }

    /// Computes success rate in basis points (1 bp = 0.01%, 10,000 = 100.00%).
    #[inline]
    pub fn success_rate_basis_points(&self) -> u32 {
        let total = self.total_requests();
        if total == 0 {
            10_000
        } else {
            ((self.requests_2xx as u128 * 10_000) / total as u128) as u32
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

        let mut scalar_dst = base_dst;
        add_pipeline_counters_scalar(&mut scalar_dst, &src);

        let mut simd_dst = base_dst;
        add_pipeline_counters(&mut simd_dst, &src);

        assert_eq!(scalar_dst, simd_dst, "SIMD and scalar must be identical");

        #[cfg(target_arch = "x86_64")]
        {
            if is_x86_feature_detected!("avx2") {
                let mut avx2_dst = base_dst;
                unsafe {
                    add_pipeline_counters_avx2(&mut avx2_dst, &src);
                }
                assert_eq!(scalar_dst, avx2_dst, "AVX2 must match scalar");
            }
            let mut sse2_dst = base_dst;
            unsafe {
                add_pipeline_counters_sse2(&mut sse2_dst, &src);
            }
            assert_eq!(scalar_dst, sse2_dst, "SSE2 must match scalar");
        }
    }

    #[test]
    fn test_pipeline_metrics_totals() {
        let mut counters = [0u64; PIPELINE_COUNTERS_LEN];
        counters[0] = 100; // 2xx
        counters[1] = 20; // 3xx
        counters[2] = 5; // 4xx
        counters[3] = 2; // 5xx

        let totals = PipelineMetricsTotals::from_counters(&counters);
        assert_eq!(totals.requests_2xx, 100);
        assert_eq!(totals.requests_3xx, 20);
        assert_eq!(totals.requests_4xx, 5);
        assert_eq!(totals.requests_5xx, 2);
        assert_eq!(totals.total_requests(), 127);
        assert_eq!(totals.to_counters(), counters);
    }
}
