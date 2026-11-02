//! Hardware Vector Register Metric Accumulation & Fixed-Point Arithmetic.
//!
//! Provides vector register accelerated integer addition across metric counter slices,
//! dynamically bound to the host CPU's supported register width (up to 256-bit max).
//!
//! # Architectural Invariants
//! - Zero Float on Metrics Hot Path: All accumulations and ratios operate strictly
//!   in pure integer arithmetic (basis points and fixed-point scaling), bypassing FPU conversions.
//! - Future-Proof Arbitrary Slice Alignment: Functions operate on general slices (`&mut [u64], &[u64]`),
//!   using bitmask alignment (`len & !3` for 256-bit, `len & !1` for 128-bit) to bulk-process
//!   vector chunks and handle any scalar remainder (0..3 counters) with zero overhead.
//!   Any new metric added in the future works automatically with zero SIMD code modifications.
//! - No Runtime Branching: A static function pointer is resolved once at boot time;
//!   subsequent aggregations execute via direct function pointer call.
//! - Banned AVX-512: The vector engine strictly caps at 256 bits, eliminating CPU downclocking.

use velda_core::hardware::RegisterWidth;

/// Number of canonical 64-bit counters per pipeline slot.
pub const PIPELINE_COUNTERS_LEN: usize = 20;

/// Signature for vector/scalar metric accumulation functions.
pub type AccumulateCountersFn = fn(dst: &mut [u64], src: &[u64]);

/// Aggregates counter slices using 256-bit vector registers (AVX2-grade) with scalar tail.
///
/// # Workload-to-Register Adaptation
/// If `len < 4`, the slice is narrower than a single 256-bit vector chunk (stride 4).
/// In this case, execution adapts immediately to 128-bit/64-bit without paying 256-bit setup costs.
#[inline]
pub fn add_counters_256(dst: &mut [u64], src: &[u64]) {
    let len = dst.len().min(src.len());
    if len < 4 {
        add_counters_128(dst, src);
        return;
    }
    let vec_len = len & !3; // bitwise alignment to multiple of 4

    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") {
            unsafe {
                add_counters_256_x86_impl(&mut dst[..vec_len], &src[..vec_len]);
            }
        } else {
            add_counters_128(dst, src);
            return;
        }
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        add_counters_128(&mut dst[..vec_len], &src[..vec_len]);
    }

    // Scalar tail: handles any leftover 0..3 counters
    for i in vec_len..len {
        dst[i] = dst[i].saturating_add(src[i]);
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn add_counters_256_x86_impl(dst: &mut [u64], src: &[u64]) {
    use std::arch::x86_64::*;
    let mut i = 0;
    while i < dst.len() {
        unsafe {
            let d = _mm256_loadu_si256(dst.as_ptr().add(i) as *const __m256i);
            let s = _mm256_loadu_si256(src.as_ptr().add(i) as *const __m256i);
            let sum = _mm256_add_epi64(d, s);
            _mm256_storeu_si256(dst.as_mut_ptr().add(i) as *mut __m256i, sum);
        }
        i += 4;
    }
}

/// Aggregates counter slices using 128-bit vector registers (SSE2 / NEON) with scalar tail.
///
/// # Workload-to-Register Adaptation
/// If `len < 2`, the slice is narrower than a 128-bit vector chunk (stride 2).
/// In this case, execution adapts immediately to standard 64-bit scalar ALU, avoiding SIMD setup waste.
#[inline]
pub fn add_counters_128(dst: &mut [u64], src: &[u64]) {
    let len = dst.len().min(src.len());
    if len < 2 {
        add_counters_64(dst, src);
        return;
    }
    let vec_len = len & !1; // bitwise alignment to multiple of 2

    #[cfg(target_arch = "x86_64")]
    {
        unsafe {
            add_counters_128_x86_impl(&mut dst[..vec_len], &src[..vec_len]);
        }
    }
    #[cfg(target_arch = "aarch64")]
    {
        if std::arch::is_aarch64_feature_detected!("neon") {
            unsafe {
                add_counters_128_neon_impl(&mut dst[..vec_len], &src[..vec_len]);
            }
        } else {
            add_counters_64(dst, src);
            return;
        }
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        add_counters_64(&mut dst[..vec_len], &src[..vec_len]);
    }

    // Scalar tail: handles any leftover 0..1 counter
    for i in vec_len..len {
        dst[i] = dst[i].saturating_add(src[i]);
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse2")]
unsafe fn add_counters_128_x86_impl(dst: &mut [u64], src: &[u64]) {
    use std::arch::x86_64::*;
    let mut i = 0;
    while i < dst.len() {
        unsafe {
            let d = _mm_loadu_si128(dst.as_ptr().add(i) as *const __m128i);
            let s = _mm_loadu_si128(src.as_ptr().add(i) as *const __m128i);
            let sum = _mm_add_epi64(d, s);
            _mm_storeu_si128(dst.as_mut_ptr().add(i) as *mut __m128i, sum);
        }
        i += 2;
    }
}

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn add_counters_128_neon_impl(dst: &mut [u64], src: &[u64]) {
    use std::arch::aarch64::*;
    let mut i = 0;
    while i < dst.len() {
        unsafe {
            let d = vld1q_u64(dst.as_ptr().add(i));
            let s = vld1q_u64(src.as_ptr().add(i));
            let sum = vaddq_u64(d, s);
            vst1q_u64(dst.as_mut_ptr().add(i), sum);
        }
        i += 2;
    }
}

/// Aggregates counter slices using standard 64-bit scalar arithmetic.
#[inline]
pub fn add_counters_64(dst: &mut [u64], src: &[u64]) {
    let len = dst.len().min(src.len());
    for i in 0..len {
        dst[i] = dst[i].saturating_add(src[i]);
    }
}

/// Aggregates counter slices using 32-bit SWAR arithmetic with carry propagation.
#[inline]
pub fn add_counters_32(dst: &mut [u64], src: &[u64]) {
    let len = dst.len().min(src.len());
    for i in 0..len {
        let d_lo = dst[i] as u32;
        let d_hi = (dst[i] >> 32) as u32;
        let s_lo = src[i] as u32;
        let s_hi = (src[i] >> 32) as u32;

        let (res_lo, carry) = d_lo.overflowing_add(s_lo);
        let res_hi = d_hi.wrapping_add(s_hi).wrapping_add(carry as u32);

        dst[i] = ((res_hi as u64) << 32) | (res_lo as u64);
    }
}

/// Selects the appropriate counter accumulation function pointer for a workload target width.
pub fn resolve_counters_fn(target: RegisterWidth) -> AccumulateCountersFn {
    let caps = velda_core::global_hardware_topology()
        .cpu
        .register_capabilities;
    match caps.select_for_workload(target) {
        RegisterWidth::Bits256 => add_counters_256,
        RegisterWidth::Bits128 => add_counters_128,
        RegisterWidth::Bits64 => add_counters_64,
        RegisterWidth::Bits32 => add_counters_32,
    }
}

/// Dynamically resolves the optimal counter accumulation function pointer adapting to both
/// host hardware capabilities and the exact workload size (number of counters).
///
/// Prevents wasteful wide vector register setup when the workload is smaller than the register chunk:
/// - `len >= 4`: Qualifies for 256-bit registers (stride 4, 32 bytes per iteration).
/// - `2 <= len < 4`: Adapts to 128-bit registers (stride 2, 16 bytes per iteration).
/// - `len == 1`: Adapts directly to 64-bit scalar ALU (zero SIMD setup waste).
/// - `len == 0`: Minimal scalar.
#[inline]
pub fn resolve_counters_fn_for_len(len: usize) -> AccumulateCountersFn {
    let caps = velda_core::global_hardware_topology()
        .cpu
        .register_capabilities;
    match caps.select_for_workload_size(len) {
        RegisterWidth::Bits256 => add_counters_256,
        RegisterWidth::Bits128 => add_counters_128,
        RegisterWidth::Bits64 => add_counters_64,
        RegisterWidth::Bits32 => add_counters_32,
    }
}

/// Pre-resolved static function pointer for accumulating metrics slices.
///
/// Resolves once at boot adapted to the canonical pipeline slot width (`PIPELINE_COUNTERS_LEN = 20`).
pub static AGGREGATE_COUNTERS: std::sync::LazyLock<AccumulateCountersFn> =
    std::sync::LazyLock::new(|| resolve_counters_fn_for_len(PIPELINE_COUNTERS_LEN));

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

    /// Computes success rate as a float ratio.
    #[inline]
    pub fn success_rate(&self) -> f64 {
        self.success_rate_basis_points() as f64 / 10_000.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vector_counter_arbitrary_sizes_equivalence() {
        // Test varying lengths: 0, 1, 3, 4, 7, 19, 20, 21, 23, 27, 64
        for size in [0, 1, 3, 4, 7, 19, 20, 21, 23, 27, 64] {
            let a: Vec<u64> = (0..size).map(|i| (i as u64 + 1) * 1_000_000).collect();
            let b: Vec<u64> = (0..size).map(|i| (i as u64 + 1) * 2_500_000).collect();

            // 64-bit baseline
            let mut res_64 = a.clone();
            add_counters_64(&mut res_64, &b);

            // 32-bit SWAR
            let mut res_32 = a.clone();
            add_counters_32(&mut res_32, &b);
            assert_eq!(res_64, res_32, "32-bit SWAR mismatch at size {}", size);

            // 128-bit vector
            let mut res_128 = a.clone();
            add_counters_128(&mut res_128, &b);
            assert_eq!(res_64, res_128, "128-bit vector mismatch at size {}", size);

            // 256-bit vector
            let mut res_256 = a.clone();
            add_counters_256(&mut res_256, &b);
            assert_eq!(res_64, res_256, "256-bit vector mismatch at size {}", size);

            // Static function pointer
            let mut res_static = a.clone();
            (*AGGREGATE_COUNTERS)(&mut res_static, &b);
            assert_eq!(
                res_64, res_static,
                "Static AGGREGATE_COUNTERS mismatch at size {}",
                size
            );
        }
    }

    #[test]
    fn test_large_integer_swar_carry() {
        let mut a = vec![0u64; 4];
        let mut b = vec![0u64; 4];
        // Set values that trigger 32-bit overflow and carry into upper 32 bits
        a[0] = 0x0000_0001_FFFF_FFFF;
        b[0] = 0x0000_0002_0000_0001;

        let mut res_64 = a.clone();
        add_counters_64(&mut res_64, &b);

        let mut res_32 = a.clone();
        add_counters_32(&mut res_32, &b);
        assert_eq!(res_64, res_32);
        assert_eq!(res_32[0], 0x0000_0004_0000_0000);
    }
}
