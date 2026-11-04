//! Hardware-Harmonized Vector Span Transport (`opentelemetry_tracer::simd`).
//!
//! Provides ultra-low-latency 256-byte vector register copying between worker stack
//! and SPSC ring buffer slots in physical DRAM, eliminating `memcpy` overhead on the frontline serving hot path.
//!
//! # Micro-Architectural Registration
//! - **AVX2 (256-bit)**: 8 × 256-bit YMM register loads & non-temporal stores (`_mm256_stream_si256`).
//! - **SSE2 / NEON (128-bit)**: 16 × 128-bit XMM / NEON vector register transfers.
//! - **SWAR (64-bit)**: 32 × 64-bit general-purpose ALU transfers with unrolled loop.
//! - **Portable (Scalar)**: Non-overlapping pointer block copy fallback.
//!
//! The optimal implementation is resolved **once at gateway bootstrap** via `LazyLock`
//! probing the probed `velda_core::global_hardware_topology().cpu.register_capabilities`.

use std::sync::LazyLock;
use velda_core::hardware::RegisterWidth;

use super::span::PackedOtlpSpan;

/// Function pointer signature for copying a 256-byte aligned `PackedOtlpSpan`.
pub type CopySpanFn = fn(dst: &mut PackedOtlpSpan, src: &PackedOtlpSpan);

/// Pre-resolved static function pointer matching host CPU register width.
pub static COPY_SPAN_FN: LazyLock<CopySpanFn> = LazyLock::new(|| {
    let caps = velda_core::global_hardware_topology()
        .cpu
        .register_capabilities;
    match caps.select_for_workload(RegisterWidth::Bits256) {
        RegisterWidth::Bits256 => copy_span_256,
        RegisterWidth::Bits128 => copy_span_128,
        RegisterWidth::Bits64 => copy_span_64,
        RegisterWidth::Bits32 => copy_span_portable,
    }
});

/// Transfers a 256-byte `PackedOtlpSpan` using the optimal CPU register width.
#[inline(always)]
pub fn copy_span(dst: &mut PackedOtlpSpan, src: &PackedOtlpSpan) {
    (*COPY_SPAN_FN)(dst, src);
}

/// Function pointer signature for non-temporal streaming store of a 256-byte `PackedOtlpSpan`.
pub type StreamSpanFn = fn(dst: &mut PackedOtlpSpan, src: &PackedOtlpSpan);

/// Pre-resolved static function pointer for non-temporal streaming writes bypassing L1/L2/L3 cache.
pub static STREAM_SPAN_FN: LazyLock<StreamSpanFn> = LazyLock::new(|| {
    let caps = velda_core::global_hardware_topology()
        .cpu
        .register_capabilities;
    match caps.select_for_workload(RegisterWidth::Bits256) {
        RegisterWidth::Bits256 => stream_span_256,
        RegisterWidth::Bits128 => stream_span_128,
        RegisterWidth::Bits64 => stream_span_64,
        RegisterWidth::Bits32 => stream_span_portable,
    }
});

/// Non-temporal streaming store: writes a 256-byte `PackedOtlpSpan` directly into RAM via
/// Write-Combining Buffers, bypassing CPU cache levels (L1/L2/L3) to prevent cache pollution.
#[inline(always)]
pub fn stream_span(dst: &mut PackedOtlpSpan, src: &PackedOtlpSpan) {
    (*STREAM_SPAN_FN)(dst, src);
}

/// Issues a non-temporal prefetch hint (`_MM_HINT_NTA`) across the 256-byte span (4 cache lines).
#[inline(always)]
pub fn prefetch_nta(span: &PackedOtlpSpan) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        use core::arch::x86_64::*;
        let p = span as *const PackedOtlpSpan as *const i8;
        _mm_prefetch(p, _MM_HINT_NTA);
        _mm_prefetch(p.add(64), _MM_HINT_NTA);
        _mm_prefetch(p.add(128), _MM_HINT_NTA);
        _mm_prefetch(p.add(192), _MM_HINT_NTA);
    }
}

/// 256-bit AVX2 non-temporal streaming store (8 × 32-byte VMOVNTDQ transfers + store fence).
#[inline]
pub fn stream_span_256(dst: &mut PackedOtlpSpan, src: &PackedOtlpSpan) {
    #[cfg(target_arch = "x86_64")]
    {
        unsafe { stream_span_avx2_impl(dst, src) }
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        stream_span_128(dst, src);
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn stream_span_avx2_impl(dst: &mut PackedOtlpSpan, src: &PackedOtlpSpan) {
    use core::arch::x86_64::*;

    let s = src as *const PackedOtlpSpan as *const __m256i;
    let d = dst as *mut PackedOtlpSpan as *mut __m256i;

    unsafe {
        let y0 = _mm256_loadu_si256(s.add(0));
        let y1 = _mm256_loadu_si256(s.add(1));
        let y2 = _mm256_loadu_si256(s.add(2));
        let y3 = _mm256_loadu_si256(s.add(3));
        let y4 = _mm256_loadu_si256(s.add(4));
        let y5 = _mm256_loadu_si256(s.add(5));
        let y6 = _mm256_loadu_si256(s.add(6));
        let y7 = _mm256_loadu_si256(s.add(7));

        _mm256_stream_si256(d.add(0), y0);
        _mm256_stream_si256(d.add(1), y1);
        _mm256_stream_si256(d.add(2), y2);
        _mm256_stream_si256(d.add(3), y3);
        _mm256_stream_si256(d.add(4), y4);
        _mm256_stream_si256(d.add(5), y5);
        _mm256_stream_si256(d.add(6), y6);
        _mm256_stream_si256(d.add(7), y7);

        _mm_sfence();
    }
}

/// 256-bit AVX2 standard cacheline copy.
#[inline]
pub fn copy_span_256(dst: &mut PackedOtlpSpan, src: &PackedOtlpSpan) {
    #[cfg(target_arch = "x86_64")]
    {
        unsafe { copy_span_avx2_impl(dst, src) }
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        copy_span_128(dst, src);
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn copy_span_avx2_impl(dst: &mut PackedOtlpSpan, src: &PackedOtlpSpan) {
    use core::arch::x86_64::*;

    let s = src as *const PackedOtlpSpan as *const __m256i;
    let d = dst as *mut PackedOtlpSpan as *mut __m256i;

    unsafe {
        let y0 = _mm256_loadu_si256(s.add(0));
        let y1 = _mm256_loadu_si256(s.add(1));
        let y2 = _mm256_loadu_si256(s.add(2));
        let y3 = _mm256_loadu_si256(s.add(3));
        let y4 = _mm256_loadu_si256(s.add(4));
        let y5 = _mm256_loadu_si256(s.add(5));
        let y6 = _mm256_loadu_si256(s.add(6));
        let y7 = _mm256_loadu_si256(s.add(7));

        _mm256_storeu_si256(d.add(0), y0);
        _mm256_storeu_si256(d.add(1), y1);
        _mm256_storeu_si256(d.add(2), y2);
        _mm256_storeu_si256(d.add(3), y3);
        _mm256_storeu_si256(d.add(4), y4);
        _mm256_storeu_si256(d.add(5), y5);
        _mm256_storeu_si256(d.add(6), y6);
        _mm256_storeu_si256(d.add(7), y7);
    }
}

/// 128-bit vector copy (SSE2 on x86_64 or NEON on aarch64).
#[inline]
pub fn copy_span_128(dst: &mut PackedOtlpSpan, src: &PackedOtlpSpan) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        use core::arch::x86_64::*;
        let s = src as *const PackedOtlpSpan as *const __m128i;
        let d = dst as *mut PackedOtlpSpan as *mut __m128i;

        for i in 0..16 {
            let x = _mm_loadu_si128(s.add(i));
            _mm_storeu_si128(d.add(i), x);
        }
    }
    #[cfg(target_arch = "aarch64")]
    unsafe {
        use core::arch::aarch64::*;
        let s = src as *const PackedOtlpSpan as *const uint8x16_t;
        let d = dst as *mut PackedOtlpSpan as *mut uint8x16_t;

        for i in 0..16 {
            let v = vld1q_u8(s.add(i) as *const u8);
            vst1q_u8(d.add(i) as *mut u8, v);
        }
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        copy_span_64(dst, src);
    }
}

/// 128-bit streaming store fallback.
#[inline]
pub fn stream_span_128(dst: &mut PackedOtlpSpan, src: &PackedOtlpSpan) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        use core::arch::x86_64::*;
        let s = src as *const PackedOtlpSpan as *const __m128i;
        let d = dst as *mut PackedOtlpSpan as *mut __m128i;

        for i in 0..16 {
            let x = _mm_loadu_si128(s.add(i));
            _mm_stream_si128(d.add(i), x);
        }
        _mm_sfence();
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        copy_span_128(dst, src);
    }
}

/// 64-bit SWAR unrolled copy (32 × 8-byte scalar transfers).
#[inline]
pub fn copy_span_64(dst: &mut PackedOtlpSpan, src: &PackedOtlpSpan) {
    let s = src as *const PackedOtlpSpan as *const u64;
    let d = dst as *mut PackedOtlpSpan as *mut u64;

    unsafe {
        *d.add(0) = *s.add(0);
        *d.add(1) = *s.add(1);
        *d.add(2) = *s.add(2);
        *d.add(3) = *s.add(3);
        *d.add(4) = *s.add(4);
        *d.add(5) = *s.add(5);
        *d.add(6) = *s.add(6);
        *d.add(7) = *s.add(7);
        *d.add(8) = *s.add(8);
        *d.add(9) = *s.add(9);
        *d.add(10) = *s.add(10);
        *d.add(11) = *s.add(11);
        *d.add(12) = *s.add(12);
        *d.add(13) = *s.add(13);
        *d.add(14) = *s.add(14);
        *d.add(15) = *s.add(15);
        *d.add(16) = *s.add(16);
        *d.add(17) = *s.add(17);
        *d.add(18) = *s.add(18);
        *d.add(19) = *s.add(19);
        *d.add(20) = *s.add(20);
        *d.add(21) = *s.add(21);
        *d.add(22) = *s.add(22);
        *d.add(23) = *s.add(23);
        *d.add(24) = *s.add(24);
        *d.add(25) = *s.add(25);
        *d.add(26) = *s.add(26);
        *d.add(27) = *s.add(27);
        *d.add(28) = *s.add(28);
        *d.add(29) = *s.add(29);
        *d.add(30) = *s.add(30);
        *d.add(31) = *s.add(31);
    }
}

/// 64-bit streaming store fallback.
#[inline]
pub fn stream_span_64(dst: &mut PackedOtlpSpan, src: &PackedOtlpSpan) {
    copy_span_64(dst, src);
}

/// Portable scalar copy fallback.
#[inline]
pub fn copy_span_portable(dst: &mut PackedOtlpSpan, src: &PackedOtlpSpan) {
    unsafe {
        std::ptr::copy_nonoverlapping(
            src as *const PackedOtlpSpan as *const u8,
            dst as *mut PackedOtlpSpan as *mut u8,
            std::mem::size_of::<PackedOtlpSpan>(),
        );
    }
}

/// Portable streaming store fallback.
#[inline]
pub fn stream_span_portable(dst: &mut PackedOtlpSpan, src: &PackedOtlpSpan) {
    copy_span_portable(dst, src);
}
