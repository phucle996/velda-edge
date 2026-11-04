//! Hardware-Harmonized Vector Record Transport (`std_http_access_log::simd`).
//!
//! Provides ultra-low-latency 256-byte vector register copying between worker stack
//! and SPSC ring buffer slots, eliminating `memcpy` overhead on the frontline serving hot path.
//!
//! # Micro-Architectural Registration
//! - **AVX2 (256-bit)**: 8 × 256-bit YMM register loads & stores (4 cycles on modern Zen/Intel cores).
//! - **SSE2 / NEON (128-bit)**: 16 × 128-bit XMM / NEON vector register transfers.
//! - **SWAR (64-bit)**: 32 × 64-bit general-purpose ALU transfers with unrolled loop.
//! - **Portable (Scalar)**: Non-overlapping pointer block copy fallback.
//!
//! The optimal implementation is resolved **once at gateway bootstrap** via `LazyLock`
//! probing the probed `velda_core::global_hardware_topology().cpu.register_capabilities`.

use std::sync::LazyLock;
use velda_core::hardware::RegisterWidth;

use super::record::PackedLogRecord;

/// Function pointer signature for copying a 256-byte aligned `PackedLogRecord`.
pub type CopyRecordFn = fn(dst: &mut PackedLogRecord, src: &PackedLogRecord);

/// Pre-resolved static function pointer matching host CPU register width.
pub static COPY_RECORD_FN: LazyLock<CopyRecordFn> = LazyLock::new(|| {
    let caps = velda_core::global_hardware_topology()
        .cpu
        .register_capabilities;
    match caps.select_for_workload(RegisterWidth::Bits256) {
        RegisterWidth::Bits256 => copy_record_256,
        RegisterWidth::Bits128 => copy_record_128,
        RegisterWidth::Bits64 => copy_record_64,
        RegisterWidth::Bits32 => copy_record_portable,
    }
});

/// Transfers a 256-byte `PackedLogRecord` using the optimal CPU register width.
#[inline(always)]
pub fn copy_record(dst: &mut PackedLogRecord, src: &PackedLogRecord) {
    (*COPY_RECORD_FN)(dst, src);
}

/// Function pointer signature for non-temporal streaming store of a 256-byte `PackedLogRecord`.
pub type StreamRecordFn = fn(dst: &mut PackedLogRecord, src: &PackedLogRecord);

/// Pre-resolved static function pointer for non-temporal streaming writes bypassing L1/L2/L3 cache.
pub static STREAM_RECORD_FN: LazyLock<StreamRecordFn> = LazyLock::new(|| {
    let caps = velda_core::global_hardware_topology()
        .cpu
        .register_capabilities;
    match caps.select_for_workload(RegisterWidth::Bits256) {
        RegisterWidth::Bits256 => stream_record_256,
        RegisterWidth::Bits128 => stream_record_128,
        RegisterWidth::Bits64 => stream_record_64,
        RegisterWidth::Bits32 => stream_record_portable,
    }
});

/// Non-temporal streaming store: writes a 256-byte `PackedLogRecord` directly into RAM via
/// Write-Combining Buffers, bypassing CPU cache levels (L1/L2/L3) to prevent cache pollution.
#[inline(always)]
pub fn stream_record(dst: &mut PackedLogRecord, src: &PackedLogRecord) {
    (*STREAM_RECORD_FN)(dst, src);
}

/// Issues a non-temporal prefetch hint (`_MM_HINT_NTA`) across the 256-byte record (4 cache lines).
///
/// Instructs the CPU hardware prefetcher to load the record into a non-temporal buffer
/// without polluting the L2 or L3 caches when read by the background Shadow Logistics Worker.
#[inline(always)]
pub fn prefetch_nta(record: &PackedLogRecord) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        use core::arch::x86_64::*;
        let p = record as *const PackedLogRecord as *const i8;
        _mm_prefetch(p, _MM_HINT_NTA);
        _mm_prefetch(p.add(64), _MM_HINT_NTA);
        _mm_prefetch(p.add(128), _MM_HINT_NTA);
        _mm_prefetch(p.add(192), _MM_HINT_NTA);
    }
}

/// 256-bit AVX2 non-temporal streaming store (8 × 32-byte VMOVNTDQ transfers + store fence).
#[inline]
pub fn stream_record_256(dst: &mut PackedLogRecord, src: &PackedLogRecord) {
    #[cfg(target_arch = "x86_64")]
    {
        unsafe { stream_record_avx2_impl(dst, src) }
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        stream_record_128(dst, src);
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn stream_record_avx2_impl(dst: &mut PackedLogRecord, src: &PackedLogRecord) {
    use core::arch::x86_64::*;

    let s = src as *const PackedLogRecord as *const __m256i;
    let d = dst as *mut PackedLogRecord as *mut __m256i;

    unsafe {
        let v0 = _mm256_loadu_si256(s.add(0));
        let v1 = _mm256_loadu_si256(s.add(1));
        let v2 = _mm256_loadu_si256(s.add(2));
        let v3 = _mm256_loadu_si256(s.add(3));
        let v4 = _mm256_loadu_si256(s.add(4));
        let v5 = _mm256_loadu_si256(s.add(5));
        let v6 = _mm256_loadu_si256(s.add(6));
        let v7 = _mm256_loadu_si256(s.add(7));

        // Non-temporal stores (VMOVNTDQ): bypass L1/L2/L3 cache into Write-Combining Buffers
        _mm256_stream_si256(d.add(0), v0);
        _mm256_stream_si256(d.add(1), v1);
        _mm256_stream_si256(d.add(2), v2);
        _mm256_stream_si256(d.add(3), v3);
        _mm256_stream_si256(d.add(4), v4);
        _mm256_stream_si256(d.add(5), v5);
        _mm256_stream_si256(d.add(6), v6);
        _mm256_stream_si256(d.add(7), v7);

        // Store fence: guarantee all non-temporal stores are drained before atomic flag release
        _mm_sfence();
    }
}

/// 128-bit vector non-temporal streaming store (16 × 16-byte MOVNTDQ transfers + store fence).
#[inline]
pub fn stream_record_128(dst: &mut PackedLogRecord, src: &PackedLogRecord) {
    #[cfg(target_arch = "x86_64")]
    {
        use core::arch::x86_64::*;
        let s = src as *const PackedLogRecord as *const __m128i;
        let d = dst as *mut PackedLogRecord as *mut __m128i;
        unsafe {
            for i in 0..16 {
                let v = _mm_loadu_si128(s.add(i));
                _mm_stream_si128(d.add(i), v);
            }
            _mm_sfence();
        }
    }
    #[cfg(target_arch = "aarch64")]
    {
        copy_record_128(dst, src);
        std::sync::atomic::fence(std::sync::atomic::Ordering::Release);
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        stream_record_64(dst, src);
    }
}

/// 64-bit SWAR streaming store with memory release barrier.
#[inline]
pub fn stream_record_64(dst: &mut PackedLogRecord, src: &PackedLogRecord) {
    copy_record_64(dst, src);
    std::sync::atomic::fence(std::sync::atomic::Ordering::Release);
}

/// Portable block-copy streaming fallback with memory release barrier.
#[inline]
pub fn stream_record_portable(dst: &mut PackedLogRecord, src: &PackedLogRecord) {
    copy_record_portable(dst, src);
    std::sync::atomic::fence(std::sync::atomic::Ordering::Release);
}

/// 256-bit vector implementation (8 × 32-byte AVX2 transfers).
#[inline]
pub fn copy_record_256(dst: &mut PackedLogRecord, src: &PackedLogRecord) {
    #[cfg(target_arch = "x86_64")]
    {
        unsafe { copy_record_avx2_impl(dst, src) }
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        copy_record_128(dst, src);
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn copy_record_avx2_impl(dst: &mut PackedLogRecord, src: &PackedLogRecord) {
    use core::arch::x86_64::*;

    let s = src as *const PackedLogRecord as *const __m256i;
    let d = dst as *mut PackedLogRecord as *mut __m256i;

    unsafe {
        // 256 bytes = 8 × 32-byte chunks (YMM0..YMM7)
        let v0 = _mm256_loadu_si256(s.add(0));
        let v1 = _mm256_loadu_si256(s.add(1));
        let v2 = _mm256_loadu_si256(s.add(2));
        let v3 = _mm256_loadu_si256(s.add(3));
        let v4 = _mm256_loadu_si256(s.add(4));
        let v5 = _mm256_loadu_si256(s.add(5));
        let v6 = _mm256_loadu_si256(s.add(6));
        let v7 = _mm256_loadu_si256(s.add(7));

        _mm256_storeu_si256(d.add(0), v0);
        _mm256_storeu_si256(d.add(1), v1);
        _mm256_storeu_si256(d.add(2), v2);
        _mm256_storeu_si256(d.add(3), v3);
        _mm256_storeu_si256(d.add(4), v4);
        _mm256_storeu_si256(d.add(5), v5);
        _mm256_storeu_si256(d.add(6), v6);
        _mm256_storeu_si256(d.add(7), v7);
    }
}

/// 128-bit vector implementation (16 × 16-byte SSE2/NEON transfers).
#[inline]
pub fn copy_record_128(dst: &mut PackedLogRecord, src: &PackedLogRecord) {
    #[cfg(target_arch = "x86_64")]
    {
        use core::arch::x86_64::*;
        let s = src as *const PackedLogRecord as *const __m128i;
        let d = dst as *mut PackedLogRecord as *mut __m128i;
        unsafe {
            for i in 0..16 {
                let v = _mm_loadu_si128(s.add(i));
                _mm_storeu_si128(d.add(i), v);
            }
        }
    }
    #[cfg(target_arch = "aarch64")]
    {
        use core::arch::aarch64::*;
        let s = src as *const PackedLogRecord as *const u8;
        let d = dst as *mut PackedLogRecord as *mut u8;
        unsafe {
            for i in 0..16 {
                let v = vld1q_u8(s.add(i * 16));
                vst1q_u8(d.add(i * 16), v);
            }
        }
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        copy_record_64(dst, src);
    }
}

/// 64-bit SWAR / ALU implementation (32 × 8-byte scalar transfers).
#[inline]
pub fn copy_record_64(dst: &mut PackedLogRecord, src: &PackedLogRecord) {
    let s = src as *const PackedLogRecord as *const u64;
    let d = dst as *mut PackedLogRecord as *mut u64;
    unsafe {
        // Unroll 32 × 8-byte transfers into 4 blocks of 8
        for b in 0..4 {
            let offset = b * 8;
            *d.add(offset) = *s.add(offset);
            *d.add(offset + 1) = *s.add(offset + 1);
            *d.add(offset + 2) = *s.add(offset + 2);
            *d.add(offset + 3) = *s.add(offset + 3);
            *d.add(offset + 4) = *s.add(offset + 4);
            *d.add(offset + 5) = *s.add(offset + 5);
            *d.add(offset + 6) = *s.add(offset + 6);
            *d.add(offset + 7) = *s.add(offset + 7);
        }
    }
}

/// Portable block-copy fallback.
#[inline]
pub fn copy_record_portable(dst: &mut PackedLogRecord, src: &PackedLogRecord) {
    unsafe {
        std::ptr::copy_nonoverlapping(
            src as *const PackedLogRecord as *const u8,
            dst as *mut PackedLogRecord as *mut u8,
            256,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simd_copy_record_integrity() {
        let mut src = PackedLogRecord::empty();
        src.timestamp_ms = 1712345678901;
        src.duration_us = 4520;
        src.content_length = 65535;
        src.status = 200;
        src.version = 3;
        src.method = 1;
        src.set_request_id(b"uuid-abc-123");
        src.set_route_id(b"rt-auth");
        src.set_upstream(b"10.0.0.1:80");
        src.set_user_agent(b"Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/123.0.0.0");

        let mut dst = PackedLogRecord::empty();
        copy_record(&mut dst, &src);

        assert_eq!(dst, src);
        assert_eq!(dst.timestamp_ms, 1712345678901);
        assert_eq!(dst.request_id(), "uuid-abc-123");
        assert_eq!(dst.route_id(), "rt-auth");
        assert_eq!(dst.upstream(), "10.0.0.1:80");
        assert_eq!(
            dst.user_agent(),
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/123.0.0.0"
        );
    }

    #[test]
    fn test_all_vector_copy_tiers() {
        let mut src = PackedLogRecord::empty();
        src.timestamp_ms = 9999999;
        src.status = 502;
        src.set_user_agent(b"Bench-Agent/1.0");

        let mut dst_256 = PackedLogRecord::empty();
        copy_record_256(&mut dst_256, &src);
        assert_eq!(dst_256, src);

        let mut dst_128 = PackedLogRecord::empty();
        copy_record_128(&mut dst_128, &src);
        assert_eq!(dst_128, src);

        let mut dst_64 = PackedLogRecord::empty();
        copy_record_64(&mut dst_64, &src);
        assert_eq!(dst_64, src);

        let mut dst_portable = PackedLogRecord::empty();
        copy_record_portable(&mut dst_portable, &src);
        assert_eq!(dst_portable, src);
    }

    #[test]
    fn test_stream_record_integrity_and_tiers() {
        let mut src = PackedLogRecord::empty();
        src.timestamp_ms = 88888888;
        src.status = 200;
        src.content_length = 1048576;
        src.set_request_id(b"stream-req-123");
        src.set_route_id(b"rt-stream");
        src.set_upstream(b"127.0.0.1:8080");
        src.set_user_agent(b"NonTemporalAgent/2.0");

        let mut dst = PackedLogRecord::empty();
        stream_record(&mut dst, &src);
        assert_eq!(dst, src);

        prefetch_nta(&dst);

        let mut dst_256 = PackedLogRecord::empty();
        stream_record_256(&mut dst_256, &src);
        assert_eq!(dst_256, src);

        let mut dst_128 = PackedLogRecord::empty();
        stream_record_128(&mut dst_128, &src);
        assert_eq!(dst_128, src);

        let mut dst_64 = PackedLogRecord::empty();
        stream_record_64(&mut dst_64, &src);
        assert_eq!(dst_64, src);

        let mut dst_portable = PackedLogRecord::empty();
        stream_record_portable(&mut dst_portable, &src);
        assert_eq!(dst_portable, src);
    }
}
