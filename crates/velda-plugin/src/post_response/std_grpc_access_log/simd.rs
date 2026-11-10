//! Hardware-Harmonized Vector Record Transport (`std_grpc_access_log::simd`).
//!
//! Provides ultra-low-latency 256-byte vector register copying between worker stack
//! and SPSC ring buffer slots, eliminating `memcpy` overhead on the frontline serving hot path.
//!
//! # Micro-Architectural Registration
//! - **AVX2 (256-bit)**: 8 × 256-bit YMM register loads & stores (4 cycles on modern Zen/Intel cores).
//! - **SSE2 / NEON (128-bit)**: 16 × 128-bit XMM / NEON vector register transfers.
//! - **SWAR (64-bit)**: 32 × 64-bit general-purpose ALU transfers with unrolled loop.
//! - **Portable (Scalar)**: Non-overlapping pointer block copy fallback.

use super::record::PackedGrpcRecord;

/// Function pointer signature for copying a 256-byte aligned `PackedGrpcRecord`.
#[allow(dead_code)]
pub type CopyRecordFn = fn(dst: &mut PackedGrpcRecord, src: &PackedGrpcRecord);

/// Transfers a 256-byte `PackedGrpcRecord` using the optimal CPU register width.
///
/// Inlined direct execution eliminating indirect function pointer overhead.
#[inline(always)]
pub fn copy_record(dst: &mut PackedGrpcRecord, src: &PackedGrpcRecord) {
    #[cfg(target_arch = "x86_64")]
    {
        if std::is_x86_feature_detected!("avx2") {
            copy_record_256(dst, src);
        } else {
            copy_record_128(dst, src);
        }
    }
    #[cfg(target_arch = "aarch64")]
    {
        copy_record_128(dst, src);
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        if usize::BITS == 64 {
            copy_record_64(dst, src);
        } else {
            copy_record_portable(dst, src);
        }
    }
}

/// Function pointer signature for non-temporal streaming store of a 256-byte `PackedGrpcRecord`.
#[allow(dead_code)]
pub type StreamRecordFn = fn(dst: &mut PackedGrpcRecord, src: &PackedGrpcRecord);

/// Non-temporal streaming store: writes a 256-byte `PackedGrpcRecord` directly into RAM via
/// Write-Combining Buffers, bypassing CPU cache levels (L1/L2/L3) to prevent cache pollution.
///
/// Inlined direct execution eliminating indirect function pointer overhead.
#[inline(always)]
pub fn stream_record(dst: &mut PackedGrpcRecord, src: &PackedGrpcRecord) {
    #[cfg(target_arch = "x86_64")]
    {
        if std::is_x86_feature_detected!("avx2") {
            unsafe { stream_record_avx2_impl(dst, src) };
        } else {
            stream_record_128(dst, src);
        }
    }
    #[cfg(target_arch = "aarch64")]
    {
        stream_record_128(dst, src);
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        if usize::BITS == 64 {
            stream_record_64(dst, src);
        } else {
            stream_record_portable(dst, src);
        }
    }
}

/// Issues a non-temporal prefetch hint (`_MM_HINT_NTA`) across the 256-byte record (4 cache lines).
#[inline(always)]
pub fn prefetch_nta(record: &PackedGrpcRecord) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        use core::arch::x86_64::*;
        let p = record as *const PackedGrpcRecord as *const i8;
        _mm_prefetch(p, _MM_HINT_NTA);
        _mm_prefetch(p.add(64), _MM_HINT_NTA);
        _mm_prefetch(p.add(128), _MM_HINT_NTA);
        _mm_prefetch(p.add(192), _MM_HINT_NTA);
    }
}

/// 256-bit AVX2 non-temporal streaming store (8 × 32-byte VMOVNTDQ transfers + store fence).
#[inline]
pub fn stream_record_256(dst: &mut PackedGrpcRecord, src: &PackedGrpcRecord) {
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
unsafe fn stream_record_avx2_impl(dst: &mut PackedGrpcRecord, src: &PackedGrpcRecord) {
    use core::arch::x86_64::*;

    let s = src as *const PackedGrpcRecord as *const __m256i;
    let d = dst as *mut PackedGrpcRecord as *mut __m256i;

    unsafe {
        let v0 = _mm256_loadu_si256(s.add(0));
        let v1 = _mm256_loadu_si256(s.add(1));
        let v2 = _mm256_loadu_si256(s.add(2));
        let v3 = _mm256_loadu_si256(s.add(3));
        let v4 = _mm256_loadu_si256(s.add(4));
        let v5 = _mm256_loadu_si256(s.add(5));
        let v6 = _mm256_loadu_si256(s.add(6));
        let v7 = _mm256_loadu_si256(s.add(7));

        _mm256_stream_si256(d.add(0), v0);
        _mm256_stream_si256(d.add(1), v1);
        _mm256_stream_si256(d.add(2), v2);
        _mm256_stream_si256(d.add(3), v3);
        _mm256_stream_si256(d.add(4), v4);
        _mm256_stream_si256(d.add(5), v5);
        _mm256_stream_si256(d.add(6), v6);
        _mm256_stream_si256(d.add(7), v7);

        _mm_sfence();
    }
}

/// 128-bit SSE2 / NEON non-temporal streaming store.
#[inline]
pub fn stream_record_128(dst: &mut PackedGrpcRecord, src: &PackedGrpcRecord) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        use core::arch::x86_64::*;
        let s = src as *const PackedGrpcRecord as *const __m128i;
        let d = dst as *mut PackedGrpcRecord as *mut __m128i;

        for i in 0..16 {
            let v = _mm_loadu_si128(s.add(i));
            _mm_stream_si128(d.add(i), v);
        }
        _mm_sfence();
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        copy_record_128(dst, src);
    }
}

/// 64-bit SWAR streaming fallback.
#[inline]
pub fn stream_record_64(dst: &mut PackedGrpcRecord, src: &PackedGrpcRecord) {
    copy_record_64(dst, src);
}

/// Portable scalar streaming fallback.
#[inline]
pub fn stream_record_portable(dst: &mut PackedGrpcRecord, src: &PackedGrpcRecord) {
    copy_record_portable(dst, src);
}

/// 256-bit AVX2 standard copy.
#[inline]
pub fn copy_record_256(dst: &mut PackedGrpcRecord, src: &PackedGrpcRecord) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        use core::arch::x86_64::*;
        let s = src as *const PackedGrpcRecord as *const __m256i;
        let d = dst as *mut PackedGrpcRecord as *mut __m256i;

        _mm256_storeu_si256(d.add(0), _mm256_loadu_si256(s.add(0)));
        _mm256_storeu_si256(d.add(1), _mm256_loadu_si256(s.add(1)));
        _mm256_storeu_si256(d.add(2), _mm256_loadu_si256(s.add(2)));
        _mm256_storeu_si256(d.add(3), _mm256_loadu_si256(s.add(3)));
        _mm256_storeu_si256(d.add(4), _mm256_loadu_si256(s.add(4)));
        _mm256_storeu_si256(d.add(5), _mm256_loadu_si256(s.add(5)));
        _mm256_storeu_si256(d.add(6), _mm256_loadu_si256(s.add(6)));
        _mm256_storeu_si256(d.add(7), _mm256_loadu_si256(s.add(7)));
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        copy_record_128(dst, src);
    }
}

/// 128-bit vector copy (SSE2 or NEON).
#[inline]
pub fn copy_record_128(dst: &mut PackedGrpcRecord, src: &PackedGrpcRecord) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        use core::arch::x86_64::*;
        let s = src as *const PackedGrpcRecord as *const __m128i;
        let d = dst as *mut PackedGrpcRecord as *mut __m128i;

        for i in 0..16 {
            _mm_storeu_si128(d.add(i), _mm_loadu_si128(s.add(i)));
        }
    }
    #[cfg(target_arch = "aarch64")]
    unsafe {
        use core::arch::aarch64::*;
        let s = src as *const PackedGrpcRecord as *const uint8x16_t;
        let d = dst as *mut PackedGrpcRecord as *mut uint8x16_t;

        for i in 0..16 {
            vst1q_u8(d.add(i) as *mut u8, vld1q_u8(s.add(i) as *const u8));
        }
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        copy_record_64(dst, src);
    }
}

/// 64-bit SWAR transfer.
#[inline]
pub fn copy_record_64(dst: &mut PackedGrpcRecord, src: &PackedGrpcRecord) {
    unsafe {
        let s = src as *const PackedGrpcRecord as *const u64;
        let d = dst as *mut PackedGrpcRecord as *mut u64;

        for i in 0..32 {
            d.add(i).write(s.add(i).read());
        }
    }
}

/// Portable byte slice copy.
#[inline]
pub fn copy_record_portable(dst: &mut PackedGrpcRecord, src: &PackedGrpcRecord) {
    unsafe {
        std::ptr::copy_nonoverlapping(
            src as *const PackedGrpcRecord as *const u8,
            dst as *mut PackedGrpcRecord as *mut u8,
            256,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_grpc_simd_copy_integrity() {
        let mut src = PackedGrpcRecord::empty();
        src.timestamp_ms = 998877665544;
        src.duration_us = 4567;
        src.grpc_status = 0;
        src.transport = 2;
        src.messages_sent = 55;
        src.messages_received = 12;
        src.content_length = 65536;
        src.set_rpc_path("/order.OrderService/StreamOrders");
        src.set_request_id(b"req-simd-001");
        src.set_route_id(b"rt-grpc");
        src.set_upstream(b"10.0.1.20:50051");
        src.set_peer_addr(b"127.0.0.1:44321");
        src.set_error_msg(b"no error");

        let mut dst_256 = PackedGrpcRecord::empty();
        copy_record_256(&mut dst_256, &src);
        assert_eq!(src, dst_256);

        let mut dst_128 = PackedGrpcRecord::empty();
        copy_record_128(&mut dst_128, &src);
        assert_eq!(src, dst_128);

        let mut dst_64 = PackedGrpcRecord::empty();
        copy_record_64(&mut dst_64, &src);
        assert_eq!(src, dst_64);

        let mut dst_port = PackedGrpcRecord::empty();
        copy_record_portable(&mut dst_port, &src);
        assert_eq!(src, dst_port);
    }
}
