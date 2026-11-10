//! Hardware-Optimized SIMD & Non-Temporal Streaming Memory Store.
//!
//! Provides dynamic CPU feature detection (AVX2, SSE2, Scalar fallback) via function pointer,
//! and flushes compressed payloads directly to RAM bypassing CPU cache lines to prevent
//! cache thrashing on edge proxy hot paths.

use bytes::Bytes;
use std::sync::LazyLock;

/// Dynamic function pointer type for hardware streaming memory copy.
pub type StreamingCopyFn = fn(src: &[u8], dst: &mut [u8]);

/// Scalar fallback for non-temporal streaming copy when SIMD is unavailable.
fn scalar_streaming_copy(src: &[u8], dst: &mut [u8]) {
    debug_assert_eq!(src.len(), dst.len());
    dst.copy_from_slice(src);
}

/// SSE2 (128-bit) Non-Temporal Streaming Store implementation.
///
/// Flushes 16-byte chunks directly to RAM via Write-Combining buffers, bypassing L1/L2/L3 cache.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse2")]
unsafe fn sse2_streaming_copy_impl(src: &[u8], dst: &mut [u8]) {
    use core::arch::x86_64::*;
    debug_assert_eq!(src.len(), dst.len());
    let len = src.len();
    if len == 0 {
        return;
    }

    unsafe {
        let mut src_ptr = src.as_ptr();
        let mut dst_ptr = dst.as_mut_ptr();
        let mut remaining = len;

        // Align dst_ptr to 16-byte boundary using scalar copy
        let align_offset = (dst_ptr as usize) & 15;
        if align_offset != 0 {
            let lead = std::cmp::min(16 - align_offset, remaining);
            std::ptr::copy_nonoverlapping(src_ptr, dst_ptr, lead);
            src_ptr = src_ptr.add(lead);
            dst_ptr = dst_ptr.add(lead);
            remaining -= lead;
        }

        // Stream 16-byte chunks bypassing CPU cache
        while remaining >= 16 {
            let v = _mm_loadu_si128(src_ptr as *const __m128i);
            _mm_stream_si128(dst_ptr as *mut __m128i, v);
            src_ptr = src_ptr.add(16);
            dst_ptr = dst_ptr.add(16);
            remaining -= 16;
        }

        if remaining > 0 {
            std::ptr::copy_nonoverlapping(src_ptr, dst_ptr, remaining);
        }

        // Memory fence: serializes and drains Write-Combining buffers to RAM
        _mm_sfence();
    }
}

/// AVX2 (256-bit) Non-Temporal Streaming Store implementation.
///
/// Flushes 32-byte chunks directly to RAM via Write-Combining buffers, bypassing L1/L2/L3 cache.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn avx2_streaming_copy_impl(src: &[u8], dst: &mut [u8]) {
    use core::arch::x86_64::*;
    debug_assert_eq!(src.len(), dst.len());
    let len = src.len();
    if len == 0 {
        return;
    }

    unsafe {
        let mut src_ptr = src.as_ptr();
        let mut dst_ptr = dst.as_mut_ptr();
        let mut remaining = len;

        // Align dst_ptr to 32-byte boundary using scalar copy
        let align_offset = (dst_ptr as usize) & 31;
        if align_offset != 0 {
            let lead = std::cmp::min(32 - align_offset, remaining);
            std::ptr::copy_nonoverlapping(src_ptr, dst_ptr, lead);
            src_ptr = src_ptr.add(lead);
            dst_ptr = dst_ptr.add(lead);
            remaining -= lead;
        }

        // Stream 32-byte chunks bypassing CPU cache
        while remaining >= 32 {
            let v = _mm256_loadu_si256(src_ptr as *const __m256i);
            _mm256_stream_si256(dst_ptr as *mut __m256i, v);
            src_ptr = src_ptr.add(32);
            dst_ptr = dst_ptr.add(32);
            remaining -= 32;
        }

        if remaining > 0 {
            std::ptr::copy_nonoverlapping(src_ptr, dst_ptr, remaining);
        }

        // Memory fence: serializes and drains Write-Combining buffers to RAM
        _mm_sfence();
    }
}

/// Detects CPU architecture and hardware features at startup, returning the optimal function pointer.
fn detect_streaming_copy_fn() -> StreamingCopyFn {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") {
            return |src, dst| unsafe { avx2_streaming_copy_impl(src, dst) };
        }
        if is_x86_feature_detected!("sse2") {
            return |src, dst| unsafe { sse2_streaming_copy_impl(src, dst) };
        }
    }
    scalar_streaming_copy
}

/// Hardware-optimized non-temporal memory streaming copy.
///
/// Inlined direct execution eliminating indirect function pointer overhead.
#[inline(always)]
pub fn streaming_copy(src: &[u8], dst: &mut [u8]) {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") {
            return unsafe { avx2_streaming_copy_impl(src, dst) };
        }
        if is_x86_feature_detected!("sse2") {
            return unsafe { sse2_streaming_copy_impl(src, dst) };
        }
    }
    scalar_streaming_copy(src, dst);
}

/// Globally bound, hardware-optimized non-temporal memory streaming copy function pointer.
pub static STREAMING_COPY: LazyLock<StreamingCopyFn> = LazyLock::new(detect_streaming_copy_fn);

/// Writes compressed bytes directly to RAM using hardware-vectorized non-temporal streaming routines.
///
/// Bypasses L1/L2/L3 CPU caches, preventing multi-kilobyte compressed payload frames
/// from displacing critical route tables or connection pool structures.
#[inline]
pub fn streaming_to_bytes(src: &[u8]) -> Bytes {
    if src.is_empty() {
        return Bytes::new();
    }
    let mut out = vec![0u8; src.len()];
    streaming_copy(src, &mut out);
    Bytes::from(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_streaming_copy_correctness() {
        let test_sizes = [
            0, 1, 7, 15, 16, 17, 31, 32, 33, 64, 127, 256, 1024, 4096, 65536,
        ];
        for &size in &test_sizes {
            let src: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();
            let mut dst = vec![0u8; size];
            (*STREAMING_COPY)(&src, &mut dst);
            assert_eq!(src, dst, "Mismatch for size {size}");

            let bytes = streaming_to_bytes(&src);
            assert_eq!(&bytes[..], &src[..], "Bytes mismatch for size {size}");
        }
    }

    #[test]
    fn test_scalar_fallback() {
        let src = b"hello world scalar test";
        let mut dst = vec![0u8; src.len()];
        scalar_streaming_copy(src, &mut dst);
        assert_eq!(&dst[..], src);
    }
}
