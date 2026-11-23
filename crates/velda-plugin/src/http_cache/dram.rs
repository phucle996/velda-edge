//! Hardware Memory Confinement & Compile-Time Non-Temporal Streaming Store.
//!
//! Large payloads (`>= 256` bytes) are written directly into DRAM using Non-Temporal Stores
//! to bypass CPU L1/L2 data cache lines and prevent cache pollution on the hot path.
//!
//! ```text
//!   stream_payload_to_dram(dst, src, len)  [compile-time target dispatch]
//!     ├─ x86_64 + target_feature("avx2")
//!     │    ├─ len >= 64  ──► head align(32) ─► _mm256_stream_si256 (32B NT) ─► sfence ─► tail
//!     │    ├─ len >= 32  ──► _mm_stream_si128 (16B NT) ─► sfence
//!     │    └─ len < 32   ──► copy_nonoverlapping
//!     ├─ x86_64 (baseline SSE2)
//!     │    ├─ len >= 32  ──► head align(16) ─► _mm_stream_si128 (16B NT) ─► sfence ─► tail
//!     │    └─ len < 32   ──► copy_nonoverlapping
//!     └─ not(x86_64) (aarch64 / portable)
//!          └─ copy_nonoverlapping
//! ```

use bytes::Bytes;

/// 128-bit vector streaming store using SSE2 `_mm_stream_si128`.
#[cfg(all(target_arch = "x86_64", not(target_feature = "avx2")))]
unsafe fn stream_payload_sse2_dispatched(dst: *mut u8, src: *const u8, len: usize) {
    if len >= 32 {
        unsafe {
            stream_payload_sse2(dst, src, len);
        }
    } else {
        unsafe {
            std::ptr::copy_nonoverlapping(src, dst, len);
        }
    }
}

/// 256-bit vector streaming store using AVX2 `_mm256_stream_si256`.
#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
unsafe fn stream_payload_avx2_dispatched(dst: *mut u8, src: *const u8, len: usize) {
    if len >= 64 {
        unsafe {
            stream_payload_avx2(dst, src, len);
        }
    } else if len >= 32 {
        unsafe {
            stream_payload_sse2(dst, src, len);
        }
    } else {
        unsafe {
            std::ptr::copy_nonoverlapping(src, dst, len);
        }
    }
}

/// Non-temporal streaming store to confine payloads into DRAM without polluting CPU caches.
///
/// # Safety
///
/// - `dst` must be valid for writes of `len` bytes.
/// - `src` must be valid for reads of `len` bytes.
/// - `dst` and `src` must not overlap.
#[inline(always)]
pub unsafe fn stream_payload_to_dram(dst: *mut u8, src: *const u8, len: usize) {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    {
        unsafe {
            stream_payload_avx2_dispatched(dst, src, len);
        }
    }
    #[cfg(all(target_arch = "x86_64", not(target_feature = "avx2")))]
    {
        unsafe {
            stream_payload_sse2_dispatched(dst, src, len);
        }
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        unsafe {
            std::ptr::copy_nonoverlapping(src, dst, len);
        }
    }
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[target_feature(enable = "avx2")]
unsafe fn stream_payload_avx2(dst: *mut u8, src: *const u8, len: usize) {
    use core::arch::x86_64::*;

    unsafe {
        let dst_addr = dst as usize;
        let head = (32 - (dst_addr & 31)) & 31;
        if head > 0 && head <= len {
            std::ptr::copy_nonoverlapping(src, dst, head);
        }

        let mut offset = head;
        while offset + 32 <= len {
            let v = _mm256_loadu_si256(src.add(offset) as *const __m256i);
            _mm256_stream_si256(dst.add(offset) as *mut __m256i, v);
            offset += 32;
        }

        _mm_sfence();

        if offset < len {
            std::ptr::copy_nonoverlapping(src.add(offset), dst.add(offset), len - offset);
        }
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse2")]
unsafe fn stream_payload_sse2(dst: *mut u8, src: *const u8, len: usize) {
    use core::arch::x86_64::*;

    unsafe {
        let dst_addr = dst as usize;
        let head = (16 - (dst_addr & 15)) & 15;
        if head > 0 && head <= len {
            std::ptr::copy_nonoverlapping(src, dst, head);
        }

        let mut offset = head;
        while offset + 16 <= len {
            let v = _mm_loadu_si128(src.add(offset) as *const __m128i);
            _mm_stream_si128(dst.add(offset) as *mut __m128i, v);
            offset += 16;
        }

        _mm_sfence();

        if offset < len {
            std::ptr::copy_nonoverlapping(src.add(offset), dst.add(offset), len - offset);
        }
    }
}

/// Allocates a standalone DRAM buffer and streams payload via Non-Temporal Stores when `>= 256` bytes.
pub fn create_dram_confined_bytes(src: &[u8]) -> Bytes {
    if src.len() < 256 {
        return Bytes::copy_from_slice(src);
    }
    let mut vec = vec![0u8; src.len()];
    unsafe {
        stream_payload_to_dram(vec.as_mut_ptr(), src.as_ptr(), src.len());
    }
    Bytes::from(vec)
}
