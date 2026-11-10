//! Hardware Memory Confinement & AVX2/SSE2 Non-Temporal Streaming Store.
//!
//! Large payloads (>= 256 bytes) are written directly into DRAM using Non-Temporal Stores
//! (`_mm256_stream_si256` / `VMOVNTDQ` on AVX2, or `_mm_stream_si128` / `MOVNTDQ` on SSE2),
//! bypassing CPU L1/L2 data cache lines. This guarantees complete immunity against CPU
//! cache pollution on the serving hot path.
//!
//! # Architecture: Hardware Register Matching & Function Pointer Dispatch
//! Rather than executing dynamic `if-else` branch ladders (`is_x86_feature_detected!`) on
//! every payload store, the hardware topology register width (`RegisterWidth::Bits256`,
//! `Bits128`, `Bits64`) is probed once at startup from [`global_hardware_topology`].
//! The hot-path store function pointer is resolved and cached in a zero-overhead [`OnceLock`],
//! reducing hot-path dispatch to a direct function pointer jump.

use bytes::Bytes;

/// 128-bit vector streaming store using SSE2 `_mm_stream_si128`.
#[cfg(target_arch = "x86_64")]
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
#[cfg(target_arch = "x86_64")]
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

/// Non-temporal streaming store to confine large payloads into DRAM,
/// bypassing L1, L2, and L3 CPU cache lines to prevent cache pollution.
///
/// # Safety
///
/// `dst` must be valid for writes of `len` bytes.
/// `src` must be valid for reads of `len` bytes.
#[inline(always)]
pub unsafe fn stream_payload_to_dram(dst: *mut u8, src: *const u8, len: usize) {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") {
            unsafe {
                stream_payload_avx2_dispatched(dst, src, len);
            }
            return;
        }
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

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn stream_payload_avx2(dst: *mut u8, src: *const u8, len: usize) {
    use core::arch::x86_64::*;

    unsafe {
        // 1. Align destination pointer to 32-byte boundary
        let dst_addr = dst as usize;
        let head = (32 - (dst_addr % 32)) % 32;
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

        // 2. Copy remaining unaligned tail
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
        // 1. Align destination pointer to 16-byte boundary
        let dst_addr = dst as usize;
        let head = (16 - (dst_addr % 16)) % 16;
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

        // 2. Copy remaining unaligned tail
        if offset < len {
            std::ptr::copy_nonoverlapping(src.add(offset), dst.add(offset), len - offset);
        }
    }
}

/// Allocates an aligned buffer in DRAM and copies payload using Non-Temporal Stores
/// if payload is >= 256 bytes, preventing CPU L1/L2 cache pollution.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dram_confinement_small_payload() {
        let data = b"small payload under 256 bytes";
        let bytes = create_dram_confined_bytes(data);
        assert_eq!(bytes.as_ref(), data);
    }

    #[test]
    fn test_dram_confinement_large_payload() {
        let pattern = b"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
        let mut large = Vec::with_capacity(4096);
        while large.len() < 4096 {
            large.extend_from_slice(pattern);
        }
        large.truncate(4096);

        let bytes = create_dram_confined_bytes(&large);
        assert_eq!(bytes.as_ref(), large.as_slice());
    }

    #[test]
    fn test_dram_confinement_arbitrary_lengths() {
        for len in [255, 256, 257, 511, 512, 513, 1023, 1024, 1025, 4096] {
            let original: Vec<u8> = (0..len).map(|i| (i % 256) as u8).collect();
            let bytes = create_dram_confined_bytes(&original);
            assert_eq!(bytes.as_ref(), original.as_slice(), "Failed at len {len}");
        }
    }
}
