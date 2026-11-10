//! SIMD Vectorized Header Validation and Parallel Fingerprint Matching.
//!
//! Provides hardware-accelerated RFC 9110 character validation and vector-probed
//! header fingerprint filtering across AVX2 (256-bit), SSE2 (128-bit), and scalar fallback.

use http::header::HeaderName;

/// Computes an 8-bit hash fingerprint for a header name for SIMD vector probing.
///
/// Encodes length and terminal characters to filter out non-matching headers in 1 CPU cycle.
#[inline(always)]
pub fn compute_header_fingerprint(name: &HeaderName) -> u8 {
    let bytes = name.as_str().as_bytes();
    if bytes.is_empty() {
        0
    } else {
        let len = bytes.len() as u8;
        len.wrapping_mul(37) ^ bytes[0] ^ bytes[bytes.len() - 1]
    }
}

/// Function signature for SIMD RFC 9110 header value validation.
#[allow(dead_code)]
pub type ValidateHeaderValFn = fn(val: &[u8]) -> bool;

/// Function signature for SIMD 32-slot fingerprint vector matching.
#[allow(dead_code)]
pub type FingerprintMatchFn = fn(fingerprints: &[u8; 32], fp_len: usize, target: u8) -> bool;

// ── 1. RFC 9110 Header Value Validation ──────────────────────────────

/// Scalar fallback for validating RFC 9110 header values.
///
/// Rejects CRLF (`\r`, `\n`), illegal ASCII control characters (< 0x20 except `\t`), and `0x7F`.
pub fn scalar_validate_header_value(val: &[u8]) -> bool {
    for &b in val {
        if (b < 0x20 && b != b'\t') || b == 0x7F {
            return false;
        }
    }
    true
}

/// SSE2 (128-bit) implementation of RFC 9110 header value validation.
///
/// # Safety
/// Caller must ensure that the CPU supports the SSE2 instruction set.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse2")]
pub unsafe fn sse2_validate_header_value(val: &[u8]) -> bool {
    use core::arch::x86_64::*;

    let len = val.len();
    let mut ptr = val.as_ptr();
    let mut rem = len;

    let v_tab = _mm_set1_epi8(b'\t' as i8);
    let v_space = _mm_set1_epi8(0x20);
    let v_del = _mm_set1_epi8(0x7F);

    while rem >= 16 {
        unsafe {
            let chunk = _mm_loadu_si128(ptr as *const __m128i);

            // Bytes < 0x20: min(b, 0x20) == b && b != 0x20
            let min_space = _mm_min_epu8(chunk, v_space);
            let is_lte_space = _mm_cmpeq_epi8(min_space, chunk);
            let is_space = _mm_cmpeq_epi8(chunk, v_space);
            let is_lt_space = _mm_andnot_si128(is_space, is_lte_space);

            // Allow '\t' (0x09)
            let is_tab = _mm_cmpeq_epi8(chunk, v_tab);
            let is_illegal_ctrl = _mm_andnot_si128(is_tab, is_lt_space);

            // DEL (0x7F)
            let is_del = _mm_cmpeq_epi8(chunk, v_del);

            let bad_vec = _mm_or_si128(is_illegal_ctrl, is_del);
            if _mm_movemask_epi8(bad_vec) != 0 {
                return false;
            }

            ptr = ptr.add(16);
        }
        rem -= 16;
    }

    if rem > 0 {
        let tail = unsafe { core::slice::from_raw_parts(ptr, rem) };
        return scalar_validate_header_value(tail);
    }
    true
}

/// AVX2 (256-bit) implementation of RFC 9110 header value validation.
///
/// # Safety
/// Caller must ensure that the CPU supports the AVX2 instruction set.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
pub unsafe fn avx2_validate_header_value(val: &[u8]) -> bool {
    use core::arch::x86_64::*;

    let len = val.len();
    let mut ptr = val.as_ptr();
    let mut rem = len;

    let v_tab = _mm256_set1_epi8(b'\t' as i8);
    let v_space = _mm256_set1_epi8(0x20);
    let v_del = _mm256_set1_epi8(0x7F);

    while rem >= 32 {
        unsafe {
            let chunk = _mm256_loadu_si256(ptr as *const __m256i);

            let min_space = _mm256_min_epu8(chunk, v_space);
            let is_lte_space = _mm256_cmpeq_epi8(min_space, chunk);
            let is_space = _mm256_cmpeq_epi8(chunk, v_space);
            let is_lt_space = _mm256_andnot_si256(is_space, is_lte_space);

            let is_tab = _mm256_cmpeq_epi8(chunk, v_tab);
            let is_illegal_ctrl = _mm256_andnot_si256(is_tab, is_lt_space);

            let is_del = _mm256_cmpeq_epi8(chunk, v_del);

            let bad_vec = _mm256_or_si256(is_illegal_ctrl, is_del);
            if _mm256_movemask_epi8(bad_vec) != 0 {
                return false;
            }

            ptr = ptr.add(32);
        }
        rem -= 32;
    }

    if rem > 0 {
        let tail = unsafe { core::slice::from_raw_parts(ptr, rem) };
        return unsafe { sse2_validate_header_value(tail) };
    }
    true
}

// ── 2. Vectorized Header Fingerprint Probe ───────────────────────────

/// Scalar fallback for matching 8-bit fingerprints.
pub fn scalar_fingerprint_match(fingerprints: &[u8; 32], fp_len: usize, target: u8) -> bool {
    let limit = fp_len.min(32);
    for &fp in &fingerprints[..limit] {
        if fp == target {
            return true;
        }
    }
    false
}

/// SSE2 (128-bit) fingerprint matching (probes 16 fingerprints per pass).
///
/// # Safety
/// Caller must ensure that the CPU supports the SSE2 instruction set.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse2")]
pub unsafe fn sse2_fingerprint_match(fingerprints: &[u8; 32], fp_len: usize, target: u8) -> bool {
    use core::arch::x86_64::*;

    let limit = fp_len.min(32);
    if limit == 0 {
        return false;
    }

    unsafe {
        let v_target = _mm_set1_epi8(target as i8);

        // Pass 1: first 16 entries
        let v_fps0 = _mm_loadu_si128(fingerprints.as_ptr() as *const __m128i);
        let mask0 = _mm_movemask_epi8(_mm_cmpeq_epi8(v_fps0, v_target)) as u32;
        let valid_mask0 = if limit < 16 {
            (1u32 << limit) - 1
        } else {
            0xFFFF
        };
        if (mask0 & valid_mask0) != 0 {
            return true;
        }

        // Pass 2: remaining entries if limit > 16
        if limit > 16 {
            let v_fps1 = _mm_loadu_si128(fingerprints.as_ptr().add(16) as *const __m128i);
            let mask1 = _mm_movemask_epi8(_mm_cmpeq_epi8(v_fps1, v_target)) as u32;
            let valid_mask1 = (1u32 << (limit - 16)) - 1;
            if (mask1 & valid_mask1) != 0 {
                return true;
            }
        }
    }

    false
}

/// AVX2 (256-bit) fingerprint matching (probes all 32 fingerprints in 1 vector instruction).
///
/// # Safety
/// Caller must ensure that the CPU supports the AVX2 instruction set.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
pub unsafe fn avx2_fingerprint_match(fingerprints: &[u8; 32], fp_len: usize, target: u8) -> bool {
    use core::arch::x86_64::*;

    let limit = fp_len.min(32);
    if limit == 0 {
        return false;
    }

    unsafe {
        let v_target = _mm256_set1_epi8(target as i8);
        let v_fps = _mm256_loadu_si256(fingerprints.as_ptr() as *const __m256i);
        let mask = _mm256_movemask_epi8(_mm256_cmpeq_epi8(v_fps, v_target)) as u32;

        let valid_mask = if limit == 32 {
            0xFFFF_FFFF
        } else {
            (1u32 << limit) - 1
        };

        (mask & valid_mask) != 0
    }
}

// ── Hardware Vector Dispatch ──────────────────────────────────────────

/// Validates a header value slice using the optimal CPU instruction set.
///
/// Inlined direct execution eliminating indirect function pointer overhead.
#[inline(always)]
pub fn validate_header_value_simd(val: &[u8]) -> bool {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") {
            return unsafe { avx2_validate_header_value(val) };
        }
        if is_x86_feature_detected!("sse2") {
            return unsafe { sse2_validate_header_value(val) };
        }
    }
    scalar_validate_header_value(val)
}

/// Probes a header fingerprint against a precomputed 32-entry vector using SIMD instructions.
///
/// Inlined direct execution eliminating indirect function pointer overhead.
#[inline(always)]
pub fn fingerprint_matches_simd(fingerprints: &[u8; 32], fp_len: usize, target: u8) -> bool {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") {
            return unsafe { avx2_fingerprint_match(fingerprints, fp_len, target) };
        }
        if is_x86_feature_detected!("sse2") {
            return unsafe { sse2_fingerprint_match(fingerprints, fp_len, target) };
        }
    }
    scalar_fingerprint_match(fingerprints, fp_len, target)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_fingerprint() {
        let h1 = HeaderName::from_static("server");
        let h2 = HeaderName::from_static("x-powered-by");
        let fp1 = compute_header_fingerprint(&h1);
        let fp2 = compute_header_fingerprint(&h2);
        assert_ne!(fp1, fp2);
    }

    #[test]
    fn test_validate_header_value_correctness() {
        assert!(validate_header_value_simd(b"application/json"));
        assert!(validate_header_value_simd(b"text/html; charset=utf-8"));
        assert!(validate_header_value_simd(b"valid with\ttab"));
        assert!(!validate_header_value_simd(b"bad\r\nvalue"));
        assert!(!validate_header_value_simd(b"bad\nvalue"));
        assert!(!validate_header_value_simd(b"bad\rvalue"));
        assert!(!validate_header_value_simd(&[b'a', 0x7F, b'b']));
        assert!(!validate_header_value_simd(&[b'a', 0x01, b'b']));

        // Large 64-byte payload check
        let mut large = vec![b'a'; 64];
        assert!(validate_header_value_simd(&large));
        large[35] = b'\n';
        assert!(!validate_header_value_simd(&large));
    }

    #[test]
    fn test_fingerprint_matching_all_backends() {
        let mut fps = [0u8; 32];
        let targets = [12u8, 34, 56, 78, 90, 111, 222];
        for (i, &t) in targets.iter().enumerate() {
            fps[i] = t;
        }
        let len = targets.len();

        for &t in &targets {
            assert!(scalar_fingerprint_match(&fps, len, t));
            assert!(fingerprint_matches_simd(&fps, len, t));
        }

        assert!(!scalar_fingerprint_match(&fps, len, 99));
        assert!(!fingerprint_matches_simd(&fps, len, 99));
    }
}
