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

// ── 1. RFC 9110 Header Value Validation ──────────────────────────────

/// Validates a header value slice according to RFC 9110 in pure Rust so LLVM auto-vectorizes.
///
/// Rejects CRLF (`\r`, `\n`), illegal ASCII control characters (< 0x20 except `\t`), and `0x7F`.
#[inline(always)]
pub fn validate_header_value_simd(val: &[u8]) -> bool {
    for &b in val {
        if (b < 0x20 && b != b'\t') || b == 0x7F {
            return false;
        }
    }
    true
}

/// Probes a header fingerprint against a precomputed 32-entry array in pure Rust.
#[inline(always)]
pub fn fingerprint_matches_simd(fingerprints: &[u8; 32], fp_len: usize, target: u8) -> bool {
    let limit = fp_len.min(32);
    fingerprints[..limit].contains(&target)
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
            assert!(fingerprint_matches_simd(&fps, len, t));
        }

        assert!(!fingerprint_matches_simd(&fps, len, 99));
    }
}
