//! W3C TraceContext Specification Codec (`w3c_trace_context`).
//!
//! Handles parsing, validation, generation, and formatting of W3C `traceparent` headers
//! conforming to the W3C TraceContext Recommendation:
//! `version-trace_id-parent_id-trace_flags` (e.g. `00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01`).

use std::sync::atomic::{AtomicU64, Ordering};

/// Global atomic sequence counter for generating unique 64-bit span IDs.
static SPAN_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Pre-computed 256-byte ASCII hex validation table (`true` for `0..=9`, `a..=f`, `A..=F`).
const HEX_VALID_TABLE: [bool; 256] = {
    let mut t = [false; 256];
    let mut i = b'0';
    while i <= b'9' {
        t[i as usize] = true;
        i += 1;
    }
    i = b'a';
    while i <= b'f' {
        t[i as usize] = true;
        i += 1;
    }
    i = b'A';
    while i <= b'F' {
        t[i as usize] = true;
        i += 1;
    }
    t
};

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn validate_hex_32_avx2_inner(buf: &[u8; 32]) -> bool {
    use core::arch::x86_64::*;
    unsafe {
        let v = _mm256_loadu_si256(buf.as_ptr() as *const __m256i);
        // Reject all-'0' trace_id ("00000000000000000000000000000000")
        let eq_zero_char = _mm256_cmpeq_epi8(v, _mm256_set1_epi8(b'0' as i8));
        if _mm256_movemask_epi8(eq_zero_char) == -1i32 {
            return false;
        }
        let is_digit = _mm256_and_si256(
            _mm256_cmpgt_epi8(v, _mm256_set1_epi8((b'0' - 1) as i8)),
            _mm256_cmpgt_epi8(_mm256_set1_epi8((b'9' + 1) as i8), v),
        );
        let lower = _mm256_or_si256(v, _mm256_set1_epi8(0x20));
        let is_alpha = _mm256_and_si256(
            _mm256_cmpgt_epi8(lower, _mm256_set1_epi8((b'a' - 1) as i8)),
            _mm256_cmpgt_epi8(_mm256_set1_epi8((b'f' + 1) as i8), lower),
        );
        let valid = _mm256_or_si256(is_digit, is_alpha);
        _mm256_movemask_epi8(valid) == -1i32
    }
}

#[cfg(target_arch = "x86_64")]
fn validate_hex_16_128(buf: &[u8; 16]) -> bool {
    use core::arch::x86_64::*;
    unsafe {
        let v = _mm_loadu_si128(buf.as_ptr() as *const __m128i);
        let eq_zero_char = _mm_cmpeq_epi8(v, _mm_set1_epi8(b'0' as i8));
        if _mm_movemask_epi8(eq_zero_char) == 0xFFFF {
            return false;
        }
        let is_digit = _mm_and_si128(
            _mm_cmpgt_epi8(v, _mm_set1_epi8((b'0' - 1) as i8)),
            _mm_cmpgt_epi8(_mm_set1_epi8((b'9' + 1) as i8), v),
        );
        let lower = _mm_or_si128(v, _mm_set1_epi8(0x20));
        let is_alpha = _mm_and_si128(
            _mm_cmpgt_epi8(lower, _mm_set1_epi8((b'a' - 1) as i8)),
            _mm_cmpgt_epi8(_mm_set1_epi8((b'f' + 1) as i8), lower),
        );
        let valid = _mm_or_si128(is_digit, is_alpha);
        _mm_movemask_epi8(valid) == 0xFFFF
    }
}

#[cfg(target_arch = "aarch64")]
fn validate_hex_16_128(buf: &[u8; 16]) -> bool {
    use core::arch::aarch64::*;
    unsafe {
        let v = vld1q_u8(buf.as_ptr());
        let eq_zero = vceqq_u8(v, vdupq_n_u8(b'0'));
        if vminvq_u8(eq_zero) == 0xFF {
            return false;
        }
        let is_digit = vandq_u8(vcgeq_u8(v, vdupq_n_u8(b'0')), vcleq_u8(v, vdupq_n_u8(b'9')));
        let lower = vorrq_u8(v, vdupq_n_u8(0x20));
        let is_alpha = vandq_u8(
            vcgeq_u8(lower, vdupq_n_u8(b'a')),
            vcleq_u8(lower, vdupq_n_u8(b'f')),
        );
        let valid = vorrq_u8(is_digit, is_alpha);
        vminvq_u8(valid) == 0xFF
    }
}

#[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
fn validate_hex_32_128(buf: &[u8; 16 + 16]) -> bool {
    let lo: &[u8; 16] = buf[0..16].try_into().unwrap();
    let hi: &[u8; 16] = buf[16..32].try_into().unwrap();
    // Note: trace_id is valid if both halves are valid hex AND at least one half is not all-'0'
    let mut all_zero = true;
    for &b in buf {
        if b != b'0' {
            all_zero = false;
            break;
        }
    }
    if all_zero {
        return false;
    }
    // Check hex validity using 128-bit lanes (ignoring all-zero check per half)
    validate_hex_only_16_128(lo) && validate_hex_only_16_128(hi)
}

#[cfg(target_arch = "x86_64")]
#[inline(always)]
fn validate_hex_only_16_128(buf: &[u8; 16]) -> bool {
    use core::arch::x86_64::*;
    unsafe {
        let v = _mm_loadu_si128(buf.as_ptr() as *const __m128i);
        let is_digit = _mm_and_si128(
            _mm_cmpgt_epi8(v, _mm_set1_epi8((b'0' - 1) as i8)),
            _mm_cmpgt_epi8(_mm_set1_epi8((b'9' + 1) as i8), v),
        );
        let lower = _mm_or_si128(v, _mm_set1_epi8(0x20));
        let is_alpha = _mm_and_si128(
            _mm_cmpgt_epi8(lower, _mm_set1_epi8((b'a' - 1) as i8)),
            _mm_cmpgt_epi8(_mm_set1_epi8((b'f' + 1) as i8), lower),
        );
        _mm_movemask_epi8(_mm_or_si128(is_digit, is_alpha)) == 0xFFFF
    }
}

#[cfg(target_arch = "aarch64")]
#[inline(always)]
fn validate_hex_only_16_128(buf: &[u8; 16]) -> bool {
    use core::arch::aarch64::*;
    unsafe {
        let v = vld1q_u8(buf.as_ptr());
        let is_digit = vandq_u8(vcgeq_u8(v, vdupq_n_u8(b'0')), vcleq_u8(v, vdupq_n_u8(b'9')));
        let lower = vorrq_u8(v, vdupq_n_u8(0x20));
        let is_alpha = vandq_u8(
            vcgeq_u8(lower, vdupq_n_u8(b'a')),
            vcleq_u8(lower, vdupq_n_u8(b'f')),
        );
        vminvq_u8(vorrq_u8(is_digit, is_alpha)) == 0xFF
    }
}

#[cfg(any(test, not(any(target_arch = "x86_64", target_arch = "aarch64"))))]
fn validate_hex_16_64(buf: &[u8; 16]) -> bool {
    const ZERO_8: u64 = 0x3030_3030_3030_3030;
    let w0 = unsafe { (buf.as_ptr() as *const u64).read_unaligned() };
    let w1 = unsafe { (buf.as_ptr().add(8) as *const u64).read_unaligned() };
    if (w0 ^ ZERO_8) | (w1 ^ ZERO_8) == 0 {
        return false;
    }
    let mut ok = true;
    for &b in buf {
        ok &= HEX_VALID_TABLE[b as usize];
    }
    ok
}

#[cfg(any(test, not(any(target_arch = "x86_64", target_arch = "aarch64"))))]
fn validate_hex_32_64(buf: &[u8; 32]) -> bool {
    const ZERO_8: u64 = 0x3030_3030_3030_3030;
    let ptr = buf.as_ptr();
    let w0 = unsafe { (ptr as *const u64).read_unaligned() } ^ ZERO_8;
    let w1 = unsafe { (ptr.add(8) as *const u64).read_unaligned() } ^ ZERO_8;
    let w2 = unsafe { (ptr.add(16) as *const u64).read_unaligned() } ^ ZERO_8;
    let w3 = unsafe { (ptr.add(24) as *const u64).read_unaligned() } ^ ZERO_8;
    if (w0 | w1 | w2 | w3) == 0 {
        return false;
    }
    let mut ok = true;
    for &b in buf {
        ok &= HEX_VALID_TABLE[b as usize];
    }
    ok
}

/// Validates a 32-byte ASCII hex slice (and rejects all-zeros) using hardware-vectorized routines.
///
/// Inlined direct execution eliminating indirect function pointer overhead.
#[inline(always)]
pub fn is_valid_nonzero_hex_32(buf: &[u8; 32]) -> bool {
    #[cfg(target_arch = "x86_64")]
    {
        if std::is_x86_feature_detected!("avx2") {
            unsafe { validate_hex_32_avx2_inner(buf) }
        } else {
            validate_hex_32_128(buf)
        }
    }
    #[cfg(target_arch = "aarch64")]
    {
        validate_hex_32_128(buf)
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        validate_hex_32_64(buf)
    }
}

/// Validates a 16-byte ASCII hex slice (and rejects all-zeros) using hardware-vectorized routines.
///
/// Inlined direct execution eliminating indirect function pointer overhead.
#[inline(always)]
pub fn is_valid_nonzero_hex_16(buf: &[u8; 16]) -> bool {
    #[cfg(target_arch = "x86_64")]
    {
        validate_hex_16_128(buf)
    }
    #[cfg(target_arch = "aarch64")]
    {
        validate_hex_16_128(buf)
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        validate_hex_16_64(buf)
    }
}

/// Parsed view of a valid incoming W3C `traceparent` header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedTraceParent<'a> {
    pub version: &'a str,
    pub trace_id: &'a str,
    pub parent_id: &'a str,
    pub flags: &'a str,
}

/// W3C TraceContext codec utility.
pub struct TraceParentCodec;

impl TraceParentCodec {
    /// Generates a new 16-hex lowercase string span ID using monotonic sequence and high-resolution entropy.
    #[inline]
    pub fn generate_span_id() -> [u8; 16] {
        let seq = SPAN_COUNTER.fetch_add(1, Ordering::Relaxed);
        let time_part = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.subsec_nanos() as u64)
            .unwrap_or(0);
        let val = (time_part << 32) ^ seq;
        let mut out = [0u8; 16];
        velda_core::request_id::format_hex_16(val.max(1), &mut out);
        out
    }

    /// Generates a new 32-hex lowercase string trace ID using unix timestamp and monotonic entropy.
    #[inline]
    pub fn generate_trace_id() -> String {
        let sec = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let seq = SPAN_COUNTER.fetch_add(1, Ordering::Relaxed);
        let mut trace_buf = [0u8; 32];
        velda_core::request_id::format_hex_16(sec, (&mut trace_buf[0..16]).try_into().unwrap());
        velda_core::request_id::format_hex_16(seq, (&mut trace_buf[16..32]).try_into().unwrap());
        std::str::from_utf8(&trace_buf)
            .unwrap_or("00000000000000000000000000000001")
            .to_string()
    }

    /// Parses and validates an incoming W3C `traceparent` header value.
    ///
    /// Conformance requirements:
    /// - Exactly 55 characters: `00-{32hex}-{16hex}-{2hex}`
    /// - Version strictly `00`
    /// - Non-zero `trace_id` (16 bytes = 32 hex chars) validated in a single 256-bit/128-bit SIMD register pass
    /// - Non-zero `parent_id` (8 bytes = 16 hex chars) validated in a single 128-bit/64-bit register pass
    /// - Valid hex characters
    #[inline]
    pub fn parse(val: &str) -> Option<ParsedTraceParent<'_>> {
        let trimmed = val.trim();
        // Exact length of W3C traceparent: 2 + 1 + 32 + 1 + 16 + 1 + 2 = 55 chars
        if trimmed.len() != 55 {
            return None;
        }

        let bytes = trimmed.as_bytes();
        if bytes[0] != b'0'
            || bytes[1] != b'0'
            || bytes[2] != b'-'
            || bytes[35] != b'-'
            || bytes[52] != b'-'
        {
            return None;
        }

        let trace_bytes: &[u8; 32] = bytes[3..35].try_into().ok()?;
        if !is_valid_nonzero_hex_32(trace_bytes) {
            return None;
        }

        let parent_bytes: &[u8; 16] = bytes[36..52].try_into().ok()?;
        if !is_valid_nonzero_hex_16(parent_bytes) {
            return None;
        }

        if !HEX_VALID_TABLE[bytes[53] as usize] || !HEX_VALID_TABLE[bytes[54] as usize] {
            return None;
        }

        Some(ParsedTraceParent {
            version: &trimmed[0..2],
            trace_id: &trimmed[3..35],
            parent_id: &trimmed[36..52],
            flags: &trimmed[53..55],
        })
    }

    /// Formats a complete W3C `traceparent` header string with single-allocation exact capacity.
    #[inline]
    pub fn format(trace_id: &str, span_id: &str, flags: &str) -> String {
        let mut out =
            String::with_capacity(3 + trace_id.len() + 1 + span_id.len() + 1 + flags.len());
        out.push_str("00-");
        out.push_str(trace_id);
        out.push('-');
        out.push_str(span_id);
        out.push('-');
        out.push_str(flags);
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_traceparent_parsing() {
        let raw = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        let parsed = TraceParentCodec::parse(raw).expect("Valid traceparent");
        assert_eq!(parsed.version, "00");
        assert_eq!(parsed.trace_id, "4bf92f3577b34da6a3ce929d0e0e4736");
        assert_eq!(parsed.parent_id, "00f067aa0ba902b7");
        assert_eq!(parsed.flags, "01");
    }

    #[test]
    fn test_invalid_traceparent_rejected() {
        assert!(TraceParentCodec::parse("").is_none());
        assert!(
            TraceParentCodec::parse("01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
                .is_none()
        );
        assert!(
            TraceParentCodec::parse("00-00000000000000000000000000000000-00f067aa0ba902b7-01")
                .is_none()
        );
        assert!(
            TraceParentCodec::parse("00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01")
                .is_none()
        );
        assert!(
            TraceParentCodec::parse("00-4bf92f3577b34da6a3ce929d0e0e473g-00f067aa0ba902b7-01")
                .is_none()
        );
        assert!(TraceParentCodec::parse("short").is_none());
    }

    #[test]
    fn test_register_width_hex_validators_parity() {
        let valid_32: &[u8; 32] = b"4bf92f3577b34da6a3ce929d0e0e4736";
        let zero_32: &[u8; 32] = b"00000000000000000000000000000000";
        let bad_32: &[u8; 32] = b"4bf92f3577b34da6a3ce929d0e0e473z";

        assert!(validate_hex_32_64(valid_32));
        assert!(is_valid_nonzero_hex_32(valid_32));
        assert!(!validate_hex_32_64(zero_32));
        assert!(!is_valid_nonzero_hex_32(zero_32));
        assert!(!validate_hex_32_64(bad_32));
        assert!(!is_valid_nonzero_hex_32(bad_32));

        let valid_16: &[u8; 16] = b"00f067aa0ba902b7";
        let zero_16: &[u8; 16] = b"0000000000000000";
        let bad_16: &[u8; 16] = b"00f067aa0ba902b/";

        assert!(validate_hex_16_64(valid_16));
        assert!(is_valid_nonzero_hex_16(valid_16));
        assert!(!validate_hex_16_64(zero_16));
        assert!(!is_valid_nonzero_hex_16(zero_16));
        assert!(!validate_hex_16_64(bad_16));
        assert!(!is_valid_nonzero_hex_16(bad_16));
    }

    #[test]
    fn test_generate_ids() {
        let span = TraceParentCodec::generate_span_id();
        assert_eq!(span.len(), 16);
        let trace = TraceParentCodec::generate_trace_id();
        assert_eq!(trace.len(), 32);
    }
}
