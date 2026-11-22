//! Hardware-Optimized 2-Digit Decimal Lookup Tables & Formatting (`velda-core::lut::decimal`).
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────────────────────┐
//! │                   VELDA EDGE SIMD DECIMAL ASCII PIPELINE (4 TIERS)                      │
//! ├───────┬────────────┬──────────────┬──────────────────┬──────────────────────────────────┤
//! │ Tier  │ Bit Width  │ Max Digits   │ Vector Algorithm │ Hardware Execution Mechanics     │
//! ├───────┼────────────┼──────────────┼──────────────────┼──────────────────────────────────┤
//! │ 16b   │ u16        │ 1..=5 digits │ Short / Lemire   │ [0..99: Direct LUT] [>=100: SIMD]│
//! │ 32b   │ u32        │ 1..=10 digits│ Lemire & Muła    │ 5x 2-digit SSE2/NEON reciprocal  │
//! │ 64b   │ u64 / i64  │ 1..=20 digits│ 9-digit SIMD Blk │ hi + lo 10^9/10^18 SIMD blocks   │
//! │ 128b  │ u128       │ 1..=39 digits│ 10^18 SIMD Block │ Hierarchical 64b SIMD reduction  │
//! ├───────┴────────────┴──────────────┴──────────────────┴──────────────────────────────────┤
//! │ Branchless Leading-Zero Trim (u32 SIMD):                                               │
//! │  ascii: __m128i ──► _mm_cmpeq_epi8(0x30) ──► _mm_movemask_epi8                         │
//! │  lz = (!mask).trailing_zeros() ──► copy_nonoverlapping(ptr.add(lz), out, 10 - lz)     │
//! └─────────────────────────────────────────────────────────────────────────────────────────┘
//! ```

/// Pre-computed 200-byte lookup table for 2-digit decimal ASCII pairs (`00` to `99`).
///
/// Resident in CPU L1 Data Cache (200 bytes, occupies ~3.1 cache lines).
pub const DIGIT_PAIRS: &[u8; 200] = b"\
    00010203040506070809\
    10111213141516171819\
    20212223242526272829\
    30313233343536373839\
    40414243444546474849\
    50515253545556575859\
    60616263646566676869\
    70717273747576777879\
    80818283848586878889\
    90919293949596979899";

// ============================================================================
// Tier 2: 32-Bit SIMD Decimal Formatting (1..=10 digits)
// ============================================================================

/// Formats an exact 10-digit unsigned 32-bit integer into ASCII bytes using the Lemire & Muła SIMD algorithm.
#[inline(always)]
pub fn format_u32_10digits_simd(n: u32, out: &mut [u8; 10]) {
    let q0 = ((n as u64 * 0x51EB_851F) >> 37) as u32;
    let p4 = n - q0 * 100;

    let q1 = ((q0 as u64 * 0x51EB_851F) >> 37) as u32;
    let p3 = q0 - q1 * 100;

    let q2 = ((q1 as u64 * 0x51EB_851F) >> 37) as u32;
    let p2 = q1 - q2 * 100;

    let q3 = ((q2 as u64 * 0x51EB_851F) >> 37) as u32;
    let p1 = q2 - q3 * 100;

    let p0 = q3;

    #[cfg(target_arch = "x86_64")]
    unsafe {
        use core::arch::x86_64::*;
        let pairs = _mm_set_epi16(
            0, 0, 0, p4 as i16, p3 as i16, p2 as i16, p1 as i16, p0 as i16,
        );
        let tens = _mm_mulhi_epu16(pairs, _mm_set1_epi16(6554));
        let ones = _mm_sub_epi16(pairs, _mm_mullo_epi16(tens, _mm_set1_epi16(10)));
        let bytes = _mm_or_si128(tens, _mm_slli_epi16(ones, 8));
        let ascii = _mm_add_epi8(bytes, _mm_set1_epi8(0x30));

        let lo = _mm_cvtsi128_si64(ascii) as u64;
        let hi = _mm_extract_epi16::<4>(ascii) as u16;
        out[..8].copy_from_slice(&lo.to_ne_bytes());
        out[8..10].copy_from_slice(&hi.to_ne_bytes());
    }

    #[cfg(not(target_arch = "x86_64"))]
    {
        let p0_idx = p0 as usize * 2;
        let p1_idx = p1 as usize * 2;
        let p2_idx = p2 as usize * 2;
        let p3_idx = p3 as usize * 2;
        let p4_idx = p4 as usize * 2;
        out[0] = DIGIT_PAIRS[p0_idx];
        out[1] = DIGIT_PAIRS[p0_idx + 1];
        out[2] = DIGIT_PAIRS[p1_idx];
        out[3] = DIGIT_PAIRS[p1_idx + 1];
        out[4] = DIGIT_PAIRS[p2_idx];
        out[5] = DIGIT_PAIRS[p2_idx + 1];
        out[6] = DIGIT_PAIRS[p3_idx];
        out[7] = DIGIT_PAIRS[p3_idx + 1];
        out[8] = DIGIT_PAIRS[p4_idx];
        out[9] = DIGIT_PAIRS[p4_idx + 1];
    }
}

/// Formats an unsigned 32-bit integer (0..=4,294,967,295) into decimal ASCII bytes directly into `out`.
///
/// Uses Lemire & Muła SIMD vector arithmetic with branchless leading-zero trimming.
/// Returns the number of bytes written (1..=10).
///
/// # Safety
/// `out` must point to a buffer with at least 10 writable bytes.
#[inline(always)]
pub unsafe fn format_u32_simd(n: u32, out: *mut u8) -> usize {
    if n < 10 {
        unsafe {
            *out = b'0' + n as u8;
            return 1;
        }
    }

    #[cfg(target_arch = "x86_64")]
    unsafe {
        use core::arch::x86_64::*;

        let q0 = ((n as u64 * 0x51EB_851F) >> 37) as u32;
        let p4 = n - q0 * 100;

        let q1 = ((q0 as u64 * 0x51EB_851F) >> 37) as u32;
        let p3 = q0 - q1 * 100;

        let q2 = ((q1 as u64 * 0x51EB_851F) >> 37) as u32;
        let p2 = q1 - q2 * 100;

        let q3 = ((q2 as u64 * 0x51EB_851F) >> 37) as u32;
        let p1 = q2 - q3 * 100;

        let p0 = q3;

        let pairs = _mm_set_epi16(
            0, 0, 0, p4 as i16, p3 as i16, p2 as i16, p1 as i16, p0 as i16,
        );
        let tens = _mm_mulhi_epu16(pairs, _mm_set1_epi16(6554));
        let ones = _mm_sub_epi16(pairs, _mm_mullo_epi16(tens, _mm_set1_epi16(10)));
        let bytes = _mm_or_si128(tens, _mm_slli_epi16(ones, 8));
        let ascii = _mm_add_epi8(bytes, _mm_set1_epi8(0x30));

        let mut raw = [0u8; 16];
        _mm_storeu_si128(raw.as_mut_ptr() as *mut __m128i, ascii);

        let cmp = _mm_cmpeq_epi8(ascii, _mm_set1_epi8(0x30));
        let mask = _mm_movemask_epi8(cmp) as u32;
        let lz = (!mask).trailing_zeros() as usize;
        let len = 10 - lz;

        core::ptr::copy_nonoverlapping(raw.as_ptr().add(lz), out, len);
        len
    }

    #[cfg(not(target_arch = "x86_64"))]
    unsafe {
        format_u32_fallback(n, out)
    }
}

#[cfg(not(target_arch = "x86_64"))]
#[inline(always)]
unsafe fn format_u32_fallback(mut n: u32, out: *mut u8) -> usize {
    let mut temp = [0u8; 10];
    let mut curr = 10;
    while n >= 100 {
        let rem = (n % 100) as usize * 2;
        n /= 100;
        curr -= 2;
        temp[curr] = DIGIT_PAIRS[rem];
        temp[curr + 1] = DIGIT_PAIRS[rem + 1];
    }
    if n < 10 {
        curr -= 1;
        temp[curr] = b'0' + n as u8;
    } else {
        let rem = n as usize * 2;
        curr -= 2;
        temp[curr] = DIGIT_PAIRS[rem];
        temp[curr + 1] = DIGIT_PAIRS[rem + 1];
    }
    let len = 10 - curr;
    unsafe {
        core::ptr::copy_nonoverlapping(temp.as_ptr().add(curr), out, len);
    }
    len
}

// ============================================================================
// Tier 1: 16-Bit SIMD Decimal Formatting (1..=5 digits)
// ============================================================================

/// Formats an unsigned 16-bit integer (0..=65535) into decimal ASCII bytes using SIMD.
///
/// Designed for ports (0..=65535), HTTP status codes (100..=599), and route IDs.
/// Returns the number of bytes written (1..=5).
///
/// # Safety
/// `out` must point to a buffer with at least 5 writable bytes.
#[inline(always)]
pub unsafe fn format_u16_simd(n: u16, out: *mut u8) -> usize {
    if n < 10 {
        unsafe {
            *out = b'0' + n as u8;
            1
        }
    } else if n < 100 {
        unsafe {
            let rem = n as usize * 2;
            *out = DIGIT_PAIRS[rem];
            *out.add(1) = DIGIT_PAIRS[rem + 1];
            2
        }
    } else {
        unsafe { format_u32_simd(n as u32, out) }
    }
}

// ============================================================================
// Tier 3: 64-Bit SIMD Decimal Formatting (1..=20 digits)
// ============================================================================

/// Formats an unsigned 64-bit integer into decimal ASCII bytes using SIMD vector pipelines.
///
/// Composes 9-digit SIMD blocks without division loops or branch mispredictions.
/// Returns the number of bytes written (1..=20).
///
/// # Safety
/// `out` must point to a buffer with at least 20 writable bytes.
#[inline(always)]
pub unsafe fn format_u64_simd(n: u64, out: *mut u8) -> usize {
    const B1: u64 = 1_000_000_000; // 10^9
    const B2: u64 = 1_000_000_000_000_000_000; // 10^18

    if n < B1 {
        unsafe { format_u32_simd(n as u32, out) }
    } else if n < B2 {
        let hi = (n / B1) as u32;
        let lo = (n % B1) as u32;
        unsafe {
            let hi_len = format_u32_simd(hi, out);
            let mut lo_buf = [0u8; 10];
            format_u32_10digits_simd(lo + B1 as u32, &mut lo_buf);
            core::ptr::copy_nonoverlapping(lo_buf.as_ptr().add(1), out.add(hi_len), 9);
            hi_len + 9
        }
    } else {
        let hi = (n / B2) as u32;
        let rem = n % B2;
        let mid = (rem / B1) as u32;
        let lo = (rem % B1) as u32;
        unsafe {
            let hi_len = format_u32_simd(hi, out);

            let mut mid_buf = [0u8; 10];
            format_u32_10digits_simd(mid + B1 as u32, &mut mid_buf);
            core::ptr::copy_nonoverlapping(mid_buf.as_ptr().add(1), out.add(hi_len), 9);

            let mut lo_buf = [0u8; 10];
            format_u32_10digits_simd(lo + B1 as u32, &mut lo_buf);
            core::ptr::copy_nonoverlapping(lo_buf.as_ptr().add(1), out.add(hi_len + 9), 9);

            hi_len + 18
        }
    }
}

// ============================================================================
// Tier 4: 128-Bit SIMD Decimal Formatting (1..=39 digits)
// ============================================================================

/// Formats an unsigned 128-bit integer into decimal ASCII bytes using hierarchical SIMD pipelines.
///
/// Returns the number of bytes written (1..=39).
///
/// # Safety
/// `out` must point to a buffer with at least 40 writable bytes.
#[inline(always)]
pub unsafe fn format_u128_simd(n: u128, out: *mut u8) -> usize {
    if n <= u64::MAX as u128 {
        unsafe { format_u64_simd(n as u64, out) }
    } else {
        const B1: u64 = 1_000_000_000; // 10^9
        const B2: u128 = 1_000_000_000_000_000_000; // 10^18
        let hi = n / B2;
        let lo = (n % B2) as u64;

        unsafe {
            let hi_len = format_u128_simd(hi, out);

            let lo_hi = (lo / B1) as u32;
            let lo_lo = (lo % B1) as u32;

            let mut lo_hi_buf = [0u8; 10];
            format_u32_10digits_simd(lo_hi + B1 as u32, &mut lo_hi_buf);
            let mut lo_lo_buf = [0u8; 10];
            format_u32_10digits_simd(lo_lo + B1 as u32, &mut lo_lo_buf);

            core::ptr::copy_nonoverlapping(lo_hi_buf.as_ptr().add(1), out.add(hi_len), 9);
            core::ptr::copy_nonoverlapping(lo_lo_buf.as_ptr().add(1), out.add(hi_len + 9), 9);

            hi_len + 18
        }
    }
}

/// Formats a 128-bit integer into a stack buffer without heap allocation.
#[inline(always)]
pub fn fast_format_u128_into(buf: &mut [u8; 40], n: u128) -> usize {
    unsafe { format_u128_simd(n, buf.as_mut_ptr()) }
}

/// Formats a 19-digit nanosecond timestamp into an exact 19-byte ASCII slice using SIMD vector instructions.
#[inline(always)]
pub fn fast_format_ts_19digits_simd(now_nanos: u128, out: &mut [u8; 19]) {
    let sec = (now_nanos / 1_000_000_000) as u64;
    let rem = (now_nanos % 1_000_000_000) as u32;

    if (1_000_000_000..=9_999_999_999).contains(&sec) {
        let mut sec_buf = [0u8; 10];
        format_u32_10digits_simd(sec as u32, &mut sec_buf);
        out[..10].copy_from_slice(&sec_buf);

        let mut rem_buf = [0u8; 10];
        format_u32_10digits_simd(rem + 1_000_000_000, &mut rem_buf);
        out[10..19].copy_from_slice(&rem_buf[1..10]);
    } else {
        let mut temp = [0u8; 40];
        let len = fast_format_u128_into(&mut temp, now_nanos);
        out.fill(b'0');
        if len <= 19 {
            out[19 - len..].copy_from_slice(&temp[..len]);
        } else {
            out.copy_from_slice(&temp[..19]);
        }
    }
}

// ============================================================================
// Public Zero-Alloc Delegation Wrappers (Backward Compatible Hot Path API)
// ============================================================================

/// Formats an unsigned 16-bit integer (0..=65535) into decimal ASCII bytes directly into `ptr`, returning bytes written.
///
/// # Safety
/// `ptr` must point to a buffer with at least 5 writable bytes.
#[inline(always)]
pub unsafe fn fast_write_u16_ptr(ptr: *mut u8, n: u16) -> usize {
    unsafe { format_u16_simd(n, ptr) }
}

/// Formats an unsigned 32-bit integer into decimal ASCII bytes directly into `ptr`, returning bytes written.
///
/// # Safety
/// `ptr` must point to a buffer with at least 10 writable bytes.
#[inline(always)]
pub unsafe fn fast_write_u32_ptr(ptr: *mut u8, n: u32) -> usize {
    unsafe { format_u32_simd(n, ptr) }
}

/// Formats an unsigned 64-bit integer into decimal ASCII bytes directly into `ptr`, returning bytes written.
///
/// # Safety
/// `ptr` must point to a buffer with at least 20 writable bytes.
#[inline(always)]
pub unsafe fn fast_write_u64_ptr(ptr: *mut u8, n: u64) -> usize {
    unsafe { format_u64_simd(n, ptr) }
}

/// Formats an unsigned 64-bit integer into decimal ASCII bytes directly appending to `buf` using SIMD.
#[inline(always)]
pub fn fast_write_u64(buf: &mut Vec<u8>, n: u64) {
    let mut temp = [0u8; 20];
    let len = unsafe { format_u64_simd(n, temp.as_mut_ptr()) };
    buf.extend_from_slice(&temp[..len]);
}

/// Formats an unsigned 64-bit integer into a stack buffer slice, returning written byte length.
#[inline(always)]
pub fn fast_write_u64_slice(out: &mut [u8; 24], n: u64) -> usize {
    unsafe { format_u64_simd(n, out.as_mut_ptr()) }
}

/// Formats a signed 64-bit integer into a stack buffer slice, returning written byte length.
#[inline(always)]
pub fn fast_write_i64_slice(out: &mut [u8; 24], n: i64) -> usize {
    if n < 0 {
        out[0] = b'-';
        let len = unsafe { format_u64_simd(n.unsigned_abs(), out.as_mut_ptr().add(1)) };
        len + 1
    } else {
        unsafe { format_u64_simd(n as u64, out.as_mut_ptr()) }
    }
}

/// Serializes signed 64-bit integer with negative sign check directly into `buf`.
#[inline(always)]
pub fn fast_write_i64(buf: &mut Vec<u8>, n: i64) {
    if n < 0 {
        buf.push(b'-');
        fast_write_u64(buf, n.unsigned_abs());
    } else {
        fast_write_u64(buf, n as u64);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_digit_pairs_exhaustive() {
        assert_eq!(DIGIT_PAIRS.len(), 200);
        for i in 0..100 {
            let expected_hi = b'0' + (i / 10) as u8;
            let expected_lo = b'0' + (i % 10) as u8;
            assert_eq!(DIGIT_PAIRS[i * 2], expected_hi);
            assert_eq!(DIGIT_PAIRS[i * 2 + 1], expected_lo);
        }
    }

    #[test]
    fn test_format_u16_simd() {
        let cases: &[u16] = &[
            0, 5, 9, 10, 42, 99, 100, 255, 999, 1000, 8080, 9999, 10000, 54321, 65535,
        ];
        for &val in cases {
            let mut raw = [0u8; 16];
            let len = unsafe { format_u16_simd(val, raw.as_mut_ptr()) };
            let s = std::str::from_utf8(&raw[..len]).unwrap();
            assert_eq!(s, val.to_string(), "Failed on u16 value {val}");
        }
    }

    #[test]
    fn test_format_u32_simd() {
        let cases: &[u32] = &[
            0,
            1,
            9,
            10,
            42,
            99,
            100,
            999,
            1000,
            9999,
            10000,
            65535,
            100000,
            999999,
            1000000,
            12345678,
            999999999,
            1000000000,
            1732011000,
            u32::MAX,
        ];
        for &val in cases {
            let mut raw = [0u8; 16];
            let len = unsafe { format_u32_simd(val, raw.as_mut_ptr()) };
            let s = std::str::from_utf8(&raw[..len]).unwrap();
            assert_eq!(s, val.to_string(), "Failed on u32 value {val}");
        }
    }

    #[test]
    fn test_format_u64_simd() {
        let cases: &[u64] = &[
            0,
            1,
            9,
            10,
            42,
            99,
            100,
            999,
            1000,
            123456789,
            999_999_999,
            1_000_000_000,
            1732011000,
            9_999_999_999,
            10_000_000_000,
            10_000_000_001,
            999_999_999_999_999_999,
            1_000_000_000_000_000_000,
            1_000_000_000_000_000_001,
            1732011000123456789,
            9876543210123456789,
            u64::MAX,
        ];
        for &val in cases {
            let mut raw = [0u8; 32];
            let len = unsafe { format_u64_simd(val, raw.as_mut_ptr()) };
            let s = std::str::from_utf8(&raw[..len]).unwrap();
            assert_eq!(s, val.to_string(), "Failed on u64 value {val}");
        }
    }

    #[test]
    fn test_format_u128_simd() {
        let cases: &[u128] = &[
            0,
            1,
            9,
            10,
            42,
            100,
            u64::MAX as u128,
            1_000_000_000_000_000_000,
            1_000_000_000_000_000_001,
            1_710_123_456_789_012_345,
            123_456_789_012_345_678_901_234_567_890,
            u128::MAX,
        ];
        for &val in cases {
            let mut raw = [0u8; 48];
            let len = unsafe { format_u128_simd(val, raw.as_mut_ptr()) };
            let s = std::str::from_utf8(&raw[..len]).unwrap();
            assert_eq!(s, val.to_string(), "Failed on u128 value {val}");
        }
    }

    #[test]
    fn test_fast_write_u64_slice() {
        let mut buf = [0u8; 24];
        let len = fast_write_u64_slice(&mut buf, 1234567890);
        assert_eq!(&buf[..len], b"1234567890");

        let len0 = fast_write_u64_slice(&mut buf, 0);
        assert_eq!(&buf[..len0], b"0");
    }

    #[test]
    fn test_fast_write_i64_slice() {
        let mut buf = [0u8; 24];
        let len = fast_write_i64_slice(&mut buf, -42);
        assert_eq!(&buf[..len], b"-42");

        let len_pos = fast_write_i64_slice(&mut buf, 12345);
        assert_eq!(&buf[..len_pos], b"12345");
    }

    #[test]
    fn test_fast_write_u64_vec() {
        let mut buf = Vec::new();
        fast_write_u64(&mut buf, 0);
        assert_eq!(&buf, b"0");

        buf.clear();
        fast_write_u64(&mut buf, 1234567890);
        assert_eq!(&buf, b"1234567890");
    }

    #[test]
    fn test_fast_write_i64() {
        let mut buf = Vec::new();
        fast_write_i64(&mut buf, 0);
        assert_eq!(&buf, b"0");

        buf.clear();
        fast_write_i64(&mut buf, -42);
        assert_eq!(&buf, b"-42");

        buf.clear();
        fast_write_i64(&mut buf, 12345);
        assert_eq!(&buf, b"12345");
    }

    #[test]
    fn test_fast_format_ts_19digits_simd() {
        let mut out = [0u8; 19];
        let nanos = 1_710_123_456_789_012_345u128;
        fast_format_ts_19digits_simd(nanos, &mut out);
        assert_eq!(std::str::from_utf8(&out).unwrap(), "1710123456789012345");
    }
}
