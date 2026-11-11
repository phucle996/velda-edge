//! Hardware-Optimized Integer Lookup Tables & SIMD Itoa (`statsd_exporter::lut`).
//!
//! Provides division-free integer formatting, 2-digit ASCII pair lookup,
//! and 128-bit SIMD integer-to-ASCII vectorization based on Lemire & Muła's algorithm.

/// Pre-computed 200-byte lookup table for 2-digit decimal ASCII pairs (`00` to `99`).
///
/// Enables branchless 2-digit integer formatting without hardware division.
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

/// Formats an exact 10-digit unsigned 32-bit integer into ASCII bytes using the Lemire & Muła SIMD algorithm.
///
/// 1. Splits 10 digits into 5 2-digit pairs via division-free reciprocal multiplication: `(val * 0x51EB851F) >> 37`.
/// 2. Converts all 5 pairs into ASCII bytes simultaneously via 128-bit SIMD vector instructions.
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
        // tens = (pairs * 6554) >> 16
        let tens = _mm_mulhi_epu16(pairs, _mm_set1_epi16(6554));
        // ones = pairs - tens * 10
        let ones = _mm_sub_epi16(pairs, _mm_mullo_epi16(tens, _mm_set1_epi16(10)));
        // pack: low byte = tens, high byte = ones (LE byte order in u16: tens at byte 0, ones at byte 1)
        let bytes = _mm_or_si128(tens, _mm_slli_epi16(ones, 8));
        // add '0' (0x30) simultaneously to all bytes
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

/// Serializes an unsigned 64-bit integer into a stack buffer slice, returning written byte length.
#[inline(always)]
pub fn fast_write_u64_slice(out: &mut [u8; 24], mut n: u64) -> usize {
    if n < 10 {
        out[0] = b'0' + n as u8;
        return 1;
    }
    if n < 100 {
        let rem = (n as usize) * 2;
        out[0] = DIGIT_PAIRS[rem];
        out[1] = DIGIT_PAIRS[rem + 1];
        return 2;
    }
    if n < 10_000 {
        let rem = ((n % 100) as usize) * 2;
        let hi = (n / 100) as usize;
        if hi < 10 {
            out[0] = b'0' + hi as u8;
            out[1] = DIGIT_PAIRS[rem];
            out[2] = DIGIT_PAIRS[rem + 1];
            return 3;
        } else {
            let rem_hi = hi * 2;
            out[0] = DIGIT_PAIRS[rem_hi];
            out[1] = DIGIT_PAIRS[rem_hi + 1];
            out[2] = DIGIT_PAIRS[rem];
            out[3] = DIGIT_PAIRS[rem + 1];
            return 4;
        }
    }
    if n < 1_000_000 {
        let rem0 = ((n % 100) as usize) * 2;
        let q0 = n / 100;
        let rem1 = ((q0 % 100) as usize) * 2;
        let hi = (q0 / 100) as usize;
        if hi < 10 {
            out[0] = b'0' + hi as u8;
            out[1] = DIGIT_PAIRS[rem1];
            out[2] = DIGIT_PAIRS[rem1 + 1];
            out[3] = DIGIT_PAIRS[rem0];
            out[4] = DIGIT_PAIRS[rem0 + 1];
            return 5;
        } else {
            let rem_hi = hi * 2;
            out[0] = DIGIT_PAIRS[rem_hi];
            out[1] = DIGIT_PAIRS[rem_hi + 1];
            out[2] = DIGIT_PAIRS[rem1];
            out[3] = DIGIT_PAIRS[rem1 + 1];
            out[4] = DIGIT_PAIRS[rem0];
            out[5] = DIGIT_PAIRS[rem0 + 1];
            return 6;
        }
    }
    if (1_000_000_000..=u32::MAX as u64).contains(&n) {
        let mut digits = [0u8; 10];
        format_u32_10digits_simd(n as u32, &mut digits);
        out[..10].copy_from_slice(&digits);
        return 10;
    }

    let mut temp = [0u8; 20];
    let mut curr = 20;

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

    let len = 20 - curr;
    out[..len].copy_from_slice(&temp[curr..20]);
    len
}

/// Serializes a signed 64-bit integer into a stack buffer slice, returning written byte length.
#[inline(always)]
pub fn fast_write_i64_slice(out: &mut [u8; 24], n: i64) -> usize {
    if n < 0 {
        out[0] = b'-';
        let mut temp = [0u8; 24];
        let len = fast_write_u64_slice(&mut temp, n.unsigned_abs());
        out[1..1 + len].copy_from_slice(&temp[..len]);
        1 + len
    } else {
        fast_write_u64_slice(out, n as u64)
    }
}

/// Serializes an unsigned 64-bit integer into ASCII bytes at the end of `buf`.
#[inline(always)]
pub fn fast_write_u64(buf: &mut Vec<u8>, n: u64) {
    let mut temp = [0u8; 24];
    let len = fast_write_u64_slice(&mut temp, n);
    buf.extend_from_slice(&temp[..len]);
}

/// Serializes a signed 64-bit integer into ASCII bytes at the end of `buf`.
#[inline(always)]
pub fn fast_write_i64(buf: &mut Vec<u8>, n: i64) {
    let mut temp = [0u8; 24];
    let len = fast_write_i64_slice(&mut temp, n);
    buf.extend_from_slice(&temp[..len]);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_digit_pairs_validity() {
        for i in 0..100 {
            let s = format!("{:02}", i);
            assert_eq!(&DIGIT_PAIRS[i * 2..i * 2 + 2], s.as_bytes());
        }
    }

    #[test]
    fn test_format_u32_10digits_simd_accuracy() {
        let cases = [
            0u32,
            1,
            9,
            42,
            999,
            12345,
            999_999,
            1_000_000_000,
            1_234_567_890,
            2_147_483_647,
            4_000_000_000,
            u32::MAX,
        ];
        for &val in &cases {
            let mut out = [0u8; 10];
            format_u32_10digits_simd(val, &mut out);
            let expected = format!("{:010}", val);
            assert_eq!(
                std::str::from_utf8(&out).unwrap(),
                expected,
                "Failed on val={val}"
            );
        }
    }

    #[test]
    fn test_fast_write_u64_slice_exhaustive_small() {
        let mut out = [0u8; 24];
        for i in 0..10_000u64 {
            let len = fast_write_u64_slice(&mut out, i);
            let s = std::str::from_utf8(&out[..len]).unwrap();
            assert_eq!(s, i.to_string(), "Mismatch on i={i}");
        }
    }

    #[test]
    fn test_fast_write_u64_slice_large_and_boundaries() {
        let cases = [
            10_000u64,
            99_999,
            100_000,
            999_999,
            1_000_000,
            999_999_999,
            1_000_000_000,
            1_234_567_890,
            9_999_999_999,
            10_000_000_000,
            123_456_789_012_345,
            u32::MAX as u64,
            u64::MAX - 1,
            u64::MAX,
        ];
        let mut out = [0u8; 24];
        for &val in &cases {
            let len = fast_write_u64_slice(&mut out, val);
            let s = std::str::from_utf8(&out[..len]).unwrap();
            assert_eq!(s, val.to_string(), "Mismatch on val={val}");
        }
    }

    #[test]
    fn test_fast_write_i64_slice_boundaries() {
        let cases = [
            0i64,
            1,
            -1,
            42,
            -42,
            99,
            -99,
            1000,
            -1000,
            1_234_567_890,
            -1_234_567_890,
            i64::MAX,
            i64::MIN + 1,
            i64::MIN,
        ];
        let mut out = [0u8; 24];
        for &val in &cases {
            let len = fast_write_i64_slice(&mut out, val);
            let s = std::str::from_utf8(&out[..len]).unwrap();
            assert_eq!(s, val.to_string(), "Mismatch on val={val}");
        }
    }

    #[test]
    fn test_fast_write_vec_matches_slice() {
        let cases = [0u64, 7, 42, 999, 1234567, u64::MAX];
        for &val in &cases {
            let mut vec = Vec::new();
            fast_write_u64(&mut vec, val);
            assert_eq!(vec, val.to_string().into_bytes());
        }

        let signed_cases = [0i64, -7, 42, -999, -1234567, i64::MIN, i64::MAX];
        for &val in &signed_cases {
            let mut vec = Vec::new();
            fast_write_i64(&mut vec, val);
            assert_eq!(vec, val.to_string().into_bytes());
        }
    }
}
