//! Hardware-Optimized Integer Lookup Tables & SIMD Itoa (`prometheus_exporter::lut`).
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

/// Division-free calculation of `(n / 100, (n % 100) * 2)` via 32-bit reciprocal multiplication.
#[inline(always)]
const fn div_rem_100_u32(n: u32) -> (u32, usize) {
    let q = ((n as u64 * 0x51EB_851F) >> 37) as u32;
    let rem = ((n - q * 100) as usize) * 2;
    (q, rem)
}

/// Formats an exact 10-digit unsigned 32-bit integer into ASCII bytes using the Lemire & Muła SIMD algorithm.
///
/// 1. Splits 10 digits into 5 2-digit pairs via division-free reciprocal multiplication: `(val * 0x51EB851F) >> 37`.
/// 2. Converts all 5 pairs into ASCII bytes simultaneously via 128-bit SIMD vector instructions.
#[inline(always)]
pub fn format_u32_10digits_simd(n: u32, out: &mut [u8; 10]) {
    let (q0, p4_idx) = div_rem_100_u32(n);
    let p4 = (p4_idx / 2) as u32;

    let (q1, p3_idx) = div_rem_100_u32(q0);
    let p3 = (p3_idx / 2) as u32;

    let (q2, p2_idx) = div_rem_100_u32(q1);
    let p2 = (p2_idx / 2) as u32;

    let (q3, p1_idx) = div_rem_100_u32(q2);
    let p1 = (p1_idx / 2) as u32;

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
        let (hi, rem) = div_rem_100_u32(n as u32);
        if hi < 10 {
            out[0] = b'0' + hi as u8;
            out[1] = DIGIT_PAIRS[rem];
            out[2] = DIGIT_PAIRS[rem + 1];
            return 3;
        } else {
            let rem_hi = (hi as usize) * 2;
            out[0] = DIGIT_PAIRS[rem_hi];
            out[1] = DIGIT_PAIRS[rem_hi + 1];
            out[2] = DIGIT_PAIRS[rem];
            out[3] = DIGIT_PAIRS[rem + 1];
            return 4;
        }
    }
    if n < 1_000_000 {
        let (q0, rem0) = div_rem_100_u32(n as u32);
        let (hi, rem1) = div_rem_100_u32(q0);
        if hi < 10 {
            out[0] = b'0' + hi as u8;
            out[1] = DIGIT_PAIRS[rem1];
            out[2] = DIGIT_PAIRS[rem1 + 1];
            out[3] = DIGIT_PAIRS[rem0];
            out[4] = DIGIT_PAIRS[rem0 + 1];
            return 5;
        } else {
            let rem_hi = (hi as usize) * 2;
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

/// Serializes an unsigned 64-bit integer directly into `buf` with zero intermediate stack buffers for common integers.
#[inline(always)]
pub fn fast_write_u64(buf: &mut Vec<u8>, n: u64) {
    if n < 10 {
        buf.push(b'0' + n as u8);
        return;
    }
    if n < 100 {
        let rem = (n as usize) * 2;
        buf.extend_from_slice(&DIGIT_PAIRS[rem..rem + 2]);
        return;
    }
    if n < 10_000 {
        let (hi, rem) = div_rem_100_u32(n as u32);
        if hi < 10 {
            buf.extend_from_slice(&[b'0' + hi as u8, DIGIT_PAIRS[rem], DIGIT_PAIRS[rem + 1]]);
        } else {
            let rem_hi = (hi as usize) * 2;
            buf.extend_from_slice(&[
                DIGIT_PAIRS[rem_hi],
                DIGIT_PAIRS[rem_hi + 1],
                DIGIT_PAIRS[rem],
                DIGIT_PAIRS[rem + 1],
            ]);
        }
        return;
    }
    if n < 1_000_000 {
        let (q0, rem0) = div_rem_100_u32(n as u32);
        let (hi, rem1) = div_rem_100_u32(q0);
        if hi < 10 {
            buf.extend_from_slice(&[
                b'0' + hi as u8,
                DIGIT_PAIRS[rem1],
                DIGIT_PAIRS[rem1 + 1],
                DIGIT_PAIRS[rem0],
                DIGIT_PAIRS[rem0 + 1],
            ]);
        } else {
            let rem_hi = (hi as usize) * 2;
            buf.extend_from_slice(&[
                DIGIT_PAIRS[rem_hi],
                DIGIT_PAIRS[rem_hi + 1],
                DIGIT_PAIRS[rem1],
                DIGIT_PAIRS[rem1 + 1],
                DIGIT_PAIRS[rem0],
                DIGIT_PAIRS[rem0 + 1],
            ]);
        }
        return;
    }
    if (1_000_000_000..=u32::MAX as u64).contains(&n) {
        let mut digits = [0u8; 10];
        format_u32_10digits_simd(n as u32, &mut digits);
        buf.extend_from_slice(&digits);
        return;
    }

    let mut temp = [0u8; 24];
    let len = fast_write_u64_slice(&mut temp, n);
    buf.extend_from_slice(&temp[..len]);
}

/// Serializes a signed 64-bit integer into ASCII bytes at the end of `buf`.
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
    fn test_fast_write_u64_boundaries() {
        let test_cases = [
            0u64,
            1,
            9,
            10,
            11,
            99,
            100,
            101,
            999,
            1_000,
            9_999,
            10_000,
            12_345,
            99_999,
            100_000,
            999_999,
            1_000_000,
            1_000_000_000,
            1_234_567_890,
            u32::MAX as u64,
            u32::MAX as u64 + 1,
            10_000_000_000,
            123_456_789_012_345,
            u64::MAX - 1,
            u64::MAX,
        ];

        for &val in &test_cases {
            let mut buf = Vec::new();
            fast_write_u64(&mut buf, val);
            let expected = val.to_string();
            assert_eq!(
                buf.as_slice(),
                expected.as_bytes(),
                "fast_write_u64 mismatch for {val}"
            );

            let mut slice_buf = [0u8; 24];
            let len = fast_write_u64_slice(&mut slice_buf, val);
            assert_eq!(
                &slice_buf[..len],
                expected.as_bytes(),
                "fast_write_u64_slice mismatch for {val}"
            );
        }
    }

    #[test]
    fn test_fast_write_i64_boundaries() {
        let test_cases = [
            0i64,
            1,
            -1,
            9,
            -9,
            10,
            -10,
            99,
            -99,
            100,
            -100,
            12345,
            -12345,
            987_654_321,
            -987_654_321,
            i32::MAX as i64,
            i32::MIN as i64,
            i64::MAX,
            i64::MIN + 1,
        ];

        for &val in &test_cases {
            let mut buf = Vec::new();
            fast_write_i64(&mut buf, val);
            let expected = val.to_string();
            assert_eq!(
                buf.as_slice(),
                expected.as_bytes(),
                "fast_write_i64 mismatch for {val}"
            );

            let mut slice_buf = [0u8; 24];
            let len = fast_write_i64_slice(&mut slice_buf, val);
            assert_eq!(
                &slice_buf[..len],
                expected.as_bytes(),
                "fast_write_i64_slice mismatch for {val}"
            );
        }
    }

    #[test]
    fn test_format_u32_10digits_simd_exhaustive_sample() {
        let samples = [
            1_000_000_000u32,
            1_000_000_001,
            1_234_567_890,
            2_000_000_000,
            3_141_592_653,
            4_000_000_000,
            u32::MAX,
        ];

        for &val in &samples {
            let mut digits = [0u8; 10];
            format_u32_10digits_simd(val, &mut digits);
            assert_eq!(
                &digits,
                val.to_string().as_bytes(),
                "format_u32_10digits_simd mismatch for {val}"
            );
        }
    }
}
