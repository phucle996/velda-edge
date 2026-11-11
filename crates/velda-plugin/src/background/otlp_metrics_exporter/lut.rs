//! Hardware-Optimized Integer & Timestamp Lookup Tables (`otlp_metrics_exporter::lut`).
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

/// Hexadecimal character lookup table.
pub const HEX_CHARS: &[u8; 16] = b"0123456789abcdef";

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

/// Formats a Unix timestamp in seconds (`ts_sec`) into ASCII bytes directly into `buf`.
///
/// Uses SIMD 10-digit decimal formatting when `ts_sec >= 1_000_000_000` (e.g. Unix timestamps
/// `1_000_000_000..=9_999_999_999`), falling back to `fast_write_u64` otherwise.
#[inline(always)]
pub fn fast_write_ts_sec(buf: &mut Vec<u8>, ts_sec: u32) {
    if (1_000_000_000..=9_999_999_999).contains(&(ts_sec as u64)) {
        let mut digits = [0u8; 10];
        format_u32_10digits_simd(ts_sec, &mut digits);
        buf.extend_from_slice(&digits);
    } else {
        fast_write_u64(buf, ts_sec as u64);
    }
}

/// Formats a Unix timestamp in nanoseconds (`ts_sec * 1_000_000_000`) into ASCII bytes directly into `buf`.
///
/// When `ts_sec >= 1_000_000_000`, formats the 10 digits of `ts_sec` via SIMD and appends 9 trailing zeros
/// in a single operation without 64-bit hardware arithmetic division.
#[inline(always)]
pub fn fast_write_ts_nano(buf: &mut Vec<u8>, ts_sec: u32) {
    if (1_000_000_000..=9_999_999_999).contains(&(ts_sec as u64)) {
        let mut digits = [0u8; 10];
        format_u32_10digits_simd(ts_sec, &mut digits);
        buf.extend_from_slice(&digits);
        buf.extend_from_slice(b"000000000");
    } else {
        fast_write_u64(buf, (ts_sec as u64).saturating_mul(1_000_000_000));
    }
}

/// Formats a 19-digit nanosecond timestamp into an exact 19-byte ASCII slice using SIMD vector instructions.
///
/// Converts high 10 digits (`sec`) and low 9 digits (`nanos % 1_000_000_000`)
/// via 128-bit SSE2 vector arithmetic without 64-bit integer division loops.
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

/// High-speed integer-to-ASCII serialization using unrolled digit paths and 2-digit lookup pairs.
#[inline(always)]
pub fn fast_write_u64(buf: &mut Vec<u8>, mut n: u64) {
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
        let rem = ((n % 100) as usize) * 2;
        let hi = (n / 100) as usize;
        if hi < 10 {
            let p0 = b'0' + hi as u8;
            let p1 = DIGIT_PAIRS[rem];
            let p2 = DIGIT_PAIRS[rem + 1];
            buf.extend_from_slice(&[p0, p1, p2]);
        } else {
            let rem_hi = hi * 2;
            let p0 = DIGIT_PAIRS[rem_hi];
            let p1 = DIGIT_PAIRS[rem_hi + 1];
            let p2 = DIGIT_PAIRS[rem];
            let p3 = DIGIT_PAIRS[rem + 1];
            buf.extend_from_slice(&[p0, p1, p2, p3]);
        }
        return;
    }
    if n < 1_000_000 {
        let rem0 = ((n % 100) as usize) * 2;
        let q0 = n / 100;
        let rem1 = ((q0 % 100) as usize) * 2;
        let hi = (q0 / 100) as usize;
        if hi < 10 {
            let p0 = b'0' + hi as u8;
            let p1 = DIGIT_PAIRS[rem1];
            let p2 = DIGIT_PAIRS[rem1 + 1];
            let p3 = DIGIT_PAIRS[rem0];
            let p4 = DIGIT_PAIRS[rem0 + 1];
            buf.extend_from_slice(&[p0, p1, p2, p3, p4]);
        } else {
            let rem_hi = hi * 2;
            let p0 = DIGIT_PAIRS[rem_hi];
            let p1 = DIGIT_PAIRS[rem_hi + 1];
            let p2 = DIGIT_PAIRS[rem1];
            let p3 = DIGIT_PAIRS[rem1 + 1];
            let p4 = DIGIT_PAIRS[rem0];
            let p5 = DIGIT_PAIRS[rem0 + 1];
            buf.extend_from_slice(&[p0, p1, p2, p3, p4, p5]);
        }
        return;
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

    buf.extend_from_slice(&temp[curr..20]);
}

/// Serializes signed 64-bit integer with negative sign check.
#[inline(always)]
pub fn fast_write_i64(buf: &mut Vec<u8>, n: i64) {
    if n < 0 {
        buf.push(b'-');
        fast_write_u64(buf, n.unsigned_abs());
    } else {
        fast_write_u64(buf, n as u64);
    }
}

/// Formats a 128-bit integer into a stack buffer without heap allocation.
#[inline(always)]
pub fn fast_format_u128_into(buf: &mut [u8; 40], mut n: u128) -> usize {
    if n < 10 {
        buf[0] = b'0' + n as u8;
        return 1;
    }

    let mut temp = [0u8; 40];
    let mut curr = 40;

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

    let len = 40 - curr;
    buf[..len].copy_from_slice(&temp[curr..40]);
    len
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_u32_10digits_simd() {
        for n in [
            1_000_000_000u32,
            1_234_567_890u32,
            1_710_000_001u32,
            1_999_999_999u32,
            4_294_967_295u32,
        ] {
            let mut out = [0u8; 10];
            format_u32_10digits_simd(n, &mut out);
            assert_eq!(std::str::from_utf8(&out).unwrap(), n.to_string());
        }
    }

    #[test]
    fn test_fast_write_ts_sec() {
        let mut buf = Vec::new();
        fast_write_ts_sec(&mut buf, 1_710_000_001);
        assert_eq!(buf, b"1710000001");

        buf.clear();
        fast_write_ts_sec(&mut buf, 42);
        assert_eq!(buf, b"42");

        buf.clear();
        fast_write_ts_sec(&mut buf, 0);
        assert_eq!(buf, b"0");
    }

    #[test]
    fn test_fast_write_ts_nano() {
        let mut buf = Vec::new();
        fast_write_ts_nano(&mut buf, 1_710_000_001);
        assert_eq!(buf, b"1710000001000000000");

        buf.clear();
        fast_write_ts_nano(&mut buf, 42);
        assert_eq!(buf, b"42000000000");
    }

    #[test]
    fn test_fast_format_ts_19digits_simd() {
        let mut out = [0u8; 19];
        let nanos = 1_710_123_456_789_012_345u128;
        fast_format_ts_19digits_simd(nanos, &mut out);
        assert_eq!(std::str::from_utf8(&out).unwrap(), "1710123456789012345");

        let nanos_zeros = 1_710_000_000_000_000_000u128;
        fast_format_ts_19digits_simd(nanos_zeros, &mut out);
        assert_eq!(std::str::from_utf8(&out).unwrap(), "1710000000000000000");

        let nanos_small_rem = 1_710_000_000_000_000_007u128;
        fast_format_ts_19digits_simd(nanos_small_rem, &mut out);
        assert_eq!(std::str::from_utf8(&out).unwrap(), "1710000000000000007");
    }

    #[test]
    fn test_fast_writers() {
        let mut buf = Vec::new();
        for val in [
            0u64,
            7,
            9,
            10,
            42,
            99,
            100,
            123,
            999,
            1000,
            1234,
            9999,
            12345,
            99999,
            123456,
            999999,
            1234567,
            u64::MAX,
        ] {
            buf.clear();
            fast_write_u64(&mut buf, val);
            assert_eq!(std::str::from_utf8(&buf).unwrap(), val.to_string());
        }

        buf.clear();
        fast_write_i64(&mut buf, -98765);
        assert_eq!(buf, b"-98765");

        buf.clear();
        fast_write_i64(&mut buf, 0);
        assert_eq!(buf, b"0");

        buf.clear();
        fast_write_i64(&mut buf, i64::MIN);
        assert_eq!(std::str::from_utf8(&buf).unwrap(), i64::MIN.to_string());

        let mut stack = [0u8; 40];
        let len = fast_format_u128_into(&mut stack, 1700000000000000000u128);
        assert_eq!(&stack[..len], b"1700000000000000000");
    }
}
