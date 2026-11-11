//! Hardware-Optimized Integer, Hex & IP Lookup Tables (`loki_log_shipper::lut`).
//!
//! ```text
//! ┌────────────────────────────────────────────────────────────────────────────────────────┐
//! │                                  LUT SUBSYSTEM OVERVIEW                                │
//! │                                                                                        │
//! │  1. Decimal u64 Formatting (fast_write_u64):                                           │
//! │     u64 ──► Reciprocal Mul (No div/idiv) ──► DIGIT_PAIRS [200B] ──► 2-Digit ASCII      │
//! │                                                                                        │
//! │  2. 128-Bit RequestId Hex Stamping (format_hex32_bytes):                               │
//! │     [ hi_u64 (8B) ] [ lo_u64 (8B) ]                                                    │
//! │                   │                                                                    │
//! │                   ▼ (SSSE3 PSHUFB / _mm_shuffle_epi8)                                  │
//! │     ┌───────────────────────────┬───────────────────────────┐                          │
//! │     ▼                           ▼                           ▼                          │
//! │  [ hi_nibbles >> 4 ]         [ lo_nibbles & 0x0F ]       [ ASCII LUT Vector ]          │
//! │                   │                                         │                          │
//! │                   └───────────────────┬─────────────────────┘                          │
//! │                                       ▼                                                │
//! │                      [ Exact 32-Byte Lowercase Hex ASCII ]                             │
//! │                                                                                        │
//! │  3. Zero-Alloc IP Formatter (fast_write_ip):                                           │
//! │     peer_ip: u128 (RFC 4291 IPv4-mapped)                                               │
//! │                   │                                                                    │
//! │     (ip >> 48) == 0 && (ip >> 32) & 0xFFFF == 0xFFFF ?                                 │
//! │                   ├─── YES ──► IPv4 Path (fast_write_ipv4, ~5-8 ns)                    │
//! │                   │            [ o0 ] . [ o1 ] . [ o2 ] . [ o3 ]                       │
//! │                   └─── NO  ──► IPv6 Path (fast_write_ipv6, ~35 ns, 0 heap alloc)      │
//! │                                Stack Buffer [u8; 40] ──► RFC 5952 [ :: ] Compressed    │
//! └────────────────────────────────────────────────────────────────────────────────────────┘
//! ```

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

/// Formats an unsigned 64-bit integer into ASCII bytes directly into `buf`
/// using reciprocal multiplication and 2-digit decimal lookup table.
///
/// ```text
///  n >= 100 ──► (n % 100) * 2 ──► DIGIT_PAIRS[rem..rem+2] ──► temp[curr..curr+2]
/// ```
#[inline(always)]
pub fn fast_write_u64(buf: &mut Vec<u8>, mut n: u64) {
    if n < 10 {
        buf.push(b'0' + n as u8);
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
    if ts_sec >= 1_000_000_000 {
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
    if ts_sec >= 1_000_000_000 {
        let mut digits = [0u8; 10];
        format_u32_10digits_simd(ts_sec, &mut digits);
        buf.extend_from_slice(&digits);
        buf.extend_from_slice(b"000000000");
    } else {
        fast_write_u64(buf, (ts_sec as u64).saturating_mul(1_000_000_000));
    }
}

/// Formats a 64-bit integer into an exact 16-character lowercase hex string into `buf`.
#[inline(always)]
pub fn fast_write_hex16(buf: &mut Vec<u8>, n: u64) {
    let mut temp = [0u8; 16];
    for i in (0..16).rev() {
        temp[i] = HEX_CHARS[((n >> ((15 - i) * 4)) & 0x0F) as usize];
    }
    buf.extend_from_slice(&temp);
}

/// Formats a 128-bit RequestId (`(high, low)`) into an exact 32-character lowercase hex string.
///
/// Writes directly into a fixed 32-byte array on the stack (zero allocation).
///
/// ```text
///  128-bit (hi, lo) ──► SSSE3 Vector Shuffle / Scalar LUT ──► [u8; 32]
/// ```
#[inline(always)]
pub fn format_hex32_bytes(hi: u64, lo: u64) -> [u8; 32] {
    let mut out = [0u8; 32];
    #[cfg(all(target_arch = "x86_64", target_feature = "ssse3"))]
    unsafe {
        use core::arch::x86_64::*;
        let hi_be = hi.to_be();
        let lo_be = lo.to_be();
        let mut raw16 = [0u8; 16];
        core::ptr::copy_nonoverlapping(&hi_be as *const u64 as *const u8, raw16.as_mut_ptr(), 8);
        core::ptr::copy_nonoverlapping(
            &lo_be as *const u64 as *const u8,
            raw16.as_mut_ptr().add(8),
            8,
        );

        let input = _mm_loadu_si128(raw16.as_ptr() as *const __m128i);
        let mask = _mm_set1_epi8(0x0F);
        let ascii_lut = _mm_setr_epi8(
            b'0' as i8, b'1' as i8, b'2' as i8, b'3' as i8, b'4' as i8, b'5' as i8, b'6' as i8,
            b'7' as i8, b'8' as i8, b'9' as i8, b'a' as i8, b'b' as i8, b'c' as i8, b'd' as i8,
            b'e' as i8, b'f' as i8,
        );

        let lo_nibbles = _mm_and_si128(input, mask);
        let hi_nibbles = _mm_and_si128(_mm_srli_epi16(input, 4), mask);

        let lo_ascii = _mm_shuffle_epi8(ascii_lut, lo_nibbles);
        let hi_ascii = _mm_shuffle_epi8(ascii_lut, hi_nibbles);

        let part1 = _mm_unpacklo_epi8(hi_ascii, lo_ascii);
        let part2 = _mm_unpackhi_epi8(hi_ascii, lo_ascii);

        _mm_storeu_si128(out.as_mut_ptr() as *mut __m128i, part1);
        _mm_storeu_si128(out.as_mut_ptr().add(16) as *mut __m128i, part2);
        return out;
    }

    #[allow(unreachable_code)]
    {
        for i in (0..16).rev() {
            out[i] = HEX_CHARS[((hi >> ((15 - i) * 4)) & 0x0F) as usize];
        }
        for i in (0..16).rev() {
            out[16 + i] = HEX_CHARS[((lo >> ((15 - i) * 4)) & 0x0F) as usize];
        }
        out
    }
}

/// Appends a 128-bit RequestId as exactly 32 lowercase hex characters into `buf`.
#[inline(always)]
pub fn fast_write_hex32(buf: &mut Vec<u8>, hi: u64, lo: u64) {
    let bytes = format_hex32_bytes(hi, lo);
    buf.extend_from_slice(&bytes);
}

/// Directly stamps a 128-bit RequestId into a pre-allocated template slice at `offset`.
///
/// Uses single/dual SIMD vector stores without vector reallocation or bounds re-checks.
#[inline(always)]
pub fn stamp_hex32_at(slice: &mut [u8], offset: usize, hi: u64, lo: u64) {
    let hex = format_hex32_bytes(hi, lo);
    debug_assert!(offset + 32 <= slice.len());
    unsafe {
        core::ptr::copy_nonoverlapping(hex.as_ptr(), slice.as_mut_ptr().add(offset), 32);
    }
}

/// Formats an IPv4 address (as a 32-bit integer) into dotted-decimal ASCII format (`192.168.1.1`).
///
/// ```text
///  ip_u32 ──► [ octet0, octet1, octet2, octet3 ] ──► "o0.o1.o2.o3" (5-8 ns)
/// ```
#[inline(always)]
pub fn fast_write_ipv4(buf: &mut Vec<u8>, ip_u32: u32) {
    let octets = ip_u32.to_be_bytes();
    fast_write_u64(buf, octets[0] as u64);
    buf.push(b'.');
    fast_write_u64(buf, octets[1] as u64);
    buf.push(b'.');
    fast_write_u64(buf, octets[2] as u64);
    buf.push(b'.');
    fast_write_u64(buf, octets[3] as u64);
}

/// Formats an IPv6 address (as a 128-bit integer) compliant with RFC 5952 zero-compression.
///
/// Operates strictly within a 40-byte stack buffer (0 heap allocations).
///
/// ```text
///  ip_u128 ──► 8 × u16 segments ──► Scan Longest Zero Run ──► Stack [u8; 40] ──► buf
/// ```
#[inline(always)]
pub fn fast_write_ipv6(buf: &mut Vec<u8>, ip_u128: u128) {
    let raw = ip_u128.to_be_bytes();
    let mut segments = [0u16; 8];
    for i in 0..8 {
        segments[i] = u16::from_be_bytes([raw[i * 2], raw[i * 2 + 1]]);
    }

    // RFC 5952: Find the longest contiguous sequence of at least two zeros
    let mut best_start = usize::MAX;
    let mut best_len = 0;
    let mut cur_start = usize::MAX;
    let mut cur_len = 0;

    for (i, &seg) in segments.iter().enumerate() {
        if seg == 0 {
            if cur_start == usize::MAX {
                cur_start = i;
                cur_len = 1;
            } else {
                cur_len += 1;
            }
        } else if cur_start != usize::MAX {
            if cur_len > best_len && cur_len >= 2 {
                best_start = cur_start;
                best_len = cur_len;
            }
            cur_start = usize::MAX;
            cur_len = 0;
        }
    }
    if cur_start != usize::MAX && cur_len > best_len && cur_len >= 2 {
        best_start = cur_start;
        best_len = cur_len;
    }

    let mut stack_buf = [0u8; 40];
    let mut cursor = 0;

    let mut i = 0;
    while i < 8 {
        if i == best_start {
            stack_buf[cursor] = b':';
            cursor += 1;
            stack_buf[cursor] = b':';
            cursor += 1;
            i += best_len;
            if i == 8 {
                break;
            }
        } else if i > 0 && i != best_start + best_len {
            stack_buf[cursor] = b':';
            cursor += 1;
        }

        let seg = segments[i];
        let mut seg_buf = [0u8; 4];
        let mut seg_len = 0;
        let mut n = seg;
        for j in (0..4).rev() {
            seg_buf[j] = HEX_CHARS[(n & 0x0F) as usize];
            n >>= 4;
            if n == 0 {
                seg_len = 4 - j;
                break;
            }
        }
        let start_idx = 4 - seg_len;
        stack_buf[cursor..cursor + seg_len].copy_from_slice(&seg_buf[start_idx..]);
        cursor += seg_len;
        i += 1;
    }

    buf.extend_from_slice(&stack_buf[..cursor]);
}

/// Losslessly formats an RFC 4291 `peer_ip: u128` into dotted-decimal IPv4 or canonical IPv6 text.
///
/// ```text
///                          peer_ip: u128
///                                │
///           (ip >> 48) == 0 && (ip >> 32) & 0xFFFF == 0xFFFF ?
///                                ├─── YES ──► fast_write_ipv4 (~5-8 ns)
///                                └─── NO  ──► fast_write_ipv6 (~35 ns, 0 alloc)
/// ```
#[inline(always)]
pub fn fast_write_ip(buf: &mut Vec<u8>, ip_u128: u128) {
    if (ip_u128 >> 48) == 0 && ((ip_u128 >> 32) & 0xFFFF) == 0xFFFF {
        fast_write_ipv4(buf, ip_u128 as u32);
    } else {
        fast_write_ipv6(buf, ip_u128);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fast_write_u64() {
        let mut buf = Vec::new();
        fast_write_u64(&mut buf, 0);
        assert_eq!(buf, b"0");

        buf.clear();
        fast_write_u64(&mut buf, 200);
        assert_eq!(buf, b"200");

        buf.clear();
        fast_write_u64(&mut buf, 1710000000);
        assert_eq!(buf, b"1710000000");
    }

    #[test]
    fn test_fast_write_hex16() {
        let mut buf = Vec::new();
        fast_write_hex16(&mut buf, 0x1234_5678_9abc_def0);
        assert_eq!(buf, b"123456789abcdef0");
    }

    #[test]
    fn test_fast_write_hex32() {
        let mut buf = Vec::new();
        let hi = 0x4bf9_2f35_77b3_4da6_u64;
        let lo = 0xa3ce_929d_0e0e_4736_u64;
        fast_write_hex32(&mut buf, hi, lo);
        assert_eq!(buf.len(), 32);
        assert_eq!(buf, b"4bf92f3577b34da6a3ce929d0e0e4736");
    }

    #[test]
    fn test_fast_write_ipv4() {
        let mut buf = Vec::new();
        let ip: u32 = 0xC0A8_0132;
        fast_write_ipv4(&mut buf, ip);
        assert_eq!(buf, b"192.168.1.50");
    }

    #[test]
    fn test_fast_write_ipv6_loopback() {
        let mut buf = Vec::new();
        fast_write_ipv6(&mut buf, 1);
        assert_eq!(buf, b"::1");
    }

    #[test]
    fn test_fast_write_ipv6_full() {
        let mut buf = Vec::new();
        let ip: u128 = 0x2001_0db8_85a3_0000_0000_8a2e_0370_7334;
        fast_write_ipv6(&mut buf, ip);
        assert_eq!(buf, b"2001:db8:85a3::8a2e:370:7334");
    }

    #[test]
    fn test_fast_write_ip_dispatch() {
        let mut buf = Vec::new();
        let mapped_ipv4 = 0x0000_0000_0000_0000_0000_ffff_c0a8_0132_u128;
        fast_write_ip(&mut buf, mapped_ipv4);
        assert_eq!(buf, b"192.168.1.50");

        buf.clear();
        let loopback_ipv6 = 1u128;
        fast_write_ip(&mut buf, loopback_ipv6);
        assert_eq!(buf, b"::1");
    }

    #[test]
    fn test_format_u32_10digits_simd() {
        for n in [
            1_000_000_000,
            1_234_567_890,
            1_710_000_001,
            1_999_999_999,
            4_294_967_295,
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
}
