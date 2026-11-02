//! Cryptographic Stateless Token Engine for CSRF Protection.
//!
//! Owns the 114-byte signed stateless token wire format, branchless hexadecimal decoding,
//! HMAC-SHA256 signature generation/verification, and 128-bit SIMD constant-time comparisons.
//!
//! # 114-Byte Stateless Token Layout
//! ```text
//! ┌───────────────────────┬───┬─────────────────────────────────┬───┬──────────────────────────────────────────────────────────────────┐
//! │ Timestamp Hex (16 B)  │ . │        Nonce Hex (32 B)         │ . │                     HMAC-SHA256 Hex (64 B)                       │
//! └───────────────────────┴───┴─────────────────────────────────┴───┴──────────────────────────────────────────────────────────────────┘
//! Total Length: 16 + 1 + 32 + 1 + 64 = 114 Bytes
//! ```

use ring::hmac;

/// Compile-time 256-byte branchless hexadecimal lookup table.
/// Maps ASCII '0'..='9', 'a'..='f', 'A'..='F' to 0..=15; all other bytes map to 0xFF.
pub const HEX_LUT: [u8; 256] = {
    let mut lut = [0xFFu8; 256];
    let mut i = 0u8;
    loop {
        if i >= b'0' && i <= b'9' {
            lut[i as usize] = i - b'0';
        } else if i >= b'a' && i <= b'f' {
            lut[i as usize] = i - b'a' + 10;
        } else if i >= b'A' && i <= b'F' {
            lut[i as usize] = i - b'A' + 10;
        }
        if i == 255 {
            break;
        }
        i += 1;
    }
    lut
};

/// Signature for register-width-specific 64-byte constant-time signature comparison.
pub type ConstantTimeEq64Fn = fn(a: &[u8; 64], b: &[u8; 64]) -> bool;

/// Pre-resolved static function pointer for 64-byte HMAC signature constant-time comparison.
pub static CONSTANT_TIME_EQ_64_FN: std::sync::LazyLock<ConstantTimeEq64Fn> =
    std::sync::LazyLock::new(|| {
        let caps = velda_core::global_hardware_topology()
            .cpu
            .register_capabilities;
        match caps.select_for_workload(velda_core::hardware::RegisterWidth::Bits256) {
            velda_core::hardware::RegisterWidth::Bits256 => constant_time_eq_64_256,
            velda_core::hardware::RegisterWidth::Bits128 => constant_time_eq_64_128,
            velda_core::hardware::RegisterWidth::Bits64 => constant_time_eq_64_64bit,
            velda_core::hardware::RegisterWidth::Bits32 => constant_time_eq_64_32bit,
        }
    });

/// Branchless 64-byte constant-time comparison using hardware vector registers (up to 256-bit max).
/// Eliminates all loop counters and branch mispredictions.
#[inline(always)]
pub fn constant_time_eq_64(a: &[u8; 64], b: &[u8; 64]) -> bool {
    (*CONSTANT_TIME_EQ_64_FN)(a, b)
}

/// 64-byte constant-time comparison using two 256-bit vector registers.
#[inline]
pub fn constant_time_eq_64_256(a: &[u8; 64], b: &[u8; 64]) -> bool {
    #[cfg(target_arch = "x86_64")]
    {
        unsafe { constant_time_eq_64_256_x86_impl(a, b) }
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        constant_time_eq_64_128(a, b)
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn constant_time_eq_64_256_x86_impl(a: &[u8; 64], b: &[u8; 64]) -> bool {
    use std::arch::x86_64::*;
    unsafe {
        let a0 = _mm256_loadu_si256(a.as_ptr() as *const __m256i);
        let b0 = _mm256_loadu_si256(b.as_ptr() as *const __m256i);
        let a1 = _mm256_loadu_si256(a.as_ptr().add(32) as *const __m256i);
        let b1 = _mm256_loadu_si256(b.as_ptr().add(32) as *const __m256i);
        let diff = _mm256_or_si256(_mm256_xor_si256(a0, b0), _mm256_xor_si256(a1, b1));
        _mm256_testz_si256(diff, diff) != 0
    }
}

/// 64-byte constant-time comparison using four 128-bit vector registers.
#[inline]
pub fn constant_time_eq_64_128(a: &[u8; 64], b: &[u8; 64]) -> bool {
    #[cfg(target_arch = "x86_64")]
    {
        unsafe { constant_time_eq_64_128_x86_impl(a, b) }
    }
    #[cfg(target_arch = "aarch64")]
    {
        unsafe { constant_time_eq_64_128_neon_impl(a, b) }
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        constant_time_eq_64_64bit(a, b)
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse2")]
unsafe fn constant_time_eq_64_128_x86_impl(a: &[u8; 64], b: &[u8; 64]) -> bool {
    use std::arch::x86_64::*;
    unsafe {
        let d0 = _mm_xor_si128(
            _mm_loadu_si128(a.as_ptr() as *const __m128i),
            _mm_loadu_si128(b.as_ptr() as *const __m128i),
        );
        let d1 = _mm_xor_si128(
            _mm_loadu_si128(a.as_ptr().add(16) as *const __m128i),
            _mm_loadu_si128(b.as_ptr().add(16) as *const __m128i),
        );
        let d2 = _mm_xor_si128(
            _mm_loadu_si128(a.as_ptr().add(32) as *const __m128i),
            _mm_loadu_si128(b.as_ptr().add(32) as *const __m128i),
        );
        let d3 = _mm_xor_si128(
            _mm_loadu_si128(a.as_ptr().add(48) as *const __m128i),
            _mm_loadu_si128(b.as_ptr().add(48) as *const __m128i),
        );
        let acc = _mm_or_si128(_mm_or_si128(d0, d1), _mm_or_si128(d2, d3));
        let cmp = _mm_cmpeq_epi8(acc, _mm_setzero_si128());
        _mm_movemask_epi8(cmp) == 0xFFFF
    }
}

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn constant_time_eq_64_128_neon_impl(a: &[u8; 64], b: &[u8; 64]) -> bool {
    use std::arch::aarch64::*;
    unsafe {
        let d0 = veorq_u8(vld1q_u8(a.as_ptr()), vld1q_u8(b.as_ptr()));
        let d1 = veorq_u8(vld1q_u8(a.as_ptr().add(16)), vld1q_u8(b.as_ptr().add(16)));
        let d2 = veorq_u8(vld1q_u8(a.as_ptr().add(32)), vld1q_u8(b.as_ptr().add(32)));
        let d3 = veorq_u8(vld1q_u8(a.as_ptr().add(48)), vld1q_u8(b.as_ptr().add(48)));
        let acc = vorrq_u8(vorrq_u8(d0, d1), vorrq_u8(d2, d3));
        vmaxvq_u8(acc) == 0
    }
}

/// 64-byte constant-time comparison using eight 64-bit scalar registers.
#[inline]
pub fn constant_time_eq_64_64bit(a: &[u8; 64], b: &[u8; 64]) -> bool {
    let mut diff = 0u64;
    for i in (0..64).step_by(8) {
        let wa = u64::from_ne_bytes(a[i..i + 8].try_into().unwrap());
        let wb = u64::from_ne_bytes(b[i..i + 8].try_into().unwrap());
        diff |= wa ^ wb;
    }
    diff == 0
}

/// 64-byte constant-time comparison using sixteen 32-bit scalar registers.
#[inline]
pub fn constant_time_eq_64_32bit(a: &[u8; 64], b: &[u8; 64]) -> bool {
    let mut diff = 0u32;
    for i in (0..64).step_by(4) {
        let wa = u32::from_ne_bytes(a[i..i + 4].try_into().unwrap());
        let wb = u32::from_ne_bytes(b[i..i + 4].try_into().unwrap());
        diff |= wa ^ wb;
    }
    diff == 0
}

/// Constant-time byte slice comparison using hardware vector registers (up to 256-bit max)
/// to eliminate timing side-channel attacks.
#[inline]
pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    crate::pre_upstream::hmac_auth::canonical::constant_time_eq(a, b)
}

/// Generates a valid 114-byte HMAC-SHA256 signed stateless token (`<timestamp_hex>.<nonce_hex>.<hmac_hex>`).
pub fn mint_token(key: &hmac::Key, nonce: &[u8; 16], now_secs: u64) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut buf = [0u8; 114];

    // 1. Timestamp (16 hex chars)
    for i in (0..16).rev() {
        let shift = (15 - i) * 4;
        buf[i] = HEX[((now_secs >> shift) & 0x0f) as usize];
    }
    buf[16] = b'.';

    // 2. Nonce (32 hex chars)
    for (i, &b) in nonce.iter().enumerate() {
        buf[17 + i * 2] = HEX[(b >> 4) as usize];
        buf[17 + i * 2 + 1] = HEX[(b & 0x0f) as usize];
    }
    buf[49] = b'.';

    // 3. HMAC-SHA256 over "<timestamp_hex>.<nonce_hex>" (49 bytes)
    let tag = hmac::sign(key, &buf[0..49]);
    for (i, &b) in tag.as_ref().iter().enumerate() {
        buf[50 + i * 2] = HEX[(b >> 4) as usize];
        buf[50 + i * 2 + 1] = HEX[(b & 0x0f) as usize];
    }

    std::str::from_utf8(&buf)
        .expect("Valid UTF-8 hex token")
        .to_string()
}

/// Verifies a signed stateless token on the stack with zero heap allocation and branchless hex decoding.
#[inline]
pub fn verify_signed_token(key: &hmac::Key, token: &[u8], ttl_secs: u64, now_secs: u64) -> bool {
    // Strict format check: 16 (ts) + 1 (.) + 32 (nonce) + 1 (.) + 64 (sig) = 114 bytes
    if token.len() != 114 || token[16] != b'.' || token[49] != b'.' {
        return false;
    }

    // Branchless 16-hex timestamp decoding via compile-time HEX_LUT
    let mut timestamp = 0u64;
    let mut invalid = 0u8;
    for &b in &token[0..16] {
        let val = HEX_LUT[b as usize];
        invalid |= val & 0xF0;
        timestamp = (timestamp << 4) | (val as u64);
    }
    if invalid != 0 {
        return false;
    }

    // TTL validation with 60-second future skew tolerance
    if timestamp > now_secs + 60 || now_secs.saturating_sub(timestamp) > ttl_secs {
        return false;
    }

    // Compute expected HMAC-SHA256 signature
    let expected_tag = hmac::sign(key, &token[0..49]);
    const HEX_CHARS: &[u8; 16] = b"0123456789abcdef";
    let mut expected_hex = [0u8; 64];
    for (i, &b) in expected_tag.as_ref().iter().enumerate() {
        expected_hex[i * 2] = HEX_CHARS[(b >> 4) as usize];
        expected_hex[i * 2 + 1] = HEX_CHARS[(b & 0x0f) as usize];
    }

    // Branchless 64-byte 128-bit SIMD constant-time signature comparison
    let token_sig: &[u8; 64] = match token[50..114].try_into() {
        Ok(s) => s,
        Err(_) => return false,
    };
    constant_time_eq_64(token_sig, &expected_hex)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mint_and_verify_token_roundtrip() {
        let secret = "sec_testing_csrf_secret_12345678";
        let key = hmac::Key::new(hmac::HMAC_SHA256, secret.as_bytes());
        let nonce = [0x5au8; 16];
        let now = 1_700_000_000u64;

        let token_str = mint_token(&key, &nonce, now);
        assert_eq!(token_str.len(), 114);

        // Valid token passes
        assert!(verify_signed_token(
            &key,
            token_str.as_bytes(),
            3600,
            now + 100
        ));

        // Tampered signature fails
        let mut tampered = token_str.as_bytes().to_vec();
        tampered[113] ^= 0x01;
        assert!(!verify_signed_token(&key, &tampered, 3600, now + 100));

        // Expired token fails
        assert!(!verify_signed_token(
            &key,
            token_str.as_bytes(),
            3600,
            now + 3601
        ));

        // Future clock skew beyond 60s fails
        assert!(!verify_signed_token(
            &key,
            token_str.as_bytes(),
            3600,
            now.saturating_sub(61)
        ));
    }

    #[test]
    fn test_hex_lut_mapping() {
        for b in b'0'..=b'9' {
            assert_eq!(HEX_LUT[b as usize], b - b'0');
        }
        for b in b'a'..=b'f' {
            assert_eq!(HEX_LUT[b as usize], b - b'a' + 10);
        }
        for b in b'A'..=b'F' {
            assert_eq!(HEX_LUT[b as usize], b - b'A' + 10);
        }
        assert_eq!(HEX_LUT[b'g' as usize], 0xFF);
        assert_eq!(HEX_LUT[b'Z' as usize], 0xFF);
        assert_eq!(HEX_LUT[b' ' as usize], 0xFF);
    }

    #[test]
    fn test_constant_time_eq_64_all_tiers_equivalence() {
        let a = [0x42u8; 64];
        let b = [0x42u8; 64];
        let mut c = [0x42u8; 64];
        c[63] = 0x43;

        for eq_fn in [
            constant_time_eq_64,
            constant_time_eq_64_256,
            constant_time_eq_64_128,
            constant_time_eq_64_64bit,
            constant_time_eq_64_32bit,
        ] {
            assert!(eq_fn(&a, &b));
            assert!(!eq_fn(&a, &c));
        }
    }
}
