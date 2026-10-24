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

/// Branchless 64-byte constant-time comparison utilizing four 128-bit vector words.
/// Eliminates all loop counters and branch mispredictions.
#[inline(always)]
pub fn constant_time_eq_64(a: &[u8; 64], b: &[u8; 64]) -> bool {
    let w0 = u128::from_ne_bytes(a[0..16].try_into().unwrap())
        ^ u128::from_ne_bytes(b[0..16].try_into().unwrap());
    let w1 = u128::from_ne_bytes(a[16..32].try_into().unwrap())
        ^ u128::from_ne_bytes(b[16..32].try_into().unwrap());
    let w2 = u128::from_ne_bytes(a[32..48].try_into().unwrap())
        ^ u128::from_ne_bytes(b[32..48].try_into().unwrap());
    let w3 = u128::from_ne_bytes(a[48..64].try_into().unwrap())
        ^ u128::from_ne_bytes(b[48..64].try_into().unwrap());

    (w0 | w1 | w2 | w3) == 0
}

/// Constant-time byte slice comparison using 128-bit vector words to eliminate timing
/// side-channel attacks while vectorizing directly into SIMD instructions on modern CPUs.
#[inline]
pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u128;
    let (chunks_a, remainder_a) = a.as_chunks::<16>();
    let (chunks_b, remainder_b) = b.as_chunks::<16>();

    for (ca, cb) in chunks_a.iter().zip(chunks_b.iter()) {
        let val_a = u128::from_ne_bytes(*ca);
        let val_b = u128::from_ne_bytes(*cb);
        diff |= val_a ^ val_b;
    }

    let mut rem_diff = 0u8;
    for (ra, rb) in remainder_a.iter().zip(remainder_b.iter()) {
        rem_diff |= ra ^ rb;
    }

    diff == 0 && rem_diff == 0
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
}
