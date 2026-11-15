//! RFC 7517 JSON Web Key (JWK) Set Parser and Contiguous Table Builder.
//!
//! ```text
//!                         JWKS JSON TO 8-SLOT TABLE PARSER
//!
//!   JSON Web Key Set (RFC 7517)
//!   {
//!     "keys": [
//!       { "kty": "RSA", "kid": "key-2026-1", "alg": "RS256", "n": "...", "e": "AQAB" },
//!       { "kty": "EC",  "kid": "key-2026-2", "alg": "ES256", "crv": "P-256", ... },
//!       ...
//!     ]
//!   }
//!          |
//!          | serde_json (In-Memory on Shadow Background Thread)
//!          v
//!   +-----------------------------------------------------------+
//!   | Cryptographic Conversion into VerifierKey                 |
//!   | - RSA: Base64URL(n, e) -> PKCS#1 RSAPublicKey DER         |
//!   | - EC:  Base64URL(x, y) -> SEC 1 Uncompressed (0x04||X||Y) |
//!   | - Oct: Base64URL(k)    -> HMAC Key                        |
//!   +-----------------------------------------------------------+
//!          |
//!          | Up to MAX_JWKS_SLOTS (8 keys)
//!          v
//!   +-----------------------------------------------------------+
//!   | Contiguous JwksKeyTable (64B Cache Line Aligned)          |
//!   | - kid_index: [u64; 8]                                     |
//!   | - slots: [JwksKeySlot; 8]                                 |
//!   +-----------------------------------------------------------+
//! ```
//!
//! # Architectural Invariants
//! - **Background-Only Parsing**: JSON deserialization and key DER transformations execute strictly
//!   on background worker threads, never touching the frontline traffic hot path.
//! - **Multi-Key Ingestion**: Populates up to 8 keys (current active key + recent previous valid keys)
//!   so that tokens signed during rotation transitions or on freshly scaled nodes validate seamlessly.
//! - **Zero Disk I/O**: Operates 100% in RAM buffers.

use std::str::FromStr;

use base64::prelude::*;
use ring::hmac;
use serde::Deserialize;

use crate::error::PluginError;
use crate::pre_upstream::jwt_auth::crypto::{JwtAlgorithm, VerifierKey};
use crate::pre_upstream::jwt_auth::table::{JwksKeySlot, JwksKeyTable, MAX_JWKS_SLOTS};

/// Root RFC 7517 JSON Web Key Set container.
#[derive(Debug, Deserialize)]
pub struct JwkSetJson {
    pub keys: Vec<JwkKeyJson>,
}

/// Single RFC 7517 JSON Web Key entry.
#[derive(Debug, Deserialize)]
pub struct JwkKeyJson {
    /// Key type: "RSA", "EC", or "oct".
    pub kty: String,

    /// Key identifier string.
    #[serde(default)]
    pub kid: Option<String>,

    /// Cryptographic algorithm intended for use with the key.
    #[serde(default)]
    pub alg: Option<String>,

    /// Intended key usage (e.g. "sig" for signature verification).
    #[serde(default)]
    pub r#use: Option<String>,

    // --- RSA specific parameters ---
    /// RSA modulus (Base64URL-encoded).
    #[serde(default)]
    pub n: Option<String>,

    /// RSA exponent (Base64URL-encoded).
    #[serde(default)]
    pub e: Option<String>,

    // --- ECDSA specific parameters ---
    /// EC curve name: "P-256" or "P-384".
    #[serde(default)]
    pub crv: Option<String>,

    /// EC x coordinate (Base64URL-encoded).
    #[serde(default)]
    pub x: Option<String>,

    /// EC y coordinate (Base64URL-encoded).
    #[serde(default)]
    pub y: Option<String>,

    // --- Symmetric / Oct specific parameters ---
    /// Symmetric key value (Base64URL-encoded).
    #[serde(default)]
    pub k: Option<String>,
}

/// Decodes an unpadded or padded Base64URL string into raw bytes.
fn decode_base64url(input: &str) -> Result<Vec<u8>, PluginError> {
    let clean = input.trim().trim_end_matches('=');
    if clean.is_empty() {
        return Err(PluginError::Config(
            "Empty Base64URL component in JWK".into(),
        ));
    }
    BASE64_URL_SAFE_NO_PAD
        .decode(clean)
        .map_err(|e| PluginError::Config(format!("Base64URL decode error in JWK: {e}")))
}

/// Encodes an ASN.1 DER length field into the destination buffer.
fn encode_der_length(len: usize, out: &mut Vec<u8>) {
    if len < 128 {
        out.push(len as u8);
    } else if len < 256 {
        out.push(0x81);
        out.push(len as u8);
    } else {
        out.push(0x82);
        out.push((len >> 8) as u8);
        out.push((len & 0xff) as u8);
    }
}

/// Encodes a big-endian unsigned integer as an ASN.1 DER INTEGER (`tag = 0x02`).
///
/// If the most significant bit is 1, a leading 0x00 byte is prepended to ensure
/// the value is interpreted as positive under two's-complement rules.
fn encode_der_integer(bytes: &[u8], out: &mut Vec<u8>) {
    out.push(0x02); // Tag: INTEGER
    let needs_zero_pad = !bytes.is_empty() && (bytes[0] & 0x80) != 0;
    let len = bytes.len() + if needs_zero_pad { 1 } else { 0 };
    encode_der_length(len, out);
    if needs_zero_pad {
        out.push(0x00);
    }
    out.extend_from_slice(bytes);
}

/// Encodes RSA modulus `n` and exponent `e` into PKCS#1 `RSAPublicKey` DER.
///
/// ASN.1 Structure: `SEQUENCE { modulus INTEGER, publicExponent INTEGER }`
pub fn encode_pkcs1_rsa(n_bytes: &[u8], e_bytes: &[u8]) -> Vec<u8> {
    let mut body = Vec::with_capacity(n_bytes.len() + e_bytes.len() + 16);
    encode_der_integer(n_bytes, &mut body);
    encode_der_integer(e_bytes, &mut body);

    let mut der = Vec::with_capacity(body.len() + 8);
    der.push(0x30); // Tag: SEQUENCE
    encode_der_length(body.len(), &mut der);
    der.extend_from_slice(&body);
    der
}

/// Parses a single [`JwkKeyJson`] entry into a concrete [`VerifierKey`] and associated [`JwtAlgorithm`].
pub fn parse_jwk_key(jwk: &JwkKeyJson) -> Result<(VerifierKey, JwtAlgorithm), PluginError> {
    // Check key usage if present (must be signature if specified)
    if let Some(usage) = &jwk.r#use
        && usage != "sig"
    {
        return Err(PluginError::Config(format!(
            "JWK with use '{usage}' is not supported; expected 'sig'"
        )));
    }

    match jwk.kty.to_uppercase().as_str() {
        "RSA" => {
            let n_str = jwk
                .n
                .as_deref()
                .ok_or_else(|| PluginError::Config("RSA JWK missing 'n' parameter".into()))?;
            let e_str = jwk
                .e
                .as_deref()
                .ok_or_else(|| PluginError::Config("RSA JWK missing 'e' parameter".into()))?;

            let n_bytes = decode_base64url(n_str)?;
            let e_bytes = decode_base64url(e_str)?;

            // Enforce minimum 2048-bit RSA key length (256 bytes)
            if n_bytes.len() < 256 {
                return Err(PluginError::Config(format!(
                    "RSA JWK key modulus length must be at least 2048 bits (got {} bits)",
                    n_bytes.len() * 8
                )));
            }

            let alg = if let Some(alg_str) = &jwk.alg {
                JwtAlgorithm::from_str(alg_str).map_err(|e| PluginError::Config(e.to_string()))?
            } else {
                JwtAlgorithm::RS256
            };

            if !alg.is_rsa() {
                return Err(PluginError::Config(format!(
                    "Algorithm '{}' is incompatible with RSA key type",
                    alg.as_str()
                )));
            }

            let pkcs1_der = encode_pkcs1_rsa(&n_bytes, &e_bytes);
            let expected_sig_len = n_bytes.len();

            Ok((
                VerifierKey::Rsa {
                    public_key_der: pkcs1_der,
                    expected_sig_len,
                },
                alg,
            ))
        }

        "EC" => {
            let x_str = jwk
                .x
                .as_deref()
                .ok_or_else(|| PluginError::Config("EC JWK missing 'x' parameter".into()))?;
            let y_str = jwk
                .y
                .as_deref()
                .ok_or_else(|| PluginError::Config("EC JWK missing 'y' parameter".into()))?;

            let x_bytes = decode_base64url(x_str)?;
            let y_bytes = decode_base64url(y_str)?;

            let crv = jwk.crv.as_deref().unwrap_or("P-256");
            let (alg, expected_coord_len, expected_sig_len) = match crv {
                "P-256" => (JwtAlgorithm::ES256, 32, 64),
                "P-384" => (JwtAlgorithm::ES384, 48, 96),
                other => {
                    return Err(PluginError::Config(format!(
                        "Unsupported EC curve '{other}'; supported: P-256, P-384"
                    )));
                }
            };

            if x_bytes.len() != expected_coord_len || y_bytes.len() != expected_coord_len {
                return Err(PluginError::Config(format!(
                    "EC JWK coordinate length mismatch for curve '{crv}': expected {expected_coord_len} bytes, got x={}, y={}",
                    x_bytes.len(),
                    y_bytes.len()
                )));
            }

            // Uncompressed SEC 1 point: 0x04 || X || Y
            let mut point = Vec::with_capacity(1 + x_bytes.len() + y_bytes.len());
            point.push(0x04);
            point.extend_from_slice(&x_bytes);
            point.extend_from_slice(&y_bytes);

            Ok((
                VerifierKey::Ecdsa {
                    public_key_raw: point,
                    expected_sig_len,
                },
                alg,
            ))
        }

        "OCT" => {
            let k_str = jwk
                .k
                .as_deref()
                .ok_or_else(|| PluginError::Config("oct JWK missing 'k' parameter".into()))?;
            let k_bytes = decode_base64url(k_str)?;

            let alg = if let Some(alg_str) = &jwk.alg {
                JwtAlgorithm::from_str(alg_str).map_err(|e| PluginError::Config(e.to_string()))?
            } else {
                JwtAlgorithm::HS256
            };

            if !alg.is_symmetric() {
                return Err(PluginError::Config(format!(
                    "Algorithm '{}' is incompatible with symmetric 'oct' key type",
                    alg.as_str()
                )));
            }

            let (hmac_alg, expected_sig_len) = match alg {
                JwtAlgorithm::HS256 => (hmac::HMAC_SHA256, 32),
                JwtAlgorithm::HS384 => (hmac::HMAC_SHA384, 48),
                JwtAlgorithm::HS512 => (hmac::HMAC_SHA512, 64),
                _ => unreachable!(),
            };

            Ok((
                VerifierKey::Hmac {
                    key: hmac::Key::new(hmac_alg, &k_bytes),
                    expected_sig_len,
                },
                alg,
            ))
        }

        other => Err(PluginError::Config(format!(
            "Unsupported JWK key type (kty): '{other}'"
        ))),
    }
}

/// Parses an entire JWKS JSON payload into a contiguous 8-slot [`JwksKeyTable`].
///
/// # Multi-Key Ingestion
/// - If the IdP returns 1 key, 1 key is populated.
/// - If the IdP returns multiple keys, all keys up to [`MAX_JWKS_SLOTS`] are populated.
/// - If `static_slot` is provided, Slot 0 preserves it, and dynamic keys populate slots 1..7.
///   Otherwise, dynamic keys populate slots 0..7.
pub fn parse_jwks_into_table(
    json_bytes: &[u8],
    static_slot: Option<JwksKeySlot>,
) -> Result<JwksKeyTable, PluginError> {
    let jwks: JwkSetJson = serde_json::from_slice(json_bytes)
        .map_err(|e| PluginError::Config(format!("Failed to parse JWKS JSON: {e}")))?;

    if jwks.keys.is_empty() && static_slot.is_none() {
        return Err(PluginError::Config(
            "JWKS JSON contains zero keys and no static key is configured".into(),
        ));
    }

    let mut table = JwksKeyTable::empty();
    let mut current_slot = 0;
    let mut hashes = [0u64; MAX_JWKS_SLOTS];

    // 1. If a static key slot is preserved, assign it to Slot 0
    if let Some(slot) = static_slot {
        hashes[0] = slot.raw_kid_hash;
        table.slots[0] = slot;
        table.active_count += 1;
        table.default_slot = 0;
        current_slot = 1;
    }

    // 2. Ingest up to MAX_JWKS_SLOTS keys from the JWKS JSON
    for jwk in &jwks.keys {
        if current_slot >= MAX_JWKS_SLOTS {
            break;
        }

        match parse_jwk_key(jwk) {
            Ok((key, alg)) => {
                let kid_str = jwk.kid.as_deref().unwrap_or("default");
                let internal_kid = (current_slot as u64) + 1;
                let slot = JwksKeySlot::new(internal_kid, kid_str, alg, key);

                hashes[current_slot] = slot.raw_kid_hash;
                table.slots[current_slot] = slot;
                table.active_count += 1;
                current_slot += 1;
            }
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    kid = ?jwk.kid,
                    "Skipping unparseable JWK key in JWKS response"
                );
            }
        }
    }

    if table.active_count == 0 {
        return Err(PluginError::Config(
            "No valid cryptographic keys could be extracted from JWKS response".into(),
        ));
    }

    // 3. Batch-commit all 8 key hashes (64 bytes = 1 L1D cache line) using SIMD vector write
    table.set_kid_index_batch(&hashes);

    Ok(table)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_single_rsa_jwks() {
        // 256-byte modulus base64url (2048-bit RSA)
        let n_raw = vec![0x99u8; 256];
        let n_b64 = BASE64_URL_SAFE_NO_PAD.encode(&n_raw);
        let e_b64 = BASE64_URL_SAFE_NO_PAD.encode([0x01, 0x00, 0x01]);

        let json = format!(
            r#"{{ "keys": [ {{ "kty": "RSA", "kid": "key-rsa-test", "alg": "RS256", "n": "{n_b64}", "e": "{e_b64}" }} ] }}"#
        );

        let table = parse_jwks_into_table(json.as_bytes(), None).unwrap();
        assert_eq!(table.active_count, 1);
        let found = table.find_by_raw_kid("key-rsa-test");
        assert!(found.is_some());
        assert_eq!(found.unwrap().alg, JwtAlgorithm::RS256);
    }

    #[test]
    fn test_parse_multi_key_jwks_up_to_8_slots() {
        // Build 10 keys (to test clamping at 8 slots)
        let mut keys = Vec::new();
        for i in 0..10 {
            let k_bytes = vec![(i + 1) as u8; 32];
            let k_b64 = BASE64_URL_SAFE_NO_PAD.encode(&k_bytes);
            keys.push(format!(
                r#"{{ "kty": "oct", "kid": "key-{i}", "alg": "HS256", "k": "{k_b64}" }}"#
            ));
        }
        let json = format!(r#"{{ "keys": [ {} ] }}"#, keys.join(","));

        let table = parse_jwks_into_table(json.as_bytes(), None).unwrap();
        // Clamped at exactly 8 slots
        assert_eq!(table.active_count, 8);

        // Keys 0 through 7 must be found
        for i in 0..8 {
            let kid = format!("key-{i}");
            assert!(
                table.find_by_raw_kid(&kid).is_some(),
                "Key {kid} should be in table"
            );
        }

        // Keys 8 and 9 should not be in table
        assert!(table.find_by_raw_kid("key-8").is_none());
        assert!(table.find_by_raw_kid("key-9").is_none());
    }

    #[test]
    fn test_parse_preserve_static_key_at_slot_0() {
        let static_key = VerifierKey::new(
            JwtAlgorithm::HS256,
            Some("static-secret-12345678901234567890"),
            None,
        )
        .unwrap();
        let static_slot = JwksKeySlot::new(1, "static-key", JwtAlgorithm::HS256, static_key);

        let mut keys = Vec::new();
        for i in 0..8 {
            let k_bytes = vec![(i + 10) as u8; 32];
            let k_b64 = BASE64_URL_SAFE_NO_PAD.encode(&k_bytes);
            keys.push(format!(
                r#"{{ "kty": "oct", "kid": "dynamic-{i}", "alg": "HS256", "k": "{k_b64}" }}"#
            ));
        }
        let json = format!(r#"{{ "keys": [ {} ] }}"#, keys.join(","));

        let table = parse_jwks_into_table(json.as_bytes(), Some(static_slot)).unwrap();
        assert_eq!(table.active_count, 8);

        // Slot 0 is the static key
        let static_found = table.find_by_raw_kid("static-key");
        assert!(static_found.is_some());
        assert_eq!(
            table.slots[0].raw_kid_hash,
            static_found.unwrap().raw_kid_hash
        );

        // Slots 1 through 7 hold dynamic-0 through dynamic-6
        for i in 0..7 {
            let kid = format!("dynamic-{i}");
            assert!(table.find_by_raw_kid(&kid).is_some());
        }

        // dynamic-7 was truncated because slot 0 was reserved for static key
        assert!(table.find_by_raw_kid("dynamic-7").is_none());
    }

    #[test]
    fn test_parse_ecdsa_p256_jwks() {
        let x_bytes = vec![0x11u8; 32];
        let y_bytes = vec![0x22u8; 32];
        let x_b64 = BASE64_URL_SAFE_NO_PAD.encode(&x_bytes);
        let y_b64 = BASE64_URL_SAFE_NO_PAD.encode(&y_bytes);

        let json = format!(
            r#"{{ "keys": [ {{ "kty": "EC", "kid": "ec-p256-key", "crv": "P-256", "alg": "ES256", "x": "{x_b64}", "y": "{y_b64}" }} ] }}"#
        );

        let table = parse_jwks_into_table(json.as_bytes(), None).unwrap();
        assert_eq!(table.active_count, 1);
        let found = table.find_by_raw_kid("ec-p256-key");
        assert!(found.is_some());
        assert_eq!(found.unwrap().alg, JwtAlgorithm::ES256);
    }
}
