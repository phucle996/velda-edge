//! Cryptographic JWT Verification and Pre-Compiled Claim Extraction (`jwt_auth::crypto`).
//!
//! ```text
//!                          JWT VERIFICATION PIPELINE
//!
//!   Inbound JWT String: [ header_b64 ] . [ payload_b64 ] . [ signature_b64 ]
//!                                |                 |                 |
//!     +--------------------------+                 |                 |
//!     | Stack base64url ([0u8; 512])               |                 |
//!     v                                            v                 |
//!   JoseHeader<'_> (zero-alloc borrow)       Claims Ingest           |
//!   - alg: "HS*" / "RS*" / "ES*" / "EdDSA"         |                 |
//!   - kid: Option<&str>                            |                 |
//!             |                                    |                 |
//!             v                                    |                 |
//!   +-------------------------+                    |                 |
//!   | JwksKeyTable::resolve   |                    |                 |
//!   | [kid_index: 8x u64]     |                    |                 |
//!   | (Single L1D Cache Line) |                    |                 |
//!   +-------------------------+                    |                 |
//!             |                                    |                 |
//!             +-----> Target Slot Key              |                 |
//!                             |                    |                 |
//!                             v                    v                 v
//!                   +----------------------------------------------------+
//!                   | Constant-Time Verify (HMAC / RSA / ECDSA / Ed25519)|
//!                   +----------------------------------------------------+
//!                                          |
//!                                          v
//!                      VerifiedToken (exp + Optional Arc<[(HeaderKey, HeaderValue)]>)
//! ```

use std::borrow::Cow;
use std::str::FromStr;
use std::sync::Arc;

use base64::prelude::*;
use http::header::HeaderValue;
use ring::hmac;
use ring::signature::{self, UnparsedPublicKey};
use serde::{Deserialize, Serialize};
use velda_core::HeaderKey;

use super::claims::{ClaimPath, eval_jwt_payload_streaming};
use super::pem::{extract_raw_public_key, extract_rsa_modulus_len, parse_pem_to_der};
use super::store::{JwksKeyTable, hash_kid_salted};
use crate::error::PluginError;

/// Sentinel `u4` value reserved for unsupported/missing JWT algorithms (including `"none"`).
pub const ALG_INVALID: u8 = 0x0F;

/// Cryptographic algorithms supported for JWT signature verification, packed as `u4` (`0..=8`).
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum JwtAlgorithm {
    HS256 = 0,
    HS384 = 1,
    HS512 = 2,
    RS256 = 3,
    RS384 = 4,
    RS512 = 5,
    ES256 = 6,
    ES384 = 7,
    EdDSA = 8,
}

impl JwtAlgorithm {
    /// Converts this algorithm to its compact 4-bit integer code (`0..=8`).
    #[inline(always)]
    pub const fn to_u4(self) -> u8 {
        self as u8
    }

    /// Decodes a 4-bit algorithm code (`0..=8`) back to [`JwtAlgorithm`].
    #[inline(always)]
    pub const fn from_u4(code: u8) -> Option<Self> {
        match code {
            0 => Some(Self::HS256),
            1 => Some(Self::HS384),
            2 => Some(Self::HS512),
            3 => Some(Self::RS256),
            4 => Some(Self::RS384),
            5 => Some(Self::RS512),
            6 => Some(Self::ES256),
            7 => Some(Self::ES384),
            8 => Some(Self::EdDSA),
            _ => None,
        }
    }

    /// Parses a JOSE `alg` header string straight to its `u4` integer (`0..=8`), returning `None`
    /// for `"none"` or unsupported algorithms.
    #[inline(always)]
    pub fn parse_alg_u4(s: &str) -> Option<u8> {
        match s {
            "HS256" => Some(0),
            "HS384" => Some(1),
            "HS512" => Some(2),
            "RS256" => Some(3),
            "RS384" => Some(4),
            "RS512" => Some(5),
            "ES256" => Some(6),
            "ES384" => Some(7),
            "EdDSA" => Some(8),
            _ => None,
        }
    }

    /// Returns the canonical RFC 7518 / RFC 8037 algorithm identifier.
    #[inline(always)]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::HS256 => "HS256",
            Self::HS384 => "HS384",
            Self::HS512 => "HS512",
            Self::RS256 => "RS256",
            Self::RS384 => "RS384",
            Self::RS512 => "RS512",
            Self::ES256 => "ES256",
            Self::ES384 => "ES384",
            Self::EdDSA => "EdDSA",
        }
    }

    /// Returns `true` if this is a symmetric HMAC algorithm.
    #[inline(always)]
    pub const fn is_symmetric(&self) -> bool {
        matches!(self, Self::HS256 | Self::HS384 | Self::HS512)
    }

    /// Returns `true` if this is an RSA PKCS#1 v1.5 algorithm.
    #[inline(always)]
    pub const fn is_rsa(&self) -> bool {
        matches!(self, Self::RS256 | Self::RS384 | Self::RS512)
    }

    /// Returns expected fixed signature length in bytes for symmetric, ECDSA, and Ed25519 algorithms.
    #[inline(always)]
    pub const fn fixed_signature_len(&self) -> Option<usize> {
        match self {
            Self::HS256 => Some(32),
            Self::HS384 => Some(48),
            Self::HS512 => Some(64),
            Self::ES256 => Some(64),
            Self::ES384 => Some(96),
            Self::EdDSA => Some(64),
            Self::RS256 | Self::RS384 | Self::RS512 => None,
        }
    }
}

impl FromStr for JwtAlgorithm {
    type Err = PluginError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_uppercase().as_str() {
            "HS256" => Ok(Self::HS256),
            "HS384" => Ok(Self::HS384),
            "HS512" => Ok(Self::HS512),
            "RS256" => Ok(Self::RS256),
            "RS384" => Ok(Self::RS384),
            "RS512" => Ok(Self::RS512),
            "ES256" => Ok(Self::ES256),
            "ES384" => Ok(Self::ES384),
            "EDDSA" => Ok(Self::EdDSA),
            other => Err(PluginError::Config(format!(
                "Unsupported JWT algorithm '{other}'. Supported: HS256, HS384, HS512, RS256, RS384, RS512, ES256, ES384, EdDSA"
            ))),
        }
    }
}

/// Typed, zero-allocation verification failure taxonomy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JwtVerifyError {
    MalformedToken,
    AlgorithmMismatch,
    NoActiveKeys,
    UnknownKid,
    SignatureLengthMismatch,
    InvalidSignature,
    MissingExp,
    TokenExpired,
    InvalidNumericDate,
    TokenNotBefore,
    IssuerMismatch,
    AudienceMismatch,
    ClaimAssertionFailed,
}

// ============================================================================
// Zero-Allocation Base64URL Decoders
// ============================================================================

/// Decodes base64url-encoded bytes directly into a caller-provided stack buffer without heap allocation.
#[inline]
pub fn decode_base64url_to_slice<'a>(
    s: &str,
    buf: &'a mut [u8],
) -> Result<&'a [u8], JwtVerifyError> {
    let s_clean = s.trim();
    if s_clean.is_empty() {
        return Err(JwtVerifyError::MalformedToken);
    }
    let len = if s_clean.ends_with('=') {
        BASE64_URL_SAFE
            .decode_slice(s_clean.as_bytes(), buf)
            .map_err(|_| JwtVerifyError::MalformedToken)?
    } else {
        BASE64_URL_SAFE_NO_PAD
            .decode_slice(s_clean.as_bytes(), buf)
            .map_err(|_| JwtVerifyError::MalformedToken)?
    };
    Ok(&buf[..len])
}

/// Fallback base64url decoder allocating a [`Vec<u8>`] for oversized token payloads (`> 2 KiB`).
pub fn decode_base64url(s: &str) -> Result<Vec<u8>, JwtVerifyError> {
    let s_clean = s.trim();
    if s_clean.is_empty() {
        return Err(JwtVerifyError::MalformedToken);
    }
    if s_clean.ends_with('=') {
        BASE64_URL_SAFE
            .decode(s_clean)
            .map_err(|_| JwtVerifyError::MalformedToken)
    } else {
        BASE64_URL_SAFE_NO_PAD
            .decode(s_clean)
            .map_err(|_| JwtVerifyError::MalformedToken)
    }
}

// ============================================================================
// Verifier Key with Pre-computed Geometry
// ============================================================================

/// Pre-parsed cryptographic verification key with pre-computed signature geometry.
#[derive(Clone)]
pub enum VerifierKey {
    Hmac {
        key: hmac::Key,
        raw_secret: Vec<u8>,
        expected_sig_len: usize,
    },
    Rsa {
        public_key_der: Vec<u8>,
        expected_sig_len: usize,
    },
    Ecdsa {
        public_key_raw: Vec<u8>,
        expected_sig_len: usize,
    },
    Ed25519 {
        public_key_raw: [u8; 32],
    },
}

impl VerifierKey {
    /// Constructs an HMAC verifier key from raw secret bytes and pinned symmetric algorithm.
    pub fn from_hmac_bytes(alg: JwtAlgorithm, secret: &[u8]) -> Result<Self, PluginError> {
        let (hmac_alg, expected_sig_len) = match alg {
            JwtAlgorithm::HS256 => (hmac::HMAC_SHA256, 32),
            JwtAlgorithm::HS384 => (hmac::HMAC_SHA384, 48),
            JwtAlgorithm::HS512 => (hmac::HMAC_SHA512, 64),
            other => {
                return Err(PluginError::Config(format!(
                    "Algorithm '{}' is not a symmetric HMAC algorithm",
                    other.as_str()
                )));
            }
        };
        if secret.is_empty() {
            return Err(PluginError::Config(
                "HMAC secret bytes cannot be empty".into(),
            ));
        }
        Ok(Self::Hmac {
            key: hmac::Key::new(hmac_alg, secret),
            raw_secret: secret.to_vec(),
            expected_sig_len,
        })
    }

    /// Constructs and pre-validates a verifier key from configuration inputs.
    pub fn new(
        alg: JwtAlgorithm,
        secret_key: Option<&str>,
        public_key_pem: Option<&str>,
    ) -> Result<Self, PluginError> {
        if alg.is_symmetric() {
            let secret = secret_key.filter(|s| !s.trim().is_empty()).ok_or_else(|| {
                PluginError::Config(format!(
                    "HMAC algorithm '{}' requires 'secret_key' to be configured",
                    alg.as_str()
                ))
            })?;
            Self::from_hmac_bytes(alg, secret.as_bytes())
        } else if alg.is_rsa() {
            let pem = public_key_pem
                .filter(|s| !s.trim().is_empty())
                .ok_or_else(|| {
                    PluginError::Config(format!(
                        "RSA algorithm '{}' requires 'public_key_pem' to be configured",
                        alg.as_str()
                    ))
                })?;
            let der = parse_pem_to_der(pem)?;
            let pkcs1_der = extract_raw_public_key(&der, true)?;
            let modulus_len = extract_rsa_modulus_len(&pkcs1_der).ok_or_else(|| {
                PluginError::Config("Failed to parse RSA modulus from public key".into())
            })?;

            if modulus_len < 256 {
                return Err(PluginError::Config(format!(
                    "RSA key modulus length must be at least 2048 bits (got {} bits)",
                    modulus_len * 8
                )));
            }

            Ok(Self::Rsa {
                public_key_der: pkcs1_der,
                expected_sig_len: modulus_len,
            })
        } else if alg == JwtAlgorithm::EdDSA {
            let pem = public_key_pem
                .filter(|s| !s.trim().is_empty())
                .ok_or_else(|| {
                    PluginError::Config(
                        "EdDSA algorithm requires 'public_key_pem' to be configured".into(),
                    )
                })?;
            let der = parse_pem_to_der(pem)?;
            let raw = extract_raw_public_key(&der, false)?;
            let public_key_raw: [u8; 32] = raw.try_into().map_err(|v: Vec<u8>| {
                PluginError::Config(format!(
                    "EdDSA requires a 32-byte Ed25519 public key (got {} bytes)",
                    v.len()
                ))
            })?;
            Ok(Self::Ed25519 { public_key_raw })
        } else {
            let pem = public_key_pem
                .filter(|s| !s.trim().is_empty())
                .ok_or_else(|| {
                    PluginError::Config(format!(
                        "ECDSA algorithm '{}' requires 'public_key_pem' to be configured",
                        alg.as_str()
                    ))
                })?;
            let der = parse_pem_to_der(pem)?;
            let raw_ec = extract_raw_public_key(&der, false)?;

            let expected_sig_len = match alg {
                JwtAlgorithm::ES256 => {
                    if raw_ec.len() != 65 || raw_ec[0] != 0x04 {
                        return Err(PluginError::Config(
                            "ES256 requires P-256 public key (65 bytes uncompressed 0x04||X||Y)"
                                .into(),
                        ));
                    }
                    64
                }
                JwtAlgorithm::ES384 => {
                    if raw_ec.len() != 97 || raw_ec[0] != 0x04 {
                        return Err(PluginError::Config(
                            "ES384 requires P-384 public key (97 bytes uncompressed 0x04||X||Y)"
                                .into(),
                        ));
                    }
                    96
                }
                _ => unreachable!(),
            };

            Ok(Self::Ecdsa {
                public_key_raw: raw_ec,
                expected_sig_len,
            })
        }
    }

    /// Returns raw cryptographic bytes stored in the contiguous Key Byte Arena.
    #[inline(always)]
    pub fn raw_bytes(&self) -> &[u8] {
        match self {
            Self::Hmac { raw_secret, .. } => raw_secret,
            Self::Rsa { public_key_der, .. } => public_key_der,
            Self::Ecdsa { public_key_raw, .. } => public_key_raw,
            Self::Ed25519 { public_key_raw } => public_key_raw,
        }
    }

    /// Verifies that this cryptographic key material matches `alg`.
    #[inline(always)]
    pub fn is_compatible_with(&self, alg: JwtAlgorithm) -> bool {
        match (self, alg) {
            (Self::Hmac { .. }, _) => alg.is_symmetric(),
            (Self::Rsa { .. }, _) => alg.is_rsa(),
            (
                Self::Ecdsa {
                    expected_sig_len, ..
                },
                JwtAlgorithm::ES256,
            ) => *expected_sig_len == 64,
            (
                Self::Ecdsa {
                    expected_sig_len, ..
                },
                JwtAlgorithm::ES384,
            ) => *expected_sig_len == 96,
            (Self::Ed25519 { .. }, JwtAlgorithm::EdDSA) => true,
            _ => false,
        }
    }

    /// Expected signature length in bytes for fail-fast geometry checks before crypto math.
    #[inline(always)]
    pub const fn expected_sig_len(&self) -> usize {
        match self {
            Self::Hmac {
                expected_sig_len, ..
            }
            | Self::Rsa {
                expected_sig_len, ..
            }
            | Self::Ecdsa {
                expected_sig_len, ..
            } => *expected_sig_len,
            Self::Ed25519 { .. } => 64,
        }
    }

    #[inline]
    pub fn verify(
        &self,
        alg: JwtAlgorithm,
        signing_input: &[u8],
        signature_bytes: &[u8],
    ) -> Result<(), JwtVerifyError> {
        let hmac_opt = match self {
            Self::Hmac { key, .. } => Some(key),
            _ => None,
        };
        verify_slot_signature(
            alg,
            self.raw_bytes(),
            hmac_opt,
            self.expected_sig_len(),
            signing_input,
            signature_bytes,
        )
    }
}

/// Constant-time signature verification directly over L3 Key Byte Arena bytes or HMAC key handle.
#[inline]
pub fn verify_slot_signature(
    alg: JwtAlgorithm,
    key_bytes: &[u8],
    hmac_key: Option<&hmac::Key>,
    expected_sig_len: usize,
    signing_input: &[u8],
    signature_bytes: &[u8],
) -> Result<(), JwtVerifyError> {
    if signature_bytes.len() != expected_sig_len {
        return Err(JwtVerifyError::SignatureLengthMismatch);
    }

    match alg {
        JwtAlgorithm::HS256 | JwtAlgorithm::HS384 | JwtAlgorithm::HS512 => {
            if let Some(key) = hmac_key {
                hmac::verify(key, signing_input, signature_bytes)
                    .map_err(|_| JwtVerifyError::InvalidSignature)
            } else {
                let ring_alg = match alg {
                    JwtAlgorithm::HS256 => hmac::HMAC_SHA256,
                    JwtAlgorithm::HS384 => hmac::HMAC_SHA384,
                    JwtAlgorithm::HS512 => hmac::HMAC_SHA512,
                    _ => unreachable!(),
                };
                hmac::verify(
                    &hmac::Key::new(ring_alg, key_bytes),
                    signing_input,
                    signature_bytes,
                )
                .map_err(|_| JwtVerifyError::InvalidSignature)
            }
        }
        JwtAlgorithm::RS256 => {
            UnparsedPublicKey::new(&signature::RSA_PKCS1_2048_8192_SHA256, key_bytes)
                .verify(signing_input, signature_bytes)
                .map_err(|_| JwtVerifyError::InvalidSignature)
        }
        JwtAlgorithm::RS384 => {
            UnparsedPublicKey::new(&signature::RSA_PKCS1_2048_8192_SHA384, key_bytes)
                .verify(signing_input, signature_bytes)
                .map_err(|_| JwtVerifyError::InvalidSignature)
        }
        JwtAlgorithm::RS512 => {
            UnparsedPublicKey::new(&signature::RSA_PKCS1_2048_8192_SHA512, key_bytes)
                .verify(signing_input, signature_bytes)
                .map_err(|_| JwtVerifyError::InvalidSignature)
        }
        JwtAlgorithm::ES256 => {
            UnparsedPublicKey::new(&signature::ECDSA_P256_SHA256_FIXED, key_bytes)
                .verify(signing_input, signature_bytes)
                .map_err(|_| JwtVerifyError::InvalidSignature)
        }
        JwtAlgorithm::ES384 => {
            UnparsedPublicKey::new(&signature::ECDSA_P384_SHA384_FIXED, key_bytes)
                .verify(signing_input, signature_bytes)
                .map_err(|_| JwtVerifyError::InvalidSignature)
        }
        JwtAlgorithm::EdDSA => UnparsedPublicKey::new(&signature::ED25519, key_bytes)
            .verify(signing_input, signature_bytes)
            .map_err(|_| JwtVerifyError::InvalidSignature),
    }
}

/// Validated token result containing expiration, owning key-slot cache tag, and optional headers.
pub struct VerifiedToken {
    pub exp_epoch_secs: u64,
    pub key_tag: u64,
    pub injected_headers: Option<Arc<[(HeaderKey, HeaderValue)]>>,
}

/// Configuration policies and hardware key store table for JWT verification.
pub struct JwtValidationPolicy<'a> {
    pub boot_salt: &'a [u8; 32],
    pub key_table: &'a JwksKeyTable,
    pub validate_exp: bool,
    pub clock_skew_secs: u64,
    pub expected_issuer: Option<&'a str>,
    pub expected_audience: Option<&'a str>,
    pub claims_to_headers: &'a [(ClaimPath, HeaderKey)],
    pub assert_claims: &'a [(ClaimPath, Box<str>)],
}

/// Borrowed JOSE header representation for zero-allocation stack parsing.
#[derive(Deserialize)]
struct JoseHeader<'a> {
    alg: &'a str,
    #[serde(default)]
    kid: Option<&'a str>,
}

/// Performs full validation of a compact JWT string against L1D/L3 key table and business policies.
///
/// Intentionally NOT annotated with `#[cold]`: when operators disable caching (`cache_ttl_secs: 0`),
/// every request enters `verify_jwt` on the hot path, and `#[cold]` causes LLVM to optimize for size
/// (`-Os`), split code into `.text.unlikely`, and invert branch prediction weights.
/// Any future isolation of its stack scratch buffers for `cache_ttl_secs > 0` should use
/// `#[inline(never)]` without `#[cold]` and be validated via `perf stat`.
pub fn verify_jwt(
    token: &str,
    policy: &JwtValidationPolicy<'_>,
    now_epoch: u64,
) -> Result<VerifiedToken, JwtVerifyError> {
    // 1. Structural check: header.payload.signature
    let first_dot = token.find('.').ok_or(JwtVerifyError::MalformedToken)?;
    let rest = &token[first_dot + 1..];
    let second_dot_offset = rest.find('.').ok_or(JwtVerifyError::MalformedToken)?;
    let second_dot = first_dot + 1 + second_dot_offset;

    if first_dot == 0
        || second_dot_offset == 0
        || second_dot + 1 >= token.len()
        || rest[second_dot_offset + 1..].contains('.')
    {
        return Err(JwtVerifyError::MalformedToken);
    }

    let header_b64 = &token[..first_dot];
    let payload_b64 = &token[first_dot + 1..second_dot];
    let signature_b64 = &token[second_dot + 1..];
    let signing_input = &token.as_bytes()[..second_dot];

    // 2. Decode JOSE header into 512B stack buffer and map `alg` straight to u4 (`0..=8`)
    let mut header_stack_buf = [0u8; 512];
    let header_bytes = decode_base64url_to_slice(header_b64, &mut header_stack_buf)?;
    let header_json: JoseHeader<'_> =
        serde_json::from_slice(header_bytes).map_err(|_| JwtVerifyError::MalformedToken)?;

    let header_alg_u4 =
        JwtAlgorithm::parse_alg_u4(header_json.alg).ok_or(JwtVerifyError::AlgorithmMismatch)?;

    // 3. Resolve slot index via L1D tag bitmap + L3 salted `u64` integer `kid` (or primary slot)
    let slot_idx = match header_json.kid {
        Some(kid_str) => {
            let kid_u64 = hash_kid_salted(kid_str.as_bytes(), policy.boot_salt);
            policy.key_table.resolve_slot_by_kid_u64(kid_u64)?
        }
        None => policy.key_table.primary_slot_idx()?,
    };

    let slot_alg_u4 = policy.key_table.alg_u4(slot_idx);
    if header_alg_u4 != slot_alg_u4 {
        return Err(JwtVerifyError::AlgorithmMismatch);
    }
    let slot_alg = JwtAlgorithm::from_u4(slot_alg_u4).ok_or(JwtVerifyError::AlgorithmMismatch)?;
    let (key_bytes, expected_sig_len) = policy.key_table.key_bytes_for(slot_idx);
    let hmac_opt = policy.key_table.hmac_key_for(slot_idx);
    let key_tag = policy.key_table.key_tags.0[slot_idx];

    // 4. Decode signature onto stack and execute constant-time verification over L3 Byte Arena
    let mut sig_stack_buf = [0u8; 1024];
    let signature_bytes = decode_base64url_to_slice(signature_b64, &mut sig_stack_buf)?;
    verify_slot_signature(
        slot_alg,
        key_bytes,
        hmac_opt,
        expected_sig_len,
        signing_input,
        signature_bytes,
    )?;

    // 5. Decode JSON payload (stack buffer for <= 2 KiB) & evaluate claims in 1 streaming pass (0 Value DOM)
    let mut payload_stack_buf = [0u8; 2048];
    let payload_bytes = if payload_b64.len() <= 2048 * 4 / 3 {
        let slice = decode_base64url_to_slice(payload_b64, &mut payload_stack_buf)?;
        Cow::Borrowed(slice)
    } else {
        Cow::Owned(decode_base64url(payload_b64)?)
    };

    let evaluated = eval_jwt_payload_streaming(
        &payload_bytes,
        policy.validate_exp,
        policy.clock_skew_secs,
        policy.expected_issuer,
        policy.expected_audience,
        policy.claims_to_headers,
        policy.assert_claims,
        now_epoch,
    )?;

    Ok(VerifiedToken {
        exp_epoch_secs: evaluated.exp_epoch_secs,
        key_tag,
        injected_headers: evaluated.injected_headers,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::header::HeaderName;
    use ring::rand::SystemRandom;
    use ring::signature::{Ed25519KeyPair, KeyPair};

    fn create_test_hs256_jwt(secret: &str, payload_json: &str) -> String {
        let header = r#"{"alg":"HS256","typ":"JWT"}"#;
        let header_b64 = BASE64_URL_SAFE_NO_PAD.encode(header.as_bytes());
        let payload_b64 = BASE64_URL_SAFE_NO_PAD.encode(payload_json.as_bytes());
        let signing_input = format!("{header_b64}.{payload_b64}");
        let key = hmac::Key::new(hmac::HMAC_SHA256, secret.as_bytes());
        let tag = hmac::sign(&key, signing_input.as_bytes());
        let sig_b64 = BASE64_URL_SAFE_NO_PAD.encode(tag.as_ref());
        format!("{signing_input}.{sig_b64}")
    }

    #[test]
    fn test_hs256_full_verification() {
        let secret = "super-secret-key-12345678901234567890";
        let payload = r#"{
            "sub": "user_42",
            "iss": "https://auth.internal",
            "aud": "velda-api",
            "exp": 2000000000,
            "roles": ["admin", "editor"],
            "profile": { "tenant_id": "corp_99" }
        }"#;

        let token = create_test_hs256_jwt(secret, payload);
        let key = VerifierKey::new(JwtAlgorithm::HS256, Some(secret), None).unwrap();

        let claims_to_headers = vec![
            (
                ClaimPath::compile("sub"),
                HeaderKey::from_name(HeaderName::from_static("x-user-id")),
            ),
            (
                ClaimPath::compile("roles"),
                HeaderKey::from_name(HeaderName::from_static("x-user-roles")),
            ),
            (
                ClaimPath::compile("profile.tenant_id"),
                HeaderKey::from_name(HeaderName::from_static("x-tenant-id")),
            ),
        ];

        let assert_claims = vec![(ClaimPath::compile("roles"), "admin".into())];
        let salt = [0x22u8; 32];
        let key_table = JwksKeyTable::with_static_key(key, JwtAlgorithm::HS256, &salt);
        let policy = JwtValidationPolicy {
            boot_salt: &salt,
            key_table: &key_table,
            validate_exp: true,
            clock_skew_secs: 60,
            expected_issuer: Some("https://auth.internal"),
            expected_audience: Some("velda-api"),
            claims_to_headers: &claims_to_headers,
            assert_claims: &assert_claims,
        };

        let verified = verify_jwt(&token, &policy, 1700000000).unwrap();
        assert_eq!(verified.exp_epoch_secs, 2000000060);
        let hdrs = verified.injected_headers.unwrap();
        assert_eq!(hdrs.len(), 3);
        assert_eq!(hdrs[0].1, "user_42");
        assert_eq!(hdrs[1].1, "admin, editor");
        assert_eq!(hdrs[2].1, "corp_99");
    }

    #[test]
    fn test_eddsa_ed25519_spki_pem_verification() {
        let rng = SystemRandom::new();
        let pkcs8 = Ed25519KeyPair::generate_pkcs8(&rng).unwrap();
        let kp = Ed25519KeyPair::from_pkcs8(pkcs8.as_ref()).unwrap();

        // Build RFC 8410 SubjectPublicKeyInfo DER for Ed25519 (OID 1.3.101.112)
        let mut spki_der = vec![
            0x30, 0x2a, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x03, 0x21, 0x00,
        ];
        spki_der.extend_from_slice(kp.public_key().as_ref());
        let pem = format!(
            "-----BEGIN PUBLIC KEY-----\n{}\n-----END PUBLIC KEY-----",
            BASE64_STANDARD.encode(&spki_der)
        );

        let salt = [0x33u8; 32];
        let verifier = VerifierKey::new(JwtAlgorithm::EdDSA, None, Some(&pem)).unwrap();
        let key_table = JwksKeyTable::with_static_key(verifier, JwtAlgorithm::EdDSA, &salt);

        let h_b64 = BASE64_URL_SAFE_NO_PAD.encode(br#"{"alg":"EdDSA","typ":"JWT","kid":"ed-1"}"#);
        let p_b64 = BASE64_URL_SAFE_NO_PAD.encode(br#"{"sub":"ed_user","exp":2000000000}"#);
        let signing_input = format!("{h_b64}.{p_b64}");
        let sig = kp.sign(signing_input.as_bytes());
        let token = format!(
            "{signing_input}.{}",
            BASE64_URL_SAFE_NO_PAD.encode(sig.as_ref())
        );

        let policy = JwtValidationPolicy {
            boot_salt: &salt,
            key_table: &key_table,
            validate_exp: true,
            clock_skew_secs: 0,
            expected_issuer: None,
            expected_audience: None,
            claims_to_headers: &[],
            assert_claims: &[],
        };

        assert!(verify_jwt(&token, &policy, 1700000000).is_ok());
    }

    #[test]
    fn test_float_and_malformed_nbf_never_bypass() {
        let secret = "test-secret";
        let salt = [0x44u8; 32];
        let key = VerifierKey::new(JwtAlgorithm::HS256, Some(secret), None).unwrap();
        let key_table = JwksKeyTable::with_static_key(key, JwtAlgorithm::HS256, &salt);
        let policy = JwtValidationPolicy {
            boot_salt: &salt,
            key_table: &key_table,
            validate_exp: false,
            clock_skew_secs: 0,
            expected_issuer: None,
            expected_audience: None,
            claims_to_headers: &[],
            assert_claims: &[],
        };

        let tok_future_float = create_test_hs256_jwt(secret, r#"{"sub":"u1","nbf":2500000000.5}"#);
        assert_eq!(
            verify_jwt(&tok_future_float, &policy, 1700000000).err(),
            Some(JwtVerifyError::TokenNotBefore)
        );

        let tok_bad_nbf = create_test_hs256_jwt(secret, r#"{"sub":"u1","nbf":"invalid"}"#);
        assert_eq!(
            verify_jwt(&tok_bad_nbf, &policy, 1700000000).err(),
            Some(JwtVerifyError::InvalidNumericDate)
        );
    }

    #[test]
    fn test_signature_length_and_kid_rotation_errors() {
        use super::super::store::KeyEntrySpec;

        let salt = [0x55u8; 32];
        let secret1 = "secret-key-1";
        let secret2 = "secret-key-2";

        let header1 = r#"{"alg":"HS256","typ":"JWT","kid":"key-1"}"#;
        let payload = r#"{"sub":"user_rotation","exp":2000000000}"#;
        let h1_b64 = BASE64_URL_SAFE_NO_PAD.encode(header1.as_bytes());
        let p_b64 = BASE64_URL_SAFE_NO_PAD.encode(payload.as_bytes());
        let input1 = format!("{h1_b64}.{p_b64}");
        let hmac_key1 = hmac::Key::new(hmac::HMAC_SHA256, secret1.as_bytes());
        let sig1 = hmac::sign(&hmac_key1, input1.as_bytes());
        let token1 = format!("{input1}.{}", BASE64_URL_SAFE_NO_PAD.encode(sig1.as_ref()));

        let tampered_len = format!("{input1}.AAAA");

        let header_unk = r#"{"alg":"HS256","typ":"JWT","kid":"unknown-kid"}"#;
        let h_unk_b64 = BASE64_URL_SAFE_NO_PAD.encode(header_unk.as_bytes());
        let input_unk = format!("{h_unk_b64}.{p_b64}");
        let sig_unk = hmac::sign(&hmac_key1, input_unk.as_bytes());
        let token_unk = format!(
            "{input_unk}.{}",
            BASE64_URL_SAFE_NO_PAD.encode(sig_unk.as_ref())
        );

        let k1 = VerifierKey::new(JwtAlgorithm::HS256, Some(secret1), None).unwrap();
        let k2 = VerifierKey::new(JwtAlgorithm::HS512, Some(secret2), None).unwrap();
        let entries = [
            KeyEntrySpec {
                raw_kid: Some("key-1".into()),
                alg: JwtAlgorithm::HS256,
                key: k1,
            },
            KeyEntrySpec {
                raw_kid: Some("key-2".into()),
                alg: JwtAlgorithm::HS512,
                key: k2,
            },
        ];
        let table = JwksKeyTable::build_from_entries(&entries, 0, false, true, &salt).unwrap();

        let policy = JwtValidationPolicy {
            boot_salt: &salt,
            key_table: &table,
            validate_exp: true,
            clock_skew_secs: 0,
            expected_issuer: None,
            expected_audience: None,
            claims_to_headers: &[],
            assert_claims: &[],
        };

        assert!(verify_jwt(&token1, &policy, 1000).is_ok());
        assert_eq!(
            verify_jwt(&tampered_len, &policy, 1000).err(),
            Some(JwtVerifyError::SignatureLengthMismatch)
        );
        assert_eq!(
            verify_jwt(&token_unk, &policy, 1000).err(),
            Some(JwtVerifyError::UnknownKid)
        );
    }
}
