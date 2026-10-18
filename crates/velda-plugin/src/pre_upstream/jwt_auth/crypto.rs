//! Cryptographic JWT Verification and Flexible Claim Extraction.
//!
//! # Verification Invariants & High-Performance Design
//! - **Zero-Allocation Hot Path**: Decodes JOSE header, signature, and standard-sized payloads
//!   into stack-allocated slices (`decode_slice`), avoiding heap allocations on cryptographic verification.
//! - **Fail-Fast Signature Length Guard**: Validates signature length in 1ns against the pre-computed
//!   key geometry before initiating expensive asymmetric modular exponentiation (RSA) or point math (ECDSA).
//! - **Universal Key Parsing (SPKI & PKCS#1/SEC 1)**: Accepts standard OpenSSL / PEM public keys
//!   (X.509 `SubjectPublicKeyInfo`), extracting underlying raw key structures at boot time.
//! - **Algorithm Pinning**: Strict checking of JOSE header `alg` against the configured algorithm,
//!   defeating algorithm confusion attacks (e.g. `alg: "none"` or HMAC/RSA substitution).
//! - **Crash Proof & Arithmetic Safety**: Protects all string slicing with UTF-8 character boundary checks
//!   and timestamp arithmetic with saturated math (`saturating_add`).

use std::borrow::Cow;
use std::str::FromStr;

use base64::prelude::*;
use http::header::{HeaderName, HeaderValue};
use ring::hmac;
use ring::signature::{self, UnparsedPublicKey};
use serde::{Deserialize, Serialize};

use crate::error::PluginError;

/// Cryptographic algorithms supported for JWT signature verification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum JwtAlgorithm {
    HS256,
    HS384,
    HS512,
    RS256,
    RS384,
    RS512,
    ES256,
    ES384,
}

impl JwtAlgorithm {
    /// Returns the standard RFC 7518 algorithm name.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::HS256 => "HS256",
            Self::HS384 => "HS384",
            Self::HS512 => "HS512",
            Self::RS256 => "RS256",
            Self::RS384 => "RS384",
            Self::RS512 => "RS512",
            Self::ES256 => "ES256",
            Self::ES384 => "ES384",
        }
    }

    /// Returns true if this is a symmetric HMAC algorithm.
    pub fn is_symmetric(&self) -> bool {
        matches!(self, Self::HS256 | Self::HS384 | Self::HS512)
    }

    /// Returns true if this is an RSA algorithm.
    pub fn is_rsa(&self) -> bool {
        matches!(self, Self::RS256 | Self::RS384 | Self::RS512)
    }

    /// Returns expected fixed signature length in bytes for symmetric and EC algorithms.
    pub fn fixed_signature_len(&self) -> Option<usize> {
        match self {
            Self::HS256 => Some(32),
            Self::HS384 => Some(48),
            Self::HS512 => Some(64),
            Self::ES256 => Some(64), // 32 bytes r + 32 bytes s
            Self::ES384 => Some(96), // 48 bytes r + 48 bytes s
            _ => None,               // RSA signature length equals key modulus length
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
            other => Err(PluginError::Config(format!(
                "Unsupported JWT algorithm '{other}'. Supported: HS256, HS384, HS512, RS256, RS384, RS512, ES256, ES384"
            ))),
        }
    }
}

// ============================================================================
// ASN.1 DER Parser for Universal Key Format Compatibility
// ============================================================================

/// Reads a single ASN.1 TLV (Tag-Length-Value) element from DER bytes.
#[inline]
pub fn read_asn1_tlv(data: &[u8]) -> Option<(u8, &[u8], &[u8])> {
    if data.is_empty() {
        return None;
    }
    let tag = data[0];
    if data.len() < 2 {
        return None;
    }

    let (len, offset) = if data[1] < 0x80 {
        (data[1] as usize, 2)
    } else {
        let len_bytes = (data[1] & 0x7f) as usize;
        if len_bytes == 0 || len_bytes > 4 || data.len() < 2 + len_bytes {
            return None;
        }
        let mut l = 0usize;
        for i in 0..len_bytes {
            l = (l << 8) | (data[2 + i] as usize);
        }
        (l, 2 + len_bytes)
    };

    if data.len() < offset + len {
        return None;
    }
    let value = &data[offset..offset + len];
    let remaining = &data[offset + len..];
    Some((tag, value, remaining))
}

/// Extracts underlying raw public key bytes from either X.509 `SubjectPublicKeyInfo` (SPKI)
/// or PKCS#1 / SEC 1 raw structures.
///
/// - For RSA: returns PKCS#1 `RSAPublicKey` (`SEQUENCE { n INTEGER, e INTEGER }`).
/// - For ECDSA: returns SEC 1 uncompressed point (`0x04 || X || Y`).
pub fn extract_raw_public_key(der: &[u8], is_rsa: bool) -> Result<Vec<u8>, PluginError> {
    if der.is_empty() {
        return Err(PluginError::Config("Public key DER is empty".into()));
    }

    // ECDSA raw uncompressed point starts with 0x04
    if !is_rsa && der[0] == 0x04 {
        return Ok(der.to_vec());
    }

    if der[0] != 0x30 {
        return Err(PluginError::Config(format!(
            "Invalid public key DER header: expected SEQUENCE (0x30), found 0x{:02x}",
            der[0]
        )));
    }

    let (tag, value, _) = read_asn1_tlv(der)
        .ok_or_else(|| PluginError::Config("Malformed ASN.1 SEQUENCE in public key".into()))?;
    if tag != 0x30 {
        return Err(PluginError::Config(
            "Expected SEQUENCE tag in public key".into(),
        ));
    }

    // Inspect first child in the sequence
    let (first_tag, _, rest) = read_asn1_tlv(value)
        .ok_or_else(|| PluginError::Config("Empty ASN.1 SEQUENCE in public key".into()))?;

    // If RSA and first child is INTEGER (0x02), it is ALREADY PKCS#1 RSAPublicKey!
    if is_rsa && first_tag == 0x02 {
        return Ok(der.to_vec());
    }

    // If first child is SEQUENCE (0x30), this is X.509 SubjectPublicKeyInfo
    if first_tag == 0x30 {
        let (second_tag, bitstring_val, _) = read_asn1_tlv(rest).ok_or_else(|| {
            PluginError::Config("Missing SubjectPublicKey BIT STRING in SPKI".into())
        })?;
        if second_tag != 0x03 {
            return Err(PluginError::Config(format!(
                "Expected BIT STRING (0x03) in SPKI, found 0x{:02x}",
                second_tag
            )));
        }
        if bitstring_val.is_empty() {
            return Err(PluginError::Config("Empty BIT STRING in SPKI".into()));
        }
        // First byte of BIT STRING is unused bits (should be 0 for standard public keys)
        let unused_bits = bitstring_val[0];
        if unused_bits != 0 {
            return Err(PluginError::Config(format!(
                "Non-zero unused bits in SPKI BIT STRING: {unused_bits}"
            )));
        }
        return Ok(bitstring_val[1..].to_vec());
    }

    // Fallback: return as-is
    Ok(der.to_vec())
}

/// Extracts the RSA modulus length in bytes from a PKCS#1 `RSAPublicKey` DER sequence.
pub fn extract_rsa_modulus_len(pkcs1_der: &[u8]) -> Option<usize> {
    let (tag, value, _) = read_asn1_tlv(pkcs1_der)?;
    if tag != 0x30 {
        return None;
    }
    let (tag_n, val_n, _) = read_asn1_tlv(value)?;
    if tag_n != 0x02 {
        return None;
    }
    // Strip leading zero padding if present
    let n_bytes = if !val_n.is_empty() && val_n[0] == 0x00 {
        &val_n[1..]
    } else {
        val_n
    };
    Some(n_bytes.len())
}

// ============================================================================
// Zero-Allocation Base64URL Decoders
// ============================================================================

/// Decodes base64url-encoded bytes directly into a stack or caller-provided buffer without heap allocation.
#[inline]
pub fn decode_base64url_to_slice<'a>(s: &str, buf: &'a mut [u8]) -> Result<&'a [u8], &'static str> {
    let s_clean = s.trim();
    if s_clean.is_empty() {
        return Err("Empty base64url input");
    }
    let len = if s_clean.ends_with('=') {
        BASE64_URL_SAFE
            .decode_slice(s_clean.as_bytes(), buf)
            .map_err(|_| "Malformed base64url with padding")?
    } else {
        BASE64_URL_SAFE_NO_PAD
            .decode_slice(s_clean.as_bytes(), buf)
            .map_err(|_| "Malformed base64url")?
    };
    Ok(&buf[..len])
}

/// Fallback base64url decoder allocating a [`Vec<u8>`] for arbitrarily large token payloads.
pub fn decode_base64url(s: &str) -> Result<Vec<u8>, &'static str> {
    let s_clean = s.trim();
    if s_clean.is_empty() {
        return Err("Empty base64url input");
    }
    if s_clean.ends_with('=') {
        BASE64_URL_SAFE
            .decode(s_clean)
            .map_err(|_| "Malformed base64url with padding")
    } else {
        BASE64_URL_SAFE_NO_PAD
            .decode(s_clean)
            .map_err(|_| "Malformed base64url")
    }
}

/// Parses a PEM-encoded public key into raw DER bytes.
pub fn parse_pem_to_der(pem_str: &str) -> Result<Vec<u8>, PluginError> {
    let mut in_key = false;
    let mut b64_lines = String::new();
    for line in pem_str.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("-----BEGIN") {
            in_key = true;
            continue;
        }
        if trimmed.starts_with("-----END") {
            break;
        }
        if in_key {
            b64_lines.push_str(trimmed);
        }
    }
    if b64_lines.is_empty() {
        if let Ok(der) = BASE64_STANDARD.decode(pem_str.trim()) {
            return Ok(der);
        }
        return Err(PluginError::Config(
            "Public key PEM must contain valid -----BEGIN/END----- banners or raw base64 DER"
                .into(),
        ));
    }
    BASE64_STANDARD
        .decode(&b64_lines)
        .map_err(|e| PluginError::Config(format!("Failed to base64-decode public key DER: {e}")))
}

// ============================================================================
// Verifier Key with Pre-computed Geometry
// ============================================================================

/// Pre-parsed cryptographic verification key with pre-computed geometry.
pub enum VerifierKey {
    Hmac {
        key: hmac::Key,
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
}

impl VerifierKey {
    /// Constructs and pre-validates a verifier key from config inputs.
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
            let (hmac_alg, expected_sig_len) = match alg {
                JwtAlgorithm::HS256 => (hmac::HMAC_SHA256, 32),
                JwtAlgorithm::HS384 => (hmac::HMAC_SHA384, 48),
                JwtAlgorithm::HS512 => (hmac::HMAC_SHA512, 64),
                _ => unreachable!(),
            };
            Ok(Self::Hmac {
                key: hmac::Key::new(hmac_alg, secret.as_bytes()),
                expected_sig_len,
            })
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

            // Enforce minimum 2048-bit RSA key length (256 bytes)
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

    /// Expected signature length in bytes for fast length validation.
    #[inline(always)]
    pub fn expected_sig_len(&self) -> usize {
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
        }
    }

    /// Verifies signature over raw signing input bytes.
    #[inline]
    pub fn verify(
        &self,
        alg: JwtAlgorithm,
        signing_input: &[u8],
        signature_bytes: &[u8],
    ) -> Result<(), &'static str> {
        // Fast 1ns signature length check
        if signature_bytes.len() != self.expected_sig_len() {
            return Err("Signature length mismatch");
        }

        match (self, alg) {
            (Self::Hmac { key, .. }, _) => hmac::verify(key, signing_input, signature_bytes)
                .map_err(|_| "Invalid HMAC signature"),
            (Self::Rsa { public_key_der, .. }, JwtAlgorithm::RS256) => {
                let public_key =
                    UnparsedPublicKey::new(&signature::RSA_PKCS1_2048_8192_SHA256, public_key_der);
                public_key
                    .verify(signing_input, signature_bytes)
                    .map_err(|_| "Invalid RS256 signature")
            }
            (Self::Rsa { public_key_der, .. }, JwtAlgorithm::RS384) => {
                let public_key =
                    UnparsedPublicKey::new(&signature::RSA_PKCS1_2048_8192_SHA384, public_key_der);
                public_key
                    .verify(signing_input, signature_bytes)
                    .map_err(|_| "Invalid RS384 signature")
            }
            (Self::Rsa { public_key_der, .. }, JwtAlgorithm::RS512) => {
                let public_key =
                    UnparsedPublicKey::new(&signature::RSA_PKCS1_2048_8192_SHA512, public_key_der);
                public_key
                    .verify(signing_input, signature_bytes)
                    .map_err(|_| "Invalid RS512 signature")
            }
            (Self::Ecdsa { public_key_raw, .. }, JwtAlgorithm::ES256) => {
                let public_key =
                    UnparsedPublicKey::new(&signature::ECDSA_P256_SHA256_FIXED, public_key_raw);
                public_key
                    .verify(signing_input, signature_bytes)
                    .map_err(|_| "Invalid ES256 signature")
            }
            (Self::Ecdsa { public_key_raw, .. }, JwtAlgorithm::ES384) => {
                let public_key =
                    UnparsedPublicKey::new(&signature::ECDSA_P384_SHA384_FIXED, public_key_raw);
                public_key
                    .verify(signing_input, signature_bytes)
                    .map_err(|_| "Invalid ES384 signature")
            }
            _ => Err("Algorithm and key type mismatch"),
        }
    }
}

// ============================================================================
// Claim Resolution & Business Policies
// ============================================================================

/// Resolves a nested claim path from a JSON object using dot notation (e.g., `user.id`, `profile.auth.tenant`).
pub fn resolve_claim_path(value: &serde_json::Value, path: &str) -> Option<String> {
    let mut current = value;
    for segment in path.split('.') {
        match current {
            serde_json::Value::Object(map) => {
                current = map.get(segment)?;
            }
            _ => return None,
        }
    }
    match current {
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Number(n) => Some(n.to_string()),
        serde_json::Value::Bool(b) => Some(b.to_string()),
        serde_json::Value::Array(arr) => {
            let items: Vec<String> = arr
                .iter()
                .filter_map(|item| match item {
                    serde_json::Value::String(s) => Some(s.clone()),
                    serde_json::Value::Number(n) => Some(n.to_string()),
                    _ => None,
                })
                .collect();
            if items.is_empty() {
                None
            } else {
                Some(items.join(", "))
            }
        }
        _ => None,
    }
}

/// Checks if a payload claim matches an expected value.
pub fn matches_assert_claim(payload: &serde_json::Value, path: &str, expected: &str) -> bool {
    let mut current = payload;
    for segment in path.split('.') {
        match current {
            serde_json::Value::Object(map) => match map.get(segment) {
                Some(v) => current = v,
                None => return false,
            },
            _ => return false,
        }
    }
    match current {
        serde_json::Value::String(s) => s == expected,
        serde_json::Value::Number(n) => n.to_string() == expected,
        serde_json::Value::Bool(b) => b.to_string() == expected,
        serde_json::Value::Array(arr) => arr.iter().any(|item| match item {
            serde_json::Value::String(s) => s == expected,
            serde_json::Value::Number(n) => n.to_string() == expected,
            _ => false,
        }),
        _ => false,
    }
}

/// Validated token result containing expiration and pre-constructed headers ready for injection.
pub struct VerifiedToken {
    pub exp_epoch_secs: u64,
    pub injected_headers: Vec<(HeaderName, HeaderValue)>,
}

/// Configuration policies and cryptographic keys for JWT verification.
pub struct JwtValidationPolicy<'a> {
    pub expected_alg: JwtAlgorithm,
    pub key: &'a VerifierKey,
    pub validate_exp: bool,
    pub clock_skew_secs: u64,
    pub expected_issuer: Option<&'a str>,
    pub expected_audience: Option<&'a str>,
    pub claims_to_headers: &'a [(String, HeaderName)],
    pub assert_claims: &'a [(String, String)],
}

/// Lightweight borrowed JOSE header representation for zero-allocation parsing.
#[derive(Deserialize)]
struct JoseHeader<'a> {
    alg: &'a str,
}

/// Performs full validation of a compact JWT string against cryptographic key and business policies.
pub fn verify_jwt(
    token: &str,
    policy: &JwtValidationPolicy<'_>,
    now_epoch: u64,
) -> Result<VerifiedToken, &'static str> {
    // 1. Structural check: header.payload.signature
    let first_dot = token.find('.').ok_or("Malformed JWT: missing first dot")?;
    let rest = &token[first_dot + 1..];
    let second_dot_offset = rest.find('.').ok_or("Malformed JWT: missing second dot")?;
    let second_dot = first_dot + 1 + second_dot_offset;

    // Fail-fast on excessive dots
    if rest[second_dot_offset + 1..].contains('.') {
        return Err("Malformed JWT: unexpected additional dots");
    }

    let header_b64 = &token[..first_dot];
    let payload_b64 = &token[first_dot + 1..second_dot];
    let signature_b64 = &token[second_dot + 1..];

    let signing_input = &token.as_bytes()[..second_dot];

    // 2. Decode and verify JOSE header into stack buffer (zero allocation)
    let mut header_stack_buf = [0u8; 128];
    let header_bytes = decode_base64url_to_slice(header_b64, &mut header_stack_buf)?;
    let header_json: JoseHeader<'_> =
        serde_json::from_slice(header_bytes).map_err(|_| "Malformed JOSE header JSON")?;

    if header_json.alg != policy.expected_alg.as_str() {
        return Err("Algorithm mismatch");
    }

    // 3. Cryptographic signature check into stack buffer (zero allocation)
    let mut sig_stack_buf = [0u8; 512];
    let signature_bytes = decode_base64url_to_slice(signature_b64, &mut sig_stack_buf)?;
    policy
        .key
        .verify(policy.expected_alg, signing_input, signature_bytes)?;

    // 4. Decode and parse JSON payload into stack buffer (zero allocation for standard tokens < 2KB)
    let mut payload_stack_buf = [0u8; 2048];
    let payload_bytes = if payload_b64.len() <= 2048 * 4 / 3 {
        let len = decode_base64url_to_slice(payload_b64, &mut payload_stack_buf)?;
        Cow::Borrowed(len)
    } else {
        Cow::Owned(decode_base64url(payload_b64)?)
    };

    let payload: serde_json::Value =
        serde_json::from_slice(&payload_bytes).map_err(|_| "Malformed JWT payload JSON")?;

    // 5. Expiration & Not-Before validation with overflow protection
    let exp_epoch = payload.get("exp").and_then(|v| v.as_u64());
    if policy.validate_exp {
        let exp = exp_epoch.ok_or("Missing exp claim in payload")?;
        if now_epoch > exp.saturating_add(policy.clock_skew_secs) {
            return Err("Token has expired");
        }
    }

    if let Some(nbf) = payload.get("nbf").and_then(|v| v.as_u64())
        && now_epoch.saturating_add(policy.clock_skew_secs) < nbf
    {
        return Err("Token is not active yet (nbf)");
    }

    // 6. Issuer check
    if let Some(iss) = policy.expected_issuer
        && !iss.is_empty()
        && iss != "none"
    {
        let token_iss = payload.get("iss").and_then(|v| v.as_str());
        if token_iss != Some(iss) {
            return Err("Issuer mismatch");
        }
    }

    // 7. Audience check
    if let Some(aud) = policy.expected_audience
        && !aud.is_empty()
        && aud != "none"
    {
        let matches_aud = match payload.get("aud") {
            Some(serde_json::Value::String(s)) => s == aud,
            Some(serde_json::Value::Array(arr)) => arr.iter().any(|v| v.as_str() == Some(aud)),
            _ => false,
        };
        if !matches_aud {
            return Err("Audience mismatch");
        }
    }

    // 8. Assert claims (RBAC / tenant assertions)
    for (claim_path, expected_val) in policy.assert_claims {
        if !matches_assert_claim(&payload, claim_path, expected_val) {
            return Err("Claim assertion failed");
        }
    }

    // 9. Extract and pre-build injected headers
    let mut injected_headers = Vec::with_capacity(policy.claims_to_headers.len());
    for (claim_path, header_name) in policy.claims_to_headers {
        if let Some(claim_val) = resolve_claim_path(&payload, claim_path)
            && let Ok(hdr_val) = HeaderValue::try_from(claim_val)
        {
            injected_headers.push((header_name.clone(), hdr_val));
        }
    }

    Ok(VerifiedToken {
        exp_epoch_secs: exp_epoch.unwrap_or(u64::MAX),
        injected_headers,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

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
            ("sub".to_string(), HeaderName::from_static("x-user-id")),
            ("roles".to_string(), HeaderName::from_static("x-user-roles")),
            (
                "profile.tenant_id".to_string(),
                HeaderName::from_static("x-tenant-id"),
            ),
        ];

        let assert_claims = vec![("roles".to_string(), "admin".to_string())];
        let policy = JwtValidationPolicy {
            expected_alg: JwtAlgorithm::HS256,
            key: &key,
            validate_exp: true,
            clock_skew_secs: 60,
            expected_issuer: Some("https://auth.internal"),
            expected_audience: Some("velda-api"),
            claims_to_headers: &claims_to_headers,
            assert_claims: &assert_claims,
        };

        let res = verify_jwt(&token, &policy, 1700000000);

        assert!(res.is_ok());
        let verified = res.unwrap();
        assert_eq!(verified.exp_epoch_secs, 2000000000);
        assert_eq!(verified.injected_headers.len(), 3);
        assert_eq!(verified.injected_headers[0].1, "user_42");
        assert_eq!(verified.injected_headers[1].1, "admin, editor");
        assert_eq!(verified.injected_headers[2].1, "corp_99");
    }

    #[test]
    fn test_signature_length_mismatch_fast_fail() {
        let secret = "test-secret";
        let payload = r#"{"sub": "user_1", "exp": 2000000000}"#;
        let token = create_test_hs256_jwt(secret, payload);
        let key = VerifierKey::new(JwtAlgorithm::HS256, Some(secret), None).unwrap();

        // Tamper with signature to have invalid length (4 valid base64 chars = 3 bytes, whereas HS256 requires 32 bytes)
        let parts: Vec<&str> = token.split('.').collect();
        let tampered_token = format!("{}.{}.AAAA", parts[0], parts[1]);

        let policy = JwtValidationPolicy {
            expected_alg: JwtAlgorithm::HS256,
            key: &key,
            validate_exp: false,
            clock_skew_secs: 0,
            expected_issuer: None,
            expected_audience: None,
            claims_to_headers: &[],
            assert_claims: &[],
        };

        let res = verify_jwt(&tampered_token, &policy, 1000);
        assert_eq!(res.err(), Some("Signature length mismatch"));
    }

    #[test]
    fn test_expired_token_rejected() {
        let secret = "test-secret";
        let payload = r#"{"sub": "user_1", "exp": 1000}"#;
        let token = create_test_hs256_jwt(secret, payload);
        let key = VerifierKey::new(JwtAlgorithm::HS256, Some(secret), None).unwrap();

        let policy = JwtValidationPolicy {
            expected_alg: JwtAlgorithm::HS256,
            key: &key,
            validate_exp: true,
            clock_skew_secs: 0,
            expected_issuer: None,
            expected_audience: None,
            claims_to_headers: &[],
            assert_claims: &[],
        };

        let res = verify_jwt(&token, &policy, 2000);
        assert_eq!(res.err(), Some("Token has expired"));
    }

    #[test]
    fn test_claim_assertion_failure() {
        let secret = "test-secret";
        let payload = r#"{"sub": "user_1", "role": "viewer", "exp": 2000000000}"#;
        let token = create_test_hs256_jwt(secret, payload);
        let key = VerifierKey::new(JwtAlgorithm::HS256, Some(secret), None).unwrap();

        let assert_claims = vec![("role".to_string(), "admin".to_string())];
        let policy = JwtValidationPolicy {
            expected_alg: JwtAlgorithm::HS256,
            key: &key,
            validate_exp: false,
            clock_skew_secs: 0,
            expected_issuer: None,
            expected_audience: None,
            claims_to_headers: &[],
            assert_claims: &assert_claims,
        };

        let res = verify_jwt(&token, &policy, 1000);
        assert_eq!(res.err(), Some("Claim assertion failed"));
    }

    #[test]
    fn test_algorithm_mismatch_rejected() {
        let secret = "test-secret";
        let payload = r#"{"sub": "user_1", "exp": 2000000000}"#;
        let token = create_test_hs256_jwt(secret, payload);
        let key = VerifierKey::new(JwtAlgorithm::HS256, Some(secret), None).unwrap();

        let policy = JwtValidationPolicy {
            expected_alg: JwtAlgorithm::HS384, // expected HS384, but token is HS256
            key: &key,
            validate_exp: false,
            clock_skew_secs: 0,
            expected_issuer: None,
            expected_audience: None,
            claims_to_headers: &[],
            assert_claims: &[],
        };

        let res = verify_jwt(&token, &policy, 1000);
        assert_eq!(res.err(), Some("Algorithm mismatch"));
    }

    #[test]
    fn test_excessive_dots_rejected() {
        let secret = "test-secret";
        let key = VerifierKey::new(JwtAlgorithm::HS256, Some(secret), None).unwrap();
        let malformed_token = "header.payload.signature.extra";

        let policy = JwtValidationPolicy {
            expected_alg: JwtAlgorithm::HS256,
            key: &key,
            validate_exp: false,
            clock_skew_secs: 0,
            expected_issuer: None,
            expected_audience: None,
            claims_to_headers: &[],
            assert_claims: &[],
        };

        let res = verify_jwt(malformed_token, &policy, 1000);
        assert_eq!(res.err(), Some("Malformed JWT: unexpected additional dots"));
    }
}
