//! ACME Cryptographic Primitives: JWS (ES256), Account Keys, and CSR Generation.
//!
//! Conforms strictly to RFC 8555 (ACME), RFC 7515 (JWS), RFC 7638 (JWK Thumbprint),
//! and RFC 2986 (PKCS#10 CSR). Uses hardware-accelerated elliptic curve arithmetic
//! via `ring` and `aws-lc-rs` with zero CPU `idiv` stalls.

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ring::rand::SystemRandom;
use ring::signature::{ECDSA_P256_SHA256_FIXED_SIGNING, EcdsaKeyPair, KeyPair};
use serde::{Deserialize, Serialize};

use crate::error::PluginError;

/// Account Key representation backed by a standard ECDSA P-256 key pair.
pub struct AccountKey {
    key_pair: EcdsaKeyPair,
    pkcs8_der: Vec<u8>,
    jwk_thumbprint: String,
    jwk_public: JwkPublicKey,
}

/// JSON Web Key (JWK) representing an elliptic curve public key (RFC 7638).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JwkPublicKey {
    pub crv: String,
    pub kty: String,
    pub x: String,
    pub y: String,
}

/// Key identifier mode in JWS Protected Header (RFC 8555 §6.2).
pub enum KeyIdentifier<'a> {
    /// In newAccount requests: embeds the public JWK directly.
    Jwk(&'a JwkPublicKey),
    /// In subsequent requests: references the registered Account URL (`kid`).
    Kid(&'a str),
}

/// Signed JWS JSON Flattened Serialization (RFC 7515 §7.2.2).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JwsMessage {
    pub protected: String,
    pub payload: String,
    pub signature: String,
}

// ============================================================================
// Fixed-width base64url encoder (32 bytes -> 43 chars) with hardware dispatch
// ============================================================================

/// Base64url (no padding) length of a 32-byte value: `ceil(32 * 4 / 3)` = 43.
pub const B64_32_LEN: usize = 43;

/// Fixed pieces of the RFC 7638 canonical JWK for EC P-256 (`crv`,`kty`,`x`,`y` order).
const JWK_PREFIX: &[u8] = br#"{"crv":"P-256","kty":"EC","x":""#; // 31 bytes
const JWK_INFIX: &[u8] = br#"","y":""#; // 7 bytes
const JWK_SUFFIX: &[u8] = br#""}"#; // 2 bytes
/// Total canonical JWK size: 31 + 43 + 7 + 43 + 2 = 126 bytes, enforced at compile time.
const CANONICAL_JWK_LEN: usize =
    JWK_PREFIX.len() + B64_32_LEN + JWK_INFIX.len() + B64_32_LEN + JWK_SUFFIX.len();
const _: () = assert!(CANONICAL_JWK_LEN == 126);

/// Encoder contract: 32 raw bytes in, 43 base64url ASCII bytes out.
pub type B64Encode32Fn = fn(src: &[u8; 32], out: &mut [u8; B64_32_LEN]);

/// Encodes 32 raw bytes into 43 base64url ASCII bytes using the best CPU instruction set.
///
/// Inlined direct execution eliminating indirect function pointer overhead.
#[inline(always)]
pub fn b64url_encode32(src: &[u8; 32], out: &mut [u8; B64_32_LEN]) {
    #[cfg(target_arch = "x86_64")]
    {
        if std::is_x86_feature_detected!("ssse3") {
            unsafe { encode32_ssse3_impl(src, out) };
            return;
        }
    }
    b64url_encode32_scalar(src, out);
}

/// Resolves the best encoder for the host CPU.
///
/// Kept for crate API compatibility; internally delegates directly to inlined `b64url_encode32`.
pub fn resolve_b64url_encode32() -> B64Encode32Fn {
    b64url_encode32
}

/// Portable fallback: delegates to the `base64` crate, writing into the caller's stack buffer.
fn b64url_encode32_scalar(src: &[u8; 32], out: &mut [u8; B64_32_LEN]) {
    // `encode_slice` into an exactly-sized buffer cannot fail for 32 input bytes.
    let _ = URL_SAFE_NO_PAD.encode_slice(src, out);
}

/// SSSE3 path (Muła/Lemire vector base64): each iteration turns 12 input bytes into 16 chars
/// held entirely in one XMM register.
///
/// Layout for 32 bytes: two vector blocks cover input `[0..12)` and `[12..24)` -> chars
/// `[0..32)`; the remaining 8 input bytes (`[24..32)`, 11 chars) go through the scalar path.
/// 24 is a multiple of 3, so the concatenation is exactly the unpadded base64url string.
#[allow(dead_code)]
fn b64url_encode32_ssse3(src: &[u8; 32], out: &mut [u8; B64_32_LEN]) {
    #[cfg(target_arch = "x86_64")]
    {
        // SAFETY: only reachable after `is_x86_feature_detected!("ssse3")` in the resolver.
        unsafe { encode32_ssse3_impl(src, out) }
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        b64url_encode32_scalar(src, out)
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "ssse3")]
unsafe fn encode32_ssse3_impl(src: &[u8; 32], out: &mut [u8; B64_32_LEN]) {
    use core::arch::x86_64::*;

    // Encodes the low 12 bytes of one 16-byte load into 16 base64url ASCII bytes.
    #[inline]
    #[target_feature(enable = "ssse3")]
    unsafe fn block(input: __m128i) -> __m128i {
        // Spread each 3-byte group over a 4-byte lane so 6-bit fields can be isolated.
        let spread = _mm_shuffle_epi8(
            input,
            _mm_setr_epi8(1, 0, 2, 1, 4, 3, 5, 4, 7, 6, 8, 7, 10, 9, 11, 10),
        );
        // Isolate sextets 0/2 and 1/3 with mask+multiply tricks (shift-by-lane without branches).
        let t0 = _mm_and_si128(spread, _mm_set1_epi32(0x0fc0_fc00));
        // Use inline asm for `pmulhuw` to prevent LLVM's DAGCombiner from decomposing
        // `mulhu(x, 2^k)` into non-uniform 16-bit shifts (which x86 lacks, ballooning into 13 unpack/shift/pack ops).
        let mut t1 = t0;
        let mult = _mm_set1_epi32(0x0400_0040);
        unsafe {
            core::arch::asm!(
                "pmulhuw {x}, {m}",
                x = inout(xmm_reg) t1,
                m = in(xmm_reg) mult,
                options(pure, nomem, nostack)
            );
        }
        let t2 = _mm_and_si128(spread, _mm_set1_epi32(0x003f_03f0));
        let t3 = _mm_mullo_epi16(t2, _mm_set1_epi32(0x0100_0010));
        let idx = _mm_or_si128(t1, t3); // 16 sextets, each 0..=63

        // Map sextet -> ASCII via a 16-entry offset LUT selected by range class.
        let mut cls = _mm_subs_epu8(idx, _mm_set1_epi8(51)); // 26..=51 -> 0, 52..=63 -> 1..=12
        let upper = _mm_cmpgt_epi8(_mm_set1_epi8(26), idx); // 0..=25 -> 0xFF
        cls = _mm_or_si128(cls, _mm_and_si128(upper, _mm_set1_epi8(13)));
        // URL-safe alphabet: '-' (62) = 45, '_' (63) = 95.
        let lut = _mm_setr_epi8(
            71, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -17, 32, 65, 0, 0,
        );
        _mm_add_epi8(idx, _mm_shuffle_epi8(lut, cls))
    }

    // SAFETY: src is 32 bytes; loads at offsets 0 and 12 read bytes [0..16) and [12..28).
    // Stores target out[0..16) and out[16..32), inside the 43-byte buffer.
    unsafe {
        let a = _mm_loadu_si128(src.as_ptr() as *const __m128i);
        let b = _mm_loadu_si128(src.as_ptr().add(12) as *const __m128i);
        _mm_storeu_si128(out.as_mut_ptr() as *mut __m128i, block(a));
        _mm_storeu_si128(out.as_mut_ptr().add(16) as *mut __m128i, block(b));
    }
    // Tail: 8 bytes -> 11 chars (no padding).
    let _ = URL_SAFE_NO_PAD.encode_slice(&src[24..], &mut out[32..]);
}

impl AccountKey {
    /// Generates a new random ECDSA P-256 account key pair using system entropy.
    pub fn generate() -> Result<Self, PluginError> {
        let rng = SystemRandom::new();
        let pkcs8 = EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, &rng)
            .map_err(|e| PluginError::Config(format!("Failed to generate account key: {e}")))?;
        Self::from_pkcs8_der(pkcs8.as_ref())
    }

    /// Loads an account key from PKCS#8 DER bytes.
    pub fn from_pkcs8_der(der: &[u8]) -> Result<Self, PluginError> {
        let rng = SystemRandom::new();
        let key_pair = EcdsaKeyPair::from_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, der, &rng)
            .map_err(|e| PluginError::Config(format!("Failed to parse PKCS#8 account key: {e}")))?;

        // Public key is 65 bytes uncompressed: 0x04 || X (32 bytes) || Y (32 bytes)
        let pub_bytes = key_pair.public_key().as_ref();
        if pub_bytes.len() != 65 || pub_bytes[0] != 0x04 {
            return Err(PluginError::Config(
                "Invalid public key encoding from keypair".into(),
            ));
        }

        // Encode X/Y (32 bytes each -> 43 base64url chars) straight into stack buffers via
        // the hardware-dispatched SIMD encoder: no heap allocation, no per-byte branching.
        let mut x_b64 = [0u8; B64_32_LEN];
        let mut y_b64 = [0u8; B64_32_LEN];
        b64url_encode32(pub_bytes[1..33].try_into().expect("32-byte X"), &mut x_b64);
        b64url_encode32(pub_bytes[33..65].try_into().expect("32-byte Y"), &mut y_b64);

        // RFC 7638 canonical JSON, lexicographic keys crv,kty,x,y. For P-256 every field has a
        // fixed width, so the document is exactly 126 bytes and is
        // assembled on the stack by plain blits instead of `format!`.
        let mut canonical = [0u8; CANONICAL_JWK_LEN];
        let mut at = 0;
        for part in [JWK_PREFIX, &x_b64[..], JWK_INFIX, &y_b64[..], JWK_SUFFIX] {
            canonical[at..at + part.len()].copy_from_slice(part);
            at += part.len();
        }
        debug_assert_eq!(at, CANONICAL_JWK_LEN);

        let digest = ring::digest::digest(&ring::digest::SHA256, &canonical);
        let mut thumb_b64 = [0u8; B64_32_LEN];
        b64url_encode32(
            digest.as_ref().try_into().expect("SHA-256 is 32 bytes"),
            &mut thumb_b64,
        );

        // Base64url output is pure ASCII, so the UTF-8 conversions cannot fail.
        let jwk_public = JwkPublicKey {
            crv: "P-256".to_string(),
            kty: "EC".to_string(),
            x: String::from_utf8_lossy(&x_b64).into_owned(),
            y: String::from_utf8_lossy(&y_b64).into_owned(),
        };
        let jwk_thumbprint = String::from_utf8_lossy(&thumb_b64).into_owned();

        Ok(Self {
            key_pair,
            pkcs8_der: der.to_vec(),
            jwk_thumbprint,
            jwk_public,
        })
    }

    /// Loads an account key from PEM format string.
    pub fn from_pem(pem: &str) -> Result<Self, PluginError> {
        let key_der = velda_tls::pem::parse_private_key_pem(pem)
            .map_err(|e| PluginError::Config(format!("Failed to parse PEM key: {e}")))?;
        Self::from_pkcs8_der(key_der.secret_der())
    }

    /// Returns the PKCS#8 DER bytes of this account key.
    pub fn pkcs8_der(&self) -> &[u8] {
        &self.pkcs8_der
    }

    /// Serializes this account key to standard PEM format.
    pub fn to_pem(&self) -> String {
        let b64 = base64::engine::general_purpose::STANDARD.encode(&self.pkcs8_der);
        let mut pem = String::from("-----BEGIN PRIVATE KEY-----\n");
        for chunk in b64.as_bytes().chunks(64) {
            if let Ok(s) = std::str::from_utf8(chunk) {
                pem.push_str(s);
                pem.push('\n');
            }
        }
        pem.push_str("-----END PRIVATE KEY-----\n");
        pem
    }

    /// Returns the RFC 7638 JWK thumbprint (base64url-encoded SHA-256).
    pub fn thumbprint(&self) -> &str {
        &self.jwk_thumbprint
    }

    /// Returns a reference to the public JWK.
    pub fn jwk(&self) -> &JwkPublicKey {
        &self.jwk_public
    }

    /// Computes the HTTP-01 key authorization string: `<token>.<thumbprint>` (RFC 8555 §8.1).
    pub fn key_authorization(&self, token: &str) -> String {
        let mut s = String::with_capacity(token.len() + 1 + self.jwk_thumbprint.len());
        s.push_str(token);
        s.push('.');
        s.push_str(&self.jwk_thumbprint);
        s
    }

    /// Signs an ACME JSON request using JSON Web Signature (JWS ES256) per RFC 8555 §6.2.
    pub fn sign(
        &self,
        url: &str,
        nonce: &str,
        key_id: KeyIdentifier<'_>,
        payload: &str,
    ) -> Result<JwsMessage, PluginError> {
        let rng = SystemRandom::new();

        // 1. Construct protected header
        let protected_json = match key_id {
            KeyIdentifier::Jwk(jwk) => serde_json::json!({
                "alg": "ES256",
                "jwk": jwk,
                "nonce": nonce,
                "url": url,
            }),
            KeyIdentifier::Kid(kid) => serde_json::json!({
                "alg": "ES256",
                "kid": kid,
                "nonce": nonce,
                "url": url,
            }),
        };

        let protected_str = serde_json::to_string(&protected_json).map_err(|e| {
            PluginError::Config(format!("Failed to serialize protected header: {e}"))
        })?;

        let b64_protected = URL_SAFE_NO_PAD.encode(protected_str.as_bytes());
        let b64_payload = if payload.is_empty() {
            String::new()
        } else {
            URL_SAFE_NO_PAD.encode(payload.as_bytes())
        };

        // 2. Compute signature over `protected.payload` using one exactly-sized buffer
        // (no `format!` parsing, no intermediate String growth).
        let mut sign_input = Vec::with_capacity(b64_protected.len() + 1 + b64_payload.len());
        sign_input.extend_from_slice(b64_protected.as_bytes());
        sign_input.push(b'.');
        sign_input.extend_from_slice(b64_payload.as_bytes());
        let sig = self
            .key_pair
            .sign(&rng, &sign_input)
            .map_err(|e| PluginError::Config(format!("Failed to compute ECDSA signature: {e}")))?;

        let b64_sig = URL_SAFE_NO_PAD.encode(sig.as_ref());

        Ok(JwsMessage {
            protected: b64_protected,
            payload: b64_payload,
            signature: b64_sig,
        })
    }
}

/// Generates an ECDSA P-256 private key and Certificate Signing Request (CSR) DER
/// covering the requested domain names.
pub fn generate_csr_and_key(domains: &[String]) -> Result<(Vec<u8>, String), PluginError> {
    if domains.is_empty() {
        return Err(PluginError::Config(
            "CSR domains list must not be empty".into(),
        ));
    }

    // 1. Generate domain private key using ECDSA P-256
    let key_pair = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256)
        .map_err(|e| PluginError::Config(format!("Failed to generate domain key pair: {e}")))?;

    // 2. Build CertificateParams with SAN domains
    let params = rcgen::CertificateParams::new(domains.to_vec())
        .map_err(|e| PluginError::Config(format!("Failed to configure CSR params: {e}")))?;

    // 3. Serialize CSR into binary DER representation
    let csr = params
        .serialize_request(&key_pair)
        .map_err(|e| PluginError::Config(format!("Failed to serialize CSR to DER: {e}")))?;

    let private_key_pem = key_pair.serialize_pem();

    Ok((csr.der().to_vec(), private_key_pem))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn b64url_encode32_all_paths_match_reference() {
        // Deterministic pseudo-random inputs incl. bytes producing '-' (62) and '_' (63).
        let mut seed = 0x9e37_79b9_7f4a_7c15u64;
        for round in 0..2000 {
            let mut src = [0u8; 32];
            for b in src.iter_mut() {
                seed ^= seed << 13;
                seed ^= seed >> 7;
                seed ^= seed << 17;
                *b = seed as u8;
            }
            if round == 0 {
                src = [0xff; 32];
            }
            let reference = URL_SAFE_NO_PAD.encode(src);
            let mut scalar = [0u8; B64_32_LEN];
            b64url_encode32_scalar(&src, &mut scalar);
            assert_eq!(&scalar[..], reference.as_bytes());
            let mut simd = [0u8; B64_32_LEN];
            b64url_encode32(&src, &mut simd);
            assert_eq!(&simd[..], reference.as_bytes());
        }
    }

    #[test]
    fn thumbprint_matches_reference_format() {
        let key = AccountKey::generate().unwrap();
        let reference_json = format!(
            r#"{{"crv":"{}","kty":"{}","x":"{}","y":"{}"}}"#,
            key.jwk().crv,
            key.jwk().kty,
            key.jwk().x,
            key.jwk().y
        );
        assert_eq!(reference_json.len(), CANONICAL_JWK_LEN);
        let digest = ring::digest::digest(&ring::digest::SHA256, reference_json.as_bytes());
        assert_eq!(key.thumbprint(), URL_SAFE_NO_PAD.encode(digest.as_ref()));
        assert_eq!(key.jwk().x.len(), B64_32_LEN);
    }

    #[test]
    fn test_account_key_generation_and_thumbprint() {
        let key = AccountKey::generate().unwrap();
        assert!(!key.thumbprint().is_empty());
        assert_eq!(key.jwk().crv, "P-256");
        assert_eq!(key.jwk().kty, "EC");
        assert!(!key.jwk().x.is_empty());
        assert!(!key.jwk().y.is_empty());

        let pem = key.to_pem();
        assert!(pem.starts_with("-----BEGIN PRIVATE KEY-----"));
        assert!(pem.ends_with("-----END PRIVATE KEY-----\n"));

        // Roundtrip via PEM
        let key2 = AccountKey::from_pem(&pem).unwrap();
        assert_eq!(key.thumbprint(), key2.thumbprint());
    }

    #[test]
    fn test_key_authorization_format() {
        let key = AccountKey::generate().unwrap();
        let token = "test_token_123";
        let auth = key.key_authorization(token);
        assert_eq!(auth, format!("{token}.{}", key.thumbprint()));
    }

    #[test]
    fn test_jws_signing() {
        let key = AccountKey::generate().unwrap();
        let jws = key
            .sign(
                "https://acme.example.com/order",
                "nonce_abc123",
                KeyIdentifier::Jwk(key.jwk()),
                r#"{"termsOfServiceAgreed":true}"#,
            )
            .unwrap();

        assert!(!jws.protected.is_empty());
        assert!(!jws.payload.is_empty());
        assert_eq!(URL_SAFE_NO_PAD.decode(&jws.signature).unwrap().len(), 64);
    }

    #[test]
    fn test_csr_generation() {
        let domains = vec![
            "api.example.com".to_string(),
            "edge.example.com".to_string(),
        ];
        let (csr_der, key_pem) = generate_csr_and_key(&domains).unwrap();
        assert!(!csr_der.is_empty());
        assert!(key_pem.contains("PRIVATE KEY"));
    }
}
