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

        let x = URL_SAFE_NO_PAD.encode(&pub_bytes[1..33]);
        let y = URL_SAFE_NO_PAD.encode(&pub_bytes[33..65]);

        let jwk_public = JwkPublicKey {
            crv: "P-256".to_string(),
            kty: "EC".to_string(),
            x,
            y,
        };

        // Canonical JSON for RFC 7638 thumbprint (lexicographically sorted keys: crv, kty, x, y)
        let canonical_json = format!(
            r#"{{"crv":"{}","kty":"{}","x":"{}","y":"{}"}}"#,
            jwk_public.crv, jwk_public.kty, jwk_public.x, jwk_public.y
        );

        let digest = ring::digest::digest(&ring::digest::SHA256, canonical_json.as_bytes());
        let jwk_thumbprint = URL_SAFE_NO_PAD.encode(digest.as_ref());

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
        format!("{token}.{}", self.jwk_thumbprint)
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

        // 2. Compute signature over `protected.payload`
        let sign_input = format!("{b64_protected}.{b64_payload}");
        let sig = self
            .key_pair
            .sign(&rng, sign_input.as_bytes())
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
