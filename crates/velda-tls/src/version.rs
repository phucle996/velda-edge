use rustls::crypto::CryptoProvider;
use std::sync::{Arc, LazyLock};

/// Global hardware-tuned [`CryptoProvider`] singleton.
///
/// Uses compile-time target features to determine hardware AES support:
/// - On targets compiled with hardware AES + Carry-less Multiplication, prioritizes AES-GCM suites.
/// - Otherwise promotes `CHACHA20_POLY1305` to the top of the preference list.
static HARDWARE_CRYPTO_PROVIDER: LazyLock<Arc<CryptoProvider>> = LazyLock::new(|| {
    let mut provider = rustls::crypto::aws_lc_rs::default_provider();

    #[cfg(any(
        all(
            target_arch = "x86_64",
            target_feature = "aes",
            target_feature = "pclmulqdq"
        ),
        all(target_arch = "aarch64", target_feature = "aes")
    ))]
    let has_hw_aes = true;

    #[cfg(not(any(
        all(
            target_arch = "x86_64",
            target_feature = "aes",
            target_feature = "pclmulqdq"
        ),
        all(target_arch = "aarch64", target_feature = "aes")
    )))]
    let has_hw_aes = false;

    if !has_hw_aes {
        // Promote ChaCha20-Poly1305 suites (which execute in pure ARX vector/scalar registers)
        // ahead of AES-GCM when hardware AES instructions are absent.
        provider.cipher_suites.sort_by_key(|suite| {
            let name = format!("{:?}", suite.suite());
            if name.contains("CHACHA20") { 0 } else { 1 }
        });
    }

    Arc::new(provider)
});

/// Returns a clone of the process-wide hardware-tuned [`CryptoProvider`] `Arc`.
#[inline]
pub fn hardware_crypto_provider() -> Arc<CryptoProvider> {
    Arc::clone(&HARDWARE_CRYPTO_PROVIDER)
}

use rustls::version::{TLS12, TLS13};
use rustls::{DEFAULT_VERSIONS, SupportedProtocolVersion};

use crate::error::TlsError;

/// Resolves user-configured protocol version strings (e.g. "tls1.2", "tls1.3")
/// into rustls [`SupportedProtocolVersion`] pointers.
///
/// If `versions` is empty, returns rustls safe default protocol versions (TLS 1.3 and TLS 1.2).
/// If any version string is unrecognized or unsupported, returns [`TlsError::UnsupportedProtocolVersion`].
pub fn resolve_protocol_versions(
    versions: &[String],
) -> Result<Vec<&'static SupportedProtocolVersion>, TlsError> {
    if versions.is_empty() {
        return Ok(DEFAULT_VERSIONS.to_vec());
    }

    let mut has_tls12 = false;
    let mut has_tls13 = false;

    for v in versions {
        let trimmed = v.trim().to_ascii_lowercase();
        match trimmed.as_str() {
            "tls1.2" | "tls12" | "1.2" => has_tls12 = true,
            "tls1.3" | "tls13" | "1.3" => has_tls13 = true,
            other => return Err(TlsError::UnsupportedProtocolVersion(other.to_string())),
        }
    }

    let mut result = Vec::with_capacity(2);
    // Prioritize TLS 1.3 first in negotiation order
    if has_tls13 {
        result.push(&TLS13);
    }
    if has_tls12 {
        result.push(&TLS12);
    }

    if result.is_empty() {
        return Ok(DEFAULT_VERSIONS.to_vec());
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_versions_fallback() {
        let empty = Vec::new();
        let resolved = resolve_protocol_versions(&empty).expect("default resolution");
        assert_eq!(resolved.len(), 2);
        assert_eq!(resolved[0], &TLS13);
        assert_eq!(resolved[1], &TLS12);
    }

    #[test]
    fn test_resolve_tls13_only() {
        let v = vec!["tls1.3".to_string()];
        let resolved = resolve_protocol_versions(&v).expect("tls1.3");
        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0], &TLS13);
    }

    #[test]
    fn test_resolve_tls12_only() {
        let v = vec!["tls1.2".to_string()];
        let resolved = resolve_protocol_versions(&v).expect("tls1.2");
        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0], &TLS12);
    }

    #[test]
    fn test_versions_deduplicate() {
        let v = vec![
            "tls1.2".to_string(),
            "TLS1.3".to_string(),
            "tls1.2".to_string(),
        ];
        let resolved = resolve_protocol_versions(&v).expect("both");
        assert_eq!(resolved.len(), 2);
        assert_eq!(resolved[0], &TLS13);
        assert_eq!(resolved[1], &TLS12);
    }

    #[test]
    fn test_resolve_unsupported_version_fails() {
        let v = vec!["tls1.0".to_string()];
        let err = resolve_protocol_versions(&v).unwrap_err();
        match err {
            TlsError::UnsupportedProtocolVersion(v) => assert_eq!(v, "tls1.0"),
            _ => panic!("Expected UnsupportedProtocolVersion, got {:?}", err),
        }
    }
}
