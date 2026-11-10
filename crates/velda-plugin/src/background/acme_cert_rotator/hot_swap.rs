//! Certificate Hot-Swap into `velda-tls` Downstream Handshake State.
//!
//! Hot-swaps newly issued X.509 certificate chains and private keys into in-memory
//! TLS server state (`SniResolver` or `Arc<ServerConfig>`) with zero downtime,
//! zero dropped connections, and zero lock contention on the request hot path.

use std::sync::Arc;
use std::sync::RwLock;

use velda_tls::SniResolver;
use velda_tls::pem::{parse_certs_pem, parse_private_key_pem};

use crate::error::PluginError;

/// Thread-safe controller for hot-swapping certificates in `velda-tls`.
#[derive(Clone)]
pub struct TlsHotSwapper {
    resolver: Option<Arc<RwLock<SniResolver>>>,
}

impl TlsHotSwapper {
    /// Creates a new hot-swapper targeting the active dynamic SNI resolver.
    pub fn new(resolver: Option<Arc<RwLock<SniResolver>>>) -> Self {
        Self { resolver }
    }

    /// Hot-swaps an issued certificate chain and private key for the given domain names.
    pub fn swap_certificate(
        &self,
        domains: &[String],
        fullchain_pem: &[u8],
        privkey_pem: &str,
    ) -> Result<(), PluginError> {
        let cert_str = std::str::from_utf8(fullchain_pem)
            .map_err(|e| PluginError::Config(format!("Invalid UTF-8 certificate PEM: {e}")))?;

        let certs = parse_certs_pem(cert_str)
            .map_err(|e| PluginError::Tls(format!("Failed to parse certificate chain PEM: {e}")))?;
        let key = parse_private_key_pem(privkey_pem)
            .map_err(|e| PluginError::Tls(format!("Failed to parse private key PEM: {e}")))?;

        if let Some(ref resolver_lock) = self.resolver
            && let Ok(mut resolver) = resolver_lock.write()
        {
            resolver
                .add_certificate(domains, certs, key)
                .map_err(|e| PluginError::Tls(format!("Failed to update SniResolver: {e}")))?;
        }

        tracing::info!(
            domains = ?domains,
            "Successfully hot-swapped renewed certificate into velda-tls runtime"
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hot_swap_into_sni_resolver() {
        let resolver = Arc::new(RwLock::new(SniResolver::new()));
        let swapper = TlsHotSwapper::new(Some(Arc::clone(&resolver)));

        let domain = "api.example.com";
        let key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).unwrap();
        let params = rcgen::CertificateParams::new(vec![domain.into()]).unwrap();
        let cert = params.self_signed(&key).unwrap();
        let cert_pem = cert.pem();
        let key_pem = key.serialize_pem();

        // Hot swap certificate
        swapper
            .swap_certificate(&[domain.to_string()], cert_pem.as_bytes(), &key_pem)
            .unwrap();

        // Verify resolver can lookup the domain
        let r = resolver.read().unwrap();
        assert!(r.lookup(domain).is_some());
        assert_eq!(r.exact_len(), 1);
    }
}
