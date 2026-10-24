//! In-memory dynamic SNI resolver.
//!
//! Enforces strict No-SNI and Unknown-SNI rejection policies.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::{ClientHello, ResolvesServerCert};
use rustls::sign::CertifiedKey;

use crate::error::TlsError;

/// In-memory SNI certificate resolver.
///
/// Matches incoming TLS `ClientHello` against pre-compiled certificates.
/// Supports both exact match (`api.example.com`) and wildcard match (`*.example.com`).
///
/// Under the strict reject policy:
/// - If ClientHello does not specify an SNI, returns `None` (handshake aborted).
/// - If ClientHello SNI is not found, returns `None` (handshake aborted).
pub struct SniResolver {
    exact_matches: HashMap<String, Arc<CertifiedKey>>,
    wildcard_matches: HashMap<String, Arc<CertifiedKey>>,
    default_key: Option<Arc<CertifiedKey>>,
}

impl fmt::Debug for SniResolver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SniResolver")
            .field("exact_count", &self.exact_matches.len())
            .field("wildcard_count", &self.wildcard_matches.len())
            .field("has_default", &self.default_key.is_some())
            .finish()
    }
}

impl Default for SniResolver {
    fn default() -> Self {
        Self::new()
    }
}

/// Canonical RFC 6125 wildcard domain prefix.
pub const WILDCARD_PREFIX: &str = "*.";

impl SniResolver {
    /// Creates a new empty `SniResolver`.
    pub fn new() -> Self {
        Self {
            exact_matches: HashMap::new(),
            wildcard_matches: HashMap::new(),
            default_key: None,
        }
    }

    /// Adds a certificate and private key under a list of SNI domain names.
    pub fn add_certificate(
        &mut self,
        snis: &[String],
        certs: Vec<CertificateDer<'static>>,
        key: PrivateKeyDer<'static>,
    ) -> Result<(), TlsError> {
        let signing_key = rustls::crypto::aws_lc_rs::sign::any_supported_type(&key)
            .map_err(|e| TlsError::InvalidPrivateKey(e.to_string()))?;

        let certified_key = Arc::new(CertifiedKey::new(certs, signing_key));
        if self.default_key.is_none() {
            self.default_key = Some(certified_key.clone());
        }

        for sni in snis {
            let s = sni.trim();
            if s.is_empty() {
                continue;
            }

            if s.starts_with(WILDCARD_PREFIX) {
                // Suffix after '*.' e.g. '*.example.com' -> 'example.com'
                let suffix = s.trim_start_matches(WILDCARD_PREFIX).to_string();
                self.wildcard_matches.insert(suffix, certified_key.clone());
            } else {
                self.exact_matches
                    .insert(s.to_string(), certified_key.clone());
            }
        }

        Ok(())
    }

    /// Resolves certified key for a given SNI string.
    ///
    /// Performs strictly case-sensitive matching on the exact target domain name,
    /// falling back to single-label wildcard subdomain match (RFC 6125 Section 6.4.3).
    pub fn lookup(&self, sni: &str) -> Option<Arc<CertifiedKey>> {
        let trimmed = sni.trim();

        // 1. Exact case-sensitive match
        if let Some(key) = self.exact_matches.get(trimmed) {
            return Some(key.clone());
        }

        // 2. Wildcard match (RFC 6125 Section 6.4.3: single-label subdomain only)
        if let Some(idx) = trimmed.find('.') {
            let parent_domain = &trimmed[idx + 1..];
            if let Some(key) = self.wildcard_matches.get(parent_domain) {
                return Some(key.clone());
            }
        }

        None
    }

    /// Returns the number of unique exact SNI mappings.
    pub fn exact_len(&self) -> usize {
        self.exact_matches.len()
    }

    /// Returns the number of unique wildcard SNI mappings.
    pub fn wildcard_len(&self) -> usize {
        self.wildcard_matches.len()
    }
}

impl ResolvesServerCert for SniResolver {
    fn resolve(&self, client_hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        match client_hello.server_name() {
            Some(server_name) => self.lookup(server_name),
            None => self.default_key.clone(),
        }
    }
}

/// In-memory SNI server configuration resolver for domain-level TLS & ALPN isolation.
#[derive(Clone, Debug)]
pub struct SniConfigResolver {
    exact_configs: HashMap<String, Arc<rustls::ServerConfig>>,
    wildcard_configs: HashMap<String, Arc<rustls::ServerConfig>>,
    pub default_config: Arc<rustls::ServerConfig>,
}

impl SniConfigResolver {
    /// Creates a new `SniConfigResolver` with a fallback default configuration.
    pub fn new(default_config: Arc<rustls::ServerConfig>) -> Self {
        Self {
            exact_configs: HashMap::new(),
            wildcard_configs: HashMap::new(),
            default_config,
        }
    }

    /// Adds an `Arc<ServerConfig>` mapped under the given SNI domain names.
    pub fn add_config(&mut self, snis: &[String], config: Arc<rustls::ServerConfig>) {
        for sni in snis {
            let s = sni.trim();
            if s.is_empty() {
                continue;
            }
            if s.starts_with(WILDCARD_PREFIX) {
                let suffix = s.trim_start_matches(WILDCARD_PREFIX).to_string();
                self.wildcard_configs.insert(suffix, config.clone());
            } else {
                self.exact_configs.insert(s.to_string(), config.clone());
            }
        }
    }

    /// Resolves the specific `ServerConfig` for the given SNI name.
    ///
    /// Performs strictly case-sensitive matching on the exact target domain name,
    /// falling back to single-label wildcard subdomain match (RFC 6125 Section 6.4.3).
    pub fn lookup(&self, sni: &str) -> Option<&Arc<rustls::ServerConfig>> {
        let trimmed = sni.trim();
        if let Some(cfg) = self.exact_configs.get(trimmed) {
            return Some(cfg);
        }
        if let Some(idx) = trimmed.find('.') {
            let parent = &trimmed[idx + 1..];
            if let Some(cfg) = self.wildcard_configs.get(parent) {
                return Some(cfg);
            }
        }
        None
    }

    /// Resolves the matching `ServerConfig`, falling back to `default_config` if not found or SNI is absent.
    pub fn resolve(&self, sni: Option<&str>) -> Arc<rustls::ServerConfig> {
        sni.and_then(|s| self.lookup(s))
            .unwrap_or(&self.default_config)
            .clone()
    }

    /// Returns the number of exact configurations.
    pub fn exact_len(&self) -> usize {
        self.exact_configs.len()
    }

    /// Returns the number of wildcard configurations.
    pub fn wildcard_len(&self) -> usize {
        self.wildcard_configs.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dummy_config() -> Arc<rustls::ServerConfig> {
        let resolver = SniResolver::new();
        Arc::new(
            rustls::ServerConfig::builder_with_provider(Arc::new(
                rustls::crypto::aws_lc_rs::default_provider(),
            ))
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_cert_resolver(Arc::new(resolver)),
        )
    }

    #[test]
    fn test_sni_config_resolver_strict_case_sensitive() {
        let default_cfg = dummy_config();
        let mut resolver = SniConfigResolver::new(default_cfg.clone());

        let cfg1 = dummy_config();
        resolver.add_config(&["Api.Example.Com".to_string()], cfg1.clone());

        // Exact case match succeeds
        assert!(resolver.lookup("Api.Example.Com").is_some());

        // Different case does NOT match, ensuring exact domain matching without lossy case folding
        assert!(resolver.lookup("api.example.com").is_none());
        assert!(resolver.lookup("API.EXAMPLE.COM").is_none());

        // resolve() returns specific config on exact match and fallback default otherwise
        assert!(Arc::ptr_eq(
            &resolver.resolve(Some("Api.Example.Com")),
            &cfg1
        ));
        assert!(Arc::ptr_eq(
            &resolver.resolve(Some("api.example.com")),
            &default_cfg
        ));
    }
}
