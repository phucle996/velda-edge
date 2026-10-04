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
}

impl fmt::Debug for SniResolver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SniResolver")
            .field("exact_count", &self.exact_matches.len())
            .field("wildcard_count", &self.wildcard_matches.len())
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

        for sni in snis {
            let s = sni.trim().to_ascii_lowercase();
            if s.is_empty() {
                continue;
            }

            if s.starts_with(WILDCARD_PREFIX) {
                // Suffix after '*.' e.g. '*.example.com' -> 'example.com'
                let suffix = s.trim_start_matches(WILDCARD_PREFIX).to_string();
                self.wildcard_matches.insert(suffix, certified_key.clone());
            } else {
                self.exact_matches.insert(s, certified_key.clone());
            }
        }

        Ok(())
    }

    /// Resolves certified key for a given SNI string.
    pub fn lookup(&self, sni: &str) -> Option<Arc<CertifiedKey>> {
        let trimmed = sni.trim();

        // Zero-allocation fast-path: ClientHello SNIs are almost universally lowercase ASCII.
        // Validating ASCII in-place directly on registers avoids allocating a String on the hot path.
        if !trimmed.bytes().any(|b| b.is_ascii_uppercase()) {
            // 1. Exact match
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

            return None;
        }

        // Slow-path: client sent uppercase SNI, allocate once to normalize
        let lower = trimmed.to_ascii_lowercase();
        if let Some(key) = self.exact_matches.get(&lower) {
            return Some(key.clone());
        }
        if let Some(idx) = lower.find('.') {
            let parent_domain = &lower[idx + 1..];
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
        let server_name = client_hello.server_name()?;
        self.lookup(server_name)
    }
}
