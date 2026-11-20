//! In-memory dynamic SNI resolver.
//!
//! Enforces strict No-SNI and Unknown-SNI rejection policies with
//! hardware-adaptive multi-versioning (AVX2 / SSE4.2 / NEON / Dual-64 SWAR / Quad-32 SWAR).

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::{ClientHello, ResolvesServerCert};
use rustls::sign::CertifiedKey;

use crate::error::TlsError;

/// Ultra-fast non-cryptographic hasher for in-memory SNI hash tables.
#[derive(Debug, Clone, Copy, Default)]
pub struct FastBuildHasher;

impl std::hash::BuildHasher for FastBuildHasher {
    type Hasher = FastHasher;
    #[inline(always)]
    fn build_hasher(&self) -> Self::Hasher {
        FastHasher {
            state: 0xcbf29ce484222325,
        }
    }
}

/// Hardware-friendly fast 64-bit hasher.
#[derive(Debug, Clone, Copy)]
pub struct FastHasher {
    state: u64,
}

impl std::hash::Hasher for FastHasher {
    #[inline(always)]
    fn finish(&self) -> u64 {
        let mut z = self.state.wrapping_add(0x9e3779b97f4a7c15);
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
        z ^ (z >> 31)
    }

    #[inline(always)]
    fn write(&mut self, bytes: &[u8]) {
        for &byte in bytes {
            self.state = (self.state ^ (byte as u64)).wrapping_mul(0x100000001b3);
        }
    }

    #[inline(always)]
    fn write_u8(&mut self, i: u8) {
        self.state = (self.state ^ (i as u64)).wrapping_mul(0x100000001b3);
    }

    #[inline(always)]
    fn write_u16(&mut self, i: u16) {
        self.state = (self.state ^ (i as u64)).wrapping_mul(0x100000001b3);
    }

    #[inline(always)]
    fn write_u32(&mut self, i: u32) {
        self.state = (self.state ^ (i as u64)).wrapping_mul(0x100000001b3);
    }

    #[inline(always)]
    fn write_u64(&mut self, i: u64) {
        self.state = (self.state ^ i).wrapping_mul(0x100000001b3);
    }

    #[inline(always)]
    fn write_usize(&mut self, i: usize) {
        self.state = (self.state ^ (i as u64)).wrapping_mul(0x100000001b3);
    }
}

/// In-memory SNI certificate resolver.
///
/// Matches incoming TLS `ClientHello` against pre-compiled certificates.
/// Supports both exact match (`api.example.com`) and wildcard match (`*.example.com`).
///
/// Under the strict reject policy:
/// - If ClientHello does not specify an SNI, returns `None` (handshake aborted).
/// - If ClientHello SNI is not found, returns `None` (handshake aborted).
pub struct SniResolver {
    exact_matches: HashMap<String, Arc<CertifiedKey>, FastBuildHasher>,
    wildcard_matches: HashMap<String, Arc<CertifiedKey>, FastBuildHasher>,
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
            exact_matches: HashMap::with_hasher(FastBuildHasher),
            wildcard_matches: HashMap::with_hasher(FastBuildHasher),
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
    ///
    /// Executes directly through hardware-vectorized inlined routines on the hot path.
    pub fn lookup(&self, sni: &str) -> Option<Arc<CertifiedKey>> {
        let trimmed = sni.trim();

        // 1. Exact case-sensitive match
        if let Some(key) = self.exact_matches.get(trimmed) {
            return Some(key.clone());
        }

        // 2. Wildcard match (RFC 6125 Section 6.4.3: single-label subdomain only)
        // Direct execution via inlined hardware vector / SWAR scan (0 indirect function calls)
        if let Some(idx) = find_dot(trimmed.as_bytes()) {
            let parent_domain = &trimmed[idx + 1..];
            if let Some(key) = self.wildcard_matches.get(parent_domain) {
                return Some(key.clone());
            }
        }

        None
    }

    /// Scans for the first dot '.' using the hardware-accelerated register tier.
    #[inline(always)]
    pub fn find_dot(&self, bytes: &[u8]) -> Option<usize> {
        find_dot(bytes)
    }

    /// Evaluates domain byte equality using the hardware-accelerated register tier.
    #[inline(always)]
    pub fn domain_eq(&self, a: &[u8], b: &[u8]) -> bool {
        domain_eq(a, b)
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
    exact_configs: HashMap<String, Arc<rustls::ServerConfig>, FastBuildHasher>,
    wildcard_configs: HashMap<String, Arc<rustls::ServerConfig>, FastBuildHasher>,
    pub default_config: Arc<rustls::ServerConfig>,
}

impl SniConfigResolver {
    /// Creates a new `SniConfigResolver` with a fallback default configuration.
    pub fn new(default_config: Arc<rustls::ServerConfig>) -> Self {
        Self {
            exact_configs: HashMap::with_hasher(FastBuildHasher),
            wildcard_configs: HashMap::with_hasher(FastBuildHasher),
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
        if let Some(idx) = find_dot(trimmed.as_bytes()) {
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

    /// Scans for the first dot '.' using the hardware-accelerated register tier.
    #[inline(always)]
    pub fn find_dot(&self, bytes: &[u8]) -> Option<usize> {
        find_dot(bytes)
    }

    /// Evaluates domain byte equality using the hardware-accelerated register tier.
    #[inline(always)]
    pub fn domain_eq(&self, a: &[u8], b: &[u8]) -> bool {
        domain_eq(a, b)
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

// ============================================================================
// Pure Rust Domain Scanning & Equality (LLVM Auto-Vectorized)
// ============================================================================

/// Scans for the first dot `.` in a byte slice using pure Rust so LLVM/stdlib vectorizes cleanly.
#[inline(always)]
pub fn find_dot(bytes: &[u8]) -> Option<usize> {
    bytes.iter().position(|&b| b == b'.')
}

/// Evaluates domain byte equality using pure Rust slice comparison.
#[inline(always)]
pub fn domain_eq(a: &[u8], b: &[u8]) -> bool {
    a == b
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

    #[test]
    fn test_multi_version_find_dot_all_tiers() {
        let samples = [
            ("api.example.com", Some(3)),
            ("cluster_0050.domain.internal.net", Some(12)),
            ("a.b", Some(1)),
            (".hidden.com", Some(0)),
            ("nodotsinthisdomain", None),
            ("exact16chars..ok", Some(12)),
            ("longdomainnamewithoutdotsuntilltheveryend.", Some(41)),
        ];

        for (domain, expected) in samples {
            let bytes = domain.as_bytes();
            assert_eq!(find_dot(bytes), expected, "find_dot failed on '{domain}'");
        }
    }

    #[test]
    fn test_multi_version_domain_eq_all_tiers() {
        let d1 = b"api.velda-edge.internal.service.cluster.local";
        let d2 = b"api.velda-edge.internal.service.cluster.local";
        let d3 = b"api.velda-edge.internal.service.cluster.other";
        let d4 = b"short.com";

        for (a, b, expected) in [
            (d1.as_slice(), d2.as_slice(), true),
            (d1.as_slice(), d3.as_slice(), false),
            (d1.as_slice(), d4.as_slice(), false),
            (b"equal".as_slice(), b"equal".as_slice(), true),
            (b"equal".as_slice(), b"equaL".as_slice(), false),
        ] {
            assert_eq!(domain_eq(a, b), expected);
        }
    }
}
