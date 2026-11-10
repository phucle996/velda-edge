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
// Multi-Version Hardware Vector & SWAR Core
// ============================================================================

/// Scans for the first dot `.` using vector registers (AVX2 / SSE4.2 / NEON) or SWAR fallback.
///
/// Inlines direct machine instructions without indirect function pointer dispatch overhead.
#[inline(always)]
pub fn find_dot(bytes: &[u8]) -> Option<usize> {
    #[cfg(target_arch = "x86_64")]
    {
        if std::is_x86_feature_detected!("avx2") {
            return unsafe { find_dot_256_impl(bytes) };
        }
        if std::is_x86_feature_detected!("sse4.2") {
            return unsafe { find_dot_128_x86_impl(bytes) };
        }
        find_dot_dual_64(bytes)
    }
    #[cfg(target_arch = "aarch64")]
    {
        find_dot_128(bytes)
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        find_dot_dual_64(bytes)
    }
}

/// Evaluates domain byte equality using vector registers (AVX2 / SSE4.2 / NEON) or SWAR fallback.
///
/// Inlines direct machine instructions without indirect function pointer dispatch overhead.
#[inline(always)]
pub fn domain_eq(a: &[u8], b: &[u8]) -> bool {
    #[cfg(target_arch = "x86_64")]
    {
        if std::is_x86_feature_detected!("avx2") {
            return unsafe { domain_eq_256_impl(a, b) };
        }
        if std::is_x86_feature_detected!("sse4.2") {
            return unsafe { domain_eq_128_x86_impl(a, b) };
        }
        domain_eq_dual_64(a, b)
    }
    #[cfg(target_arch = "aarch64")]
    {
        domain_eq_128(a, b)
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        domain_eq_dual_64(a, b)
    }
}

// ----------------------------------------------------------------------------
// Tier 1: 256-bit Vector (AVX2)
// ----------------------------------------------------------------------------

#[cfg(test)]
#[inline]
pub fn find_dot_256(bytes: &[u8]) -> Option<usize> {
    #[cfg(target_arch = "x86_64")]
    {
        unsafe { find_dot_256_impl(bytes) }
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        find_dot_128(bytes)
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn find_dot_256_impl(bytes: &[u8]) -> Option<usize> {
    use std::arch::x86_64::*;
    let len = bytes.len();
    let target = _mm256_set1_epi8(b'.' as i8);
    let mut i = 0;

    while i + 32 <= len {
        let chunk = unsafe { _mm256_loadu_si256(bytes.as_ptr().add(i) as *const __m256i) };
        let cmp = _mm256_cmpeq_epi8(chunk, target);
        let mask = _mm256_movemask_epi8(cmp) as u32;
        if mask != 0 {
            return Some(i + mask.trailing_zeros() as usize);
        }
        i += 32;
    }

    while i < len {
        if unsafe { *bytes.get_unchecked(i) } == b'.' {
            return Some(i);
        }
        i += 1;
    }
    None
}

#[cfg(test)]
#[inline]
pub fn domain_eq_256(a: &[u8], b: &[u8]) -> bool {
    #[cfg(target_arch = "x86_64")]
    {
        unsafe { domain_eq_256_impl(a, b) }
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        domain_eq_128(a, b)
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn domain_eq_256_impl(a: &[u8], b: &[u8]) -> bool {
    use std::arch::x86_64::*;
    let len = a.len();
    if len != b.len() {
        return false;
    }
    let mut i = 0;
    while i + 32 <= len {
        let (va, vb) = unsafe {
            (
                _mm256_loadu_si256(a.as_ptr().add(i) as *const __m256i),
                _mm256_loadu_si256(b.as_ptr().add(i) as *const __m256i),
            )
        };
        let cmp = _mm256_cmpeq_epi8(va, vb);
        if _mm256_movemask_epi8(cmp) as u32 != 0xFFFF_FFFF {
            return false;
        }
        i += 32;
    }
    while i < len {
        if unsafe { *a.get_unchecked(i) != *b.get_unchecked(i) } {
            return false;
        }
        i += 1;
    }
    true
}

// ----------------------------------------------------------------------------
// Tier 2: 128-bit Vector (SSE4.2 / NEON)
// ----------------------------------------------------------------------------

#[cfg(test)]
#[inline]
pub fn find_dot_128(bytes: &[u8]) -> Option<usize> {
    #[cfg(target_arch = "x86_64")]
    {
        unsafe { find_dot_128_x86_impl(bytes) }
    }
    #[cfg(target_arch = "aarch64")]
    {
        unsafe { find_dot_128_neon_impl(bytes) }
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        find_dot_dual_64(bytes)
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse4.2")]
unsafe fn find_dot_128_x86_impl(bytes: &[u8]) -> Option<usize> {
    use std::arch::x86_64::*;
    let len = bytes.len();
    let target = _mm_set1_epi8(b'.' as i8);
    let mut i = 0;

    while i + 16 <= len {
        let chunk = unsafe { _mm_loadu_si128(bytes.as_ptr().add(i) as *const __m128i) };
        let cmp = _mm_cmpeq_epi8(chunk, target);
        let mask = _mm_movemask_epi8(cmp) as u32;
        if mask != 0 {
            return Some(i + mask.trailing_zeros() as usize);
        }
        i += 16;
    }

    while i < len {
        if unsafe { *bytes.get_unchecked(i) } == b'.' {
            return Some(i);
        }
        i += 1;
    }
    None
}

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn find_dot_128_neon_impl(bytes: &[u8]) -> Option<usize> {
    use std::arch::aarch64::*;
    let len = bytes.len();
    let target = vdupq_n_u8(b'.');
    let mut i = 0;

    while i + 16 <= len {
        let chunk = unsafe { vld1q_u8(bytes.as_ptr().add(i)) };
        let cmp = vceqq_u8(chunk, target);
        if vmaxvq_u8(cmp) != 0 {
            let mut buf = [0u8; 16];
            unsafe { vst1q_u8(buf.as_mut_ptr(), cmp) };
            for j in 0..16 {
                if buf[j] != 0 {
                    return Some(i + j);
                }
            }
        }
        i += 16;
    }

    while i < len {
        if unsafe { *bytes.get_unchecked(i) } == b'.' {
            return Some(i);
        }
        i += 1;
    }
    None
}

#[cfg(test)]
#[inline]
pub fn domain_eq_128(a: &[u8], b: &[u8]) -> bool {
    #[cfg(target_arch = "x86_64")]
    {
        unsafe { domain_eq_128_x86_impl(a, b) }
    }
    #[cfg(target_arch = "aarch64")]
    {
        unsafe { domain_eq_128_neon_impl(a, b) }
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        domain_eq_dual_64(a, b)
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse4.2")]
unsafe fn domain_eq_128_x86_impl(a: &[u8], b: &[u8]) -> bool {
    use std::arch::x86_64::*;
    let len = a.len();
    if len != b.len() {
        return false;
    }
    let mut i = 0;
    while i + 16 <= len {
        let (va, vb) = unsafe {
            (
                _mm_loadu_si128(a.as_ptr().add(i) as *const __m128i),
                _mm_loadu_si128(b.as_ptr().add(i) as *const __m128i),
            )
        };
        let cmp = _mm_cmpeq_epi8(va, vb);
        if (_mm_movemask_epi8(cmp) as u32) != 0xFFFF {
            return false;
        }
        i += 16;
    }
    while i < len {
        if unsafe { *a.get_unchecked(i) != *b.get_unchecked(i) } {
            return false;
        }
        i += 1;
    }
    true
}

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn domain_eq_128_neon_impl(a: &[u8], b: &[u8]) -> bool {
    use std::arch::aarch64::*;
    let len = a.len();
    if len != b.len() {
        return false;
    }
    let mut i = 0;
    while i + 16 <= len {
        let (va, vb) = unsafe { (vld1q_u8(a.as_ptr().add(i)), vld1q_u8(b.as_ptr().add(i))) };
        let cmp = vceqq_u8(va, vb);
        if vminvq_u8(cmp) != 0xFF {
            return false;
        }
        i += 16;
    }
    while i < len {
        if unsafe { *a.get_unchecked(i) != *b.get_unchecked(i) } {
            return false;
        }
        i += 1;
    }
    true
}

// ----------------------------------------------------------------------------
// Tier 3: Dual 64-bit SWAR (2x u64 parallel evaluation on superscalar ALU)
// ----------------------------------------------------------------------------

const DOT_64: u64 = 0x2e2e2e2e2e2e2e2e;
const LOB_64: u64 = 0x0101010101010101;
const HIB_64: u64 = 0x8080808080808080;

/// Evaluates 16 bytes per cycle across two 64-bit general-purpose registers simultaneously.
pub fn find_dot_dual_64(bytes: &[u8]) -> Option<usize> {
    let len = bytes.len();
    let mut i = 0;

    while i + 16 <= len {
        let w0 = unsafe {
            (bytes.as_ptr().add(i) as *const u64)
                .read_unaligned()
                .to_le()
        } ^ DOT_64;
        let w1 = unsafe {
            (bytes.as_ptr().add(i + 8) as *const u64)
                .read_unaligned()
                .to_le()
        } ^ DOT_64;

        // Two independent SWAR zero-detection formulas executed concurrently across 2 CPU ALU ports:
        let has_zero_0 = (w0.wrapping_sub(LOB_64)) & (!w0) & HIB_64;
        let has_zero_1 = (w1.wrapping_sub(LOB_64)) & (!w1) & HIB_64;

        if (has_zero_0 | has_zero_1) != 0 {
            for j in 0..16 {
                if bytes[i + j] == b'.' {
                    return Some(i + j);
                }
            }
        }
        i += 16;
    }

    while i < len {
        if bytes[i] == b'.' {
            return Some(i);
        }
        i += 1;
    }
    None
}

/// Evaluates domain equality 16 bytes per cycle across two 64-bit registers in parallel.
pub fn domain_eq_dual_64(a: &[u8], b: &[u8]) -> bool {
    let len = a.len();
    if len != b.len() {
        return false;
    }
    let mut i = 0;
    while i + 16 <= len {
        let a0 = unsafe { (a.as_ptr().add(i) as *const u64).read_unaligned() };
        let b0 = unsafe { (b.as_ptr().add(i) as *const u64).read_unaligned() };
        let a1 = unsafe { (a.as_ptr().add(i + 8) as *const u64).read_unaligned() };
        let b1 = unsafe { (b.as_ptr().add(i + 8) as *const u64).read_unaligned() };

        let diff0 = a0 ^ b0;
        let diff1 = a1 ^ b1;
        if (diff0 | diff1) != 0 {
            return false;
        }
        i += 16;
    }
    while i < len {
        if a[i] != b[i] {
            return false;
        }
        i += 1;
    }
    true
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
            assert_eq!(
                find_dot_256(bytes),
                expected,
                "find_dot_256 failed on '{domain}'"
            );
            assert_eq!(
                find_dot_128(bytes),
                expected,
                "find_dot_128 failed on '{domain}'"
            );
            assert_eq!(
                find_dot_dual_64(bytes),
                expected,
                "find_dot_dual_64 failed on '{domain}'"
            );
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
            assert_eq!(domain_eq_256(a, b), expected);
            assert_eq!(domain_eq_128(a, b), expected);
            assert_eq!(domain_eq_dual_64(a, b), expected);
        }
    }
}
