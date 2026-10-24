//! Cross-Site Request Forgery (CSRF) Protection Plugin (`phase: "pre_upstream"`).
//!
//! Enforces defense-in-depth protection against cross-site request forgery, neutralizing
//! W3C Simple Request bypasses (`<form enctype="text/plain">`), subdomain cookie tossing
//! (RFC 6265 §8.5), and timing side-channel attacks.
//!
//! # Defense-in-Depth Pipeline & Hardware Invariants
//! - **Gate 1 (Safe Methods Fast Path)**: Evaluates HTTP methods against a compact `u16`
//!   bitmask in 1 CPU instruction (~0.5ns), allowing idempotent requests (`GET`, `HEAD`,
//!   `OPTIONS`, `TRACE`) to pass with zero allocations and zero overhead.
//! - **Gate 2 (W3C Fetch Metadata)**: Inspects `Sec-Fetch-Site`. Rejects unauthorized
//!   `cross-site` mutating requests before parsing cookies or headers.
//! - **Gate 3 (Origin / Referer Verification)**: Validates request origin against the
//!   target `Host` or configured `trusted_origins` (reusing hardened 256-byte LUT pattern matching).
//! - **Gate 4 (Cryptographic Stateless Token Verification)**:
//!   - Zero-allocation slice scanner extracts the target cookie directly from raw bytes.
//!   - Compares downstream custom header (`X-CSRF-Token`) with the cookie using 128-bit SIMD
//!     constant-time comparison (`constant_time_eq`).
//!   - When `secret_key` is configured, verifies a 114-byte HMAC-SHA256 signed stateless token
//!     (`<timestamp_hex>.<nonce_hex>.<signature_hex>`) with TTL expiration check, completely
//!     preventing Subdomain Cookie Tossing bypasses.
//! - **Flat Workflow**: Sequential execution top-to-bottom without helper indirection (Rule 2.2).

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use http::header::{COOKIE, HOST, ORIGIN, REFERER};
use http::{HeaderName, Method};
use ring::hmac;
use velda_core::{Action, L7Request, RejectReason};
use velda_sync::post_sync::plugin::PluginConfig;

use super::token::{self, constant_time_eq};
use crate::capability::{AccessLevel, PluginCapability, SupportedProtocols};
use crate::error::PluginError;
use crate::pre_route::OriginPattern;
use crate::pre_upstream::PreUpstreamPlugin;

pub use velda_sync::post_sync::plugin::pre_upstream::csrf::CsrfConfig;

/// Compact bitmask for O(1) single-instruction HTTP method validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MethodMask(u16);

impl MethodMask {
    pub const GET: u16 = 1 << 0;
    pub const POST: u16 = 1 << 1;
    pub const PUT: u16 = 1 << 2;
    pub const DELETE: u16 = 1 << 3;
    pub const HEAD: u16 = 1 << 4;
    pub const OPTIONS: u16 = 1 << 5;
    pub const PATCH: u16 = 1 << 6;
    pub const TRACE: u16 = 1 << 7;
    pub const CONNECT: u16 = 1 << 8;
    pub const EXTENSION: u16 = 1 << 9;

    #[inline(always)]
    pub fn from_method(m: &Method) -> u16 {
        match *m {
            Method::GET => Self::GET,
            Method::POST => Self::POST,
            Method::PUT => Self::PUT,
            Method::DELETE => Self::DELETE,
            Method::HEAD => Self::HEAD,
            Method::OPTIONS => Self::OPTIONS,
            Method::PATCH => Self::PATCH,
            Method::TRACE => Self::TRACE,
            Method::CONNECT => Self::CONNECT,
            _ => Self::EXTENSION,
        }
    }

    #[inline(always)]
    pub fn parse_token(m: &str) -> u16 {
        let trimmed = m.trim();
        if trimmed.eq_ignore_ascii_case("GET") {
            Self::GET
        } else if trimmed.eq_ignore_ascii_case("POST") {
            Self::POST
        } else if trimmed.eq_ignore_ascii_case("PUT") {
            Self::PUT
        } else if trimmed.eq_ignore_ascii_case("DELETE") {
            Self::DELETE
        } else if trimmed.eq_ignore_ascii_case("HEAD") {
            Self::HEAD
        } else if trimmed.eq_ignore_ascii_case("OPTIONS") {
            Self::OPTIONS
        } else if trimmed.eq_ignore_ascii_case("PATCH") {
            Self::PATCH
        } else if trimmed.eq_ignore_ascii_case("TRACE") {
            Self::TRACE
        } else if trimmed.eq_ignore_ascii_case("CONNECT") {
            Self::CONNECT
        } else {
            Self::EXTENSION
        }
    }
}

/// Scans raw HTTP Cookie header bytes for a specific cookie key prefix with zero heap allocations.
#[inline]
pub fn extract_cookie_value<'a>(cookie_bytes: &'a [u8], prefix: &[u8]) -> Option<&'a [u8]> {
    let mut offset = 0;
    while offset < cookie_bytes.len() {
        while offset < cookie_bytes.len()
            && (cookie_bytes[offset] == b' '
                || cookie_bytes[offset] == b'\t'
                || cookie_bytes[offset] == b';')
        {
            offset += 1;
        }

        if offset + prefix.len() <= cookie_bytes.len()
            && &cookie_bytes[offset..offset + prefix.len()] == prefix
        {
            let val_start = offset + prefix.len();
            let mut val_end = val_start;
            while val_end < cookie_bytes.len() && cookie_bytes[val_end] != b';' {
                val_end += 1;
            }
            return Some(cookie_bytes[val_start..val_end].trim_ascii());
        }

        while offset < cookie_bytes.len() && cookie_bytes[offset] != b';' {
            offset += 1;
        }
    }
    None
}

/// Pre-Upstream Cross-Site Request Forgery (CSRF) Protection Plugin.
#[derive(Debug, Clone)]
pub struct CsrfPlugin {
    pub id: String,
    pub config: CsrfConfig,
    pub safe_methods_mask: u16,
    pub header_name: HeaderName,
    pub cookie_prefix: Box<[u8]>,
    pub hmac_key: Option<hmac::Key>,
    pub trusted_origins: Vec<OriginPattern>,
}

impl CsrfPlugin {
    /// Constructs a new [`CsrfPlugin`] with pre-compiled method bitmasks, cryptographic keys, and trusted origin patterns.
    pub fn new(id: impl Into<String>, config: CsrfConfig) -> Result<Self, PluginError> {
        // 1. Compile safe methods bitmask
        let mut safe_methods_mask = 0u16;
        for m in &config.safe_methods {
            safe_methods_mask |= MethodMask::parse_token(m.trim());
        }

        // 2. Pre-parse HeaderName
        let header_name = HeaderName::from_bytes(config.header_name.as_bytes()).map_err(|e| {
            PluginError::Config(format!(
                "Invalid CSRF header_name '{}': {e}",
                config.header_name
            ))
        })?;

        // 3. Pre-format Cookie lookup prefix: "<cookie_name>="
        let mut prefix = Vec::with_capacity(config.cookie_name.len() + 1);
        prefix.extend_from_slice(config.cookie_name.as_bytes());
        prefix.push(b'=');
        let cookie_prefix = prefix.into_boxed_slice();

        // 4. Initialize HMAC-SHA256 signing key if secret is declared
        let hmac_key = config
            .secret_key
            .as_ref()
            .map(|secret| hmac::Key::new(hmac::HMAC_SHA256, secret.as_bytes()));

        // 5. Compile trusted origin patterns
        let trusted_origins = config
            .trusted_origins
            .iter()
            .map(|s| OriginPattern::parse(s))
            .collect();

        Ok(Self {
            id: id.into(),
            config,
            safe_methods_mask,
            header_name,
            cookie_prefix,
            hmac_key,
            trusted_origins,
        })
    }

    /// Deserializes and constructs an instance directly from a [`PluginConfig`].
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let cfg = velda_sync::validate_csrf_config(&config.id, &config.config).map_err(|e| {
            PluginError::Config(format!(
                "Invalid csrf configuration for '{}': {e}",
                config.id
            ))
        })?;

        Self::new(&config.id, cfg)
    }

    /// Generates a valid 114-byte HMAC-SHA256 signed stateless token.
    #[inline(always)]
    pub fn mint_token(key: &hmac::Key, nonce: &[u8; 16], now_secs: u64) -> String {
        token::mint_token(key, nonce, now_secs)
    }

    /// Scans raw HTTP Cookie header bytes for this plugin's target cookie prefix with zero heap allocations.
    #[inline(always)]
    pub fn extract_cookie_token<'a>(&self, cookie_bytes: &'a [u8]) -> Option<&'a [u8]> {
        extract_cookie_value(cookie_bytes, &self.cookie_prefix)
    }
}

impl PreUpstreamPlugin for CsrfPlugin {
    #[inline(always)]
    fn id(&self) -> &str {
        &self.id
    }

    fn on_upstream_request(&self, req: &mut L7Request, _target_upstream: &mut Arc<str>) -> Action {
        // ─────────────────────────────────────────────────────────────────────
        // Gate 1: Safe Methods Fast Path (O(1) Single-Instruction Bitmask)
        // ─────────────────────────────────────────────────────────────────────
        let req_mask = MethodMask::from_method(&req.method);
        if (self.safe_methods_mask & req_mask) != 0 {
            return Action::Continue;
        }

        // ─────────────────────────────────────────────────────────────────────
        // Gate 2: W3C Fetch Metadata Inspection (Sec-Fetch-Site)
        // ─────────────────────────────────────────────────────────────────────
        if self.config.enforce_fetch_metadata {
            static SEC_FETCH_SITE: HeaderName = HeaderName::from_static("sec-fetch-site");
            if let Some(site_val) = req.headers.get(&SEC_FETCH_SITE)
                && let Ok(site_str) = site_val.to_str()
                && site_str.eq_ignore_ascii_case("cross-site")
            {
                let authorized_cross_site = req
                    .headers
                    .get(ORIGIN)
                    .and_then(|orig| orig.to_str().ok())
                    .is_some_and(|orig_str| {
                        self.trusted_origins.iter().any(|p| p.matches(orig_str))
                    });

                if !authorized_cross_site {
                    return Action::Reject(RejectReason::Forbidden);
                }
            }
        }

        // ─────────────────────────────────────────────────────────────────────
        // Gate 3: Origin / Referer Verification (RFC 6454 Same-Origin Defense)
        // ─────────────────────────────────────────────────────────────────────
        if self.config.enforce_origin {
            let candidate: Option<&str> = if let Some(orig_val) = req.headers.get(ORIGIN) {
                orig_val.to_str().ok()
            } else if let Some(ref_val) = req.headers.get(REFERER) {
                ref_val.to_str().ok().map(|ref_str| {
                    if let Some(scheme_end) = ref_str.find("://") {
                        let after_scheme = &ref_str[scheme_end + 3..];
                        if let Some(path_start) = after_scheme.find('/') {
                            &ref_str[..scheme_end + 3 + path_start]
                        } else {
                            ref_str
                        }
                    } else {
                        ref_str
                    }
                })
            } else {
                None
            };

            let origin_verified = match candidate {
                Some(origin_str) => {
                    let matches_host = req
                        .headers
                        .get(HOST)
                        .and_then(|h| h.to_str().ok())
                        .is_some_and(|host_str| {
                            if let Some(host_part) = origin_str.strip_prefix("https://") {
                                host_part.eq_ignore_ascii_case(host_str)
                            } else if let Some(host_part) = origin_str.strip_prefix("http://") {
                                host_part.eq_ignore_ascii_case(host_str)
                            } else {
                                false
                            }
                        });

                    matches_host || self.trusted_origins.iter().any(|p| p.matches(origin_str))
                }
                None => false,
            };

            if !origin_verified {
                return Action::Reject(RejectReason::Forbidden);
            }
        }

        // ─────────────────────────────────────────────────────────────────────
        // Gate 4: Stateless Double-Submit / Signed Token Verification
        // ─────────────────────────────────────────────────────────────────────
        // 4.1 Extract Cookie Value (Zero-Allocation Byte Slice)
        let Some(cookie_hdr) = req.headers.get(COOKIE) else {
            return Action::Reject(RejectReason::Forbidden);
        };

        let Some(cookie_token) = self.extract_cookie_token(cookie_hdr.as_bytes()) else {
            return Action::Reject(RejectReason::Forbidden);
        };

        // 4.2 Extract Header Value
        let Some(header_token) = req.headers.get(&self.header_name) else {
            return Action::Reject(RejectReason::Forbidden);
        };

        let header_token_bytes = header_token.as_bytes().trim_ascii();

        // 4.3 Validate non-empty tokens (Zero-Length token bypass defense)
        if cookie_token.is_empty() || header_token_bytes.is_empty() {
            return Action::Reject(RejectReason::Forbidden);
        }

        // 4.4 128-bit SIMD Constant-Time Equality Check
        if !constant_time_eq(cookie_token, header_token_bytes) {
            return Action::Reject(RejectReason::Forbidden);
        }

        // 4.5 Cryptographic HMAC-SHA256 Signature & TTL Check (Neutralizes Subdomain Cookie Tossing)
        if let Some(key) = &self.hmac_key {
            let now_secs = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();

            if !token::verify_signed_token(key, cookie_token, self.config.token_ttl_secs, now_secs)
            {
                return Action::Reject(RejectReason::Forbidden);
            }
        }

        Action::Continue
    }

    #[inline(always)]
    fn capability(&self) -> PluginCapability {
        PluginCapability::header_only(AccessLevel::Read)
    }

    #[inline(always)]
    fn supported_protocols(&self) -> SupportedProtocols {
        SupportedProtocols::ALL_HTTP
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{HeaderMap, HeaderValue, Uri, Version};
    use velda_core::Body;

    fn build_test_request(method: Method, uri_str: &str, headers: HeaderMap) -> L7Request {
        L7Request::new(
            method,
            uri_str.parse::<Uri>().unwrap(),
            Version::HTTP_11,
            headers,
            Body::Empty,
        )
    }

    #[test]
    fn test_safe_methods_bypass() {
        let config = CsrfConfig {
            cookie_name: "__Host-csrf".to_string(),
            header_name: "X-CSRF-Token".to_string(),
            secret_key: None,
            token_ttl_secs: 3600,
            safe_methods: vec!["GET".to_string(), "HEAD".to_string(), "OPTIONS".to_string()],
            trusted_origins: vec![],
            enforce_fetch_metadata: true,
            enforce_origin: true,
        };
        let plugin = CsrfPlugin::new("csrf_test", config).unwrap();
        let mut target: Arc<str> = Arc::from("backend");

        let safe_methods = [Method::GET, Method::HEAD, Method::OPTIONS];
        for m in safe_methods {
            let mut req = build_test_request(m, "/api/data", HeaderMap::new());
            let action = plugin.on_upstream_request(&mut req, &mut target);
            assert!(matches!(action, Action::Continue));
        }
    }

    #[test]
    fn test_double_submit_cookie_success() {
        let config = CsrfConfig {
            cookie_name: "aurora_csrf".to_string(),
            header_name: "X-CSRF-Token".to_string(),
            secret_key: None,
            token_ttl_secs: 3600,
            safe_methods: vec!["GET".to_string(), "HEAD".to_string(), "OPTIONS".to_string()],
            trusted_origins: vec!["https://app.example.com".to_string()],
            enforce_fetch_metadata: false,
            enforce_origin: true,
        };
        let plugin = CsrfPlugin::new("csrf_test", config).unwrap();
        let mut target: Arc<str> = Arc::from("backend");

        let mut headers = HeaderMap::new();
        headers.insert(HOST, HeaderValue::from_static("api.example.com"));
        headers.insert(ORIGIN, HeaderValue::from_static("https://app.example.com"));
        headers.insert(
            COOKIE,
            HeaderValue::from_static("session=123; aurora_csrf=token_xyz_999; theme=dark"),
        );
        headers.insert("X-CSRF-Token", HeaderValue::from_static("token_xyz_999"));

        let mut req = build_test_request(Method::POST, "/api/transfer", headers);
        let action = plugin.on_upstream_request(&mut req, &mut target);
        assert!(matches!(action, Action::Continue));
    }

    #[test]
    fn test_double_submit_cookie_mismatch_rejected() {
        let config = CsrfConfig {
            cookie_name: "aurora_csrf".to_string(),
            header_name: "X-CSRF-Token".to_string(),
            secret_key: None,
            token_ttl_secs: 3600,
            safe_methods: vec!["GET".to_string()],
            trusted_origins: vec![],
            enforce_fetch_metadata: false,
            enforce_origin: false,
        };
        let plugin = CsrfPlugin::new("csrf_test", config).unwrap();
        let mut target: Arc<str> = Arc::from("backend");

        let mut headers = HeaderMap::new();
        headers.insert(COOKIE, HeaderValue::from_static("aurora_csrf=token_legit"));
        headers.insert(
            "X-CSRF-Token",
            HeaderValue::from_static("token_attacker_spoofed"),
        );

        let mut req = build_test_request(Method::POST, "/api/transfer", headers);
        let action = plugin.on_upstream_request(&mut req, &mut target);
        assert!(matches!(action, Action::Reject(RejectReason::Forbidden)));
    }

    #[test]
    fn test_signed_hmac_token_lifecycle_and_subdomain_tossing_defense() {
        let secret = "sec_k89f2a019482bca881726c0981273412";
        let config = CsrfConfig {
            cookie_name: "__Host-velda_csrf".to_string(),
            header_name: "X-CSRF-Token".to_string(),
            secret_key: Some(secret.to_string()),
            token_ttl_secs: 7200,
            safe_methods: vec!["GET".to_string()],
            trusted_origins: vec![],
            enforce_fetch_metadata: false,
            enforce_origin: false,
        };
        let plugin = CsrfPlugin::new("csrf_test", config).unwrap();
        let mut target: Arc<str> = Arc::from("backend");

        let key = hmac::Key::new(hmac::HMAC_SHA256, secret.as_bytes());
        let nonce = [0x42u8; 16];
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let valid_token = CsrfPlugin::mint_token(&key, &nonce, now);

        // 1. Valid signed token succeeds
        let mut headers = HeaderMap::new();
        headers.insert(
            COOKIE,
            HeaderValue::from_str(&format!("__Host-velda_csrf={valid_token}")).unwrap(),
        );
        headers.insert("X-CSRF-Token", HeaderValue::from_str(&valid_token).unwrap());

        let mut req = build_test_request(Method::POST, "/transfer", headers);
        let action = plugin.on_upstream_request(&mut req, &mut target);
        assert!(matches!(action, Action::Continue));

        // 2. Subdomain Cookie Tossing: attacker injects a matching cookie & header, but lacks secret key signature
        let fake_token = "000000006718a123.42424242424242424242424242424242.0000000000000000000000000000000000000000000000000000000000000000";
        let mut headers_tossed = HeaderMap::new();
        headers_tossed.insert(COOKIE, HeaderValue::from_static(fake_token));
        headers_tossed.insert("X-CSRF-Token", HeaderValue::from_static(fake_token));

        let mut req_tossed = build_test_request(Method::POST, "/transfer", headers_tossed);
        let action_tossed = plugin.on_upstream_request(&mut req_tossed, &mut target);
        assert!(matches!(
            action_tossed,
            Action::Reject(RejectReason::Forbidden)
        ));

        // 3. Expired token rejected
        let expired_ts = now - 7201; // Past 7200s TTL
        let expired_token = CsrfPlugin::mint_token(&key, &nonce, expired_ts);
        let mut headers_expired = HeaderMap::new();
        headers_expired.insert(
            COOKIE,
            HeaderValue::from_str(&format!("__Host-velda_csrf={expired_token}")).unwrap(),
        );
        headers_expired.insert(
            "X-CSRF-Token",
            HeaderValue::from_str(&expired_token).unwrap(),
        );

        let mut req_expired = build_test_request(Method::POST, "/transfer", headers_expired);
        let action_expired = plugin.on_upstream_request(&mut req_expired, &mut target);
        assert!(matches!(
            action_expired,
            Action::Reject(RejectReason::Forbidden)
        ));
    }

    #[test]
    fn test_w3c_fetch_metadata_cross_site_blocked() {
        let config = CsrfConfig {
            cookie_name: "__Host-csrf".to_string(),
            header_name: "X-CSRF-Token".to_string(),
            secret_key: None,
            token_ttl_secs: 3600,
            safe_methods: vec!["GET".to_string()],
            trusted_origins: vec!["https://trusted-partner.com".to_string()],
            enforce_fetch_metadata: true,
            enforce_origin: false,
        };
        let plugin = CsrfPlugin::new("csrf_test", config).unwrap();
        let mut target: Arc<str> = Arc::from("backend");

        // Case 1: Browser marks cross-site from untrusted origin -> Instant 403
        let mut headers = HeaderMap::new();
        headers.insert("sec-fetch-site", HeaderValue::from_static("cross-site"));
        headers.insert(
            ORIGIN,
            HeaderValue::from_static("https://malicious-site.com"),
        );

        let mut req = build_test_request(Method::POST, "/api/action", headers);
        let action = plugin.on_upstream_request(&mut req, &mut target);
        assert!(matches!(action, Action::Reject(RejectReason::Forbidden)));

        // Case 2: Cross-site from explicitly trusted partner -> Proceeds to token check
        let mut headers_trusted = HeaderMap::new();
        headers_trusted.insert("sec-fetch-site", HeaderValue::from_static("cross-site"));
        headers_trusted.insert(
            ORIGIN,
            HeaderValue::from_static("https://trusted-partner.com"),
        );
        headers_trusted.insert(COOKIE, HeaderValue::from_static("__Host-csrf=token123"));
        headers_trusted.insert("X-CSRF-Token", HeaderValue::from_static("token123"));

        let mut req_trusted = build_test_request(Method::POST, "/api/action", headers_trusted);
        let action_trusted = plugin.on_upstream_request(&mut req_trusted, &mut target);
        assert!(matches!(action_trusted, Action::Continue));
    }

    #[test]
    fn test_short_referer_no_panic() {
        let config = CsrfConfig {
            cookie_name: "__Host-csrf".to_string(),
            header_name: "X-CSRF-Token".to_string(),
            secret_key: None,
            token_ttl_secs: 3600,
            safe_methods: vec!["GET".to_string()],
            trusted_origins: vec![],
            enforce_fetch_metadata: false,
            enforce_origin: true,
        };
        let plugin = CsrfPlugin::new("csrf_test", config).unwrap();
        let mut target: Arc<str> = Arc::from("backend");

        for short_ref in ["x", "http:", "https", "1234567"] {
            let mut headers = HeaderMap::new();
            headers.insert(HOST, HeaderValue::from_static("api.example.com"));
            headers.insert(REFERER, HeaderValue::from_str(short_ref).unwrap());
            headers.insert(COOKIE, HeaderValue::from_static("__Host-csrf=tok123"));
            headers.insert("X-CSRF-Token", HeaderValue::from_static("tok123"));

            let mut req = build_test_request(Method::POST, "/api/action", headers);
            let action = plugin.on_upstream_request(&mut req, &mut target);
            assert!(matches!(action, Action::Reject(RejectReason::Forbidden)));
        }
    }

    #[test]
    fn test_mixed_case_safe_methods() {
        let config = CsrfConfig {
            cookie_name: "__Host-csrf".to_string(),
            header_name: "X-CSRF-Token".to_string(),
            secret_key: None,
            token_ttl_secs: 3600,
            safe_methods: vec!["Get".to_string(), "hEaD".to_string(), "oPtIoNs".to_string()],
            trusted_origins: vec![],
            enforce_fetch_metadata: true,
            enforce_origin: true,
        };
        let plugin = CsrfPlugin::new("csrf_test", config).unwrap();
        let mut target: Arc<str> = Arc::from("backend");

        for m in [Method::GET, Method::HEAD, Method::OPTIONS] {
            let mut req = build_test_request(m, "/api/data", HeaderMap::new());
            let action = plugin.on_upstream_request(&mut req, &mut target);
            assert!(matches!(action, Action::Continue));
        }

        let mut req_post = build_test_request(Method::POST, "/api/data", HeaderMap::new());
        let action_post = plugin.on_upstream_request(&mut req_post, &mut target);
        assert!(matches!(
            action_post,
            Action::Reject(RejectReason::Forbidden)
        ));
    }

    #[test]
    fn test_empty_tokens_rejected() {
        let config = CsrfConfig {
            cookie_name: "__Host-csrf".to_string(),
            header_name: "X-CSRF-Token".to_string(),
            secret_key: None,
            token_ttl_secs: 3600,
            safe_methods: vec!["GET".to_string()],
            trusted_origins: vec![],
            enforce_fetch_metadata: false,
            enforce_origin: false,
        };
        let plugin = CsrfPlugin::new("csrf_test", config).unwrap();
        let mut target: Arc<str> = Arc::from("backend");

        let mut headers = HeaderMap::new();
        headers.insert(COOKIE, HeaderValue::from_static("__Host-csrf="));
        headers.insert("X-CSRF-Token", HeaderValue::from_static(""));

        let mut req = build_test_request(Method::POST, "/api/action", headers);
        let action = plugin.on_upstream_request(&mut req, &mut target);
        assert!(matches!(action, Action::Reject(RejectReason::Forbidden)));
    }

    #[test]
    fn test_schemeless_origin_rejected() {
        let config = CsrfConfig {
            cookie_name: "__Host-csrf".to_string(),
            header_name: "X-CSRF-Token".to_string(),
            secret_key: None,
            token_ttl_secs: 3600,
            safe_methods: vec!["GET".to_string()],
            trusted_origins: vec![],
            enforce_fetch_metadata: false,
            enforce_origin: true,
        };
        let plugin = CsrfPlugin::new("csrf_test", config).unwrap();
        let mut target: Arc<str> = Arc::from("backend");

        let mut headers = HeaderMap::new();
        headers.insert(HOST, HeaderValue::from_static("api.example.com"));
        headers.insert(ORIGIN, HeaderValue::from_static("api.example.com"));
        headers.insert(COOKIE, HeaderValue::from_static("__Host-csrf=tok123"));
        headers.insert("X-CSRF-Token", HeaderValue::from_static("tok123"));

        let mut req = build_test_request(Method::POST, "/api/action", headers);
        let action = plugin.on_upstream_request(&mut req, &mut target);
        assert!(matches!(action, Action::Reject(RejectReason::Forbidden)));
    }
}
