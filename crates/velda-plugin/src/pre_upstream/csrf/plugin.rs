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

use bytes::Bytes;
use http::header::{CONTENT_LENGTH, CONTENT_TYPE, COOKIE, HOST, ORIGIN, REFERER};
use http::{HeaderName, HeaderValue, Method, StatusCode};
use ring::hmac;
use velda_core::{Action, L7Request, L7Response};
use velda_sync::post_sync::plugin::PluginConfig;

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
        match m.as_bytes() {
            b"GET" | b"get" => Self::GET,
            b"POST" | b"post" => Self::POST,
            b"PUT" | b"put" => Self::PUT,
            b"DELETE" | b"delete" => Self::DELETE,
            b"HEAD" | b"head" => Self::HEAD,
            b"OPTIONS" | b"options" => Self::OPTIONS,
            b"PATCH" | b"patch" => Self::PATCH,
            b"TRACE" | b"trace" => Self::TRACE,
            b"CONNECT" | b"connect" => Self::CONNECT,
            _ => Self::EXTENSION,
        }
    }
}

/// Constant-time byte slice comparison using 128-bit vector words to eliminate timing
/// side-channel attacks while vectorizing directly into SIMD instructions on modern CPUs.
#[inline]
pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u128;
    let (chunks_a, remainder_a) = a.as_chunks::<16>();
    let (chunks_b, remainder_b) = b.as_chunks::<16>();

    for (ca, cb) in chunks_a.iter().zip(chunks_b.iter()) {
        let val_a = u128::from_ne_bytes(*ca);
        let val_b = u128::from_ne_bytes(*cb);
        diff |= val_a ^ val_b;
    }

    let mut rem_diff = 0u8;
    for (ra, rb) in remainder_a.iter().zip(remainder_b.iter()) {
        rem_diff |= ra ^ rb;
    }

    diff == 0 && rem_diff == 0
}

/// Scans raw HTTP Cookie header bytes for a specific cookie key prefix with zero heap allocations.
///
/// Ensures boundary correctness: the key must start at index 0 or be immediately preceded
/// by `;` or whitespace after `;`, preventing partial prefix collisions (e.g. `other_csrf=`).
#[inline]
pub fn extract_cookie_value<'a>(cookie_bytes: &'a [u8], prefix: &[u8]) -> Option<&'a [u8]> {
    let mut offset = 0;
    while offset < cookie_bytes.len() {
        // Skip leading whitespace or separators
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

        // Advance to next cookie entry
        while offset < cookie_bytes.len() && cookie_bytes[offset] != b';' {
            offset += 1;
        }
    }
    None
}

/// Helper to get current epoch seconds.
#[inline]
fn current_epoch_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
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
    /// Constructs a new [`CsrfPlugin`] with pre-compiled method bitmasks and cryptographic keys.
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

    /// Generates a valid 114-byte HMAC-SHA256 signed stateless token (`<timestamp_hex>.<nonce_hex>.<hmac_hex>`).
    pub fn mint_token(key: &hmac::Key, nonce: &[u8; 16], now_secs: u64) -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut buf = [0u8; 114];

        // 1. Timestamp (16 hex chars)
        for i in (0..16).rev() {
            let shift = (15 - i) * 4;
            buf[i] = HEX[((now_secs >> shift) & 0x0f) as usize];
        }
        buf[16] = b'.';

        // 2. Nonce (32 hex chars)
        for (i, &b) in nonce.iter().enumerate() {
            buf[17 + i * 2] = HEX[(b >> 4) as usize];
            buf[17 + i * 2 + 1] = HEX[(b & 0x0f) as usize];
        }
        buf[49] = b'.';

        // 3. HMAC-SHA256 over "<timestamp_hex>.<nonce_hex>" (49 bytes)
        let tag = hmac::sign(key, &buf[0..49]);
        for (i, &b) in tag.as_ref().iter().enumerate() {
            buf[50 + i * 2] = HEX[(b >> 4) as usize];
            buf[50 + i * 2 + 1] = HEX[(b & 0x0f) as usize];
        }

        std::str::from_utf8(&buf)
            .expect("Valid UTF-8 hex token")
            .to_string()
    }

    /// Verifies a signed stateless token on the stack with zero heap allocation.
    #[inline]
    fn verify_signed_token(&self, key: &hmac::Key, token: &[u8], now_secs: u64) -> bool {
        // Strict format check: 16 (ts) + 1 (.) + 32 (nonce) + 1 (.) + 64 (sig) = 114 bytes
        if token.len() != 114 || token[16] != b'.' || token[49] != b'.' {
            return false;
        }

        // Parse timestamp hex
        let Ok(ts_str) = std::str::from_utf8(&token[0..16]) else {
            return false;
        };
        let Ok(timestamp) = u64::from_str_radix(ts_str, 16) else {
            return false;
        };

        // TTL validation with 60-second future skew tolerance
        if timestamp > now_secs + 60
            || now_secs.saturating_sub(timestamp) > self.config.token_ttl_secs
        {
            return false;
        }

        // Compute expected HMAC-SHA256 signature
        let expected_tag = hmac::sign(key, &token[0..49]);
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut expected_hex = [0u8; 64];
        for (i, &b) in expected_tag.as_ref().iter().enumerate() {
            expected_hex[i * 2] = HEX[(b >> 4) as usize];
            expected_hex[i * 2 + 1] = HEX[(b & 0x0f) as usize];
        }

        // 128-bit SIMD constant-time signature comparison
        constant_time_eq(&token[50..114], &expected_hex)
    }

    /// Checks whether an Origin or Referer matches the request Host header.
    #[inline]
    fn matches_host(&self, origin: &str, host_val: &HeaderValue) -> bool {
        let Ok(host_str) = host_val.to_str() else {
            return false;
        };

        // Origin format is "scheme://host[:port]"
        if let Some(host_part) = origin.strip_prefix("https://") {
            host_part == host_str
        } else if let Some(host_part) = origin.strip_prefix("http://") {
            host_part == host_str
        } else {
            origin == host_str
        }
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
            return Action::Continue; // GET, HEAD, OPTIONS, TRACE bypass immediately (~0.5ns)
        }

        // ─────────────────────────────────────────────────────────────────────
        // Gate 2: W3C Fetch Metadata Inspection (Sec-Fetch-Site)
        // ─────────────────────────────────────────────────────────────────────
        if self.config.enforce_fetch_metadata {
            static SEC_FETCH_SITE: HeaderName = HeaderName::from_static("sec-fetch-site");
            if let Some(site_val) = req.headers.get(&SEC_FETCH_SITE)
                && let Ok(site_str) = site_val.to_str()
            {
                // If the browser explicitly marks this mutating request as "cross-site",
                // it MUST match one of the explicitly trusted cross-origin domains.
                if site_str.eq_ignore_ascii_case("cross-site") {
                    let mut authorized_cross_site = false;
                    if let Some(orig) = req.headers.get(ORIGIN)
                        && let Ok(orig_str) = orig.to_str()
                    {
                        authorized_cross_site =
                            self.trusted_origins.iter().any(|p| p.matches(orig_str));
                    }

                    if !authorized_cross_site {
                        return Action::Respond(
                            L7Response::from_bytes(
                                StatusCode::FORBIDDEN,
                                Bytes::from_static(
                                    b"403 Forbidden: CSRF cross-site request disallowed\n",
                                ),
                            )
                            .with_header(
                                CONTENT_TYPE,
                                HeaderValue::from_static("text/plain; charset=utf-8"),
                            )
                            .with_header(CONTENT_LENGTH, HeaderValue::from_static("49")),
                        );
                    }
                }
            }
        }

        // ─────────────────────────────────────────────────────────────────────
        // Gate 3: Origin / Referer Verification (RFC 6454 Same-Origin Defense)
        // ─────────────────────────────────────────────────────────────────────
        if self.config.enforce_origin {
            let host_opt = req.headers.get(HOST);
            let mut origin_verified = false;

            if let Some(orig_val) = req.headers.get(ORIGIN)
                && let Ok(orig_str) = orig_val.to_str()
            {
                // 1. Check if Origin matches Host
                if let Some(host_val) = host_opt {
                    origin_verified = self.matches_host(orig_str, host_val);
                }
                // 2. Check if Origin matches trusted_origins
                if !origin_verified {
                    origin_verified = self.trusted_origins.iter().any(|p| p.matches(orig_str));
                }
            } else if let Some(ref_val) = req.headers.get(REFERER)
                && let Ok(ref_str) = ref_val.to_str()
            {
                // Fallback to Referer: extract "scheme://host[:port]"
                let origin_portion = if let Some(idx) = ref_str[8..].find('/') {
                    &ref_str[..8 + idx]
                } else {
                    ref_str
                };

                if let Some(host_val) = host_opt {
                    origin_verified = self.matches_host(origin_portion, host_val);
                }
                if !origin_verified {
                    origin_verified = self
                        .trusted_origins
                        .iter()
                        .any(|p| p.matches(origin_portion));
                }
            } else {
                // Mutating requests without Origin or Referer are rejected when strict origin is enforced
                origin_verified = false;
            }

            if !origin_verified {
                return Action::Respond(
                    L7Response::from_bytes(
                        StatusCode::FORBIDDEN,
                        Bytes::from_static(b"403 Forbidden: CSRF origin verification failed\n"),
                    )
                    .with_header(
                        CONTENT_TYPE,
                        HeaderValue::from_static("text/plain; charset=utf-8"),
                    )
                    .with_header(CONTENT_LENGTH, HeaderValue::from_static("46")),
                );
            }
        }

        // ─────────────────────────────────────────────────────────────────────
        // Gate 4: Stateless Double-Submit / Signed Token Verification
        // ─────────────────────────────────────────────────────────────────────
        // 4.1 Extract Cookie Value (Zero-Allocation Byte Slice)
        let Some(cookie_hdr) = req.headers.get(COOKIE) else {
            return Action::Respond(
                L7Response::from_bytes(
                    StatusCode::FORBIDDEN,
                    Bytes::from_static(b"403 Forbidden: CSRF cookie missing\n"),
                )
                .with_header(
                    CONTENT_TYPE,
                    HeaderValue::from_static("text/plain; charset=utf-8"),
                )
                .with_header(CONTENT_LENGTH, HeaderValue::from_static("35")),
            );
        };

        let Some(cookie_token) = extract_cookie_value(cookie_hdr.as_bytes(), &self.cookie_prefix)
        else {
            return Action::Respond(
                L7Response::from_bytes(
                    StatusCode::FORBIDDEN,
                    Bytes::from_static(b"403 Forbidden: CSRF cookie missing\n"),
                )
                .with_header(
                    CONTENT_TYPE,
                    HeaderValue::from_static("text/plain; charset=utf-8"),
                )
                .with_header(CONTENT_LENGTH, HeaderValue::from_static("35")),
            );
        };

        // 4.2 Extract Header Value
        let Some(header_token) = req.headers.get(&self.header_name) else {
            return Action::Respond(
                L7Response::from_bytes(
                    StatusCode::FORBIDDEN,
                    Bytes::from_static(b"403 Forbidden: CSRF token header missing\n"),
                )
                .with_header(
                    CONTENT_TYPE,
                    HeaderValue::from_static("text/plain; charset=utf-8"),
                )
                .with_header(CONTENT_LENGTH, HeaderValue::from_static("41")),
            );
        };

        // 4.3 128-bit SIMD Constant-Time Equality Check
        if !constant_time_eq(cookie_token, header_token.as_bytes().trim_ascii()) {
            return Action::Respond(
                L7Response::from_bytes(
                    StatusCode::FORBIDDEN,
                    Bytes::from_static(b"403 Forbidden: CSRF token mismatch\n"),
                )
                .with_header(
                    CONTENT_TYPE,
                    HeaderValue::from_static("text/plain; charset=utf-8"),
                )
                .with_header(CONTENT_LENGTH, HeaderValue::from_static("36")),
            );
        }

        // 4.4 Cryptographic HMAC-SHA256 Signature & TTL Check (Neutralizes Subdomain Cookie Tossing)
        if let Some(key) = &self.hmac_key {
            let now = current_epoch_secs();
            if !self.verify_signed_token(key, cookie_token, now) {
                return Action::Respond(
                    L7Response::from_bytes(
                        StatusCode::FORBIDDEN,
                        Bytes::from_static(
                            b"403 Forbidden: CSRF signed token invalid or expired\n",
                        ),
                    )
                    .with_header(
                        CONTENT_TYPE,
                        HeaderValue::from_static("text/plain; charset=utf-8"),
                    )
                    .with_header(CONTENT_LENGTH, HeaderValue::from_static("51")),
                );
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
    use http::{HeaderMap, Uri, Version};
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
        let Action::Respond(resp) = action else {
            panic!("Expected Action::Respond(403)");
        };
        assert_eq!(resp.status, StatusCode::FORBIDDEN);
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
        let now = current_epoch_secs();
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
        let Action::Respond(resp) = action_tossed else {
            panic!("Expected Action::Respond(403)");
        };
        assert_eq!(resp.status, StatusCode::FORBIDDEN);

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
        let Action::Respond(resp_expired) = action_expired else {
            panic!("Expected Action::Respond(403)");
        };
        assert_eq!(resp_expired.status, StatusCode::FORBIDDEN);
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
        let Action::Respond(resp) = action else {
            panic!("Expected 403 for cross-site fetch metadata");
        };
        assert_eq!(resp.status, StatusCode::FORBIDDEN);

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
}
