//! Cross-Site Request Forgery (CSRF) Protection Plugin (`csrf::plugin`).
//!
//! ```text
//! on_upstream_request(req) [AccessLevel::Read, HTTP/1.1 | HTTP/2 | HTTP/3, 0 alloc, 0 clock syscalls]
//!   ├─► 1. Safe Method Bitmask (`GET|HEAD|OPTIONS|TRACE`)             ──► Continue (~1 inst)
//!   ├─► 2. Sec-Fetch-Site Present (Modern Browsers):
//!   │     ├─ "same-origin" | "none"                                 ──► provenance OK
//!   │     ├─ "same-site" && allow_same_site                         ──► provenance OK
//!   │     └─ else -> Origin in trusted_origins?                     ──► No -> 403 Forbidden
//!   ├─► 3. Sec-Fetch-Site Absent (Legacy Browsers / Proxies / APIs):
//!   │     ├─ Origin or Referer scheme+authority present?
//!   │     │    └─ matches Host/:authority or trusted_origins?       ──► No -> 403 Forbidden
//!   │     └─ Missing all provenance + Cookie present & enforce_origin ─► 403 Forbidden
//!   └─► 4. Optional Double-Submit (`double_submit: Some`):
//!         └─ Scan all COOKIE headers == header_name (constant-time)  ──► No -> 403 Forbidden
//! ```

use http::header::{COOKIE, HOST, ORIGIN, REFERER};
use http::{HeaderName, Method};
use velda_core::{Action, L7Request, RejectReason};

use super::validation::{CsrfConfig, validate_csrf_config};
use crate::capability::{AccessLevel, PluginCapability, SupportedProtocols};
use crate::config::PluginConfig;
use crate::error::PluginError;
use crate::pre_route::OriginPattern;
use crate::pre_upstream::PreUpstreamPlugin;
use crate::pre_upstream::hmac_auth::canonical::constant_time_eq;

static SEC_FETCH_SITE: HeaderName = HeaderName::from_static("sec-fetch-site");

/// Compact bitmask for single-instruction safe HTTP method filtering.
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

/// Scans a raw `Cookie` header value for `<name>=<value>` without heap allocation.
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

/// Extracts `scheme://host[:port]` from a `Referer` header, stopping cleanly before `/`, `?`, `#`,
/// and rejecting malformed userinfo (`@`).
#[inline]
fn extract_referer_origin(referer: &str) -> Option<&str> {
    let scheme_end = referer.find("://")?;
    let scheme = &referer[..scheme_end];
    if !scheme.eq_ignore_ascii_case("https") && !scheme.eq_ignore_ascii_case("http") {
        return None;
    }
    let after_scheme = &referer[scheme_end + 3..];
    let end = after_scheme
        .find(['/', '?', '#'])
        .unwrap_or(after_scheme.len());
    let authority = &after_scheme[..end];
    if authority.is_empty() || authority.contains('@') {
        return None;
    }
    Some(&referer[..scheme_end + 3 + end])
}

/// Pre-compiled Double-Submit Cookie lookup specification.
#[derive(Debug, Clone)]
pub struct CompiledDoubleSubmit {
    pub header_name: HeaderName,
    pub cookie_prefix: Box<[u8]>,
}

/// Modern Cross-Origin & Optional Double-Submit CSRF Protection Plugin.
#[derive(Debug, Clone)]
pub struct CsrfPlugin {
    pub id: String,
    pub config: CsrfConfig,
    pub safe_methods_mask: u16,
    pub trusted_origins: Box<[OriginPattern]>,
    pub double_submit: Option<CompiledDoubleSubmit>,
}

impl CsrfPlugin {
    /// Constructs a [`CsrfPlugin`] from validated configuration.
    pub fn new(id: impl Into<String>, config: CsrfConfig) -> Result<Self, PluginError> {
        let mut safe_methods_mask = 0u16;
        for m in &config.safe_methods {
            safe_methods_mask |= MethodMask::parse_token(m.trim());
        }

        let trusted_origins: Vec<OriginPattern> = config
            .trusted_origins
            .iter()
            .map(|s| OriginPattern::parse(s))
            .collect();

        let double_submit = if let Some(ds) = &config.double_submit {
            let trimmed_hdr = ds.header_name.trim();
            let header_name = HeaderName::from_bytes(trimmed_hdr.as_bytes()).map_err(|e| {
                PluginError::Config(format!("Invalid CSRF header_name '{trimmed_hdr}': {e}"))
            })?;
            let trimmed_cookie = ds.cookie_name.trim();
            let mut prefix = Vec::with_capacity(trimmed_cookie.len() + 1);
            prefix.extend_from_slice(trimmed_cookie.as_bytes());
            prefix.push(b'=');
            Some(CompiledDoubleSubmit {
                header_name,
                cookie_prefix: prefix.into_boxed_slice(),
            })
        } else {
            None
        };

        Ok(Self {
            id: id.into(),
            config,
            safe_methods_mask,
            trusted_origins: trusted_origins.into_boxed_slice(),
            double_submit,
        })
    }

    /// Parses, validates, and constructs an instance directly from a [`PluginConfig`].
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let cfg = validate_csrf_config(&config.id, &config.config).map_err(|e| {
            PluginError::Config(format!(
                "Invalid csrf configuration for '{}': {e}",
                config.id
            ))
        })?;
        Self::new(&config.id, cfg)
    }

    #[inline]
    fn matches_trusted(&self, origin: &str) -> bool {
        self.trusted_origins.iter().any(|p| p.matches(origin))
    }

    #[cold]
    #[inline(never)]
    fn verify_legacy_origin_fallback(&self, req: &L7Request) -> bool {
        if let Some(orig_val) = req.headers.get(ORIGIN) {
            let Some(orig) = orig_val.to_str().ok().map(str::trim) else {
                return false;
            };
            return Self::matches_request_host(req, orig) || self.matches_trusted(orig);
        }

        if let Some(ref_val) = req.headers.get(REFERER) {
            let Some(orig) = ref_val
                .to_str()
                .ok()
                .and_then(|s| extract_referer_origin(s.trim()))
            else {
                return false;
            };
            return Self::matches_request_host(req, orig) || self.matches_trusted(orig);
        }

        // Missing Sec-Fetch-Site, Origin, AND Referer:
        // reject if browser cookies are present; permit pure stateless/Bearer API callers.
        req.headers.get(COOKIE).is_none()
    }

    #[inline(never)]
    fn matches_request_host(req: &L7Request, origin: &str) -> bool {
        let host_part = if let Some(h) = origin.strip_prefix("https://") {
            h
        } else if let Some(h) = origin.strip_prefix("http://") {
            h
        } else {
            return false;
        };

        let req_host = req
            .headers
            .get(HOST)
            .and_then(|h| h.to_str().ok())
            .or_else(|| req.uri.authority().map(|a| a.as_str()));

        req_host.is_some_and(|host| !host.is_empty() && host_part.eq_ignore_ascii_case(host))
    }

    #[cold]
    #[inline(never)]
    fn verify_double_submit(ds: &CompiledDoubleSubmit, req: &L7Request) -> bool {
        let Some(hdr_tok) = req
            .headers
            .get(&ds.header_name)
            .map(|v| v.as_bytes().trim_ascii())
            .filter(|b| !b.is_empty())
        else {
            return false;
        };

        for cookie_hdr in req.headers.get_all(COOKIE) {
            if let Some(cookie_tok) = extract_cookie_value(cookie_hdr.as_bytes(), &ds.cookie_prefix)
                && !cookie_tok.is_empty()
                && constant_time_eq(cookie_tok, hdr_tok)
            {
                return true;
            }
        }
        false
    }
}

impl PreUpstreamPlugin for CsrfPlugin {
    #[inline(always)]
    fn id(&self) -> &str {
        &self.id
    }

    #[inline(always)]
    fn pbww(&self) -> u32 {
        0
    }

    fn on_upstream_request(&self, req: &mut L7Request, _targets: &mut u32) -> Action {
        // Gate 1: Safe idempotent methods bypass in 1 bitmask instruction
        if (self.safe_methods_mask & MethodMask::from_method(&req.method)) != 0 {
            return Action::Continue;
        }

        // Gate 2 & 3: Modern Cross-Origin Protection (Sec-Fetch-Site fast path; cold legacy fallback)
        if let Some(site_val) = req.headers.get(&SEC_FETCH_SITE) {
            let Ok(site) = site_val.to_str() else {
                return Action::Reject(RejectReason::Forbidden);
            };
            let site_trimmed = site.trim();
            let allowed_fetch = if site_trimmed.eq_ignore_ascii_case("same-origin")
                || site_trimmed.eq_ignore_ascii_case("none")
                || (self.config.allow_same_site && site_trimmed.eq_ignore_ascii_case("same-site"))
            {
                true
            } else {
                req.headers
                    .get(ORIGIN)
                    .and_then(|o| o.to_str().ok())
                    .is_some_and(|orig| self.matches_trusted(orig.trim()))
            };

            if !allowed_fetch {
                return Action::Reject(RejectReason::Forbidden);
            }
        } else if self.config.enforce_origin && !self.verify_legacy_origin_fallback(req) {
            return Action::Reject(RejectReason::Forbidden);
        }

        // Gate 4: Optional Legacy Double-Submit Cookie Check (out-of-line for clean modern L1i)
        if let Some(ds) = &self.double_submit
            && !Self::verify_double_submit(ds, req)
        {
            return Action::Reject(RejectReason::Forbidden);
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
    use http::{HeaderValue, Uri, Version};
    use velda_core::{Body, HeaderWorkspace};

    fn build_req(method: Method, uri: &str, headers: HeaderWorkspace) -> L7Request {
        L7Request::new(
            method,
            uri.parse::<Uri>().unwrap(),
            Version::HTTP_11,
            headers,
            Body::Empty,
        )
    }

    fn modern_cfg(allow_same_site: bool) -> CsrfConfig {
        CsrfConfig {
            safe_methods: vec!["GET".into(), "HEAD".into(), "OPTIONS".into()],
            trusted_origins: vec!["https://partner.example.com".into()],
            allow_same_site,
            enforce_origin: true,
            double_submit: None,
        }
    }

    #[test]
    fn test_modern_fetch_metadata_protections() {
        let plugin = CsrfPlugin::new("csrf_modern", modern_cfg(false)).unwrap();
        let mut target = 0;

        // Safe methods bypass
        for m in [Method::GET, Method::HEAD, Method::OPTIONS] {
            let mut r = build_req(m, "/api/data", HeaderWorkspace::empty(2));
            assert!(matches!(
                plugin.on_upstream_request(&mut r, &mut target),
                Action::Continue
            ));
        }

        // same-origin and none pass with zero cookie or token needed
        for site in ["same-origin", "none"] {
            let mut h = HeaderWorkspace::empty(4);
            h.insert("sec-fetch-site", HeaderValue::from_static(site));
            h.insert(COOKIE, HeaderValue::from_static("sid=secret"));
            let mut r = build_req(Method::POST, "/api/pay", h);
            assert!(matches!(
                plugin.on_upstream_request(&mut r, &mut target),
                Action::Continue
            ));
        }

        // cross-site and untrusted same-site blocked
        for (site, orig) in [
            ("cross-site", "https://evil.com"),
            ("same-site", "https://blog.example.com"),
        ] {
            let mut h = HeaderWorkspace::empty(4);
            h.insert("sec-fetch-site", HeaderValue::from_static(site));
            h.insert(ORIGIN, HeaderValue::from_static(orig));
            let mut r = build_req(Method::POST, "/api/pay", h);
            assert!(matches!(
                plugin.on_upstream_request(&mut r, &mut target),
                Action::Reject(RejectReason::Forbidden)
            ));
        }

        // Trusted partner cross-site passes
        let mut h_partner = HeaderWorkspace::empty(4);
        h_partner.insert("sec-fetch-site", HeaderValue::from_static("cross-site"));
        h_partner.insert(
            ORIGIN,
            HeaderValue::from_static("https://partner.example.com"),
        );
        let mut r_partner = build_req(Method::POST, "/api/webhook", h_partner);
        assert!(matches!(
            plugin.on_upstream_request(&mut r_partner, &mut target),
            Action::Continue
        ));

        // allow_same_site = true permits same-site
        let permissive = CsrfPlugin::new("csrf_same_site", modern_cfg(true)).unwrap();
        let mut h_sub = HeaderWorkspace::empty(2);
        h_sub.insert("sec-fetch-site", HeaderValue::from_static("same-site"));
        let mut r_sub = build_req(Method::POST, "/api/sub", h_sub);
        assert!(matches!(
            permissive.on_upstream_request(&mut r_sub, &mut target),
            Action::Continue
        ));
    }

    #[test]
    fn test_legacy_origin_referer_and_h2_authority_fallback() {
        let plugin = CsrfPlugin::new("csrf_fallback", modern_cfg(false)).unwrap();
        let mut target = 0;

        // Legacy browser without Sec-Fetch-Site: Origin matches H1 Host
        let mut h_ok = HeaderWorkspace::empty(4);
        h_ok.insert(HOST, HeaderValue::from_static("app.example.com"));
        h_ok.insert(ORIGIN, HeaderValue::from_static("https://app.example.com"));
        let mut r_ok = build_req(Method::POST, "/submit", h_ok);
        assert!(matches!(
            plugin.on_upstream_request(&mut r_ok, &mut target),
            Action::Continue
        ));

        // HTTP/2 authority in Uri without Host header
        let mut h_h2 = HeaderWorkspace::empty(4);
        h_h2.insert(ORIGIN, HeaderValue::from_static("https://app.example.com"));
        let mut r_h2 = build_req(Method::POST, "https://app.example.com/submit", h_h2);
        assert!(matches!(
            plugin.on_upstream_request(&mut r_h2, &mut target),
            Action::Continue
        ));

        // Referer with query/fragment and Host match passes; userinfo @ spoof rejected
        let mut h_ref = HeaderWorkspace::empty(4);
        h_ref.insert(HOST, HeaderValue::from_static("app.example.com"));
        h_ref.insert(
            REFERER,
            HeaderValue::from_static("https://app.example.com/form?step=2#top"),
        );
        let mut r_ref = build_req(Method::POST, "/submit", h_ref);
        assert!(matches!(
            plugin.on_upstream_request(&mut r_ref, &mut target),
            Action::Continue
        ));

        let mut h_spoof = HeaderWorkspace::empty(4);
        h_spoof.insert(HOST, HeaderValue::from_static("app.example.com"));
        h_spoof.insert(
            REFERER,
            HeaderValue::from_static("https://evil.com@app.example.com/"),
        );
        let mut r_spoof = build_req(Method::POST, "/submit", h_spoof);
        assert!(matches!(
            plugin.on_upstream_request(&mut r_spoof, &mut target),
            Action::Reject(RejectReason::Forbidden)
        ));

        // Missing all provenance + Cookie present -> rejected
        let mut h_cookie_only = HeaderWorkspace::empty(2);
        h_cookie_only.insert(COOKIE, HeaderValue::from_static("sid=xyz"));
        let mut r_cookie_only = build_req(Method::POST, "/submit", h_cookie_only);
        assert!(matches!(
            plugin.on_upstream_request(&mut r_cookie_only, &mut target),
            Action::Reject(RejectReason::Forbidden)
        ));

        // Non-browser Bearer API client (no cookies, no fetch metadata, no origin) -> allowed
        let mut h_cli = HeaderWorkspace::empty(2);
        h_cli.insert("authorization", HeaderValue::from_static("Bearer abc"));
        let mut r_cli = build_req(Method::POST, "/api/cli", h_cli);
        assert!(matches!(
            plugin.on_upstream_request(&mut r_cli, &mut target),
            Action::Continue
        ));
    }

    #[test]
    fn test_optional_double_submit_multi_cookie_headers_http2() {
        let plugin = CsrfPlugin::from_config(&PluginConfig {
            id: "csrf".into(),
            config: serde_json::json!({
                "safe_methods": ["GET", "HEAD"],
                "trusted_origins": [],
                "allow_same_site": false,
                "enforce_origin": true,
                "double_submit": {
                    "cookie_name": "XSRF-TOKEN",
                    "header_name": "X-XSRF-TOKEN"
                }
            }),
        })
        .unwrap();
        let mut target = 0;

        // Multi-Cookie header (RFC 9113 HTTP/2 splitting cookie headers across lines)
        let mut h = HeaderWorkspace::empty(6);
        h.insert("sec-fetch-site", HeaderValue::from_static("same-origin"));
        h.append(COOKIE, HeaderValue::from_static("session=user_01"));
        h.append(COOKIE, HeaderValue::from_static("XSRF-TOKEN=valid_tok_123"));
        h.insert("X-XSRF-TOKEN", HeaderValue::from_static("valid_tok_123"));
        let mut r_ok = build_req(Method::POST, "/legacy/action", h);
        assert!(matches!(
            plugin.on_upstream_request(&mut r_ok, &mut target),
            Action::Continue
        ));

        // Empty or mismatched header rejected
        let mut h_bad = HeaderWorkspace::empty(4);
        h_bad.insert("sec-fetch-site", HeaderValue::from_static("same-origin"));
        h_bad.insert(COOKIE, HeaderValue::from_static("XSRF-TOKEN=valid_tok_123"));
        h_bad.insert("X-XSRF-TOKEN", HeaderValue::from_static("wrong_tok"));
        let mut r_bad = build_req(Method::POST, "/legacy/action", h_bad);
        assert!(matches!(
            plugin.on_upstream_request(&mut r_bad, &mut target),
            Action::Reject(RejectReason::Forbidden)
        ));
    }
}
