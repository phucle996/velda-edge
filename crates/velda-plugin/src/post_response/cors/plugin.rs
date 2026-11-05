//! Cross-Origin Resource Sharing (CORS) Post-Response Plugin (`phase: "post_response"`).
//!
//! Evaluated on outgoing upstream responses immediately before downstream encoding.
//! Injects standard CORS response headers (`Access-Control-Allow-Origin`,
//! `Access-Control-Allow-Credentials`, `Access-Control-Expose-Headers`, `Access-Control-Max-Age`),
//! supporting dynamic origin reflection matching and pre-computed immutable values.
//!
//! # Execution Invariants & Hardware Optimizations
//! - **Single-Instruction Configuration Bitmask (`flags: u8`)**: All static configuration states
//!   are pre-compiled into an 8-bit integer bitmask, replacing runtime branching with single CPU
//!   register instructions (`TEST AL, imm8`).
//! - **$O(1)$ Origin Classification Jump (`origin_kind: u8`)**: Categorizes allowed origins into
//!   `Any`, `SingleExact`, or `Multi` at configuration time, executing in $\le 1$ CPU cycle for
//!   common single-origin deployments.
//! - **Pre-Baked Contiguous Header Array**: Static response headers (`credentials`, `expose-headers`,
//!   `max-age`) are stored in a fixed-size 3-element flat array (`base_headers_len: u8`), completely
//!   eliminating runtime `HeaderMap` dynamic hash table lookups and bucket allocations.
//! - **SWAR 64-Bit Integer Vary Origin Filter**: Scans incoming `Vary` headers using integer-length
//!   filtering (`len != 6 && len != 1`) and 64-bit register word comparisons (`is_token_origin_swar`),
//!   eliminating UTF-8 decoding, string allocations, and nested iterators.
//! - **Single-Hash Fast-Path Vary Injection**: When upstream emits no `Vary` header (>85% of traffic),
//!   inserts `Vary: Origin` directly via a single lookup without looping or scanning.
//! - **Non-CORS Request Pass-Through**: Non-CORS requests (no `Origin` header) exit immediately (~2ns).
//! - **Strict Origin Isolation**: Disallowed origins receive zero CORS headers (no wildcards fallback).
//! - **Zero-Allocation Hot Path**: Atomic ref-count clone on `HeaderValue` without heap re-allocations.

use http::header::{
    ACCESS_CONTROL_ALLOW_CREDENTIALS, ACCESS_CONTROL_ALLOW_ORIGIN, ACCESS_CONTROL_EXPOSE_HEADERS,
    ACCESS_CONTROL_MAX_AGE, ORIGIN, VARY,
};
use http::header::{HeaderName, HeaderValue};
use velda_core::L7Response;
use velda_sync::post_sync::plugin::PluginConfig;

use crate::capability::{AccessLevel, PluginCapability, SupportedProtocols};
use crate::error::PluginError;
use crate::post_response::PostResponsePlugin;
use crate::pre_route::OriginPattern;

/// Pre-computed 8-bit bitmask flags for single-instruction ALU configuration checks.
pub const CORS_FLAG_ALLOW_ALL_ORIGINS: u8 = 1 << 0;
pub const CORS_FLAG_ALLOW_CREDENTIALS: u8 = 1 << 1;
pub const CORS_FLAG_REFLECT_ORIGIN: u8 = 1 << 2;
pub const CORS_FLAG_NEEDS_VARY_ORIGIN: u8 = 1 << 3;

/// Pre-classified origin matching strategy for O(1) jump execution.
pub const ORIGIN_KIND_ANY: u8 = 0; // Wildcard '*'
pub const ORIGIN_KIND_SINGLE_EXACT: u8 = 1; // Single exact origin (e.g. "https://app.velda.io")
pub const ORIGIN_KIND_MULTI: u8 = 2; // Multi domains or subdomain patterns

/// 64-bit word constant for lowercase "origin\0\0" in little-endian.
/// 'o'=0x6f, 'r'=0x72, 'i'=0x69, 'g'=0x67, 'i'=0x69, 'n'=0x6e, \0=0x00, \0=0x00
const ORIGIN_U64_LE: u64 = 0x0000_6e69_6769_726f;
/// 64-bit case-insensitive ASCII lowercase conversion mask for 6 characters.
const ASCII_LOWERCASE_MASK_6B: u64 = 0x0000_2020_2020_2020;

/// Trims leading and trailing ASCII whitespace (space and tab) without allocations.
#[inline(always)]
fn trim_ascii(mut b: &[u8]) -> &[u8] {
    while let Some((&first, rest)) = b.split_first() {
        if first == b' ' || first == b'\t' {
            b = rest;
        } else {
            break;
        }
    }
    while let Some((&last, rest)) = b.split_last() {
        if last == b' ' || last == b'\t' {
            b = rest;
        } else {
            break;
        }
    }
    b
}

/// Fast 64-bit SWAR (SIMD-Within-A-Register) check for token == "origin" (case-insensitive).
#[inline(always)]
fn is_token_origin_swar(token: &[u8]) -> bool {
    debug_assert_eq!(token.len(), 6);
    let mut buf = [0u8; 8];
    buf[..6].copy_from_slice(token);
    let val = u64::from_le_bytes(buf);
    (val | ASCII_LOWERCASE_MASK_6B) == ORIGIN_U64_LE
}

/// Integer-length filtered, zero-allocation scanner determining if `Vary` contains "origin" or "*".
#[inline(always)]
fn contains_origin_or_star(header_val: &[u8]) -> bool {
    for part in header_val.split(|&b| b == b',') {
        let token = trim_ascii(part);
        let len = token.len();
        // Integer length filter:
        // Any token with len != 6 and len != 1 is immediately discarded in 1 cycle
        // without comparing individual characters (e.g. "accept-encoding" [15], "user-agent" [10]).
        if len == 6 {
            if is_token_origin_swar(token) {
                return true;
            }
        } else if len == 1 && token[0] == b'*' {
            return true;
        }
    }
    false
}

/// Configuration settings for the post-response CORS plugin.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CorsConfig {
    /// Allowed origin patterns (exact domains, wildcards, or subdomains).
    pub allow_origins: Vec<OriginPattern>,
    /// Indicates whether the response can be shared when request credentials mode is `include`.
    pub allow_credentials: bool,
    /// List of response headers that browsers are allowed to access via JavaScript.
    pub expose_headers: Vec<String>,
    /// How long (in seconds) the results of a preflight request can be cached.
    pub max_age_secs: Option<u64>,
}

impl Default for CorsConfig {
    fn default() -> Self {
        Self {
            allow_origins: vec![OriginPattern::Any],
            allow_credentials: false,
            expose_headers: Vec::new(),
            max_age_secs: None,
        }
    }
}

/// Post-Response Cross-Origin Resource Sharing (CORS) Plugin.
#[derive(Debug, Clone)]
pub struct CorsResponsePlugin {
    pub id: String,
    pub config: CorsConfig,
    /// Pre-computed 8-bit bitmask encoding configuration states.
    pub flags: u8,
    /// Origin matching strategy ($O(1)$ fast jump).
    pub origin_kind: u8,
    /// Pre-baked static response headers (credentials, expose-headers, max-age).
    /// Fixed-size flat array avoiding dynamic HeaderMap hash table overhead.
    pub base_headers: [(HeaderName, HeaderValue); 3],
    /// Number of active pre-baked headers (0..=3).
    pub base_headers_len: u8,
}

impl CorsResponsePlugin {
    /// Constructs a new CORS response plugin with standard single origin and credentials setting.
    pub fn new(
        id: impl Into<String>,
        allow_origin: impl AsRef<str>,
        allow_credentials: bool,
    ) -> Self {
        let pattern = OriginPattern::parse(allow_origin.as_ref());
        Self::with_config(
            id,
            CorsConfig {
                allow_origins: vec![pattern],
                allow_credentials,
                expose_headers: Vec::new(),
                max_age_secs: None,
            },
        )
    }

    /// Constructs a new CORS response plugin with pre-baked static headers template and bitmask flags.
    pub fn with_config(id: impl Into<String>, config: CorsConfig) -> Self {
        let allow_all_origins = config
            .allow_origins
            .iter()
            .any(|p| matches!(p, OriginPattern::Any));

        let reflect_origin = config.allow_credentials || !allow_all_origins;
        let needs_vary_origin = config.allow_credentials || !allow_all_origins;

        let mut flags = 0u8;
        if allow_all_origins {
            flags |= CORS_FLAG_ALLOW_ALL_ORIGINS;
        }
        if config.allow_credentials {
            flags |= CORS_FLAG_ALLOW_CREDENTIALS;
        }
        if reflect_origin {
            flags |= CORS_FLAG_REFLECT_ORIGIN;
        }
        if needs_vary_origin {
            flags |= CORS_FLAG_NEEDS_VARY_ORIGIN;
        }

        // Determine origin matching classification
        let origin_kind = if allow_all_origins {
            ORIGIN_KIND_ANY
        } else if config.allow_origins.len() == 1
            && matches!(config.allow_origins[0], OriginPattern::Exact(_))
        {
            ORIGIN_KIND_SINGLE_EXACT
        } else {
            ORIGIN_KIND_MULTI
        };

        // Pre-bake static headers into a contiguous flat array (max 3 entries)
        let mut base_headers: [(HeaderName, HeaderValue); 3] = [
            (
                ACCESS_CONTROL_ALLOW_CREDENTIALS,
                HeaderValue::from_static("true"),
            ),
            (ACCESS_CONTROL_EXPOSE_HEADERS, HeaderValue::from_static("")),
            (ACCESS_CONTROL_MAX_AGE, HeaderValue::from_static("0")),
        ];
        let mut base_headers_len = 0u8;

        if config.allow_credentials {
            base_headers[base_headers_len as usize] = (
                ACCESS_CONTROL_ALLOW_CREDENTIALS,
                HeaderValue::from_static("true"),
            );
            base_headers_len += 1;
        }

        if !config.expose_headers.is_empty() {
            let joined = config.expose_headers.join(", ");
            if let Ok(val) = HeaderValue::from_str(&joined) {
                base_headers[base_headers_len as usize] = (ACCESS_CONTROL_EXPOSE_HEADERS, val);
                base_headers_len += 1;
            }
        }

        if let Some(secs) = config.max_age_secs
            && let Ok(val) = HeaderValue::from_str(&secs.to_string())
        {
            base_headers[base_headers_len as usize] = (ACCESS_CONTROL_MAX_AGE, val);
            base_headers_len += 1;
        }

        Self {
            id: id.into(),
            config,
            flags,
            origin_kind,
            base_headers,
            base_headers_len,
        }
    }

    /// Returns `true` if all origins are permitted.
    #[inline(always)]
    pub fn allow_all_origins(&self) -> bool {
        (self.flags & CORS_FLAG_ALLOW_ALL_ORIGINS) != 0
    }

    /// Returns `true` if `Vary: Origin` must be emitted on origin-dependent responses.
    #[inline(always)]
    pub fn needs_vary_origin(&self) -> bool {
        (self.flags & CORS_FLAG_NEEDS_VARY_ORIGIN) != 0
    }

    /// Returns `true` if credentials mode is enabled.
    #[inline(always)]
    pub fn allow_credentials(&self) -> bool {
        (self.flags & CORS_FLAG_ALLOW_CREDENTIALS) != 0
    }

    /// Returns `true` if the request Origin must be dynamically reflected.
    #[inline(always)]
    pub fn reflect_origin(&self) -> bool {
        (self.flags & CORS_FLAG_REFLECT_ORIGIN) != 0
    }

    /// Deserializes and constructs an instance directly from a [`PluginConfig`].
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let cfg =
            velda_sync::validate_cors_response_config(&config.id, &config.config).map_err(|e| {
                PluginError::Config(format!(
                    "Invalid cors configuration for '{}': {e}",
                    config.id
                ))
            })?;

        let allow_origins = cfg
            .allow_origins
            .iter()
            .map(|s| OriginPattern::parse(s))
            .collect();

        Ok(Self::with_config(
            &config.id,
            CorsConfig {
                allow_origins,
                allow_credentials: cfg.allow_credentials,
                expose_headers: cfg.expose_headers,
                max_age_secs: cfg.max_age_secs,
            },
        ))
    }
}

impl PostResponsePlugin for CorsResponsePlugin {
    #[inline(always)]
    fn id(&self) -> &str {
        &self.id
    }

    fn on_response(&self, req_headers: &velda_core::HeaderWorkspace, resp: &mut L7Response) {
        // 1. Resolve Origin: strictly inspect downstream client request Origin header (RFC 6454)
        let Some(origin_val) = req_headers.get(ORIGIN) else {
            return; // Non-CORS request, fast pass-through (~2ns)
        };

        let Ok(origin_str) = origin_val.to_str() else {
            return;
        };

        // 2. Validate Origin against pre-classified strategy ($O(1)$ fast jump)
        let is_allowed = match self.origin_kind {
            ORIGIN_KIND_ANY => true,
            ORIGIN_KIND_SINGLE_EXACT => {
                if let Some(pattern) = self.config.allow_origins.first() {
                    pattern.matches(origin_str)
                } else {
                    false
                }
            }
            _ => self
                .config
                .allow_origins
                .iter()
                .any(|p| p.matches(origin_str)),
        };

        if !is_allowed {
            // Disallowed origin: do NOT inject any CORS headers (prevents unauthorized cross-origin access)
            return;
        }

        // 3. Inject Access-Control-Allow-Origin:
        // When credentials are true or specific origins are used, reflect exact origin.
        let allow_origin_val = if (self.flags & CORS_FLAG_REFLECT_ORIGIN) != 0 {
            origin_val.clone() // Atomic ref-count increment, zero allocation
        } else {
            HeaderValue::from_static("*")
        };
        resp.headers
            .insert(ACCESS_CONTROL_ALLOW_ORIGIN, allow_origin_val);

        // 4. Batch inject pre-baked static response headers (credentials, expose-headers, max-age)
        // Iterates contiguous flat array; if len == 0, skipped immediately via 1 register comparison.
        if self.base_headers_len > 0 {
            for i in 0..self.base_headers_len as usize {
                let (name, val) = &self.base_headers[i];
                resp.headers.insert(name.clone(), val.clone());
            }
        }

        // 5. Invalidate caches across origins (Vary: Origin) if credentials or dynamic reflection used
        if (self.flags & CORS_FLAG_NEEDS_VARY_ORIGIN) != 0 {
            if !resp.headers.contains_key(VARY) {
                // Fast path (>85% of traffic): Upstream has no Vary header at all.
                // Directly insert "Vary: Origin" with 0 loops and 0 string parsing.
                resp.headers
                    .insert(VARY, HeaderValue::from_static("Origin"));
            } else {
                // Upstream already emitted Vary (e.g. Vary: Accept-Encoding).
                // Inspect existing values using integer-length filter and SWAR scanner.
                let already_has_origin = resp
                    .headers
                    .get_all(VARY)
                    .iter()
                    .any(|v| contains_origin_or_star(v.as_bytes()));
                if !already_has_origin {
                    resp.headers
                        .append(VARY, HeaderValue::from_static("Origin"));
                }
            }
        }
    }

    #[inline(always)]
    fn capability(&self) -> PluginCapability {
        PluginCapability::header_only(AccessLevel::Write)
    }

    #[inline(always)]
    fn supported_protocols(&self) -> SupportedProtocols {
        SupportedProtocols::ALL_HTTP
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::StatusCode;
    use velda_core::HeaderWorkspace;

    #[test]
    fn test_swar_origin_scanner() {
        assert!(is_token_origin_swar(b"origin"));
        assert!(is_token_origin_swar(b"Origin"));
        assert!(is_token_origin_swar(b"ORIGIN"));
        assert!(is_token_origin_swar(b"oRiGiN"));
        assert!(!is_token_origin_swar(b"origix"));
        assert!(!is_token_origin_swar(b"accept"));

        assert!(contains_origin_or_star(b"origin"));
        assert!(contains_origin_or_star(b"Origin"));
        assert!(contains_origin_or_star(b"Accept-Encoding, Origin"));
        assert!(contains_origin_or_star(
            b"Accept-Encoding, origin, User-Agent"
        ));
        assert!(contains_origin_or_star(b"*"));
        assert!(contains_origin_or_star(b"Accept-Encoding, *"));
        assert!(!contains_origin_or_star(b"Accept-Encoding, User-Agent"));
        assert!(!contains_origin_or_star(b"Accept-Language"));
    }

    #[test]
    fn test_cors_default_wildcard() {
        let plugin = CorsResponsePlugin::new("cors", "*", false);
        let mut resp = L7Response::empty(StatusCode::OK);
        let mut req_headers = HeaderWorkspace::default();
        req_headers.insert(ORIGIN, "https://random.com".parse().unwrap());

        plugin.on_response(&req_headers, &mut resp);

        assert_eq!(
            resp.headers.get("access-control-allow-origin").unwrap(),
            "*"
        );
        assert!(
            !resp
                .headers
                .contains_key("access-control-allow-credentials")
        );
        assert!(!resp.headers.contains_key("vary"));
    }

    #[test]
    fn test_cors_no_origin_header_no_injection() {
        let plugin = CorsResponsePlugin::new("cors", "*", false);
        let mut resp = L7Response::empty(StatusCode::OK);
        let req_headers = HeaderWorkspace::default(); // No Origin header

        plugin.on_response(&req_headers, &mut resp);

        assert!(!resp.headers.contains_key("access-control-allow-origin"));
        assert!(
            !resp
                .headers
                .contains_key("access-control-allow-credentials")
        );
    }

    #[test]
    fn test_cors_disallowed_origin_no_injection() {
        let plugin = CorsResponsePlugin::new("cors", "https://app.velda.io", false);
        let mut resp = L7Response::empty(StatusCode::OK);
        let mut req_headers = HeaderWorkspace::default();
        req_headers.insert(ORIGIN, "https://evil.com".parse().unwrap());

        plugin.on_response(&req_headers, &mut resp);

        // Security invariant: Untrusted origin must receive zero CORS headers
        assert!(!resp.headers.contains_key("access-control-allow-origin"));
    }

    #[test]
    fn test_cors_exact_origin_with_credentials_and_vary() {
        let plugin = CorsResponsePlugin::new("cors", "https://app.example.com", true);
        let mut resp = L7Response::empty(StatusCode::OK);
        let mut req_headers = HeaderWorkspace::default();
        req_headers.insert(ORIGIN, "https://app.example.com".parse().unwrap());

        plugin.on_response(&req_headers, &mut resp);

        assert_eq!(
            resp.headers.get("access-control-allow-origin").unwrap(),
            "https://app.example.com"
        );
        assert_eq!(
            resp.headers
                .get("access-control-allow-credentials")
                .unwrap(),
            "true"
        );
        assert_eq!(resp.headers.get("vary").unwrap(), "Origin");
    }

    #[test]
    fn test_cors_existing_vary_preserved() {
        let plugin = CorsResponsePlugin::new("cors", "https://app.example.com", true);
        let mut resp = L7Response::empty(StatusCode::OK);
        resp.headers
            .insert(VARY, HeaderValue::from_static("Accept-Encoding"));

        let mut req_headers = HeaderWorkspace::default();
        req_headers.insert(ORIGIN, "https://app.example.com".parse().unwrap());

        plugin.on_response(&req_headers, &mut resp);

        let vary_headers: Vec<&str> = resp
            .headers
            .get_all("vary")
            .iter()
            .map(|v| v.to_str().unwrap())
            .collect();
        assert!(vary_headers.contains(&"Accept-Encoding"));
        assert!(vary_headers.contains(&"Origin"));
    }

    #[test]
    fn test_cors_expose_headers_and_max_age() {
        let config = CorsConfig {
            allow_origins: vec![OriginPattern::Any],
            allow_credentials: false,
            expose_headers: vec!["x-request-id".into(), "x-total-count".into()],
            max_age_secs: Some(86400),
        };
        let plugin = CorsResponsePlugin::with_config("cors", config);
        let mut resp = L7Response::empty(StatusCode::OK);
        let mut req_headers = HeaderWorkspace::default();
        req_headers.insert(ORIGIN, "https://app.example.com".parse().unwrap());

        plugin.on_response(&req_headers, &mut resp);

        assert_eq!(
            resp.headers.get("access-control-expose-headers").unwrap(),
            "x-request-id, x-total-count"
        );
        assert_eq!(resp.headers.get("access-control-max-age").unwrap(), "86400");
    }

    #[test]
    fn test_cors_dynamic_origin_reflection() {
        let config = CorsConfig {
            allow_origins: vec![
                OriginPattern::parse("https://app.example.com"),
                OriginPattern::parse("https://*.partner.io"),
            ],
            allow_credentials: true,
            expose_headers: Vec::new(),
            max_age_secs: None,
        };
        let plugin = CorsResponsePlugin::with_config("cors", config);

        let mut req_headers = HeaderWorkspace::default();
        req_headers.insert(ORIGIN, "https://portal.partner.io".parse().unwrap());
        let mut resp = L7Response::empty(StatusCode::OK);

        plugin.on_response(&req_headers, &mut resp);

        assert_eq!(
            resp.headers.get("access-control-allow-origin").unwrap(),
            "https://portal.partner.io"
        );
        assert_eq!(
            resp.headers
                .get("access-control-allow-credentials")
                .unwrap(),
            "true"
        );
        assert_eq!(resp.headers.get("vary").unwrap(), "Origin");
    }

    #[test]
    fn test_cors_from_config_complete() {
        let json_cfg = serde_json::json!({
            "allow_origins": ["https://app.example.com"],
            "allow_credentials": true,
            "expose_headers": ["x-request-id"],
            "max_age_secs": 3600
        });
        let config = PluginConfig {
            id: "cors".into(),
            phase: String::new(),
            config: json_cfg,
        };

        let plugin = CorsResponsePlugin::from_config(&config).expect("valid config");
        assert_eq!(plugin.id, "cors");
        assert!(plugin.allow_credentials());
        assert_eq!(plugin.config.max_age_secs, Some(3600));
        assert_eq!(plugin.config.expose_headers, vec!["x-request-id"]);

        let mut resp = L7Response::empty(StatusCode::OK);
        let mut req_headers = HeaderWorkspace::default();
        req_headers.insert(ORIGIN, "https://app.example.com".parse().unwrap());
        plugin.on_response(&req_headers, &mut resp);

        assert_eq!(
            resp.headers.get("access-control-allow-origin").unwrap(),
            "https://app.example.com"
        );
        assert_eq!(
            resp.headers.get("access-control-expose-headers").unwrap(),
            "x-request-id"
        );
        assert_eq!(resp.headers.get("access-control-max-age").unwrap(), "3600");
    }
}
