//! Cross-Origin Resource Sharing (CORS) Post-Response Plugin.
//!
//! Validate-then-write CORS enforcement on outgoing upstream responses
//! (`RFC 9110 §12.5.5` & `WHATWG Fetch`). The response header map is only written after every
//! decision is made; no header is written and then removed.
//!
//! ```text
//!   req_headers["origin"] ──► is_origin_allowed()   (read-only, no response access)
//!         │
//!         ▼
//!   [1] needs_vary_origin (`allow_credentials || !allow_all_origins`)
//!         └─► ensure `Vary: Origin` (SWAR scan of existing `Vary`), for ALL outcomes
//!         │
//!         ▼
//!   [2] Origin missing or disallowed ─► return (no `Access-Control-*` written)
//!         │
//!         ▼
//!   [3] Origin allowed ─► one `insert` per enabled header:
//!         • `Access-Control-Allow-Origin`      (`*` or reflected `Origin`)
//!         • `Access-Control-Allow-Credentials` (`"true"`, only if enabled)
//!         • `Access-Control-Expose-Headers`    (pre-baked, only if configured)
//! ```

use http::header::HeaderValue;
use http::header::{
    ACCESS_CONTROL_ALLOW_CREDENTIALS, ACCESS_CONTROL_ALLOW_ORIGIN, ACCESS_CONTROL_EXPOSE_HEADERS,
    ORIGIN, VARY,
};
use velda_core::{HeaderWorkspace, L7Response};

use crate::capability::{AccessLevel, PluginCapability, SupportedProtocols};
use crate::config::PluginConfig;
use crate::error::PluginError;
use crate::post_response::PostResponsePlugin;
use crate::post_response::cors::origin::{CorsOriginPattern, is_valid_header_name};
use crate::post_response::cors::validation::validate_cors_response_config;

/// Pre-computed bitmask flags for single-instruction configuration branching.
pub const CORS_FLAG_ALLOW_ALL_ORIGINS: u8 = 1 << 0;
pub const CORS_FLAG_ALLOW_CREDENTIALS: u8 = 1 << 1;
pub const CORS_FLAG_REFLECT_ORIGIN: u8 = 1 << 2;
pub const CORS_FLAG_NEEDS_VARY_ORIGIN: u8 = 1 << 3;

/// Pre-classified origin matching strategy.
pub const ORIGIN_KIND_ANY: u8 = 0;
pub const ORIGIN_KIND_SINGLE_EXACT: u8 = 1;
pub const ORIGIN_KIND_MULTI: u8 = 2;

/// 64-bit little-endian word for `"origin\0\0"`.
const ORIGIN_U64_LE: u64 = 0x0000_6e69_6769_726f;
/// 64-bit ASCII lowercase conversion bitmask for 6 characters.
const ASCII_LOWERCASE_MASK_6B: u64 = 0x0000_2020_2020_2020;

/// 64-bit SWAR case-insensitive comparison for 6-byte slice == `"origin"`.
#[inline(always)]
fn is_token_origin_swar(token: &[u8]) -> bool {
    debug_assert_eq!(token.len(), 6);
    let mut buf = [0u8; 8];
    buf[..6].copy_from_slice(token);
    (u64::from_le_bytes(buf) | ASCII_LOWERCASE_MASK_6B) == ORIGIN_U64_LE
}

/// Zero-allocation comma-delimited scanner checking if `Vary` contains `Origin` or `*`.
#[inline(always)]
fn vary_contains_origin_or_star(header_val: &[u8]) -> bool {
    for part in header_val.split(|&b| b == b',') {
        let token = part.trim_ascii();
        if token.len() == 6 {
            if is_token_origin_swar(token) {
                return true;
            }
        } else if token == b"*" {
            return true;
        }
    }
    false
}

/// Compiled configuration for [`CorsResponsePlugin`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CorsConfig {
    pub allow_origins: Vec<CorsOriginPattern>,
    pub allow_credentials: bool,
    pub expose_headers: Vec<String>,
}

impl Default for CorsConfig {
    fn default() -> Self {
        Self {
            allow_origins: vec![CorsOriginPattern::Any],
            allow_credentials: false,
            expose_headers: Vec::new(),
        }
    }
}

/// Post-Response CORS enforcement plugin (hot-path only, `pbww = 0`).
#[derive(Debug, Clone)]
pub struct CorsResponsePlugin {
    pub id: String,
    pub config: CorsConfig,
    pub flags: u8,
    pub origin_kind: u8,
    pub expose_headers: Option<HeaderValue>,
}

impl CorsResponsePlugin {
    /// Constructs a validated single-origin CORS plugin enforcing RFC 6454 credential rules.
    pub fn new(
        id: impl Into<String>,
        allow_origin: impl AsRef<str>,
        allow_credentials: bool,
    ) -> Result<Self, PluginError> {
        let pattern = CorsOriginPattern::parse(allow_origin.as_ref());
        Self::with_config(
            id,
            CorsConfig {
                allow_origins: vec![pattern],
                allow_credentials,
                expose_headers: Vec::new(),
            },
        )
    }

    /// Constructs a validated [`CorsResponsePlugin`] from [`CorsConfig`], enforcing RFC 6454
    /// and W3C Fetch invariants regardless of whether invoked via JSON sync or Rust API.
    pub fn with_config(id: impl Into<String>, config: CorsConfig) -> Result<Self, PluginError> {
        let plugin_id = id.into();
        if config.allow_origins.is_empty() {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': allow_origins cannot be empty"
            )));
        }

        if config.allow_credentials
            && config
                .allow_origins
                .iter()
                .any(CorsOriginPattern::is_wildcard_or_null)
        {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': allow_credentials cannot be true with '*' or 'null' origin (RFC 6454 violation)"
            )));
        }

        for h in &config.expose_headers {
            if !is_valid_header_name(h) {
                return Err(PluginError::Config(format!(
                    "Plugin '{plugin_id}': invalid header token '{h}' in expose_headers"
                )));
            }
            if config.allow_credentials && h.trim() == "*" {
                return Err(PluginError::Config(format!(
                    "Plugin '{plugin_id}': expose_headers cannot contain '*' when allow_credentials is true (W3C Fetch violation)"
                )));
            }
        }

        let allow_all_origins = config
            .allow_origins
            .iter()
            .any(|p| matches!(p, CorsOriginPattern::Any));
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

        let origin_kind = if allow_all_origins {
            ORIGIN_KIND_ANY
        } else if config.allow_origins.len() == 1
            && matches!(config.allow_origins[0], CorsOriginPattern::Exact(_))
        {
            ORIGIN_KIND_SINGLE_EXACT
        } else {
            ORIGIN_KIND_MULTI
        };

        for pattern in &config.allow_origins {
            let is_ascii_valid = match pattern {
                CorsOriginPattern::Any => true,
                CorsOriginPattern::Exact(b) => b.iter().all(|&c| (0x21..=0x7e).contains(&c)),
                CorsOriginPattern::Subdomain { prefix, suffix } => {
                    prefix.iter().all(|&c| (0x21..=0x7e).contains(&c))
                        && suffix.iter().all(|&c| (0x21..=0x7e).contains(&c))
                }
            };
            if !is_ascii_valid {
                return Err(PluginError::Config(format!(
                    "Plugin '{plugin_id}': allow_origins patterns must contain visible ASCII characters only"
                )));
            }
        }

        let expose_headers = if config.expose_headers.is_empty() {
            None
        } else {
            let joined = config.expose_headers.join(", ");
            let val = HeaderValue::from_str(&joined).map_err(|e| {
                PluginError::Config(format!(
                    "Plugin '{plugin_id}': invalid expose_headers value '{joined}': {e}"
                ))
            })?;
            Some(val)
        };

        Ok(Self {
            id: plugin_id,
            config,
            flags,
            origin_kind,
            expose_headers,
        })
    }

    /// Deserializes and constructs a [`CorsResponsePlugin`] from declarative [`PluginConfig`].
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let cfg = validate_cors_response_config(&config.id, &config.config)?;
        let allow_origins = cfg
            .allow_origins
            .iter()
            .map(|s| CorsOriginPattern::parse(s))
            .collect();

        Self::with_config(
            &config.id,
            CorsConfig {
                allow_origins,
                allow_credentials: cfg.allow_credentials,
                expose_headers: cfg.expose_headers,
            },
        )
    }

    #[inline(always)]
    fn is_origin_allowed(&self, origin: &HeaderValue) -> bool {
        match self.origin_kind {
            ORIGIN_KIND_ANY => origin.to_str().is_ok(),
            ORIGIN_KIND_SINGLE_EXACT => self
                .config
                .allow_origins
                .first()
                .is_some_and(|p| p.matches_bytes(origin.as_bytes())),
            _ => self
                .config
                .allow_origins
                .iter()
                .any(|p| p.matches_bytes(origin.as_bytes())),
        }
    }
}

impl PostResponsePlugin for CorsResponsePlugin {
    #[inline(always)]
    fn id(&self) -> &str {
        &self.id
    }

    #[inline(always)]
    fn pbww(&self) -> u32 {
        0
    }

    #[inline]
    fn on_response(
        &self,
        _req_state: &velda_core::RequestState,
        req_headers: &HeaderWorkspace,
        resp: &mut L7Response,
    ) {
        // Step 1 (read-only): decide everything before touching the response.
        let allowed_origin = req_headers
            .get(ORIGIN)
            .filter(|val| self.is_origin_allowed(val));

        // Step 2 (write): `Vary: Origin` for origin-dependent policies (RFC 9110 §12.5.5),
        // emitted for allowed, disallowed and missing `Origin` alike.
        if (self.flags & CORS_FLAG_NEEDS_VARY_ORIGIN) != 0 {
            let mut vary_iter = resp.headers.get_all(VARY);
            match vary_iter.next() {
                None => {
                    resp.headers
                        .insert(VARY, HeaderValue::from_static("Origin"));
                }
                Some(first) => {
                    let has_origin = vary_contains_origin_or_star(first.as_bytes())
                        || vary_iter.any(|v| vary_contains_origin_or_star(v.as_bytes()));
                    if !has_origin {
                        resp.headers
                            .append(VARY, HeaderValue::from_static("Origin"));
                    }
                }
            }
        }

        // Step 3: missing or disallowed Origin -> no `Access-Control-*` header is written.
        let Some(origin_val) = allowed_origin else {
            return;
        };

        // Step 4 (write): allowed Origin -> each enabled header is inserted exactly once.
        let allow_origin_val = if (self.flags & CORS_FLAG_REFLECT_ORIGIN) != 0 {
            origin_val.clone()
        } else {
            HeaderValue::from_static("*")
        };
        resp.headers
            .insert(ACCESS_CONTROL_ALLOW_ORIGIN, allow_origin_val);

        if (self.flags & CORS_FLAG_ALLOW_CREDENTIALS) != 0 {
            resp.headers.insert(
                ACCESS_CONTROL_ALLOW_CREDENTIALS,
                HeaderValue::from_static("true"),
            );
        }

        if let Some(ref expose_val) = self.expose_headers {
            resp.headers
                .insert(ACCESS_CONTROL_EXPOSE_HEADERS, expose_val.clone());
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

    #[test]
    fn test_swar_vary_scanner() {
        assert!(is_token_origin_swar(b"origin"));
        assert!(is_token_origin_swar(b"Origin"));
        assert!(is_token_origin_swar(b"ORIGIN"));
        assert!(!is_token_origin_swar(b"origix"));

        assert!(vary_contains_origin_or_star(b"Accept-Encoding, Origin"));
        assert!(vary_contains_origin_or_star(b"Accept-Encoding, *"));
        assert!(!vary_contains_origin_or_star(
            b"Accept-Encoding, User-Agent"
        ));
    }

    #[test]
    fn test_wildcard_without_credentials() {
        let plugin = CorsResponsePlugin::new("cors", "*", false).unwrap();
        let mut resp = L7Response::empty(StatusCode::OK);
        let mut req_headers = HeaderWorkspace::default();
        req_headers.insert(ORIGIN, HeaderValue::from_static("https://random.com"));

        plugin.on_response(
            &velda_core::RequestState::default(),
            &req_headers,
            &mut resp,
        );

        assert_eq!(resp.headers.get(ACCESS_CONTROL_ALLOW_ORIGIN).unwrap(), "*");
        assert!(!resp.headers.contains_key(ACCESS_CONTROL_ALLOW_CREDENTIALS));
        assert!(!resp.headers.contains_key(VARY));
    }

    #[test]
    fn test_rust_constructor_rejects_wildcard_with_credentials() {
        assert!(CorsResponsePlugin::new("cors", "*", true).is_err());
        assert!(CorsResponsePlugin::new("cors", "null", true).is_err());
    }

    #[test]
    fn test_disallowed_origin_writes_no_cors_headers() {
        let plugin = CorsResponsePlugin::new("cors", "https://app.velda.io", true).unwrap();

        for origin in [Some("https://evil.com"), None] {
            let mut resp = L7Response::empty(StatusCode::OK);
            let len_before = resp.headers.len();
            let mut req_headers = HeaderWorkspace::default();
            if let Some(o) = origin {
                req_headers.insert(ORIGIN, HeaderValue::from_static(o));
            }

            plugin.on_response(
                &velda_core::RequestState::default(),
                &req_headers,
                &mut resp,
            );

            assert!(!resp.headers.contains_key(ACCESS_CONTROL_ALLOW_ORIGIN));
            assert!(!resp.headers.contains_key(ACCESS_CONTROL_ALLOW_CREDENTIALS));
            assert!(!resp.headers.contains_key(ACCESS_CONTROL_EXPOSE_HEADERS));
            // RFC 9110 §12.5.5: origin-dependent policy emits Vary: Origin even on rejected origin
            assert_eq!(resp.headers.get(VARY).unwrap(), "Origin");
            // Only `Vary` was added; nothing else was written.
            assert_eq!(resp.headers.len(), len_before + 1);
        }
    }

    #[test]
    fn test_allowed_origin_writes_only_enabled_headers() {
        let plugin = CorsResponsePlugin::new("cors", "https://app.velda.io", false).unwrap();

        let mut resp = L7Response::empty(StatusCode::OK);
        let mut req_headers = HeaderWorkspace::default();
        req_headers.insert(ORIGIN, HeaderValue::from_static("https://app.velda.io"));

        plugin.on_response(
            &velda_core::RequestState::default(),
            &req_headers,
            &mut resp,
        );

        assert_eq!(
            resp.headers.get(ACCESS_CONTROL_ALLOW_ORIGIN).unwrap(),
            "https://app.velda.io"
        );
        assert!(!resp.headers.contains_key(ACCESS_CONTROL_ALLOW_CREDENTIALS));
        assert!(!resp.headers.contains_key(ACCESS_CONTROL_EXPOSE_HEADERS));
        assert_eq!(resp.headers.get(VARY).unwrap(), "Origin");
    }

    #[test]
    fn test_from_config_complete_with_expose_headers() {
        let cfg = PluginConfig {
            id: "cors".into(),
            config: serde_json::json!({
                "allow_origins": ["https://app.example.com", "https://*.partner.io"],
                "allow_credentials": true,
                "expose_headers": ["x-request-id", "x-total-count"]
            }),
        };

        let plugin = CorsResponsePlugin::from_config(&cfg).expect("valid config");
        let mut resp = L7Response::empty(StatusCode::OK);
        resp.headers
            .insert(VARY, HeaderValue::from_static("Accept-Encoding"));

        let mut req_headers = HeaderWorkspace::default();
        req_headers.insert(
            ORIGIN,
            HeaderValue::from_static("https://portal.partner.io"),
        );

        plugin.on_response(
            &velda_core::RequestState::default(),
            &req_headers,
            &mut resp,
        );

        assert_eq!(
            resp.headers.get(ACCESS_CONTROL_ALLOW_ORIGIN).unwrap(),
            "https://portal.partner.io"
        );
        assert_eq!(
            resp.headers.get(ACCESS_CONTROL_ALLOW_CREDENTIALS).unwrap(),
            "true"
        );
        assert_eq!(
            resp.headers.get(ACCESS_CONTROL_EXPOSE_HEADERS).unwrap(),
            "x-request-id, x-total-count"
        );
        let vary_vals: Vec<_> = resp
            .headers
            .get_all(VARY)
            .iter()
            .map(|v| v.to_str().unwrap())
            .collect();
        assert_eq!(vary_vals, vec!["Accept-Encoding", "Origin"]);
    }
}
