//! Cross-Origin Resource Sharing (CORS) Preflight Pre-Route Plugin (`phase: "pre_route"`).
//!
//! Intercepts downstream HTTP `OPTIONS` preflight requests prior to route matching,
//! authentication evaluation, or upstream dispatch, immediately returning an RFC-compliant
//! `204 No Content` response with pre-configured CORS permissions.
//!
//! # Execution Invariants & Hardware Optimizations
//! - **Pre-Baked Header Template (Zero-Branch Hot Path)**: Pre-computes all static response headers
//!   (`methods`, `credentials`, `max-age`, `vary`, `content-length`) at initialization. On request,
//!   simply clones the template into the response, eliminating 5+ runtime `if` checks.
//! - **Bitmask Method Validation**: Maps HTTP methods to a compact `u16` bitmask for $O(1)$
//!   single-instruction bitwise validation (`AND`), replacing $O(N)$ linear scans and branching.
//! - **Hardened Origin Matching**: Enforces strict DNS label syntax via branchless 256-byte lookup
//!   table, rejecting CVE-2020-7662 wildcard injection probes without CPU pipeline stalls.
//! - **Credential Restrictions (RFC 6454)**: When `allow_credentials` is true, wildcards (`*`) are
//!   strictly prohibited; the exact requesting origin is reflected.
//! - **Flat Workflow**: Entire execution is linear top-to-bottom without helper indirection (Rule 2.2).

use http::header::{
    ACCESS_CONTROL_ALLOW_CREDENTIALS, ACCESS_CONTROL_ALLOW_HEADERS, ACCESS_CONTROL_ALLOW_METHODS,
    ACCESS_CONTROL_ALLOW_ORIGIN, ACCESS_CONTROL_MAX_AGE, ACCESS_CONTROL_REQUEST_HEADERS,
    ACCESS_CONTROL_REQUEST_METHOD, CONTENT_LENGTH, ORIGIN, VARY,
};
use http::{HeaderMap, HeaderValue, Method, StatusCode};
use velda_core::{Action, L7Request, L7Response, RejectReason};
use velda_sync::post_sync::plugin::PluginConfig;

use crate::capability::{AccessLevel, Direction, PluginCapability, SupportedProtocols};
use crate::error::PluginError;
use crate::pre_route::PreRoutePlugin;

use super::origin::OriginPattern;

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

    /// Maps standard HTTP method ASCII tokens to bit flags.
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

/// Pre-Route CORS Preflight Interceptor Plugin.
#[derive(Debug, Clone)]
pub struct CorsPreflightPlugin {
    pub id: String,
    pub allowed_origins: Vec<OriginPattern>,
    pub allow_all_origins: bool,
    pub allowed_methods_mask: u16,
    pub base_headers: HeaderMap,
    pub allow_all_headers: bool,
    pub allow_credentials: bool,
}

impl CorsPreflightPlugin {
    /// Constructs a new preflight interceptor with pre-computed header template and bitmask.
    pub fn new(
        id: impl Into<String>,
        allowed_origins: Vec<OriginPattern>,
        allowed_methods_val: HeaderValue,
        allowed_headers_val: Option<HeaderValue>,
        allow_all_headers: bool,
        allow_credentials: bool,
        max_age_val: Option<HeaderValue>,
    ) -> Self {
        let allow_all_origins = allowed_origins
            .iter()
            .any(|p| matches!(p, OriginPattern::Any));

        // Pre-compute bitmask for fast O(1) single-instruction method validation
        let mut allowed_methods_mask = 0u16;
        if let Ok(methods_str) = allowed_methods_val.to_str() {
            for m in methods_str.split(',') {
                allowed_methods_mask |= MethodMask::parse_token(m.trim());
            }
        }

        // Pre-bake static headers template (eliminates runtime if-else checks)
        let mut base_headers = HeaderMap::with_capacity(6);
        base_headers.insert(ACCESS_CONTROL_ALLOW_METHODS, allowed_methods_val);
        if allow_credentials {
            base_headers.insert(
                ACCESS_CONTROL_ALLOW_CREDENTIALS,
                HeaderValue::from_static("true"),
            );
        }
        if let Some(max_age) = max_age_val {
            base_headers.insert(ACCESS_CONTROL_MAX_AGE, max_age);
        }
        if !allow_all_headers && let Some(headers) = allowed_headers_val {
            base_headers.insert(ACCESS_CONTROL_ALLOW_HEADERS, headers);
        }
        base_headers.insert(
            VARY,
            HeaderValue::from_static(
                "Origin, Access-Control-Request-Method, Access-Control-Request-Headers",
            ),
        );
        base_headers.insert(CONTENT_LENGTH, HeaderValue::from_static("0"));

        Self {
            id: id.into(),
            allowed_origins,
            allow_all_origins,
            allowed_methods_mask,
            base_headers,
            allow_all_headers,
            allow_credentials,
        }
    }

    /// Deserializes and constructs an instance directly from a [`PluginConfig`].
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let cfg = velda_sync::validate_cors_preflight_config(&config.id, &config.config).map_err(
            |e| {
                PluginError::Config(format!(
                    "Invalid cors_preflight configuration for '{}': {e}",
                    config.id
                ))
            },
        )?;

        let allowed_origins = cfg
            .allowed_origins
            .iter()
            .map(|s| OriginPattern::parse(s))
            .collect();

        let methods_str = cfg.allowed_methods.join(", ");
        let allowed_methods_val = HeaderValue::from_str(&methods_str).map_err(|e| {
            PluginError::Config(format!(
                "Invalid allowed_methods '{methods_str}' for plugin '{}': {e}",
                config.id
            ))
        })?;

        let allow_all_headers = cfg.allowed_headers.iter().any(|h| h == "*");
        let allowed_headers_val = if allow_all_headers || cfg.allowed_headers.is_empty() {
            None
        } else {
            let joined = cfg.allowed_headers.join(", ");
            Some(HeaderValue::from_str(&joined).map_err(|e| {
                PluginError::Config(format!(
                    "Invalid allowed_headers '{joined}' for plugin '{}': {e}",
                    config.id
                ))
            })?)
        };

        let max_age_val = if cfg.max_age_secs > 0 {
            Some(
                HeaderValue::from_str(&cfg.max_age_secs.to_string())
                    .map_err(|e| PluginError::Config(format!("Invalid max_age_secs: {e}")))?,
            )
        } else {
            None
        };

        Ok(Self::new(
            &config.id,
            allowed_origins,
            allowed_methods_val,
            allowed_headers_val,
            allow_all_headers,
            cfg.allow_credentials,
            max_age_val,
        ))
    }
}

impl PreRoutePlugin for CorsPreflightPlugin {
    #[inline(always)]
    fn id(&self) -> &str {
        &self.id
    }

    fn on_request(&self, req: &mut L7Request) -> Action {
        // 1. Fast path: Must be HTTP OPTIONS method
        if req.method != Method::OPTIONS {
            return Action::Continue;
        }

        // 2. Must contain Origin header (RFC 6454)
        let Some(origin_val) = req.headers.get(ORIGIN) else {
            return Action::Continue;
        };

        let Ok(origin_str) = origin_val.to_str() else {
            return Action::Continue;
        };

        // 3. Must contain Access-Control-Request-Method header (W3C CORS preflight marker)
        let Some(req_method_val) = req.headers.get(ACCESS_CONTROL_REQUEST_METHOD) else {
            return Action::Continue;
        };

        let Ok(req_method_str) = req_method_val.to_str() else {
            return Action::Continue;
        };

        // 4. Validate requested HTTP method via bitmask ($O(1)$ single-instruction check)
        let req_mask = MethodMask::parse_token(req_method_str);
        if (self.allowed_methods_mask & req_mask) == 0 {
            return Action::Reject(RejectReason::Forbidden);
        }

        // 5. Validate Origin against allowed patterns
        let is_allowed =
            self.allow_all_origins || self.allowed_origins.iter().any(|p| p.matches(origin_str));

        if !is_allowed {
            return Action::Reject(RejectReason::Forbidden);
        }

        // 6. Zero-Branch Response Assembly via Pre-Baked Header Template
        let mut resp =
            L7Response::empty(StatusCode::NO_CONTENT).with_headers(self.base_headers.clone());

        // Reflect Origin: When credentials are true, W3C CORS prohibits wildcard '*'
        let allow_origin_val = if self.allow_credentials || !self.allow_all_origins {
            origin_val.clone()
        } else {
            HeaderValue::from_static("*")
        };
        resp.headers
            .insert(ACCESS_CONTROL_ALLOW_ORIGIN, allow_origin_val);

        // Reflect requested headers if configured for wildcard headers reflection
        if self.allow_all_headers
            && let Some(req_headers) = req.headers.get(ACCESS_CONTROL_REQUEST_HEADERS)
        {
            resp.headers
                .insert(ACCESS_CONTROL_ALLOW_HEADERS, req_headers.clone());
        }

        Action::Respond(resp)
    }

    #[inline(always)]
    fn capability(&self) -> PluginCapability {
        PluginCapability::header_only(AccessLevel::Read)
    }

    #[inline(always)]
    fn supported_protocols(&self) -> SupportedProtocols {
        SupportedProtocols::ALL_HTTP
    }

    #[inline(always)]
    fn direction(&self) -> Direction {
        Direction::Request
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderMap;
    use http::{Uri, Version};
    use serde_json::json;
    use velda_core::Body;

    fn build_request(method: Method, uri: &str, headers: HeaderMap) -> L7Request {
        L7Request::new(
            method,
            uri.parse::<Uri>().unwrap(),
            Version::HTTP_11,
            headers,
            Body::Empty,
        )
    }

    #[test]
    fn test_preflight_short_circuit_success() {
        let plugin = CorsPreflightPlugin::new(
            "test_cors",
            vec![OriginPattern::parse("https://*.velda.io")],
            HeaderValue::from_static("GET, POST, PUT, OPTIONS"),
            Some(HeaderValue::from_static("Authorization, Content-Type")),
            false,
            true,
            Some(HeaderValue::from_static("86400")),
        );

        let mut headers = HeaderMap::new();
        headers.insert(ORIGIN, "https://app.velda.io".parse().unwrap());
        headers.insert(ACCESS_CONTROL_REQUEST_METHOD, "POST".parse().unwrap());
        headers.insert(
            ACCESS_CONTROL_REQUEST_HEADERS,
            "Authorization, Content-Type".parse().unwrap(),
        );

        let mut req = build_request(Method::OPTIONS, "/api/v1/orders", headers);
        let action = plugin.on_request(&mut req);

        match action {
            Action::Respond(resp) => {
                assert_eq!(resp.status, StatusCode::NO_CONTENT);
                assert_eq!(
                    resp.headers.get(ACCESS_CONTROL_ALLOW_ORIGIN).unwrap(),
                    "https://app.velda.io"
                );
                assert_eq!(
                    resp.headers.get(ACCESS_CONTROL_ALLOW_METHODS).unwrap(),
                    "GET, POST, PUT, OPTIONS"
                );
                assert_eq!(
                    resp.headers.get(ACCESS_CONTROL_ALLOW_CREDENTIALS).unwrap(),
                    "true"
                );
                assert_eq!(resp.headers.get(ACCESS_CONTROL_MAX_AGE).unwrap(), "86400");
                assert_eq!(resp.headers.get(CONTENT_LENGTH).unwrap(), "0");
                assert_eq!(
                    resp.headers.get(VARY).unwrap(),
                    "Origin, Access-Control-Request-Method, Access-Control-Request-Headers"
                );
            }
            _ => panic!("Expected Action::Respond(204 No Content), got {:?}", action),
        }
    }

    #[test]
    fn test_preflight_disallowed_origin_rejected() {
        let plugin = CorsPreflightPlugin::new(
            "test_cors",
            vec![OriginPattern::parse("https://app.velda.io")],
            HeaderValue::from_static("GET, POST, OPTIONS"),
            None,
            true,
            false,
            None,
        );

        let mut headers = HeaderMap::new();
        headers.insert(ORIGIN, "https://malicious.evil.com".parse().unwrap());
        headers.insert(ACCESS_CONTROL_REQUEST_METHOD, "POST".parse().unwrap());

        let mut req = build_request(Method::OPTIONS, "/api/v1/orders", headers);
        let action = plugin.on_request(&mut req);
        assert!(
            matches!(action, Action::Reject(RejectReason::Forbidden)),
            "Expected Action::Reject(Forbidden), got {:?}",
            action
        );
    }

    #[test]
    fn test_preflight_disallowed_method_rejected() {
        let plugin = CorsPreflightPlugin::new(
            "test_cors",
            vec![OriginPattern::parse("https://app.velda.io")],
            HeaderValue::from_static("GET, POST, OPTIONS"),
            None,
            true,
            false,
            None,
        );

        let mut headers = HeaderMap::new();
        headers.insert(ORIGIN, "https://app.velda.io".parse().unwrap());
        // Requested method DELETE is not in allowed methods (GET, POST, OPTIONS)
        headers.insert(ACCESS_CONTROL_REQUEST_METHOD, "DELETE".parse().unwrap());

        let mut req = build_request(Method::OPTIONS, "/api/v1/orders", headers);
        let action = plugin.on_request(&mut req);
        assert!(
            matches!(action, Action::Reject(RejectReason::Forbidden)),
            "Expected Action::Reject(Forbidden), got {:?}",
            action
        );
    }

    #[test]
    fn test_preflight_wildcard_without_credentials() {
        let plugin = CorsPreflightPlugin::new(
            "test_cors",
            vec![OriginPattern::Any],
            HeaderValue::from_static("GET, OPTIONS"),
            None,
            true,
            false,
            None,
        );

        let mut headers = HeaderMap::new();
        headers.insert(ORIGIN, "https://external.org".parse().unwrap());
        headers.insert(ACCESS_CONTROL_REQUEST_METHOD, "GET".parse().unwrap());

        let mut req = build_request(Method::OPTIONS, "/public/data", headers);
        let action = plugin.on_request(&mut req);

        match action {
            Action::Respond(resp) => {
                assert_eq!(resp.status, StatusCode::NO_CONTENT);
                assert_eq!(resp.headers.get(ACCESS_CONTROL_ALLOW_ORIGIN).unwrap(), "*");
                assert!(!resp.headers.contains_key(ACCESS_CONTROL_ALLOW_CREDENTIALS));
            }
            _ => panic!("Expected Action::Respond(204 No Content), got {:?}", action),
        }
    }

    #[test]
    fn test_non_preflight_options_continues() {
        let plugin = CorsPreflightPlugin::new(
            "test_cors",
            vec![OriginPattern::Any],
            HeaderValue::from_static("GET, OPTIONS"),
            None,
            true,
            false,
            None,
        );

        // OPTIONS request without Origin header (e.g. cURL -X OPTIONS)
        let headers = HeaderMap::new();
        let mut req = build_request(Method::OPTIONS, "*", headers);
        assert!(matches!(plugin.on_request(&mut req), Action::Continue));

        // OPTIONS request with Origin but missing Access-Control-Request-Method
        let mut headers2 = HeaderMap::new();
        headers2.insert(ORIGIN, "https://app.velda.io".parse().unwrap());
        let mut req2 = build_request(Method::OPTIONS, "/api/v1/orders", headers2);
        assert!(matches!(plugin.on_request(&mut req2), Action::Continue));
    }

    #[test]
    fn test_non_options_method_continues() {
        let plugin = CorsPreflightPlugin::new(
            "test_cors",
            vec![OriginPattern::Any],
            HeaderValue::from_static("GET, POST"),
            None,
            true,
            false,
            None,
        );

        let mut headers = HeaderMap::new();
        headers.insert(ORIGIN, "https://app.velda.io".parse().unwrap());
        let mut req = build_request(Method::GET, "/api/v1/orders", headers);
        assert!(matches!(plugin.on_request(&mut req), Action::Continue));
    }

    #[test]
    fn test_from_config_full_deserialization() {
        let config = PluginConfig {
            id: "cors_preflight".into(),
            phase: "pre_route".into(),
            config: json!({
                "allowed_origins": ["https://*.example.com", "https://app.test.io"],
                "allowed_methods": ["GET", "POST", "PUT", "DELETE"],
                "allowed_headers": ["Authorization", "Content-Type", "X-Custom"],
                "allow_credentials": true,
                "max_age_secs": 3600
            }),
        };

        let plugin = CorsPreflightPlugin::from_config(&config).expect("valid config");
        assert_eq!(plugin.id, "cors_preflight");
        assert_eq!(plugin.allowed_origins.len(), 2);
        assert!(!plugin.allow_all_origins);
        assert!(plugin.allow_credentials);
        assert_eq!(
            plugin.base_headers.get(ACCESS_CONTROL_MAX_AGE).unwrap(),
            "3600"
        );

        // Incomplete config must fail validation
        let bad_config = PluginConfig {
            id: "cors_preflight".into(),
            phase: "pre_route".into(),
            config: json!({}),
        };
        assert!(CorsPreflightPlugin::from_config(&bad_config).is_err());
    }
}
