//! Cross-Origin Resource Sharing (CORS) Preflight Pre-Route Plugin (`phase: "pre_route"`).
//!
//! Intercepts downstream HTTP `OPTIONS` preflight requests prior to route matching,
//! authentication evaluation, or upstream dispatch, immediately returning an RFC-compliant
//! `204 No Content` response with pre-configured CORS permissions.
//!
//! Full schema specification, allowed values, and configuration examples:
//! See [`README.md`](./README.md).

use http::header::{
    ACCESS_CONTROL_ALLOW_CREDENTIALS, ACCESS_CONTROL_ALLOW_HEADERS, ACCESS_CONTROL_ALLOW_METHODS,
    ACCESS_CONTROL_ALLOW_ORIGIN, ACCESS_CONTROL_MAX_AGE, ACCESS_CONTROL_REQUEST_HEADERS,
    ACCESS_CONTROL_REQUEST_METHOD, CONTENT_LENGTH, CONTENT_TYPE, ORIGIN, VARY,
};
use http::{HeaderValue, Method, StatusCode};
use velda_core::{Action, L7Request, L7Response};
use velda_sync::post_sync::plugin::PluginConfig;

use crate::capability::{AccessLevel, Direction, PluginCapability, SupportedProtocols};
use crate::error::PluginError;
use crate::pre_route::PreRoutePlugin;

/// Origin matching pattern supporting exact domain and wildcard subdomain matching.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OriginPattern {
    /// Wildcard matching all origins (`*`).
    Any,
    /// Exact match (e.g. `https://app.velda.io`).
    Exact(String),
    /// Subdomain wildcard (e.g. `https://*.velda.io`).
    Subdomain { prefix: String, suffix: String },
}

impl OriginPattern {
    /// Parses an origin string into an [`OriginPattern`].
    pub fn parse(s: &str) -> Self {
        let s = s.trim();
        if s == "*" {
            Self::Any
        } else if let Some((prefix, suffix)) = s.split_once('*') {
            Self::Subdomain {
                prefix: prefix.to_string(),
                suffix: suffix.to_string(),
            }
        } else {
            Self::Exact(s.to_string())
        }
    }

    /// Evaluates if an incoming `Origin` matches this pattern.
    #[inline]
    pub fn matches(&self, origin: &str) -> bool {
        match self {
            Self::Any => true,
            Self::Exact(expected) => expected.eq_ignore_ascii_case(origin),
            Self::Subdomain { prefix, suffix } => {
                origin.len() >= prefix.len() + suffix.len()
                    && origin.starts_with(prefix)
                    && origin.ends_with(suffix)
            }
        }
    }
}

/// Pre-Route CORS Preflight Interceptor Plugin.
#[derive(Debug, Clone)]
pub struct CorsPreflightPlugin {
    pub id: String,
    pub allowed_origins: Vec<OriginPattern>,
    pub allow_all_origins: bool,
    pub allowed_methods: HeaderValue,
    pub allowed_headers: Option<HeaderValue>,
    pub allow_all_headers: bool,
    pub allow_credentials: bool,
    pub max_age: Option<HeaderValue>,
    pub vary_header: HeaderValue,
}

impl CorsPreflightPlugin {
    /// Constructs a new preflight interceptor with pre-computed header values.
    pub fn new(
        id: impl Into<String>,
        allowed_origins: Vec<OriginPattern>,
        allowed_methods: HeaderValue,
        allowed_headers: Option<HeaderValue>,
        allow_all_headers: bool,
        allow_credentials: bool,
        max_age: Option<HeaderValue>,
    ) -> Self {
        let allow_all_origins = allowed_origins
            .iter()
            .any(|p| matches!(p, OriginPattern::Any));
        Self {
            id: id.into(),
            allowed_origins,
            allow_all_origins,
            allowed_methods,
            allowed_headers,
            allow_all_headers,
            allow_credentials,
            max_age,
            vary_header: HeaderValue::from_static(
                "Origin, Access-Control-Request-Method, Access-Control-Request-Headers",
            ),
        }
    }

    /// Deserializes and constructs an instance directly from a [`PluginConfig`].
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let origins_raw = config
            .config
            .get("allowed_origins")
            .and_then(|v| v.as_array());

        let allowed_origins = match origins_raw {
            Some(arr) => arr
                .iter()
                .filter_map(|v| v.as_str())
                .map(OriginPattern::parse)
                .collect(),
            None => vec![OriginPattern::Any],
        };

        let methods_str = config
            .config
            .get("allowed_methods")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            })
            .unwrap_or_else(|| "GET, POST, PUT, DELETE, PATCH, HEAD, OPTIONS".to_string());

        let allowed_methods = HeaderValue::from_str(&methods_str).map_err(|e| {
            PluginError::Config(format!(
                "Invalid allowed_methods '{methods_str}' for plugin '{}': {e}",
                config.id
            ))
        })?;

        let mut allow_all_headers = false;
        let mut allowed_headers = None;

        if let Some(arr) = config
            .config
            .get("allowed_headers")
            .and_then(|v| v.as_array())
        {
            let str_list: Vec<&str> = arr.iter().filter_map(|v| v.as_str()).collect();
            if str_list.contains(&"*") {
                allow_all_headers = true;
            } else if !str_list.is_empty() {
                let joined = str_list.join(", ");
                let hv = HeaderValue::from_str(&joined).map_err(|e| {
                    PluginError::Config(format!(
                        "Invalid allowed_headers '{joined}' for plugin '{}': {e}",
                        config.id
                    ))
                })?;
                allowed_headers = Some(hv);
            }
        } else {
            // Default: allow all requested headers
            allow_all_headers = true;
        }

        let allow_credentials = config
            .config
            .get("allow_credentials")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        let max_age_secs = config
            .config
            .get("max_age_secs")
            .and_then(|v| v.as_u64())
            .unwrap_or(86400);

        let max_age = if max_age_secs > 0 {
            Some(
                HeaderValue::from_str(&max_age_secs.to_string())
                    .unwrap_or_else(|_| HeaderValue::from_static("86400")),
            )
        } else {
            None
        };

        Ok(Self::new(
            &config.id,
            allowed_origins,
            allowed_methods,
            allowed_headers,
            allow_all_headers,
            allow_credentials,
            max_age,
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
        if !req.headers.contains_key(ACCESS_CONTROL_REQUEST_METHOD) {
            return Action::Continue;
        }

        // 4. Validate Origin against allowed patterns
        let is_allowed =
            self.allow_all_origins || self.allowed_origins.iter().any(|p| p.matches(origin_str));

        if !is_allowed {
            return Action::Respond(
                L7Response::from_bytes(
                    StatusCode::FORBIDDEN,
                    b"403 Forbidden: CORS origin not allowed\n".to_vec(),
                )
                .with_header(
                    CONTENT_TYPE,
                    HeaderValue::from_static("text/plain; charset=utf-8"),
                ),
            );
        }

        // 5. Short-circuit: Construct 204 No Content preflight response
        let mut resp = L7Response::empty(StatusCode::NO_CONTENT);

        // Access-Control-Allow-Origin:
        // When credentials are true, W3C CORS prohibits wildcard '*'. We must reflect the exact Origin.
        let allow_origin_val = if self.allow_credentials || !self.allow_all_origins {
            origin_val.clone()
        } else {
            HeaderValue::from_static("*")
        };
        resp.headers
            .insert(ACCESS_CONTROL_ALLOW_ORIGIN, allow_origin_val);

        // Access-Control-Allow-Methods:
        resp.headers
            .insert(ACCESS_CONTROL_ALLOW_METHODS, self.allowed_methods.clone());

        // Access-Control-Allow-Headers:
        if self.allow_all_headers {
            if let Some(req_headers) = req.headers.get(ACCESS_CONTROL_REQUEST_HEADERS) {
                resp.headers
                    .insert(ACCESS_CONTROL_ALLOW_HEADERS, req_headers.clone());
            } else if let Some(ref default_headers) = self.allowed_headers {
                resp.headers
                    .insert(ACCESS_CONTROL_ALLOW_HEADERS, default_headers.clone());
            }
        } else if let Some(ref allowed_headers) = self.allowed_headers {
            resp.headers
                .insert(ACCESS_CONTROL_ALLOW_HEADERS, allowed_headers.clone());
        }

        // Access-Control-Allow-Credentials:
        if self.allow_credentials {
            resp.headers.insert(
                ACCESS_CONTROL_ALLOW_CREDENTIALS,
                HeaderValue::from_static("true"),
            );
        }

        // Access-Control-Max-Age:
        if let Some(ref max_age) = self.max_age {
            resp.headers.insert(ACCESS_CONTROL_MAX_AGE, max_age.clone());
        }

        // Vary & Content-Length:
        resp.headers.insert(VARY, self.vary_header.clone());
        resp.headers
            .insert(CONTENT_LENGTH, HeaderValue::from_static("0"));

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
    use http::header::HeaderMap;
    use http::{Uri, Version};
    use serde_json::json;
    use velda_core::Body;

    fn build_request(method: Method, uri: &str, headers: HeaderMap) -> L7Request {
        L7Request {
            method,
            uri: uri.parse::<Uri>().unwrap(),
            version: Version::HTTP_11,
            headers,
            body: Body::Empty,
        }
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

        match action {
            Action::Respond(resp) => {
                assert_eq!(resp.status, StatusCode::FORBIDDEN);
            }
            _ => panic!("Expected Action::Respond(403 Forbidden), got {:?}", action),
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
        assert_eq!(plugin.max_age.unwrap(), "3600");
    }
}
