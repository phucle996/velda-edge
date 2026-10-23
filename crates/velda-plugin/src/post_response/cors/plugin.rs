//! Cross-Origin Resource Sharing (CORS) Post-Response Plugin (`phase: "post_response"`).
//!
//! Evaluated on outgoing upstream responses immediately before downstream encoding.
//! Injects standard CORS response headers (`Access-Control-Allow-Origin`,
//! `Access-Control-Allow-Credentials`, `Access-Control-Expose-Headers`, `Access-Control-Max-Age`),
//! supporting dynamic origin reflection matching and pre-computed immutable values.
//!
//! # Execution Invariants & Hardware Optimizations
//! - **Pre-Baked Header Template**: Static response headers (`credentials`, `expose-headers`,
//!   `max-age`) are pre-baked at initialization into an immutable template, eliminating runtime
//!   `if-else` branching on outgoing responses.
//! - **Native Multi-Value Vary Append**: Uses HTTP `HeaderMap::append` to preserve existing
//!   `Vary` headers without string parsing, string splitting, or string reallocations.
//! - **Non-CORS Request Pass-Through**: Non-CORS requests (no `Origin` header) exit immediately.
//! - **Strict Origin Isolation**: Disallowed origins receive zero CORS headers (no wildcards fallback).
//! - **Zero-Allocation Hot Path**: Atomic ref-count clone on `HeaderValue` without heap re-allocations.

use http::header::{
    ACCESS_CONTROL_ALLOW_CREDENTIALS, ACCESS_CONTROL_ALLOW_ORIGIN, ACCESS_CONTROL_EXPOSE_HEADERS,
    ACCESS_CONTROL_MAX_AGE, ORIGIN, VARY,
};
use http::{HeaderMap, HeaderValue};
use velda_core::L7Response;
use velda_sync::post_sync::plugin::PluginConfig;

use crate::capability::{AccessLevel, PluginCapability, SupportedProtocols};
use crate::error::PluginError;
use crate::post_response::PostResponsePlugin;
use crate::pre_route::OriginPattern;

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
    pub allow_all_origins: bool,
    pub base_headers: HeaderMap,
    pub needs_vary_origin: bool,
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

    /// Constructs a new CORS response plugin with pre-baked static headers template.
    pub fn with_config(id: impl Into<String>, config: CorsConfig) -> Self {
        let allow_all_origins = config
            .allow_origins
            .iter()
            .any(|p| matches!(p, OriginPattern::Any));

        // Pre-bake static headers template (credentials, expose-headers, max-age)
        let mut base_headers = HeaderMap::with_capacity(3);

        if config.allow_credentials {
            base_headers.insert(
                ACCESS_CONTROL_ALLOW_CREDENTIALS,
                HeaderValue::from_static("true"),
            );
        }

        if !config.expose_headers.is_empty() {
            let joined = config.expose_headers.join(", ");
            if let Ok(val) = HeaderValue::from_str(&joined) {
                base_headers.insert(ACCESS_CONTROL_EXPOSE_HEADERS, val);
            }
        }

        if let Some(secs) = config.max_age_secs
            && let Ok(val) = HeaderValue::from_str(&secs.to_string())
        {
            base_headers.insert(ACCESS_CONTROL_MAX_AGE, val);
        }

        let needs_vary_origin = config.allow_credentials || !allow_all_origins;

        Self {
            id: id.into(),
            config,
            allow_all_origins,
            base_headers,
            needs_vary_origin,
        }
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
            return; // Non-CORS request, do not inject CORS headers
        };

        let Ok(origin_str) = origin_val.to_str() else {
            return;
        };

        // 2. Validate Origin against allowed patterns
        let is_allowed = self.allow_all_origins
            || self
                .config
                .allow_origins
                .iter()
                .any(|p| p.matches(origin_str));

        if !is_allowed {
            // Disallowed origin: do NOT inject any CORS headers (prevents unauthorized cross-origin access)
            return;
        }

        // 3. Inject Access-Control-Allow-Origin:
        // When credentials are true, W3C CORS prohibits wildcard '*'. We must reflect the exact Origin.
        let allow_origin_val = if self.config.allow_credentials || !self.allow_all_origins {
            origin_val.clone() // Atomic ref-count increment, zero allocation
        } else {
            HeaderValue::from_static("*")
        };
        resp.headers
            .insert(ACCESS_CONTROL_ALLOW_ORIGIN, allow_origin_val);

        // 4. Batch inject pre-baked static response headers (credentials, expose-headers, max-age)
        for (name, val) in &self.base_headers {
            resp.headers.insert(name.clone(), val.clone());
        }

        // 5. Invalidate caches across origins (Vary: Origin) if credentials or dynamic reflection used
        if self.needs_vary_origin {
            let already_has_origin = resp.headers.get_all(VARY).iter().any(|v| {
                v.to_str().is_ok_and(|s| {
                    s.split(',').any(|part| {
                        part.trim().eq_ignore_ascii_case("origin") || part.trim() == "*"
                    })
                })
            });
            if !already_has_origin {
                resp.headers
                    .append(VARY, HeaderValue::from_static("Origin"));
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
    fn test_cors_default_wildcard() {
        let plugin = CorsResponsePlugin::new("cors", "*", false);
        let mut resp = L7Response::empty(StatusCode::OK);
        let mut req_headers = HeaderWorkspace::empty();
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
        let req_headers = HeaderWorkspace::empty(); // No Origin header

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
        let mut req_headers = HeaderWorkspace::empty();
        req_headers.insert(ORIGIN, "https://evil.com".parse().unwrap());

        plugin.on_response(&req_headers, &mut resp);

        // Security invariant: Untrusted origin must receive zero CORS headers
        assert!(!resp.headers.contains_key("access-control-allow-origin"));
    }

    #[test]
    fn test_cors_exact_origin_with_credentials_and_vary() {
        let plugin = CorsResponsePlugin::new("cors", "https://app.example.com", true);
        let mut resp = L7Response::empty(StatusCode::OK);
        let mut req_headers = HeaderWorkspace::empty();
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

        let mut req_headers = HeaderWorkspace::empty();
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
        let mut req_headers = HeaderWorkspace::empty();
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

        let mut req_headers = HeaderWorkspace::empty();
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
        assert!(plugin.config.allow_credentials);
        assert_eq!(plugin.config.max_age_secs, Some(3600));
        assert_eq!(plugin.config.expose_headers, vec!["x-request-id"]);

        let mut resp = L7Response::empty(StatusCode::OK);
        let mut req_headers = HeaderWorkspace::empty();
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
