//! Cross-Origin Resource Sharing (CORS) Post-Response Plugin (`cors`).
//!
//! Evaluated on outgoing upstream responses immediately before downstream encoding.
//! Injects standard CORS response headers (`Access-Control-Allow-Origin`,
//! `Access-Control-Allow-Credentials`, `Access-Control-Expose-Headers`, `Access-Control-Max-Age`),
//! supporting dynamic origin reflection matching and pre-computed immutable values.

use http::HeaderValue;
use http::header::{
    ACCESS_CONTROL_ALLOW_CREDENTIALS, ACCESS_CONTROL_ALLOW_ORIGIN, ACCESS_CONTROL_EXPOSE_HEADERS,
    ACCESS_CONTROL_MAX_AGE, VARY,
};
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
    expose_headers_val: Option<HeaderValue>,
    credentials_val: HeaderValue,
    max_age_val: Option<HeaderValue>,
    vary_val: HeaderValue,
    default_origin_val: HeaderValue,
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

    /// Constructs a new CORS response plugin with full configuration.
    pub fn with_config(id: impl Into<String>, config: CorsConfig) -> Self {
        let expose_headers_val = if !config.expose_headers.is_empty() {
            let joined = config.expose_headers.join(", ");
            HeaderValue::from_str(&joined).ok()
        } else {
            None
        };

        let max_age_val = config
            .max_age_secs
            .and_then(|secs| HeaderValue::from_str(&secs.to_string()).ok());

        let default_origin_val = if let Some(first) = config.allow_origins.first() {
            match first {
                OriginPattern::Exact(s) => {
                    HeaderValue::from_str(s).unwrap_or_else(|_| HeaderValue::from_static("*"))
                }
                _ => HeaderValue::from_static("*"),
            }
        } else {
            HeaderValue::from_static("*")
        };

        Self {
            id: id.into(),
            config,
            expose_headers_val,
            credentials_val: HeaderValue::from_static("true"),
            max_age_val,
            vary_val: HeaderValue::from_static("Origin"),
            default_origin_val,
        }
    }

    /// Deserializes and constructs an instance directly from a [`PluginConfig`].
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let defaults = CorsConfig::default();

        if config.config.is_null() {
            return Ok(Self::with_config(&config.id, defaults));
        }

        let raw = config.config.as_object().ok_or_else(|| {
            PluginError::Config(format!("Plugin '{}' config must be an object", config.id))
        })?;

        // 1. Resolve allow_origins
        let mut allow_origins = Vec::new();
        if let Some(arr) = raw.get("allow_origins").and_then(|v| v.as_array()) {
            for item in arr {
                if let Some(s) = item.as_str() {
                    allow_origins.push(OriginPattern::parse(s));
                }
            }
        }

        if allow_origins.is_empty() {
            allow_origins = defaults.allow_origins;
        }

        // 2. Resolve allow_credentials
        let allow_credentials = raw
            .get("allow_credentials")
            .and_then(|v| v.as_bool())
            .unwrap_or(defaults.allow_credentials);

        // 3. Resolve expose_headers
        let mut expose_headers = Vec::new();
        if let Some(arr) = raw.get("expose_headers").and_then(|v| v.as_array()) {
            for item in arr {
                if let Some(s) = item.as_str() {
                    expose_headers.push(s.to_string());
                }
            }
        }

        // 4. Resolve max_age_secs
        let max_age_secs = raw.get("max_age_secs").and_then(|v| v.as_u64());

        Ok(Self::with_config(
            &config.id,
            CorsConfig {
                allow_origins,
                allow_credentials,
                expose_headers,
                max_age_secs,
            },
        ))
    }
}

impl PostResponsePlugin for CorsResponsePlugin {
    #[inline(always)]
    fn id(&self) -> &str {
        &self.id
    }

    fn on_response(&self, req_headers: &http::HeaderMap, resp: &mut L7Response) {
        // 1. Resolve Origin: Check borrowed request headers from client workspace,
        // falling back to response headers if forwarded.
        let origin_header = req_headers
            .get("origin")
            .or_else(|| req_headers.get("x-origin"))
            .or_else(|| resp.headers.get("origin"))
            .or_else(|| resp.headers.get("x-origin"))
            .and_then(|v| v.to_str().ok());

        let mut matched_origin = None;
        if let Some(client_origin) = origin_header {
            for pattern in &self.config.allow_origins {
                if pattern.matches(client_origin) {
                    if let Ok(val) = HeaderValue::from_str(client_origin) {
                        matched_origin = Some(val);
                    }
                    break;
                }
            }
        }

        let allow_origin_val = matched_origin.unwrap_or_else(|| self.default_origin_val.clone());
        resp.headers
            .insert(ACCESS_CONTROL_ALLOW_ORIGIN, allow_origin_val);

        // 2. Invalidate caches across origins if credentials or specific origin reflection is used
        if self.config.allow_credentials {
            resp.headers.insert(
                ACCESS_CONTROL_ALLOW_CREDENTIALS,
                self.credentials_val.clone(),
            );
            resp.headers.insert(VARY, self.vary_val.clone());
        }

        // 3. Expose configured headers to frontend JavaScript
        if let Some(ref expose_val) = self.expose_headers_val {
            resp.headers
                .insert(ACCESS_CONTROL_EXPOSE_HEADERS, expose_val.clone());
        }

        // 4. Cache duration
        if let Some(ref max_age_val) = self.max_age_val {
            resp.headers
                .insert(ACCESS_CONTROL_MAX_AGE, max_age_val.clone());
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
    use http::{HeaderMap, StatusCode};

    #[test]
    fn test_cors_default_wildcard() {
        let plugin = CorsResponsePlugin::new("cors", "*", false);
        let mut resp = L7Response::empty(StatusCode::OK);
        let req_headers = HeaderMap::new();

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
    fn test_cors_exact_origin_with_credentials_and_vary() {
        let plugin = CorsResponsePlugin::new("cors", "https://app.example.com", true);
        let mut resp = L7Response::empty(StatusCode::OK);
        let req_headers = HeaderMap::new();

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
    fn test_cors_expose_headers_and_max_age() {
        let config = CorsConfig {
            allow_origins: vec![OriginPattern::Any],
            allow_credentials: false,
            expose_headers: vec!["x-request-id".into(), "x-total-count".into()],
            max_age_secs: Some(86400),
        };
        let plugin = CorsResponsePlugin::with_config("cors", config);
        let mut resp = L7Response::empty(StatusCode::OK);
        let req_headers = HeaderMap::new();

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

        // Request with Origin matching subdomain in client request headers workspace
        let mut req_headers = HeaderMap::new();
        req_headers.insert("origin", "https://portal.partner.io".parse().unwrap());
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
        let req_headers = HeaderMap::new();
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
