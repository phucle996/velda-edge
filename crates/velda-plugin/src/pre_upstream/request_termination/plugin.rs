//! Request Termination Pre-Upstream Plugin (`request_termination`).
//!
//! # Architecture & Pipeline Invariants
//! - **Phase**: Pre-Upstream (`pre_upstream`). Statically inferred by Velda Sync (never require `phase` in user config).
//! - **Zero Upstream Contact**: Returns [`Action::Respond`] immediately, bypassing upstream pool selection,
//!   endpoint resolution, connection leasing, and network dispatch.
//! - **Hot-Path Invariant (Zero Allocation)**: Pre-compiles the base [`L7Response`] during configuration ingestion.
//!   Payload is pre-allocated from static bytes (`Bytes::from_static`), achieving 100% zero heap allocation.
//! - **System-Level Request ID Invariant**: Automatically echoes the incoming request's `x-request-id` header
//!   into the synthetic response without requiring any opt-in plugin configuration flags.
//! - **Protocol-Owned Canonical Error Responses**: Directly uses canonical RFC plain text bodies mapped to the
//!   specified HTTP status code, eliminating arbitrary body schema bloat at the Edge.
//! - **Protocol Compatibility**: Strictly restricted to HTTP protocol families (`ALL_HTTP`). Forbidden on gRPC/Raw.

use bytes::Bytes;
use http::header::{CONTENT_LENGTH, CONTENT_TYPE, HeaderName, HeaderValue};
use http::{HeaderMap, StatusCode, Version};
use std::sync::Arc;
use velda_core::{Action, Body, L7Request, L7Response};
use velda_sync::post_sync::plugin::PluginConfig;

use crate::capability::{AccessLevel, PluginCapability, SupportedProtocols};
use crate::error::PluginError;
use crate::header::X_REQUEST_ID;
use crate::pre_upstream::PreUpstreamPlugin;

/// Returns the canonical static plain-text body for standard HTTP status codes.
///
/// For redirect and empty-body status codes (204, 301, 302, 304, etc.), returns empty bytes.
/// For error and informational status codes, returns standardized RFC reason plain text.
#[inline]
pub fn canonical_http_body(status: StatusCode) -> &'static [u8] {
    match status {
        StatusCode::NO_CONTENT | StatusCode::NOT_MODIFIED | StatusCode::RESET_CONTENT => b"",
        StatusCode::MOVED_PERMANENTLY
        | StatusCode::FOUND
        | StatusCode::SEE_OTHER
        | StatusCode::TEMPORARY_REDIRECT
        | StatusCode::PERMANENT_REDIRECT => b"",
        StatusCode::OK => b"200 OK\n",
        StatusCode::CREATED => b"201 Created\n",
        StatusCode::ACCEPTED => b"202 Accepted\n",
        StatusCode::BAD_REQUEST => b"400 Bad Request\n",
        StatusCode::UNAUTHORIZED => b"401 Unauthorized\n",
        StatusCode::FORBIDDEN => b"403 Forbidden\n",
        StatusCode::NOT_FOUND => b"404 Not Found\n",
        StatusCode::METHOD_NOT_ALLOWED => b"405 Method Not Allowed\n",
        StatusCode::REQUEST_TIMEOUT => b"408 Request Timeout\n",
        StatusCode::CONFLICT => b"409 Conflict\n",
        StatusCode::GONE => b"410 Gone\n",
        StatusCode::PAYLOAD_TOO_LARGE => b"413 Payload Too Large\n",
        StatusCode::TOO_MANY_REQUESTS => b"429 Too Many Requests\n",
        StatusCode::INTERNAL_SERVER_ERROR => b"500 Internal Server Error\n",
        StatusCode::NOT_IMPLEMENTED => b"501 Not Implemented\n",
        StatusCode::BAD_GATEWAY => b"502 Bad Gateway\n",
        StatusCode::SERVICE_UNAVAILABLE => b"503 Service Unavailable\n",
        StatusCode::GATEWAY_TIMEOUT => b"504 Gateway Timeout\n",
        _ => b"HTTP Error\n",
    }
}

/// Configuration schema for [`RequestTerminationPlugin`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestTerminationConfig {
    /// HTTP status code to return (default: 200, valid range: 200..=599).
    pub status_code: u16,
    /// Custom static HTTP headers to inject into the synthetic response.
    pub headers: Vec<(HeaderName, HeaderValue)>,
    /// Custom synthetic response body. If None or empty, standard RFC canonical text is used.
    pub body: Option<String>,
}

impl Default for RequestTerminationConfig {
    fn default() -> Self {
        Self {
            status_code: 200,
            headers: Vec::new(),
            body: None,
        }
    }
}

/// Request Termination Plugin instance.
#[derive(Debug, Clone)]
pub struct RequestTerminationPlugin {
    pub id: String,
    pub config: RequestTerminationConfig,
    base_response: L7Response,
}

impl RequestTerminationPlugin {
    /// Constructs a new [`RequestTerminationPlugin`] with explicit configuration.
    pub fn new(
        id: impl Into<String>,
        config: RequestTerminationConfig,
    ) -> Result<Self, PluginError> {
        if !(200..=599).contains(&config.status_code) {
            return Err(PluginError::Config(format!(
                "HTTP status code must be between 200 and 599, got {}",
                config.status_code
            )));
        }

        let status = StatusCode::from_u16(config.status_code).map_err(|e| {
            PluginError::Config(format!(
                "Invalid HTTP status code {}: {e}",
                config.status_code
            ))
        })?;

        // Retrieve canonical static body for this protocol status code
        let canonical_bytes = canonical_http_body(status);

        let body_bytes: Bytes = match &config.body {
            Some(b) if !b.is_empty() => Bytes::copy_from_slice(b.as_bytes()),
            _ => {
                if canonical_bytes.is_empty() {
                    Bytes::new()
                } else {
                    Bytes::from_static(canonical_bytes)
                }
            }
        };

        let mut header_map = HeaderMap::with_capacity(config.headers.len() + 2);
        let mut has_content_type = false;
        for (name, val) in &config.headers {
            if name == CONTENT_TYPE {
                has_content_type = true;
            }
            header_map.insert(name.clone(), val.clone());
        }

        // If body is non-empty and user did not specify Content-Type, default to text/plain; charset=utf-8
        if !body_bytes.is_empty() && !has_content_type {
            header_map.insert(
                CONTENT_TYPE,
                HeaderValue::from_static("text/plain; charset=utf-8"),
            );
        }

        // Ensure Content-Length matches the actual body length
        header_map.insert(
            CONTENT_LENGTH,
            HeaderValue::from_str(&body_bytes.len().to_string())
                .unwrap_or_else(|_| HeaderValue::from_static("0")),
        );

        let body = if body_bytes.is_empty() {
            Body::Empty
        } else {
            Body::Bytes(body_bytes)
        };

        let base_response = L7Response::new(status, Version::HTTP_11, header_map, body);

        Ok(Self {
            id: id.into(),
            config,
            base_response,
        })
    }

    /// Deserializes and constructs an instance directly from a [`PluginConfig`].
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let cfg = velda_sync::validate_request_termination_config(&config.id, &config.config)
            .map_err(|e| {
                PluginError::Config(format!(
                    "Invalid request_termination configuration for '{}': {e}",
                    config.id
                ))
            })?;

        let mut headers = Vec::with_capacity(cfg.headers.len() + 1);
        for (k, v) in &cfg.headers {
            let hname = HeaderName::from_bytes(k.as_bytes()).map_err(|e| {
                PluginError::Config(format!(
                    "Invalid header name '{k}' in plugin '{}': {e}",
                    config.id
                ))
            })?;
            let hval = HeaderValue::from_str(v).map_err(|e| {
                PluginError::Config(format!(
                    "Invalid header value '{v}' in plugin '{}': {e}",
                    config.id
                ))
            })?;
            headers.push((hname, hval));
        }

        if !cfg.content_type.trim().is_empty() {
            let ct_val = HeaderValue::from_str(&cfg.content_type).map_err(|e| {
                PluginError::Config(format!(
                    "Invalid content_type '{}' in plugin '{}': {e}",
                    cfg.content_type, config.id
                ))
            })?;
            headers.push((CONTENT_TYPE, ct_val));
        }

        Self::new(
            &config.id,
            RequestTerminationConfig {
                status_code: cfg.status_code,
                headers,
                body: Some(cfg.body),
            },
        )
    }

    /// Returns a reference to the pre-compiled base [`L7Response`].
    #[inline(always)]
    pub fn base_response(&self) -> &L7Response {
        &self.base_response
    }
}

impl PreUpstreamPlugin for RequestTerminationPlugin {
    #[inline(always)]
    fn id(&self) -> &str {
        &self.id
    }

    /// Short-circuits the request pipeline immediately with the synthetic response.
    ///
    /// # Zero-Copy Hot Path
    /// 1. Clones the pre-compiled [`L7Response`] (`Bytes` ref-count increment or static slice).
    /// 2. Automatically preserves incoming `x-request-id` into the response.
    /// 3. Returns [`Action::Respond`] directly to bypass upstream dispatch.
    fn on_upstream_request(&self, req: &mut L7Request, _target_upstream: &mut Arc<str>) -> Action {
        let mut resp = self.base_response.clone();
        if let Some(req_id) = req.headers.get(&X_REQUEST_ID) {
            resp.headers.insert(X_REQUEST_ID, req_id.clone());
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{HeaderMap, Method, Uri};

    #[test]
    fn test_default_termination_200_ok() {
        let plugin =
            RequestTerminationPlugin::new("terminate-default", RequestTerminationConfig::default())
                .expect("should construct default plugin");

        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("x-request-id"),
            HeaderValue::from_static("req-abc-123"),
        );

        let mut req = L7Request::new(
            Method::GET,
            Uri::from_static("/test"),
            Version::HTTP_11,
            headers,
            Body::Empty,
        );

        let mut target = Arc::from("default-upstream");
        let action = plugin.on_upstream_request(&mut req, &mut target);
        match action {
            Action::Respond(resp) => {
                assert_eq!(resp.status, StatusCode::OK);
                assert_eq!(resp.headers.get("x-request-id").unwrap(), "req-abc-123");
                assert_eq!(
                    resp.headers.get("content-type").unwrap(),
                    "text/plain; charset=utf-8"
                );
                match resp.body {
                    Body::Bytes(b) => assert_eq!(b.as_ref(), b"200 OK\n"),
                    _ => panic!("Expected Body::Bytes"),
                }
            }
            _ => panic!("Expected Action::Respond"),
        }
    }

    #[test]
    fn test_maintenance_mode_503_canonical_body_and_headers() {
        let cfg = RequestTerminationConfig {
            status_code: 503,
            headers: vec![
                (
                    HeaderName::from_static("retry-after"),
                    HeaderValue::from_static("300"),
                ),
                (
                    HeaderName::from_static("x-maintenance-window"),
                    HeaderValue::from_static("2026-10-20-0300"),
                ),
            ],
            body: None,
        };

        let plugin = RequestTerminationPlugin::new("maint-503", cfg).unwrap();

        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("x-request-id"),
            HeaderValue::from_static("req-maint-503"),
        );

        let mut req = L7Request::new(
            Method::POST,
            Uri::from_static("/api/orders"),
            Version::HTTP_11,
            headers,
            Body::Empty,
        );

        let mut target = Arc::from("default-upstream");
        let action = plugin.on_upstream_request(&mut req, &mut target);
        match action {
            Action::Respond(resp) => {
                assert_eq!(resp.status, StatusCode::SERVICE_UNAVAILABLE);
                assert_eq!(resp.headers.get("retry-after").unwrap(), "300");
                assert_eq!(
                    resp.headers.get("x-maintenance-window").unwrap(),
                    "2026-10-20-0300"
                );
                assert_eq!(
                    resp.headers.get("content-type").unwrap(),
                    "text/plain; charset=utf-8"
                );
                assert_eq!(resp.headers.get("x-request-id").unwrap(), "req-maint-503");
                match resp.body {
                    Body::Bytes(b) => {
                        assert_eq!(b.as_ref(), b"503 Service Unavailable\n");
                    }
                    _ => panic!("Expected Body::Bytes"),
                }
            }
            _ => panic!("Expected Action::Respond"),
        }
    }

    #[test]
    fn test_redirect_301_with_location_header() {
        let cfg = RequestTerminationConfig {
            status_code: 301,
            headers: vec![(
                HeaderName::from_static("location"),
                HeaderValue::from_static("https://example.com/login"),
            )],
            body: None,
        };

        let plugin = RequestTerminationPlugin::new("redirect-login", cfg).unwrap();

        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("x-request-id"),
            HeaderValue::from_static("req-redirect-301"),
        );

        let mut req = L7Request::new(
            Method::GET,
            Uri::from_static("/old-login"),
            Version::HTTP_11,
            headers,
            Body::Empty,
        );

        let mut target = Arc::from("default-upstream");
        let action = plugin.on_upstream_request(&mut req, &mut target);
        match action {
            Action::Respond(resp) => {
                assert_eq!(resp.status, StatusCode::MOVED_PERMANENTLY);
                assert_eq!(
                    resp.headers.get("location").unwrap(),
                    "https://example.com/login"
                );
                assert_eq!(
                    resp.headers.get("x-request-id").unwrap(),
                    "req-redirect-301"
                );
                assert_eq!(resp.body, Body::Empty);
                assert_eq!(resp.headers.get("content-length").unwrap(), "0");
            }
            _ => panic!("Expected Action::Respond"),
        }
    }

    #[test]
    fn test_custom_content_type_preserved() {
        let cfg = RequestTerminationConfig {
            status_code: 200,
            headers: vec![(CONTENT_TYPE, HeaderValue::from_static("application/json"))],
            body: None,
        };

        let plugin = RequestTerminationPlugin::new("custom-ct", cfg).unwrap();
        let resp = plugin.base_response();
        assert_eq!(
            resp.headers.get("content-type").unwrap(),
            "application/json"
        );
    }

    #[test]
    fn test_custom_body_preserved() {
        let cfg = RequestTerminationConfig {
            status_code: 400,
            headers: vec![(CONTENT_TYPE, HeaderValue::from_static("application/json"))],
            body: Some(r#"{"error":"Custom JSON payload"}"#.to_string()),
        };

        let plugin = RequestTerminationPlugin::new("custom-body", cfg).unwrap();
        let resp = plugin.base_response();
        assert_eq!(
            resp.headers.get("content-type").unwrap(),
            "application/json"
        );
        match &resp.body {
            Body::Bytes(b) => {
                assert_eq!(b.as_ref(), br#"{"error":"Custom JSON payload"}"#);
            }
            _ => panic!("Expected Body::Bytes"),
        }
    }

    #[test]
    fn test_invalid_status_code_rejected() {
        let res1 = RequestTerminationPlugin::new(
            "bad-status-low",
            RequestTerminationConfig {
                status_code: 101,
                ..Default::default()
            },
        );
        assert!(res1.is_err());

        let res2 = RequestTerminationPlugin::new(
            "bad-status-high",
            RequestTerminationConfig {
                status_code: 600,
                ..Default::default()
            },
        );
        assert!(res2.is_err());
    }

    #[test]
    fn test_from_config_json_parsing() {
        let json_cfg = serde_json::json!({
            "status_code": 418,
            "body": "I'm a teapot\n",
            "content_type": "text/plain",
            "headers": {
                "x-tea-type": "EarlGrey",
                "retry-after": "60"
            }
        });

        let plugin_cfg = PluginConfig {
            id: "request_termination".into(),
            phase: "pre_upstream".into(),
            config: json_cfg,
        };

        let plugin = RequestTerminationPlugin::from_config(&plugin_cfg).unwrap();
        assert_eq!(plugin.config.status_code, 418);
        assert_eq!(
            plugin.base_response().headers.get("x-tea-type").unwrap(),
            "EarlGrey"
        );
        assert_eq!(
            plugin.base_response().headers.get("retry-after").unwrap(),
            "60"
        );

        // Incomplete config must fail validation
        let bad_config = PluginConfig {
            id: "request_termination".into(),
            phase: "pre_upstream".into(),
            config: serde_json::json!({}),
        };
        assert!(RequestTerminationPlugin::from_config(&bad_config).is_err());
    }
}
