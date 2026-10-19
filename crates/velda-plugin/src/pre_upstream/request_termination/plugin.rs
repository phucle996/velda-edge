//! Request Termination Pre-Upstream Plugin (`request_termination`).
//!
//! # Architecture & Pipeline Invariants
//! - **Phase**: Pre-Upstream (`pre_upstream`). Statically inferred by Velda Sync (never require `phase` in user config).
//! - **Zero Upstream Contact**: Returns [`Action::Respond`] immediately, bypassing upstream pool selection,
//!   endpoint resolution, connection leasing, and network dispatch.
//! - **Hot-Path Invariant (Zero Allocation)**: Pre-compiles the base [`L7Response`] during configuration ingestion.
//!   Hot-path responses clone the payload via zero-copy atomic reference-count increment on [`bytes::Bytes`].
//! - **Request ID Propagation**: Automatically echoes the incoming request's `x-request-id` header into
//!   the response if present and `echo_request_id: true` (default `true`).
//! - **Protocol Compatibility**: Strictly restricted to HTTP protocol families (`ALL_HTTP`). Forbidden on gRPC/Raw.

use bytes::Bytes;
use http::header::{CONTENT_LENGTH, CONTENT_TYPE, HeaderName, HeaderValue};
use http::{HeaderMap, StatusCode, Version};
use serde_json::Value;
use velda_core::{Action, Body, L7Request, L7Response};
use velda_sync::post_sync::plugin::PluginConfig;

use crate::capability::{AccessLevel, PluginCapability, SupportedProtocols};
use crate::error::PluginError;
use crate::pre_upstream::PreUpstreamPlugin;

/// Configuration schema for [`RequestTerminationPlugin`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestTerminationConfig {
    /// HTTP status code to return (default: 200, valid range: 200..=599).
    pub status_code: u16,
    /// Response body payload (default: empty `""`).
    pub body: String,
    /// Custom static HTTP headers to inject into the synthetic response.
    pub headers: Vec<(HeaderName, HeaderValue)>,
    /// Whether to echo incoming `x-request-id` into the synthetic response (default: `true`).
    pub echo_request_id: bool,
}

impl Default for RequestTerminationConfig {
    fn default() -> Self {
        Self {
            status_code: 200,
            body: String::new(),
            headers: Vec::new(),
            echo_request_id: true,
        }
    }
}

/// Helper function to convert JSON value (string, integer, boolean) to a header value string.
fn value_to_header_str(v: &Value) -> Option<String> {
    if let Some(s) = v.as_str() {
        Some(s.to_string())
    } else if let Some(n) = v.as_i64() {
        Some(n.to_string())
    } else if let Some(u) = v.as_u64() {
        Some(u.to_string())
    } else {
        v.as_bool().map(|b| b.to_string())
    }
}

/// Parses static headers from JSON object or array of `{ name, value }` objects.
fn parse_headers(val: &Value) -> Result<Vec<(HeaderName, HeaderValue)>, PluginError> {
    let mut headers = Vec::new();

    if let Some(obj) = val.as_object() {
        for (k, v) in obj {
            let name = HeaderName::from_bytes(k.as_bytes())
                .map_err(|e| PluginError::Config(format!("Invalid header name '{k}': {e}")))?;
            let val_str = value_to_header_str(v).ok_or_else(|| {
                PluginError::Config(format!(
                    "Header value for '{k}' must be scalar (string/number)"
                ))
            })?;
            let value = HeaderValue::from_str(&val_str).map_err(|e| {
                PluginError::Config(format!(
                    "Invalid header value '{val_str}' for header '{k}': {e}"
                ))
            })?;
            headers.push((name, value));
        }
    } else if let Some(arr) = val.as_array() {
        for item in arr {
            if let Some(item_obj) = item.as_object() {
                let name_str = item_obj
                    .get("name")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| {
                        PluginError::Config("Header item must contain 'name' string field".into())
                    })?;
                let val_node = item_obj.get("value").ok_or_else(|| {
                    PluginError::Config("Header item must contain 'value' field".into())
                })?;
                let val_str = value_to_header_str(val_node).ok_or_else(|| {
                    PluginError::Config(format!(
                        "Header value for '{name_str}' must be scalar (string/number)"
                    ))
                })?;

                let name = HeaderName::from_bytes(name_str.as_bytes()).map_err(|e| {
                    PluginError::Config(format!("Invalid header name '{name_str}': {e}"))
                })?;
                let value = HeaderValue::from_str(&val_str).map_err(|e| {
                    PluginError::Config(format!(
                        "Invalid header value '{val_str}' for header '{name_str}': {e}"
                    ))
                })?;
                headers.push((name, value));
            }
        }
    }

    Ok(headers)
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

        let body_bytes = Bytes::from(config.body.clone().into_bytes());

        let mut header_map = HeaderMap::with_capacity(config.headers.len() + 2);
        let mut has_content_type = false;
        for (name, val) in &config.headers {
            if name == CONTENT_TYPE {
                has_content_type = true;
            }
            header_map.insert(name.clone(), val.clone());
        }

        // If body is present and user did not specify Content-Type, default to text/plain; charset=utf-8
        if !body_bytes.is_empty() && !has_content_type {
            header_map.insert(
                CONTENT_TYPE,
                HeaderValue::from_static("text/plain; charset=utf-8"),
            );
        }

        // Ensure Content-Length matches the actual synthetic body length
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
        let defaults = RequestTerminationConfig::default();

        if config.config.is_null() {
            return Self::new(&config.id, defaults);
        }

        let status_code = match config.config.get("status_code") {
            Some(v) => {
                let code = v.as_u64().ok_or_else(|| {
                    PluginError::Config(
                        "'status_code' must be an integer between 200 and 599".into(),
                    )
                })?;
                if !(200..=599).contains(&code) {
                    return Err(PluginError::Config(format!(
                        "HTTP status code must be between 200 and 599, got {code}"
                    )));
                }
                code as u16
            }
            None => defaults.status_code,
        };

        let body = match config.config.get("body") {
            Some(v) => v
                .as_str()
                .ok_or_else(|| PluginError::Config("'body' must be a string".into()))?
                .to_string(),
            None => defaults.body,
        };

        let headers = match config.config.get("headers") {
            Some(v) => parse_headers(v)?,
            None => defaults.headers,
        };

        let echo_request_id = config
            .config
            .get("echo_request_id")
            .and_then(|v| v.as_bool())
            .unwrap_or(defaults.echo_request_id);

        let termination_config = RequestTerminationConfig {
            status_code,
            body,
            headers,
            echo_request_id,
        };

        Self::new(&config.id, termination_config)
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
    /// 1. Clones the pre-compiled [`L7Response`] (`Bytes` ref-count increment).
    /// 2. If `echo_request_id: true` and incoming request contains `x-request-id`, injects it.
    /// 3. Returns [`Action::Respond`] directly to bypass upstream dispatch.
    fn on_upstream_request(&self, req: &mut L7Request) -> Action {
        let mut resp = self.base_response.clone();
        if self.config.echo_request_id
            && let Some(req_id) = req.headers.get("x-request-id")
        {
            resp.headers
                .insert(HeaderName::from_static("x-request-id"), req_id.clone());
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

        let action = plugin.on_upstream_request(&mut req);
        match action {
            Action::Respond(resp) => {
                assert_eq!(resp.status, StatusCode::OK);
                assert_eq!(resp.body, Body::Empty);
                assert_eq!(resp.headers.get("content-length").unwrap(), "0");
                assert_eq!(resp.headers.get("x-request-id").unwrap(), "req-abc-123");
                assert!(!resp.headers.contains_key("content-type"));
            }
            _ => panic!("Expected Action::Respond"),
        }
    }

    #[test]
    fn test_maintenance_mode_503_with_plain_text_and_headers() {
        let cfg = RequestTerminationConfig {
            status_code: 503,
            body: "Service Under Scheduled Maintenance\n".to_string(),
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
            echo_request_id: true,
        };

        let plugin = RequestTerminationPlugin::new("maint-503", cfg).unwrap();

        let mut req = L7Request::new(
            Method::POST,
            Uri::from_static("/api/orders"),
            Version::HTTP_11,
            HeaderMap::new(),
            Body::Empty,
        );

        let action = plugin.on_upstream_request(&mut req);
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
                assert_eq!(
                    resp.headers.get("content-length").unwrap(),
                    &resp.body.len().to_string()
                );
                match resp.body {
                    Body::Bytes(b) => {
                        assert_eq!(b.as_ref(), b"Service Under Scheduled Maintenance\n");
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
            body: String::new(),
            headers: vec![(
                HeaderName::from_static("location"),
                HeaderValue::from_static("https://example.com/login"),
            )],
            echo_request_id: false,
        };

        let plugin = RequestTerminationPlugin::new("redirect-login", cfg).unwrap();

        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("x-request-id"),
            HeaderValue::from_static("req-ignored"),
        );

        let mut req = L7Request::new(
            Method::GET,
            Uri::from_static("/old-login"),
            Version::HTTP_11,
            headers,
            Body::Empty,
        );

        let action = plugin.on_upstream_request(&mut req);
        match action {
            Action::Respond(resp) => {
                assert_eq!(resp.status, StatusCode::MOVED_PERMANENTLY);
                assert_eq!(
                    resp.headers.get("location").unwrap(),
                    "https://example.com/login"
                );
                // echo_request_id is false, so request-id should NOT be in response
                assert!(!resp.headers.contains_key("x-request-id"));
                assert_eq!(resp.body, Body::Empty);
            }
            _ => panic!("Expected Action::Respond"),
        }
    }

    #[test]
    fn test_custom_content_type_preserved() {
        let cfg = RequestTerminationConfig {
            status_code: 200,
            body: "{\"status\":\"ok\"}".to_string(),
            headers: vec![(CONTENT_TYPE, HeaderValue::from_static("application/json"))],
            echo_request_id: true,
        };

        let plugin = RequestTerminationPlugin::new("custom-ct", cfg).unwrap();
        let resp = plugin.base_response();
        assert_eq!(
            resp.headers.get("content-type").unwrap(),
            "application/json"
        );
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
            "body": "I'm a teapot",
            "headers": {
                "x-tea-type": "EarlGrey",
                "retry-after": 60
            },
            "echo_request_id": true
        });

        let plugin_cfg = PluginConfig {
            id: "request_termination".into(),
            phase: "pre_upstream".into(),
            config: json_cfg,
        };

        let plugin = RequestTerminationPlugin::from_config(&plugin_cfg).unwrap();
        assert_eq!(plugin.config.status_code, 418);
        assert_eq!(plugin.config.body, "I'm a teapot");
        assert!(plugin.config.echo_request_id);
        assert_eq!(
            plugin.base_response().headers.get("x-tea-type").unwrap(),
            "EarlGrey"
        );
        assert_eq!(
            plugin.base_response().headers.get("retry-after").unwrap(),
            "60"
        );
        assert_eq!(
            plugin.base_response().headers.get("content-type").unwrap(),
            "text/plain; charset=utf-8"
        );
    }
}
