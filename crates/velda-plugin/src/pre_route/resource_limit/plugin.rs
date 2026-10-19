//! Pre-Route Resource Limit Plugin (`resource_limit`).
//!
//! Provides ultra-low-latency ingress boundary defense before routing, body streaming,
//! and upstream connection leasing by enforcing strict thresholds on URI length,
//! header counts, and cumulative header bytes.

use http::StatusCode;
use http::header::HeaderValue;
use velda_core::{Action, L7Request, L7Response};
use velda_sync::post_sync::plugin::PluginConfig;

use crate::capability::{AccessLevel, PluginCapability, SupportedProtocols};
use crate::error::PluginError;
use crate::pre_route::PreRoutePlugin;

/// Configurable thresholds for request URI and header resource consumption.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResourceLimits {
    /// Maximum allowed total URI length in bytes (Path + Query string).
    pub max_uri_length: usize,
    /// Maximum allowed number of distinct header entries.
    pub max_headers_count: usize,
    /// Maximum allowed cumulative size of all headers in bytes (names + values + wire framing).
    pub max_header_bytes: usize,
    /// Maximum allowed size for any single header value in bytes.
    pub max_single_header_bytes: usize,
}

impl Default for ResourceLimits {
    fn default() -> Self {
        Self {
            max_uri_length: 4096,
            max_headers_count: 64,
            max_header_bytes: 16384,
            max_single_header_bytes: 8192,
        }
    }
}

/// Pre-Route Resource Limit Plugin.
pub struct ResourceLimitPlugin {
    pub id: String,
    pub limits: ResourceLimits,
    uri_too_long_response: L7Response,
    header_fields_too_large_response: L7Response,
}

impl ResourceLimitPlugin {
    /// Constructs a new `ResourceLimitPlugin` with pre-computed immutable error responses.
    pub fn new(id: impl Into<String>, limits: ResourceLimits) -> Self {
        let uri_too_long_response =
            L7Response::from_bytes(StatusCode::URI_TOO_LONG, "{\"error\":\"URI Too Long\"}")
                .with_header(
                    http::header::CONTENT_TYPE,
                    HeaderValue::from_static("application/json"),
                );

        let header_fields_too_large_response = L7Response::from_bytes(
            StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE,
            "{\"error\":\"Request Header Fields Too Large\"}",
        )
        .with_header(
            http::header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );

        Self {
            id: id.into(),
            limits,
            uri_too_long_response,
            header_fields_too_large_response,
        }
    }

    /// Deserializes and constructs an instance directly from a [`PluginConfig`].
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let defaults = ResourceLimits::default();

        if config.config.is_null() {
            return Ok(Self::new(&config.id, defaults));
        }

        let raw = config.config.as_object().ok_or_else(|| {
            PluginError::Config(format!("Plugin '{}' config must be an object", config.id))
        })?;

        let max_uri_length = raw
            .get("max_uri_length")
            .and_then(|v| v.as_u64())
            .map(|v| v as usize)
            .unwrap_or(defaults.max_uri_length);

        let max_headers_count = raw
            .get("max_headers_count")
            .and_then(|v| v.as_u64())
            .map(|v| v as usize)
            .unwrap_or(defaults.max_headers_count);

        let max_header_bytes = raw
            .get("max_header_bytes")
            .and_then(|v| v.as_u64())
            .map(|v| v as usize)
            .unwrap_or(defaults.max_header_bytes);

        let max_single_header_bytes = raw
            .get("max_single_header_bytes")
            .and_then(|v| v.as_u64())
            .map(|v| v as usize)
            .unwrap_or(defaults.max_single_header_bytes);

        Ok(Self::new(
            &config.id,
            ResourceLimits {
                max_uri_length,
                max_headers_count,
                max_header_bytes,
                max_single_header_bytes,
            },
        ))
    }
}

impl PreRoutePlugin for ResourceLimitPlugin {
    #[inline(always)]
    fn id(&self) -> &str {
        &self.id
    }

    fn on_request(&self, req: &mut L7Request) -> Action {
        // 1. Inspect URI Length: Path + Query String
        // Note: Even though Radix Trie route lookup stops early on prefix match,
        // oversized URIs (especially huge query strings) severely impact downstream
        // components (WAF scanners, access loggers, and upstream serialization).
        let uri_len = req.uri.path().len() + req.uri.query().map_or(0, |q| q.len() + 1);
        if uri_len > self.limits.max_uri_length {
            return Action::Respond(self.uri_too_long_response.clone());
        }

        // 2. Inspect Header Count
        // Prevents header bombing and forces request headers to fit within
        // pre-warmed L1/L2 HeaderWorkspace cache slots without dynamic heap spilling.
        let header_count = req.headers.len();
        if header_count > self.limits.max_headers_count {
            return Action::Respond(self.header_fields_too_large_response.clone());
        }

        // 3. Inspect Total Header Bytes & Single Header Limits
        let mut total_header_bytes = 0usize;
        for (name, val) in &req.headers {
            let val_len = val.as_bytes().len();
            if val_len > self.limits.max_single_header_bytes {
                return Action::Respond(self.header_fields_too_large_response.clone());
            }

            // Wire framing estimate: name + ": " (2 bytes) + value + "\r\n" (2 bytes)
            total_header_bytes += name.as_str().len() + val_len + 4;
            if total_header_bytes > self.limits.max_header_bytes {
                return Action::Respond(self.header_fields_too_large_response.clone());
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
    use http::{HeaderMap, Method, Uri, Version};
    use serde_json::json;
    use velda_core::Body;

    fn build_test_request(path_and_query: &str, headers: HeaderMap) -> L7Request {
        L7Request::new(
            Method::GET,
            path_and_query.parse::<Uri>().unwrap(),
            Version::HTTP_11,
            headers,
            Body::Empty,
        )
    }

    #[test]
    fn test_normal_request_within_limits_passes() {
        let plugin = ResourceLimitPlugin::new("resource_limit", ResourceLimits::default());
        let mut headers = HeaderMap::new();
        headers.insert("accept", "application/json".parse().unwrap());
        headers.insert("user-agent", "Mozilla/5.0".parse().unwrap());

        let mut req = build_test_request("/api/v1/users?page=1&limit=20", headers);
        let action = plugin.on_request(&mut req);

        assert!(matches!(action, Action::Continue));
    }

    #[test]
    fn test_uri_too_long_returns_414() {
        let limits = ResourceLimits {
            max_uri_length: 30,
            ..Default::default()
        };
        let plugin = ResourceLimitPlugin::new("resource_limit", limits);

        // Path (14) + '?' + Query (20) = 35 bytes > 30 bytes
        let mut req = build_test_request("/api/v1/orders?filter_date=2026-10-19", HeaderMap::new());
        let action = plugin.on_request(&mut req);

        match action {
            Action::Respond(resp) => {
                assert_eq!(resp.status, StatusCode::URI_TOO_LONG);
                assert_eq!(
                    resp.headers.get("content-type").unwrap(),
                    "application/json"
                );
            }
            _ => panic!("Expected Action::Respond(414 URI Too Long)"),
        }
    }

    #[test]
    fn test_headers_count_exceeded_returns_431() {
        let limits = ResourceLimits {
            max_headers_count: 2,
            ..Default::default()
        };
        let plugin = ResourceLimitPlugin::new("resource_limit", limits);

        let mut headers = HeaderMap::new();
        headers.insert("h1", "v1".parse().unwrap());
        headers.insert("h2", "v2".parse().unwrap());
        headers.insert("h3", "v3".parse().unwrap());

        let mut req = build_test_request("/api/test", headers);
        let action = plugin.on_request(&mut req);

        match action {
            Action::Respond(resp) => {
                assert_eq!(resp.status, StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE);
            }
            _ => panic!("Expected Action::Respond(431 Request Header Fields Too Large)"),
        }
    }

    #[test]
    fn test_total_header_bytes_exceeded_returns_431() {
        let limits = ResourceLimits {
            max_header_bytes: 40,
            ..Default::default()
        };
        let plugin = ResourceLimitPlugin::new("resource_limit", limits);

        let mut headers = HeaderMap::new();
        // name (11) + ": " (2) + val (20) + "\r\n" (2) = 35
        headers.insert("x-custom-h1", "01234567890123456789".parse().unwrap());
        // Second header pushes it well beyond 40 bytes
        headers.insert("x-custom-h2", "01234567890123456789".parse().unwrap());

        let mut req = build_test_request("/api/test", headers);
        let action = plugin.on_request(&mut req);

        match action {
            Action::Respond(resp) => {
                assert_eq!(resp.status, StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE);
            }
            _ => panic!("Expected Action::Respond(431 Request Header Fields Too Large)"),
        }
    }

    #[test]
    fn test_single_header_bytes_exceeded_returns_431() {
        let limits = ResourceLimits {
            max_single_header_bytes: 20,
            ..Default::default()
        };
        let plugin = ResourceLimitPlugin::new("resource_limit", limits);

        let mut headers = HeaderMap::new();
        // Single value of 25 bytes > 20 bytes threshold
        headers.insert(
            "x-large-cookie",
            "session_id=123456789012345".parse().unwrap(),
        );

        let mut req = build_test_request("/api/test", headers);
        let action = plugin.on_request(&mut req);

        match action {
            Action::Respond(resp) => {
                assert_eq!(resp.status, StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE);
            }
            _ => panic!("Expected Action::Respond(431 Request Header Fields Too Large)"),
        }
    }

    #[test]
    fn test_from_config_custom_and_defaults() {
        let config = PluginConfig {
            id: "resource_limit".into(),
            phase: String::new(),
            config: json!({
                "max_uri_length": 8192,
                "max_headers_count": 32,
                "max_header_bytes": 32768,
                "max_single_header_bytes": 4096
            }),
        };

        let plugin = ResourceLimitPlugin::from_config(&config).expect("valid config");
        assert_eq!(plugin.id, "resource_limit");
        assert_eq!(plugin.limits.max_uri_length, 8192);
        assert_eq!(plugin.limits.max_headers_count, 32);
        assert_eq!(plugin.limits.max_header_bytes, 32768);
        assert_eq!(plugin.limits.max_single_header_bytes, 4096);

        // Empty config fallback to defaults
        let empty_config = PluginConfig {
            id: "resource_limit".into(),
            phase: String::new(),
            config: json!({}),
        };
        let plugin_default = ResourceLimitPlugin::from_config(&empty_config).expect("defaults");
        assert_eq!(plugin_default.limits, ResourceLimits::default());
    }
}
