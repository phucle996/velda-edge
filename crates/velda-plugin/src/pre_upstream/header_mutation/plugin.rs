//! Header Mutation Pre-Upstream Plugin (`header_mutation`).
//!
//! Adds, mutates, renames, or strips HTTP headers before proxying requests to upstream backends.
//! Provides support for:
//! - Stripping RFC 9110 / RFC 7230 hop-by-hop headers.
//! - Removing sensitive or spoofable headers.
//! - Renaming headers to match upstream expectations.
//! - Setting static or dynamic variable headers (`$client_ip`, `$request_id`, `$route_id`, `$upstream_id`, `$scheme`, `$host`, `$method`, `$path`).
//! - Appending values to existing headers (e.g. `X-Forwarded-For`).

use std::sync::atomic::{AtomicU64, Ordering};

use http::HeaderMap;
use http::header::{HeaderName, HeaderValue};
use velda_core::{Action, L7Request};
use velda_sync::post_sync::plugin::PluginConfig;

use crate::capability::{AccessLevel, PluginCapability, SupportedProtocols};
use crate::error::PluginError;
use crate::pre_upstream::PreUpstreamPlugin;

static REQUEST_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Dynamic variables that can be evaluated and interpolated into headers at runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DynamicVar {
    /// Client IP address (resolved from `x-forwarded-for`, `x-real-ip`, or `"127.0.0.1"`).
    ClientIp,
    /// Unique request identifier (from existing `x-request-id` or generated monotonic hex ID).
    RequestId,
    /// Logical route identifier (from `x-route-id` or `x-velda-route`).
    RouteId,
    /// Logical upstream pool identifier (from `x-upstream-id`).
    UpstreamId,
    /// URI scheme (e.g., `"http"`, `"https"`).
    Scheme,
    /// Host authority header.
    Host,
    /// HTTP method (e.g., `"GET"`, `"POST"`).
    Method,
    /// HTTP URI path.
    Path,
}

impl DynamicVar {
    /// Parses a variable name from a dollar-prefixed identifier string.
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "$client_ip" => Some(Self::ClientIp),
            "$request_id" => Some(Self::RequestId),
            "$route_id" => Some(Self::RouteId),
            "$upstream_id" => Some(Self::UpstreamId),
            "$scheme" => Some(Self::Scheme),
            "$host" => Some(Self::Host),
            "$method" => Some(Self::Method),
            "$path" => Some(Self::Path),
            _ => None,
        }
    }
}

/// Specification for header values, either a pre-compiled immutable static string or a dynamic variable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HeaderValueSpec {
    /// Static header value pre-computed at configuration compile time.
    Static(HeaderValue),
    /// Dynamic variable resolved per request.
    Variable(DynamicVar),
}

impl HeaderValueSpec {
    /// Parses a header value specification from string.
    pub fn parse(s: &str) -> Result<Self, PluginError> {
        if let Some(var) = DynamicVar::parse(s) {
            Ok(Self::Variable(var))
        } else {
            let val = HeaderValue::from_str(s).map_err(|e| {
                PluginError::Config(format!("Invalid header value string '{s}': {e}"))
            })?;
            Ok(Self::Static(val))
        }
    }

    /// Resolves the header value against the incoming request.
    #[inline]
    pub fn resolve(&self, req: &L7Request) -> Option<HeaderValue> {
        match self {
            Self::Static(val) => Some(val.clone()),
            Self::Variable(var) => match var {
                DynamicVar::ClientIp => {
                    let ip_str = req
                        .headers
                        .get("x-forwarded-for")
                        .and_then(|v| v.to_str().ok())
                        .and_then(|s| s.split(',').next())
                        .map(|s| s.trim())
                        .or_else(|| {
                            req.headers
                                .get("x-real-ip")
                                .and_then(|v| v.to_str().ok())
                                .map(|s| s.trim())
                        })
                        .unwrap_or("127.0.0.1");
                    HeaderValue::from_str(ip_str).ok()
                }
                DynamicVar::RequestId => {
                    if let Some(val) = req.headers.get("x-request-id") {
                        Some(val.clone())
                    } else {
                        let id = REQUEST_COUNTER.fetch_add(1, Ordering::Relaxed);
                        HeaderValue::from_str(&format!("req-{id:016x}")).ok()
                    }
                }
                DynamicVar::RouteId => {
                    let route = req
                        .headers
                        .get("x-route-id")
                        .or_else(|| req.headers.get("x-velda-route"))
                        .and_then(|v| v.to_str().ok())
                        .unwrap_or("unknown-route");
                    HeaderValue::from_str(route).ok()
                }
                DynamicVar::UpstreamId => {
                    let upstream = req
                        .headers
                        .get("x-upstream-id")
                        .and_then(|v| v.to_str().ok())
                        .unwrap_or("default-upstream");
                    HeaderValue::from_str(upstream).ok()
                }
                DynamicVar::Scheme => {
                    let scheme = req
                        .uri
                        .scheme_str()
                        .or_else(|| {
                            req.headers
                                .get("x-forwarded-proto")
                                .and_then(|v| v.to_str().ok())
                        })
                        .unwrap_or("http");
                    HeaderValue::from_str(scheme).ok()
                }
                DynamicVar::Host => req
                    .headers
                    .get("host")
                    .cloned()
                    .or_else(|| req.uri.host().and_then(|h| HeaderValue::from_str(h).ok())),
                DynamicVar::Method => HeaderValue::from_str(req.method.as_str()).ok(),
                DynamicVar::Path => HeaderValue::from_str(req.uri.path()).ok(),
            },
        }
    }
}

/// Configuration settings for the header mutation plugin.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeaderMutationConfig {
    /// Strips RFC 9110 / RFC 7230 hop-by-hop headers before upstream forwarding.
    pub strip_hop_by_hop: bool,
    /// Headers to be explicitly stripped before upstream dispatch.
    pub remove: Vec<HeaderName>,
    /// Headers to be renamed from `(source_name, target_name)`.
    pub rename: Vec<(HeaderName, HeaderName)>,
    /// Headers to be set or overwritten.
    pub set: Vec<(HeaderName, HeaderValueSpec)>,
    /// Headers to be appended to existing values.
    pub append: Vec<(HeaderName, HeaderValueSpec)>,
}

impl Default for HeaderMutationConfig {
    fn default() -> Self {
        Self {
            strip_hop_by_hop: true,
            remove: Vec::new(),
            rename: Vec::new(),
            set: Vec::new(),
            append: Vec::new(),
        }
    }
}

/// Pre-Upstream Header Mutation Plugin.
#[derive(Debug, Clone)]
pub struct HeaderMutationPlugin {
    pub id: String,
    pub config: HeaderMutationConfig,
}

impl HeaderMutationPlugin {
    /// Constructs a new header mutation plugin with set and remove lists (backward compatibility).
    pub fn new(
        id: impl Into<String>,
        set_headers: Vec<(HeaderName, HeaderValue)>,
        remove_headers: Vec<HeaderName>,
    ) -> Self {
        let set = set_headers
            .into_iter()
            .map(|(k, v)| (k, HeaderValueSpec::Static(v)))
            .collect();
        Self::with_config(
            id,
            HeaderMutationConfig {
                strip_hop_by_hop: true,
                remove: remove_headers,
                rename: Vec::new(),
                set,
                append: Vec::new(),
            },
        )
    }

    /// Constructs a new header mutation plugin with complete configuration.
    pub fn with_config(id: impl Into<String>, config: HeaderMutationConfig) -> Self {
        Self {
            id: id.into(),
            config,
        }
    }

    /// Deserializes and constructs an instance directly from a [`PluginConfig`].
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let defaults = HeaderMutationConfig::default();

        if config.config.is_null() {
            return Ok(Self::with_config(&config.id, defaults));
        }

        let raw = config.config.as_object().ok_or_else(|| {
            PluginError::Config(format!("Plugin '{}' config must be an object", config.id))
        })?;

        // 1. strip_hop_by_hop (defaults to true)
        let strip_hop_by_hop = raw
            .get("strip_hop_by_hop")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);

        // 2. remove list
        let mut remove = Vec::new();
        if let Some(rem_arr) = raw.get("remove").and_then(|v| v.as_array()) {
            for item in rem_arr {
                if let Some(s) = item.as_str() {
                    let name = HeaderName::from_bytes(s.as_bytes()).map_err(|e| {
                        PluginError::Config(format!("Invalid remove header name '{s}': {e}"))
                    })?;
                    remove.push(name);
                }
            }
        }

        // 3. rename map
        let mut rename = Vec::new();
        if let Some(rename_obj) = raw.get("rename").and_then(|v| v.as_object()) {
            for (from_k, to_v) in rename_obj {
                let from_name = HeaderName::from_bytes(from_k.as_bytes()).map_err(|e| {
                    PluginError::Config(format!("Invalid rename source header '{from_k}': {e}"))
                })?;
                let to_str = to_v.as_str().ok_or_else(|| {
                    PluginError::Config(format!(
                        "Invalid rename target header for '{from_k}': must be string"
                    ))
                })?;
                let to_name = HeaderName::from_bytes(to_str.as_bytes()).map_err(|e| {
                    PluginError::Config(format!("Invalid rename target header '{to_str}': {e}"))
                })?;
                rename.push((from_name, to_name));
            }
        }

        // 4. set map
        let mut set = Vec::new();
        if let Some(set_obj) = raw.get("set").and_then(|v| v.as_object()) {
            for (k, v) in set_obj {
                let name = HeaderName::from_bytes(k.as_bytes()).map_err(|e| {
                    PluginError::Config(format!("Invalid set header name '{k}': {e}"))
                })?;
                let val_str = v.as_str().ok_or_else(|| {
                    PluginError::Config(format!(
                        "Invalid set header value for '{k}': must be string"
                    ))
                })?;
                let spec = HeaderValueSpec::parse(val_str)?;
                set.push((name, spec));
            }
        }

        // 5. append map
        let mut append = Vec::new();
        if let Some(append_obj) = raw.get("append").and_then(|v| v.as_object()) {
            for (k, v) in append_obj {
                let name = HeaderName::from_bytes(k.as_bytes()).map_err(|e| {
                    PluginError::Config(format!("Invalid append header name '{k}': {e}"))
                })?;
                let val_str = v.as_str().ok_or_else(|| {
                    PluginError::Config(format!(
                        "Invalid append header value for '{k}': must be string"
                    ))
                })?;
                let spec = HeaderValueSpec::parse(val_str)?;
                append.push((name, spec));
            }
        }

        Ok(Self::with_config(
            &config.id,
            HeaderMutationConfig {
                strip_hop_by_hop,
                remove,
                rename,
                set,
                append,
            },
        ))
    }

    /// Strips RFC 9110 / RFC 7230 hop-by-hop headers from headers map.
    #[inline]
    fn strip_hop_by_hop_headers(headers: &mut HeaderMap) {
        // Collect custom connection-token headers if specified in Connection header
        if let Some(raw) = headers
            .get(http::header::CONNECTION)
            .and_then(|v| v.to_str().ok())
        {
            let custom_tokens: Vec<String> = raw
                .split(',')
                .map(|token| token.trim().to_ascii_lowercase())
                .filter(|token| !token.is_empty() && token != "close" && token != "keep-alive")
                .collect();

            for token in custom_tokens {
                if let Ok(name) = HeaderName::from_bytes(token.as_bytes()) {
                    headers.remove(name);
                }
            }
        }

        // Standard RFC Hop-by-Hop headers
        headers.remove(http::header::CONNECTION);
        headers.remove("keep-alive");
        headers.remove(http::header::PROXY_AUTHENTICATE);
        headers.remove(http::header::PROXY_AUTHORIZATION);
        headers.remove(http::header::TE);
        headers.remove(http::header::TRAILER);
        headers.remove(http::header::TRANSFER_ENCODING);
        headers.remove(http::header::UPGRADE);
    }
}

impl PreUpstreamPlugin for HeaderMutationPlugin {
    #[inline(always)]
    fn id(&self) -> &str {
        &self.id
    }

    fn on_upstream_request(&self, req: &mut L7Request) -> Action {
        // Step 1: Strip RFC Hop-by-hop headers if configured
        if self.config.strip_hop_by_hop {
            Self::strip_hop_by_hop_headers(&mut req.headers);
        }

        // Step 2: Remove blacklisted / unneeded headers
        for name in &self.config.remove {
            req.headers.remove(name);
        }

        // Step 3: Rename headers
        for (from_name, to_name) in &self.config.rename {
            if from_name != to_name {
                let values: Vec<HeaderValue> =
                    req.headers.get_all(from_name).iter().cloned().collect();
                if !values.is_empty() {
                    req.headers.remove(from_name);
                    for v in values {
                        req.headers.append(to_name.clone(), v);
                    }
                }
            }
        }

        // Step 4: Set / overwrite headers
        for (name, spec) in &self.config.set {
            if let Some(val) = spec.resolve(req) {
                req.headers.insert(name.clone(), val);
            }
        }

        // Step 5: Append headers
        for (name, spec) in &self.config.append {
            if let Some(val) = spec.resolve(req) {
                req.headers.append(name.clone(), val);
            }
        }

        Action::Continue
    }

    #[inline(always)]
    fn capability(&self) -> PluginCapability {
        PluginCapability::header_only(AccessLevel::Write)
    }

    #[inline(always)]
    fn supported_protocols(&self) -> SupportedProtocols {
        SupportedProtocols::ALL_L7
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{HeaderValue, Method, Uri, Version};
    use velda_core::Body;

    #[test]
    fn test_backward_compatibility_new() {
        let set = vec![(
            HeaderName::from_static("x-custom-gateway"),
            HeaderValue::from_static("velda"),
        )];
        let remove = vec![HeaderName::from_static("x-sensitive")];
        let plugin = HeaderMutationPlugin::new("hm-1", set, remove);

        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("x-sensitive"),
            HeaderValue::from_static("secret"),
        );
        let mut req = L7Request::new(
            Method::GET,
            Uri::from_static("/test"),
            Version::HTTP_11,
            headers,
            Body::Empty,
        );

        let action = plugin.on_upstream_request(&mut req);
        assert!(matches!(action, Action::Continue));
        assert!(!req.headers.contains_key("x-sensitive"));
        assert_eq!(req.headers.get("x-custom-gateway").unwrap(), "velda");
    }

    #[test]
    fn test_header_rename() {
        let config = HeaderMutationConfig {
            strip_hop_by_hop: false,
            remove: Vec::new(),
            rename: vec![(
                HeaderName::from_static("x-legacy-token"),
                HeaderName::from_static("authorization"),
            )],
            set: Vec::new(),
            append: Vec::new(),
        };
        let plugin = HeaderMutationPlugin::with_config("hm-rename", config);

        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("x-legacy-token"),
            HeaderValue::from_static("Bearer token123"),
        );
        let mut req = L7Request::new(
            Method::POST,
            Uri::from_static("/api"),
            Version::HTTP_11,
            headers,
            Body::Empty,
        );

        plugin.on_upstream_request(&mut req);
        assert!(!req.headers.contains_key("x-legacy-token"));
        assert_eq!(req.headers.get("authorization").unwrap(), "Bearer token123");
    }

    #[test]
    fn test_header_append() {
        let config = HeaderMutationConfig {
            strip_hop_by_hop: false,
            remove: Vec::new(),
            rename: Vec::new(),
            set: Vec::new(),
            append: vec![(
                HeaderName::from_static("x-forwarded-for"),
                HeaderValueSpec::Static(HeaderValue::from_static("10.0.0.2")),
            )],
        };
        let plugin = HeaderMutationPlugin::with_config("hm-append", config);

        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("x-forwarded-for"),
            HeaderValue::from_static("192.168.1.1"),
        );
        let mut req = L7Request::new(
            Method::GET,
            Uri::from_static("/"),
            Version::HTTP_11,
            headers,
            Body::Empty,
        );

        plugin.on_upstream_request(&mut req);

        let all: Vec<_> = req.headers.get_all("x-forwarded-for").iter().collect();
        assert_eq!(all.len(), 2);
        assert_eq!(all[0], "192.168.1.1");
        assert_eq!(all[1], "10.0.0.2");
    }

    #[test]
    fn test_strip_hop_by_hop_and_custom_connection_tokens() {
        let config = HeaderMutationConfig {
            strip_hop_by_hop: true,
            remove: Vec::new(),
            rename: Vec::new(),
            set: Vec::new(),
            append: Vec::new(),
        };
        let plugin = HeaderMutationPlugin::with_config("hm-hop", config);

        let mut headers = HeaderMap::new();
        headers.insert(
            "connection",
            HeaderValue::from_static("close, X-Custom-Hop"),
        );
        headers.insert("x-custom-hop", HeaderValue::from_static("temp-val"));
        headers.insert("keep-alive", HeaderValue::from_static("timeout=5"));
        headers.insert("transfer-encoding", HeaderValue::from_static("chunked"));
        headers.insert("upgrade", HeaderValue::from_static("websocket"));
        headers.insert("x-regular-header", HeaderValue::from_static("keep-me"));

        let mut req = L7Request::new(
            Method::GET,
            Uri::from_static("/"),
            Version::HTTP_11,
            headers,
            Body::Empty,
        );

        plugin.on_upstream_request(&mut req);

        assert!(!req.headers.contains_key("connection"));
        assert!(!req.headers.contains_key("x-custom-hop"));
        assert!(!req.headers.contains_key("keep-alive"));
        assert!(!req.headers.contains_key("transfer-encoding"));
        assert!(!req.headers.contains_key("upgrade"));
        assert_eq!(req.headers.get("x-regular-header").unwrap(), "keep-me");
    }

    #[test]
    fn test_dynamic_variables_interpolation() {
        let config = HeaderMutationConfig {
            strip_hop_by_hop: false,
            remove: Vec::new(),
            rename: Vec::new(),
            set: vec![
                (
                    HeaderName::from_static("x-client-ip"),
                    HeaderValueSpec::Variable(DynamicVar::ClientIp),
                ),
                (
                    HeaderName::from_static("x-request-id"),
                    HeaderValueSpec::Variable(DynamicVar::RequestId),
                ),
                (
                    HeaderName::from_static("x-route-id"),
                    HeaderValueSpec::Variable(DynamicVar::RouteId),
                ),
                (
                    HeaderName::from_static("x-scheme"),
                    HeaderValueSpec::Variable(DynamicVar::Scheme),
                ),
                (
                    HeaderName::from_static("x-method"),
                    HeaderValueSpec::Variable(DynamicVar::Method),
                ),
                (
                    HeaderName::from_static("x-path"),
                    HeaderValueSpec::Variable(DynamicVar::Path),
                ),
            ],
            append: Vec::new(),
        };
        let plugin = HeaderMutationPlugin::with_config("hm-vars", config);

        let mut headers = HeaderMap::new();
        headers.insert(
            "x-forwarded-for",
            "203.0.113.195, 10.0.0.1".parse().unwrap(),
        );
        headers.insert("x-route-id", "route-checkout-api".parse().unwrap());

        let mut req = L7Request::new(
            Method::POST,
            Uri::from_static("https://api.example.com/checkout/order"),
            Version::HTTP_2,
            headers,
            Body::Empty,
        );

        plugin.on_upstream_request(&mut req);

        assert_eq!(req.headers.get("x-client-ip").unwrap(), "203.0.113.195");
        assert_eq!(req.headers.get("x-route-id").unwrap(), "route-checkout-api");
        assert_eq!(req.headers.get("x-scheme").unwrap(), "https");
        assert_eq!(req.headers.get("x-method").unwrap(), "POST");
        assert_eq!(req.headers.get("x-path").unwrap(), "/checkout/order");
        assert!(req.headers.contains_key("x-request-id"));
    }

    #[test]
    fn test_from_config_complete() {
        let json_cfg = serde_json::json!({
            "strip_hop_by_hop": true,
            "remove": ["x-internal-secret"],
            "rename": {
                "x-old": "x-new"
            },
            "set": {
                "x-gateway": "velda-edge",
                "x-req-id": "$request_id"
            },
            "append": {
                "x-tags": "edge"
            }
        });
        let config = PluginConfig {
            id: "header_mutation".into(),
            phase: String::new(),
            config: json_cfg,
        };

        let plugin = HeaderMutationPlugin::from_config(&config).expect("valid config");
        assert_eq!(plugin.id, "header_mutation");
        assert!(plugin.config.strip_hop_by_hop);
        assert_eq!(plugin.config.remove.len(), 1);
        assert_eq!(plugin.config.rename.len(), 1);
        assert_eq!(plugin.config.set.len(), 2);
        assert_eq!(plugin.config.append.len(), 1);

        let mut headers = HeaderMap::new();
        headers.insert("x-internal-secret", "classified".parse().unwrap());
        headers.insert("x-old", "payload".parse().unwrap());
        headers.insert("connection", "close".parse().unwrap());

        let mut req = L7Request::new(
            Method::GET,
            Uri::from_static("/"),
            Version::HTTP_11,
            headers,
            Body::Empty,
        );

        plugin.on_upstream_request(&mut req);

        assert!(!req.headers.contains_key("x-internal-secret"));
        assert!(!req.headers.contains_key("connection"));
        assert!(!req.headers.contains_key("x-old"));
        assert_eq!(req.headers.get("x-new").unwrap(), "payload");
        assert_eq!(req.headers.get("x-gateway").unwrap(), "velda-edge");
        assert!(req.headers.contains_key("x-req-id"));
        assert_eq!(req.headers.get("x-tags").unwrap(), "edge");
    }
}
