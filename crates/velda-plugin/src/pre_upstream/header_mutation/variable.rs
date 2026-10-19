//! Dynamic Request Variables and Header Value Specifications.
//!
//! Provides variable parsing and runtime evaluation for dynamic headers:
//! `$client_ip`, `$request_id`, `$route_id`, `$upstream_id`, `$scheme`, `$host`, `$method`, `$path`.

use std::sync::atomic::{AtomicU64, Ordering};

use http::header::HeaderValue;
use velda_core::L7Request;

use crate::error::PluginError;

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

#[cfg(test)]
mod tests {
    use super::*;
    use http::{HeaderMap, Method, Uri, Version};
    use velda_core::Body;

    #[test]
    fn test_dynamic_var_parse() {
        assert_eq!(DynamicVar::parse("$client_ip"), Some(DynamicVar::ClientIp));
        assert_eq!(DynamicVar::parse("$path"), Some(DynamicVar::Path));
        assert_eq!(DynamicVar::parse("$unknown"), None);
    }

    #[test]
    fn test_variable_resolution() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-forwarded-for",
            HeaderValue::from_static("203.0.113.195, 10.0.0.1"),
        );
        headers.insert("host", HeaderValue::from_static("api.example.com"));

        let req = L7Request::new(
            Method::POST,
            Uri::from_static("/v2/checkout"),
            Version::HTTP_11,
            headers,
            Body::Empty,
        );

        let ip_spec = HeaderValueSpec::parse("$client_ip").unwrap();
        assert_eq!(ip_spec.resolve(&req).unwrap(), "203.0.113.195");

        let host_spec = HeaderValueSpec::parse("$host").unwrap();
        assert_eq!(host_spec.resolve(&req).unwrap(), "api.example.com");

        let path_spec = HeaderValueSpec::parse("$path").unwrap();
        assert_eq!(path_spec.resolve(&req).unwrap(), "/v2/checkout");

        let method_spec = HeaderValueSpec::parse("$method").unwrap();
        assert_eq!(method_spec.resolve(&req).unwrap(), "POST");
    }
}
