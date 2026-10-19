//! Dynamic Response Variables and Header Value Specifications.
//!
//! Evaluates dynamic runtime variables for response headers:
//! `$request_id`, `$route_id`, `$upstream_addr`, `$status`, `$timestamp_epoch`.

use std::time::{SystemTime, UNIX_EPOCH};

use http::header::HeaderValue;
use velda_core::L7Response;

use crate::error::PluginError;

/// Dynamic variables evaluated at runtime against the outgoing [`L7Response`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResponseDynamicVar {
    /// In-flight request ID (from `x-request-id` header or fallback).
    RequestId,
    /// Route identifier (from `x-route-id` or `x-velda-route`).
    RouteId,
    /// Upstream target IP/port (from `x-upstream-addr` or `x-upstream-id`).
    UpstreamAddr,
    /// HTTP status code as a numeric string (e.g. "200", "404").
    Status,
    /// Current UNIX epoch timestamp in seconds.
    TimestampEpoch,
}

impl ResponseDynamicVar {
    /// Parses a variable from a dollar-prefixed identifier string.
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "$request_id" => Some(Self::RequestId),
            "$route_id" => Some(Self::RouteId),
            "$upstream_addr" => Some(Self::UpstreamAddr),
            "$status" => Some(Self::Status),
            "$timestamp_epoch" => Some(Self::TimestampEpoch),
            _ => None,
        }
    }
}

/// Specification for response header values: static value or dynamic variable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResponseHeaderValueSpec {
    /// Static header value pre-computed at configuration compile time.
    Static(HeaderValue),
    /// Dynamic variable resolved per response.
    Variable(ResponseDynamicVar),
}

impl ResponseHeaderValueSpec {
    /// Parses a header value specification from a string.
    pub fn parse(s: &str) -> Result<Self, PluginError> {
        if let Some(var) = ResponseDynamicVar::parse(s) {
            Ok(Self::Variable(var))
        } else {
            let val = HeaderValue::from_str(s).map_err(|e| {
                PluginError::Config(format!("Invalid header value string '{s}': {e}"))
            })?;
            Ok(Self::Static(val))
        }
    }

    /// Resolves the header value against the outgoing response.
    #[inline]
    pub fn resolve(&self, resp: &L7Response) -> Option<HeaderValue> {
        match self {
            Self::Static(val) => Some(val.clone()),
            Self::Variable(var) => match var {
                ResponseDynamicVar::RequestId => resp
                    .headers
                    .get("x-request-id")
                    .cloned()
                    .or_else(|| Some(velda_core::next_request_id().1)),
                ResponseDynamicVar::RouteId => resp.headers.get("x-route-id").cloned(),
                ResponseDynamicVar::UpstreamAddr => resp.headers.get("x-upstream-addr").cloned(),
                ResponseDynamicVar::Status => HeaderValue::from_str(resp.status.as_str()).ok(),
                ResponseDynamicVar::TimestampEpoch => {
                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs();
                    HeaderValue::from_str(&now.to_string()).ok()
                }
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{HeaderMap, StatusCode, Version};
    use velda_core::Body;

    #[test]
    fn test_resolve_static_and_variables() {
        let mut headers = HeaderMap::new();
        headers.insert("x-request-id", "req-xyz-123".parse().unwrap());
        headers.insert("x-route-id", "route-api".parse().unwrap());
        headers.insert("x-upstream-addr", "10.0.0.5:8080".parse().unwrap());

        let resp = L7Response {
            status: StatusCode::OK,
            version: Version::HTTP_11,
            headers,
            body: Body::Empty,
        };

        let spec_static = ResponseHeaderValueSpec::parse("velda-edge").unwrap();
        assert_eq!(
            spec_static.resolve(&resp).unwrap(),
            HeaderValue::from_static("velda-edge")
        );

        let spec_req_id = ResponseHeaderValueSpec::parse("$request_id").unwrap();
        assert_eq!(
            spec_req_id.resolve(&resp).unwrap(),
            HeaderValue::from_static("req-xyz-123")
        );

        let spec_status = ResponseHeaderValueSpec::parse("$status").unwrap();
        assert_eq!(
            spec_status.resolve(&resp).unwrap(),
            HeaderValue::from_static("200")
        );

        let spec_route = ResponseHeaderValueSpec::parse("$route_id").unwrap();
        assert_eq!(
            spec_route.resolve(&resp).unwrap(),
            HeaderValue::from_static("route-api")
        );
    }
}
