//! HTTP Plugin header constants.
//!
//! Standardized header names used across HTTP plugin stages for metadata propagation,
//! tracing, and dynamic variable interpolation.

use http::header::HeaderName;

/// Authoritative request correlation ID propagated across edge filters.
pub const X_REQUEST_ID: HeaderName = HeaderName::from_static("x-request-id");

/// Matched route identifier header.
pub const X_ROUTE_ID: HeaderName = HeaderName::from_static("x-route-id");

/// Physical upstream target address header.
pub const X_UPSTREAM_ADDR: HeaderName = HeaderName::from_static("x-upstream-addr");

/// Selected upstream cluster/service identifier header.
pub const X_UPSTREAM_ID: HeaderName = HeaderName::from_static("x-upstream-id");

/// End-to-end response time header.
pub const X_RESPONSE_TIME: HeaderName = HeaderName::from_static("x-response-time");

/// Canonical distributed trace ID header.
pub const X_TRACE_ID: HeaderName = HeaderName::from_static("x-trace-id");

/// Canonical distributed span ID header.
pub const X_SPAN_ID: HeaderName = HeaderName::from_static("x-span-id");

/// Standard W3C TraceContext traceparent header.
pub const TRACEPARENT: HeaderName = HeaderName::from_static("traceparent");

/// Standard W3C TraceContext tracestate header.
pub const TRACESTATE: HeaderName = HeaderName::from_static("tracestate");
