//! Downstream HTTP/3 Pre-Compiled Canonical Early Response Engine (RFC 9114).
//!
//! # Protocol Ownership & Wire Mechanics
//! Enforces Single Responsibility Principle (SRP) by keeping HTTP/3 canonical error
//! response construction strictly isolated within `velda-http3`.
//!
//! # Zero-Copy Hot Path
//! Directly pairs static ASCII bodies with pre-allocated headers and optional L7 `x-request-id`
//! without any dynamic heap formatting (`format!`), JSON serialization, or `.to_vec()` heap churn.

use bytes::Bytes;
use http::header::{CACHE_CONTROL, CONTENT_LENGTH, CONTENT_TYPE, HeaderValue};
use http::{StatusCode, Version};
use velda_core::{Body, HeaderWorkspace, L7Response};

/// Pre-compiles a canonical HTTP/3 early error [`L7Response`] with Zero-Copy static payload and pre-allocated headers.
///
/// Respects SRP by keeping HTTP/3 response construction strictly within `velda-http3`.
pub fn precompiled_early_response(
    status: StatusCode,
    request_id: Option<&HeaderValue>,
) -> L7Response {
    let (body, len_str, status_code): (&'static [u8], &'static str, StatusCode) = match status {
        StatusCode::BAD_REQUEST => (b"400 Bad Request\n", "16", StatusCode::BAD_REQUEST),
        StatusCode::UNAUTHORIZED => (b"401 Unauthorized\n", "17", StatusCode::UNAUTHORIZED),
        StatusCode::FORBIDDEN => (b"403 Forbidden\n", "14", StatusCode::FORBIDDEN),
        StatusCode::NOT_FOUND => (b"404 Not Found\n", "14", StatusCode::NOT_FOUND),
        StatusCode::METHOD_NOT_ALLOWED => (
            b"405 Method Not Allowed\n",
            "23",
            StatusCode::METHOD_NOT_ALLOWED,
        ),
        StatusCode::REQUEST_TIMEOUT => {
            (b"408 Request Timeout\n", "20", StatusCode::REQUEST_TIMEOUT)
        }
        StatusCode::CONFLICT => (b"409 Conflict\n", "13", StatusCode::CONFLICT),
        StatusCode::PAYLOAD_TOO_LARGE => (
            b"413 Payload Too Large\n",
            "22",
            StatusCode::PAYLOAD_TOO_LARGE,
        ),
        StatusCode::URI_TOO_LONG => (b"414 URI Too Long\n", "17", StatusCode::URI_TOO_LONG),
        StatusCode::UNSUPPORTED_MEDIA_TYPE => (
            b"415 Unsupported Media Type\n",
            "27",
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
        ),
        StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE => (
            b"431 Request Header Fields Too Large\n",
            "36",
            StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE,
        ),
        StatusCode::TOO_EARLY => (
            b"425 Too Early: non-idempotent request in 0-RTT\n",
            "45",
            StatusCode::TOO_EARLY,
        ),
        StatusCode::TOO_MANY_REQUESTS => (
            b"429 Too Many Requests\n",
            "22",
            StatusCode::TOO_MANY_REQUESTS,
        ),
        StatusCode::INTERNAL_SERVER_ERROR => (
            b"500 Internal Server Error\n",
            "26",
            StatusCode::INTERNAL_SERVER_ERROR,
        ),
        StatusCode::BAD_GATEWAY => (b"502 Bad Gateway\n", "16", StatusCode::BAD_GATEWAY),
        StatusCode::SERVICE_UNAVAILABLE => (
            b"503 Service Unavailable\n",
            "24",
            StatusCode::SERVICE_UNAVAILABLE,
        ),
        StatusCode::GATEWAY_TIMEOUT => {
            (b"504 Gateway Timeout\n", "20", StatusCode::GATEWAY_TIMEOUT)
        }
        s if s.as_u16() == 499 => (b"499 Client Closed Request\n", "26", s),
        _ => (
            b"500 Internal Server Error\n",
            "26",
            StatusCode::INTERNAL_SERVER_ERROR,
        ),
    };

    let mut ws = match request_id {
        Some(req_id) => HeaderWorkspace::with_request_id(req_id.clone(), 5),
        None => HeaderWorkspace::with_capacity(5),
    };
    ws.insert(
        CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    ws.insert(CONTENT_LENGTH, HeaderValue::from_static(len_str));
    // CPDoS defense: edge-generated errors depend on client-induced conditions (oversized URI,
    // header bombs, blocked origins) and must never be stored by shared caches/CDNs.
    ws.insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    if status_code == StatusCode::TOO_MANY_REQUESTS {
        ws.insert_bypass(http::header::RETRY_AFTER, HeaderValue::from_static("1"));
    }

    L7Response {
        status: status_code,
        version: Version::HTTP_3,
        headers: ws,
        body: Body::Bytes(Bytes::from_static(body)),
    }
}

/// Pre-compiles an HTTP/3 canonical rejection response directly from protocol-agnostic [`velda_core::RejectReason`].
pub fn precompiled_reject_response(
    reason: velda_core::RejectReason,
    request_id: Option<&HeaderValue>,
) -> L7Response {
    use velda_core::RejectReason;
    let status = match reason {
        RejectReason::BadRequest => StatusCode::BAD_REQUEST,
        RejectReason::Unauthorized => StatusCode::UNAUTHORIZED,
        RejectReason::Forbidden => StatusCode::FORBIDDEN,
        RejectReason::NotFound => StatusCode::NOT_FOUND,
        RejectReason::MethodNotAllowed => StatusCode::METHOD_NOT_ALLOWED,
        RejectReason::RequestTimeout => StatusCode::REQUEST_TIMEOUT,
        RejectReason::Conflict => StatusCode::CONFLICT,
        RejectReason::PayloadTooLarge => StatusCode::PAYLOAD_TOO_LARGE,
        RejectReason::UriTooLong => StatusCode::URI_TOO_LONG,
        RejectReason::UnsupportedMediaType => StatusCode::UNSUPPORTED_MEDIA_TYPE,
        RejectReason::TooManyRequests => StatusCode::TOO_MANY_REQUESTS,
        RejectReason::HeaderFieldsTooLarge => StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE,
        RejectReason::ClientClosedRequest => {
            StatusCode::from_u16(499).unwrap_or(StatusCode::BAD_REQUEST)
        }
        RejectReason::InternalError => StatusCode::INTERNAL_SERVER_ERROR,
        RejectReason::BadGateway => StatusCode::BAD_GATEWAY,
        RejectReason::ServiceUnavailable => StatusCode::SERVICE_UNAVAILABLE,
        RejectReason::GatewayTimeout => StatusCode::GATEWAY_TIMEOUT,
    };
    precompiled_early_response(status, request_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_precompiled_early_response_http3() {
        let req_id = HeaderValue::from_static("0123456789abcdef0123456789abcdef");
        let resp = precompiled_early_response(StatusCode::TOO_EARLY, Some(&req_id));

        assert_eq!(resp.status, StatusCode::TOO_EARLY);
        assert_eq!(resp.version, Version::HTTP_3);
        assert_eq!(
            resp.headers.get("content-type").unwrap(),
            "text/plain; charset=utf-8"
        );
        assert_eq!(resp.headers.get("content-length").unwrap(), "45");
        assert_eq!(
            resp.headers.get("x-request-id").unwrap(),
            "0123456789abcdef0123456789abcdef"
        );
        assert_eq!(
            resp.body.to_bytes(),
            Bytes::from_static(b"425 Too Early: non-idempotent request in 0-RTT\n")
        );
    }

    #[test]
    fn test_reject_reasons_keep_their_status_and_forbid_caching() {
        use velda_core::RejectReason;
        // Regression: limits-related rejects once fell through to the 500 fallback on HTTP/3,
        // and edge errors lacked `Cache-Control: no-store` (CPDoS defense).
        for (reason, expected) in [
            (RejectReason::UriTooLong, StatusCode::URI_TOO_LONG),
            (
                RejectReason::HeaderFieldsTooLarge,
                StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE,
            ),
            (RejectReason::PayloadTooLarge, StatusCode::PAYLOAD_TOO_LARGE),
            (
                RejectReason::MethodNotAllowed,
                StatusCode::METHOD_NOT_ALLOWED,
            ),
            (RejectReason::Forbidden, StatusCode::FORBIDDEN),
        ] {
            let resp = precompiled_reject_response(reason, None);
            assert_eq!(resp.status, expected);
            assert_eq!(resp.headers.get("cache-control").unwrap(), "no-store");
        }
    }

    #[test]
    fn test_precompiled_early_response_http3_fallback() {
        let resp = precompiled_early_response(StatusCode::NOT_IMPLEMENTED, None);
        assert_eq!(resp.status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(resp.version, Version::HTTP_3);
        // HeaderWorkspace is unconditionally pre-loaded with an authoritative Request ID in Slot 0
        assert!(resp.headers.get("x-request-id").is_some());
    }
}
