//! Downstream HTTP/3 Pre-Compiled Canonical Early Response Engine (RFC 9114).
//!
//! ```text
//!   StatusCode (Early 0-RTT / Connection Error) ──► precompiled_early_response(status, req_id)
//!   RejectReason (Plugin / WAF / RateLimit)     ──► reject_response(reason, req.headers)
//!     └─► headers.retain_bypass_only() + RejectStaticMeta { status, content_len_str, body }
//! ```

use bytes::Bytes;
use http::header::HeaderValue;
use http::{StatusCode, Version};
use velda_core::{Body, HeaderKey, HeaderWorkspace, L7Response, RejectReason};

#[inline]
const fn status_payload(status: StatusCode) -> (StatusCode, &'static str, &'static [u8]) {
    let reason = match status {
        StatusCode::BAD_REQUEST => RejectReason::BadRequest,
        StatusCode::UNAUTHORIZED => RejectReason::Unauthorized,
        StatusCode::FORBIDDEN => RejectReason::Forbidden,
        StatusCode::NOT_FOUND => RejectReason::NotFound,
        StatusCode::METHOD_NOT_ALLOWED => RejectReason::MethodNotAllowed,
        StatusCode::REQUEST_TIMEOUT => RejectReason::RequestTimeout,
        StatusCode::CONFLICT => RejectReason::Conflict,
        StatusCode::PAYLOAD_TOO_LARGE => RejectReason::PayloadTooLarge,
        StatusCode::URI_TOO_LONG => RejectReason::UriTooLong,
        StatusCode::UNSUPPORTED_MEDIA_TYPE => RejectReason::UnsupportedMediaType,
        StatusCode::TOO_EARLY => {
            return (
                StatusCode::TOO_EARLY,
                "47",
                b"425 Too Early: non-idempotent request in 0-RTT\n",
            );
        }
        StatusCode::TOO_MANY_REQUESTS => RejectReason::TooManyRequests,
        StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE => RejectReason::HeaderFieldsTooLarge,
        s if s.as_u16() == 499 => RejectReason::ClientClosedRequest,
        StatusCode::BAD_GATEWAY => RejectReason::BadGateway,
        StatusCode::SERVICE_UNAVAILABLE => RejectReason::ServiceUnavailable,
        StatusCode::GATEWAY_TIMEOUT => RejectReason::GatewayTimeout,
        _ => RejectReason::InternalError,
    };
    let meta = velda_core::lut::get_reject_meta(reason);
    (meta.status, meta.content_len_str, meta.body)
}

/// Pre-compiles a canonical HTTP/3 early error [`L7Response`] with Zero-Copy static payload.
#[cold]
pub fn precompiled_early_response(
    status: StatusCode,
    request_id: Option<&HeaderValue>,
) -> L7Response {
    const KEY_CACHE_CTRL: HeaderKey = HeaderKey::from_static("cache-control");
    const KEY_CONTENT_TYPE: HeaderKey = HeaderKey::from_static("content-type");
    const KEY_CONTENT_LEN: HeaderKey = HeaderKey::from_static("content-length");
    const KEY_RETRY_AFTER: HeaderKey = HeaderKey::from_static("retry-after");

    let (status_code, content_len_str, body) = status_payload(status);
    let mut ws = match request_id {
        Some(req_id) => HeaderWorkspace::with_request_id(req_id.clone(), 5),
        None => HeaderWorkspace::new(5),
    };
    ws.insert_prehashed(&KEY_CACHE_CTRL, HeaderValue::from_static("no-store"));
    ws.insert_prehashed(
        &KEY_CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    ws.insert_prehashed(&KEY_CONTENT_LEN, HeaderValue::from_static(content_len_str));
    if status_code == StatusCode::TOO_MANY_REQUESTS && !ws.contains_prehashed(&KEY_RETRY_AFTER) {
        ws.insert_prehashed(&KEY_RETRY_AFTER, HeaderValue::from_static("1"));
    }

    L7Response {
        status: status_code,
        version: Version::HTTP_3,
        headers: ws,
        body: Body::Bytes(Bytes::from_static(body)),
    }
}

/// Builds an HTTP/3 rejection [`L7Response`] in-place from a request [`HeaderWorkspace`],
/// preserving bypass headers (`x-request-id`, `x-ratelimit-*`, `retry-after`) while stamping
/// canonical HTTP/3 error headers and static body bytes.
#[cold]
pub fn reject_response(reason: RejectReason, mut headers: HeaderWorkspace) -> L7Response {
    const KEY_CACHE_CTRL: HeaderKey = HeaderKey::from_static("cache-control");
    const KEY_CONTENT_TYPE: HeaderKey = HeaderKey::from_static("content-type");
    const KEY_CONTENT_LEN: HeaderKey = HeaderKey::from_static("content-length");
    const KEY_RETRY_AFTER: HeaderKey = HeaderKey::from_static("retry-after");

    let meta = velda_core::lut::get_reject_meta(reason);
    headers.retain_bypass_only();
    headers.insert_prehashed(&KEY_CACHE_CTRL, HeaderValue::from_static("no-store"));
    headers.insert_prehashed(
        &KEY_CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    headers.insert_prehashed(
        &KEY_CONTENT_LEN,
        HeaderValue::from_static(meta.content_len_str),
    );
    if meta.status == StatusCode::TOO_MANY_REQUESTS && !headers.contains_prehashed(&KEY_RETRY_AFTER)
    {
        headers.insert_prehashed(&KEY_RETRY_AFTER, HeaderValue::from_static("1"));
    }

    L7Response {
        status: meta.status,
        version: Version::HTTP_3,
        headers,
        body: Body::Bytes(Bytes::from_static(meta.body)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::header::{CACHE_CONTROL, CONTENT_LENGTH, CONTENT_TYPE};

    #[test]
    fn test_precompiled_early_response_http3() {
        let req_id = HeaderValue::from_static("0123456789abcdef0123456789abcdef");
        let resp = precompiled_early_response(StatusCode::TOO_EARLY, Some(&req_id));

        assert_eq!(resp.status, StatusCode::TOO_EARLY);
        assert_eq!(resp.version, Version::HTTP_3);
        assert_eq!(
            resp.headers.get(CONTENT_TYPE).unwrap(),
            "text/plain; charset=utf-8"
        );
        assert_eq!(resp.headers.get(CONTENT_LENGTH).unwrap(), "47");
        assert_eq!(
            resp.headers.get(&HeaderWorkspace::X_REQUEST_ID).unwrap(),
            "0123456789abcdef0123456789abcdef"
        );
        assert_eq!(
            resp.body.to_bytes(),
            Bytes::from_static(b"425 Too Early: non-idempotent request in 0-RTT\n")
        );
    }

    #[test]
    fn test_reject_reasons_keep_their_status_and_forbid_caching() {
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
            let resp = reject_response(reason, HeaderWorkspace::new(4));
            assert_eq!(resp.status, expected);
            assert_eq!(resp.headers.get(CACHE_CONTROL).unwrap(), "no-store");
        }
    }

    #[test]
    fn test_precompiled_early_response_http3_fallback() {
        let resp = precompiled_early_response(StatusCode::NOT_IMPLEMENTED, None);
        assert_eq!(resp.status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(resp.version, Version::HTTP_3);
        assert!(resp.headers.get(&HeaderWorkspace::X_REQUEST_ID).is_some());
    }

    #[test]
    fn test_reject_response_http3_retains_bypass_and_sets_defaults() {
        let mut ws = HeaderWorkspace::new(32);
        ws.insert(http::header::COOKIE, HeaderValue::from_static("a=b"));
        let resp = reject_response(RejectReason::TooManyRequests, ws);
        assert_eq!(resp.status, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(resp.version, Version::HTTP_3);
        assert!(!resp.headers.contains_key(http::header::COOKIE));
        assert_eq!(resp.headers.get(CACHE_CONTROL).unwrap(), "no-store");
        assert_eq!(resp.headers.get(http::header::RETRY_AFTER).unwrap(), "1");
    }
}
