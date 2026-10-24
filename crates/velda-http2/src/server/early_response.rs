//! Downstream HTTP/2 Pre-Compiled Canonical Early Response Engine (RFC 9113).
//!
//! # Protocol Ownership & Wire Mechanics
//! Enforces Single Responsibility Principle (SRP) by keeping HTTP/2 binary framing and
//! HPACK encoding strictly isolated within `velda-http2`.
//!
//! # Zero-Body Invariant (RFC 9113)
//! Emits a single `HEADERS` frame with `END_STREAM = true`, containing `:status` and `x-request-id`.
//! Completely bypasses `DATA` frame emission, heap payload allocations, and stream flow control bookkeeping.

use bytes::Bytes;
use h2::server::SendResponse;
use http::header::{CACHE_CONTROL, CONTENT_TYPE, HeaderValue};
use http::{Response, StatusCode, Version};
use velda_core::{Body, HeaderWorkspace, L7Response};

use crate::error::Http2Error;
use crate::headers::X_REQUEST_ID;

/// Pre-compiles an HTTP/2 canonical early error [`L7Response`] with Zero-Body.
///
/// Respects SRP by keeping HTTP/2 error response pre-compilation strictly within `velda-http2`.
/// Setting `Body::Empty` ensures that only a single `HEADERS` frame with `END_STREAM = true`
/// is emitted, completely bypassing `DATA` frame overhead and flow-control window calculations.
pub fn precompiled_early_response(
    status: StatusCode,
    request_id: Option<&HeaderValue>,
) -> L7Response {
    let mut ws = match request_id {
        Some(req_id) => HeaderWorkspace::with_request_id(req_id.clone(), 4),
        None => HeaderWorkspace::with_capacity(4),
    };
    ws.insert(
        CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    // CPDoS defense: edge-generated errors depend on client-induced conditions (oversized URI,
    // header bombs, blocked origins) and must never be stored by shared caches/CDNs.
    ws.insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    if status == StatusCode::TOO_MANY_REQUESTS {
        ws.insert_bypass(http::header::RETRY_AFTER, HeaderValue::from_static("1"));
    }

    L7Response {
        status,
        version: Version::HTTP_2,
        headers: ws,
        body: Body::Empty,
    }
}

/// Pre-compiles an HTTP/2 canonical rejection response directly from protocol-agnostic [`velda_core::RejectReason`].
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

/// Sends a pre-compiled canonical early error response directly over an HTTP/2 stream send handle.
///
/// Emits a single `HEADERS` frame with `end_of_stream = true`, cutting out 100% of `DATA` frame overhead.
pub fn send_early_error(
    respond: &mut SendResponse<Bytes>,
    status: StatusCode,
    request_id: Option<&HeaderValue>,
    alt_svc: Option<&HeaderValue>,
) -> Result<(), Http2Error> {
    let mut response = Response::new(());
    *response.status_mut() = status;
    *response.version_mut() = Version::HTTP_2;

    let headers = response.headers_mut();
    headers.reserve(4);
    headers.insert(
        CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    if status == StatusCode::TOO_MANY_REQUESTS {
        headers.insert(http::header::RETRY_AFTER, HeaderValue::from_static("1"));
    }
    if let Some(req_id) = request_id {
        headers.insert(X_REQUEST_ID, req_id.clone());
    }
    if let Some(alt) = alt_svc {
        headers.insert(http::header::ALT_SVC, alt.clone());
    }

    respond.send_response(response, true)?;
    Ok(())
}

/// Sends a pre-compiled canonical early error response and immediately resets the incoming stream
/// to cancel any pending peer upload without hanging connection buffers.
pub fn send_early_error_and_cancel_upload(
    respond: &mut SendResponse<Bytes>,
    status: StatusCode,
    request_id: Option<&HeaderValue>,
    alt_svc: Option<&HeaderValue>,
) -> Result<(), Http2Error> {
    let mut response = Response::new(());
    *response.status_mut() = status;
    *response.version_mut() = Version::HTTP_2;

    let headers = response.headers_mut();
    headers.reserve(4);
    headers.insert(
        CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    if status == StatusCode::TOO_MANY_REQUESTS {
        headers.insert(http::header::RETRY_AFTER, HeaderValue::from_static("1"));
    }
    if let Some(req_id) = request_id {
        headers.insert(X_REQUEST_ID, req_id.clone());
    }
    if let Some(alt) = alt_svc {
        headers.insert(http::header::ALT_SVC, alt.clone());
    }

    let mut send_stream = respond.send_response(response, true)?;
    send_stream.send_reset(h2::Reason::NO_ERROR);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_precompiled_early_response_zero_body() {
        let req_id = HeaderValue::from_static("0123456789abcdef0123456789abcdef");
        let resp = precompiled_early_response(StatusCode::FORBIDDEN, Some(&req_id));

        assert_eq!(resp.status, StatusCode::FORBIDDEN);
        assert_eq!(resp.version, Version::HTTP_2);
        assert!(resp.body.is_empty());
        assert_eq!(
            resp.headers.get("content-type").unwrap(),
            "text/plain; charset=utf-8"
        );
        assert_eq!(
            resp.headers.get("x-request-id").unwrap(),
            "0123456789abcdef0123456789abcdef"
        );
    }
}
