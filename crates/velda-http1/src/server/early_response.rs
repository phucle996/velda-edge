//! Downstream HTTP/1.1 Pre-Compiled Canonical Early Response Engine (RFC 9112).
//!
//! # Protocol Ownership & Wire Mechanics
//! Enforces Single Responsibility Principle (SRP) by keeping HTTP/1.1 ASCII wire formatting
//! strictly isolated within `velda-http1`.
//!
//! # Zero-Allocation Hot Path
//! Directly formats canonical HTTP status lines, static content headers, and the optional
//! L7 `x-request-id` into a reusable buffer or destination socket without any dynamic
//! `HeaderMap` allocation, string formatting, or `.to_vec()` heap churn.

use bytes::{BufMut, Bytes, BytesMut};
use http::header::{CACHE_CONTROL, CONTENT_LENGTH, CONTENT_TYPE, HeaderValue};
use http::{StatusCode, Version};
use tokio::io::{AsyncWrite, AsyncWriteExt};
use velda_core::{Body, HeaderWorkspace, L7Response};

use crate::error::Http1Error;

/// Pre-compiles an HTTP/1.1 canonical early error [`L7Response`] with static slice body and minimal headers.
///
/// Respects SRP by keeping HTTP/1.1 response pre-compilation strictly within `velda-http1`.
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
        StatusCode::TOO_MANY_REQUESTS => (
            b"429 Too Many Requests\n",
            "22",
            StatusCode::TOO_MANY_REQUESTS,
        ),
        StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE => (
            b"431 Request Header Fields Too Large\n",
            "36",
            StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE,
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
        version: Version::HTTP_11,
        headers: ws,
        body: Body::Bytes(Bytes::from_static(body)),
    }
}

/// Pre-compiles an HTTP/1.1 canonical rejection response directly from protocol-agnostic [`velda_core::RejectReason`].
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

/// Serializes a pre-compiled, zero-allocation canonical error response directly into the destination buffer.
///
/// Directly formats the HTTP/1.1 ASCII status line, standard content headers, and optional `x-request-id`
/// into `dst` without any dynamic `HeaderMap` construction or heap allocations (`.to_vec()`).
pub fn encode_early_error(
    status: StatusCode,
    dst: &mut BytesMut,
    request_id: Option<&HeaderValue>,
    alt_svc: Option<&HeaderValue>,
    close: bool,
) {
    let (status_line, content_len_line, body): (&[u8], &[u8], &[u8]) = match status {
        StatusCode::BAD_REQUEST => (
            b"HTTP/1.1 400 Bad Request\r\n",
            b"content-length: 16\r\n",
            b"400 Bad Request\n",
        ),
        StatusCode::UNAUTHORIZED => (
            b"HTTP/1.1 401 Unauthorized\r\n",
            b"content-length: 17\r\n",
            b"401 Unauthorized\n",
        ),
        StatusCode::FORBIDDEN => (
            b"HTTP/1.1 403 Forbidden\r\n",
            b"content-length: 14\r\n",
            b"403 Forbidden\n",
        ),
        StatusCode::NOT_FOUND => (
            b"HTTP/1.1 404 Not Found\r\n",
            b"content-length: 14\r\n",
            b"404 Not Found\n",
        ),
        StatusCode::METHOD_NOT_ALLOWED => (
            b"HTTP/1.1 405 Method Not Allowed\r\n",
            b"content-length: 23\r\n",
            b"405 Method Not Allowed\n",
        ),
        StatusCode::REQUEST_TIMEOUT => (
            b"HTTP/1.1 408 Request Timeout\r\n",
            b"content-length: 20\r\n",
            b"408 Request Timeout\n",
        ),
        StatusCode::CONFLICT => (
            b"HTTP/1.1 409 Conflict\r\n",
            b"content-length: 13\r\n",
            b"409 Conflict\n",
        ),
        StatusCode::PAYLOAD_TOO_LARGE => (
            b"HTTP/1.1 413 Payload Too Large\r\n",
            b"content-length: 22\r\n",
            b"413 Payload Too Large\n",
        ),
        StatusCode::URI_TOO_LONG => (
            b"HTTP/1.1 414 URI Too Long\r\n",
            b"content-length: 17\r\n",
            b"414 URI Too Long\n",
        ),
        StatusCode::UNSUPPORTED_MEDIA_TYPE => (
            b"HTTP/1.1 415 Unsupported Media Type\r\n",
            b"content-length: 27\r\n",
            b"415 Unsupported Media Type\n",
        ),
        StatusCode::TOO_EARLY => (
            b"HTTP/1.1 425 Too Early\r\n",
            b"content-length: 14\r\n",
            b"425 Too Early\n",
        ),
        StatusCode::TOO_MANY_REQUESTS => (
            b"HTTP/1.1 429 Too Many Requests\r\n",
            b"content-length: 22\r\n",
            b"429 Too Many Requests\n",
        ),
        StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE => (
            b"HTTP/1.1 431 Request Header Fields Too Large\r\n",
            b"content-length: 36\r\n",
            b"431 Request Header Fields Too Large\n",
        ),
        s if s.as_u16() == 499 => (
            b"HTTP/1.1 499 Client Closed Request\r\n",
            b"content-length: 26\r\n",
            b"499 Client Closed Request\n",
        ),
        StatusCode::INTERNAL_SERVER_ERROR => (
            b"HTTP/1.1 500 Internal Server Error\r\n",
            b"content-length: 26\r\n",
            b"500 Internal Server Error\n",
        ),
        StatusCode::BAD_GATEWAY => (
            b"HTTP/1.1 502 Bad Gateway\r\n",
            b"content-length: 16\r\n",
            b"502 Bad Gateway\n",
        ),
        StatusCode::SERVICE_UNAVAILABLE => (
            b"HTTP/1.1 503 Service Unavailable\r\n",
            b"content-length: 24\r\n",
            b"503 Service Unavailable\n",
        ),
        StatusCode::GATEWAY_TIMEOUT => (
            b"HTTP/1.1 504 Gateway Timeout\r\n",
            b"content-length: 20\r\n",
            b"504 Gateway Timeout\n",
        ),
        _ => (
            b"HTTP/1.1 500 Internal Server Error\r\n",
            b"content-length: 26\r\n",
            b"500 Internal Server Error\n",
        ),
    };

    dst.put_slice(status_line);
    dst.put_slice(b"content-type: text/plain; charset=utf-8\r\n");
    dst.put_slice(content_len_line);
    dst.put_slice(b"cache-control: no-store\r\n");

    if status == StatusCode::TOO_MANY_REQUESTS {
        dst.put_slice(b"retry-after: 1\r\n");
    }

    if let Some(req_id) = request_id {
        dst.put_slice(b"x-request-id: ");
        dst.put_slice(req_id.as_bytes());
        dst.put_slice(b"\r\n");
    }

    if let Some(alt) = alt_svc {
        dst.put_slice(b"alt-svc: ");
        dst.put_slice(alt.as_bytes());
        dst.put_slice(b"\r\n");
    }

    if close {
        dst.put_slice(b"connection: close\r\n");
    } else {
        dst.put_slice(b"connection: keep-alive\r\n");
    }

    dst.put_slice(b"\r\n");
    dst.put_slice(body);
}

/// Flushes a pre-compiled canonical error response directly to the downstream socket stream.
pub async fn send_early_error<W>(
    stream: &mut W,
    write_buf: &mut BytesMut,
    status: StatusCode,
    request_id: Option<&HeaderValue>,
    alt_svc: Option<&HeaderValue>,
    close: bool,
) -> Result<(), Http1Error>
where
    W: AsyncWrite + Unpin,
{
    write_buf.clear();
    encode_early_error(status, write_buf, request_id, alt_svc, close);
    stream.write_all(write_buf).await?;
    stream.flush().await?;
    write_buf.clear();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_precompiled_early_response_http1() {
        let req_id = HeaderValue::from_static("test-h1-trace");
        let resp = precompiled_early_response(StatusCode::UNAUTHORIZED, Some(&req_id));
        assert_eq!(resp.status, StatusCode::UNAUTHORIZED);
        assert_eq!(resp.version, Version::HTTP_11);
        assert_eq!(
            resp.headers.get("content-type").unwrap(),
            "text/plain; charset=utf-8"
        );
        assert_eq!(resp.headers.get("content-length").unwrap(), "17");
        assert_eq!(resp.headers.get("x-request-id").unwrap(), "test-h1-trace");
        assert_eq!(
            resp.body.to_bytes(),
            Bytes::from_static(b"401 Unauthorized\n")
        );
    }

    #[tokio::test]
    async fn test_encode_early_error_403() {
        let mut buf = BytesMut::new();
        let req_id = HeaderValue::from_static("0123456789abcdef0123456789abcdef");
        encode_early_error(StatusCode::FORBIDDEN, &mut buf, Some(&req_id), None, true);

        let out = std::str::from_utf8(&buf).unwrap();
        assert!(out.starts_with("HTTP/1.1 403 Forbidden\r\n"));
        assert!(out.contains("content-type: text/plain; charset=utf-8\r\n"));
        assert!(out.contains("content-length: 14\r\n"));
        assert!(out.contains("x-request-id: 0123456789abcdef0123456789abcdef\r\n"));
        assert!(out.contains("connection: close\r\n"));
        assert!(out.ends_with("\r\n\r\n403 Forbidden\n"));
    }

    #[tokio::test]
    async fn test_send_early_error_socket() {
        let mut stream = Vec::new();
        let mut buf = BytesMut::new();
        let req_id = HeaderValue::from_static("test-id");
        send_early_error(
            &mut stream,
            &mut buf,
            StatusCode::BAD_REQUEST,
            Some(&req_id),
            None,
            false,
        )
        .await
        .unwrap();

        let out = std::str::from_utf8(&stream).unwrap();
        assert!(out.starts_with("HTTP/1.1 400 Bad Request\r\n"));
        assert!(out.contains("cache-control: no-store\r\n"));
        assert!(out.contains("connection: keep-alive\r\n"));
        assert!(out.ends_with("400 Bad Request\n"));
    }

    #[test]
    fn test_encode_early_error_protocol_limits_408_413_431() {
        for (status, expected_line, expected_len, expected_body) in [
            (
                StatusCode::REQUEST_TIMEOUT,
                "HTTP/1.1 408 Request Timeout\r\n",
                "content-length: 20\r\n",
                "\r\n\r\n408 Request Timeout\n",
            ),
            (
                StatusCode::PAYLOAD_TOO_LARGE,
                "HTTP/1.1 413 Payload Too Large\r\n",
                "content-length: 22\r\n",
                "\r\n\r\n413 Payload Too Large\n",
            ),
            (
                StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE,
                "HTTP/1.1 431 Request Header Fields Too Large\r\n",
                "content-length: 36\r\n",
                "\r\n\r\n431 Request Header Fields Too Large\n",
            ),
        ] {
            let mut buf = BytesMut::new();
            encode_early_error(status, &mut buf, None, None, true);
            let out = std::str::from_utf8(&buf).unwrap();
            assert!(out.starts_with(expected_line));
            assert!(out.contains(expected_len));
            assert!(out.contains("cache-control: no-store\r\n"));
            assert!(out.ends_with(expected_body));
        }
    }
}
