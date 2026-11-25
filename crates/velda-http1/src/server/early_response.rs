//! Downstream HTTP/1.1 Early Error & Rejection Engine (RFC 9112).
//!
//! ```text
//!   Action::Reject(reason) ──► reject_response(reason, req.headers)
//!     ├─ headers.retain_bypass_only()         [Keep MSB bypass headers: x-request-id, retry-after]
//!     └─ stamp RejectStaticMeta               [status, content-len, body + cache-control: no-store]
//!
//!   Parse / Connection Error (no L7Request) ──► send_early_error(status)
//!     └─ status_payload(status) ──► encode_early_error() ──► direct write_buf ASCII serialization
//! ```

use bytes::{BufMut, Bytes, BytesMut};
use http::header::HeaderValue;
use http::{StatusCode, Version};
use tokio::io::{AsyncWrite, AsyncWriteExt};
use velda_core::{Body, HeaderKey, HeaderWorkspace, L7Response, RejectReason};

use crate::error::Http1Error;

#[inline]
const fn status_payload(status: StatusCode) -> (&'static str, &'static [u8]) {
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
        StatusCode::TOO_EARLY => return ("14", b"425 Too Early\n"),
        StatusCode::TOO_MANY_REQUESTS => RejectReason::TooManyRequests,
        StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE => RejectReason::HeaderFieldsTooLarge,
        s if s.as_u16() == 499 => RejectReason::ClientClosedRequest,
        StatusCode::BAD_GATEWAY => RejectReason::BadGateway,
        StatusCode::SERVICE_UNAVAILABLE => RejectReason::ServiceUnavailable,
        StatusCode::GATEWAY_TIMEOUT => RejectReason::GatewayTimeout,
        _ => RejectReason::InternalError,
    };
    let meta = velda_core::lut::get_reject_meta(reason);
    (meta.content_len_str, meta.body)
}

/// Builds an HTTP/1.1 rejection [`L7Response`] in-place from a request [`HeaderWorkspace`],
/// preserving bypass headers (`x-request-id`, `x-ratelimit-*`, `retry-after`) while stamping
/// canonical HTTP/1.1 error headers and static body bytes from [`velda_core::lut::RejectStaticMeta`].
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
        version: Version::HTTP_11,
        headers,
        body: Body::Bytes(Bytes::from_static(meta.body)),
    }
}

/// Serializes a pre-compiled canonical error response directly into `dst` (zero [`HeaderWorkspace`] alloc).
#[cold]
pub fn encode_early_error(
    status: StatusCode,
    dst: &mut BytesMut,
    request_id: Option<&HeaderValue>,
    alt_svc: Option<&HeaderValue>,
    close: bool,
) {
    let (content_len_str, body) = status_payload(status);
    let status_text = &body[..body.len().saturating_sub(1)];

    dst.put_slice(b"HTTP/1.1 ");
    dst.put_slice(status_text);
    dst.put_slice(b"\r\ncontent-type: text/plain; charset=utf-8\r\ncontent-length: ");
    dst.put_slice(content_len_str.as_bytes());
    dst.put_slice(b"\r\ncache-control: no-store\r\n");

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
    dst.put_slice(if close {
        b"connection: close\r\n\r\n"
    } else {
        b"connection: keep-alive\r\n\r\n"
    });
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
    use http::header::{CACHE_CONTROL, CONTENT_LENGTH, CONTENT_TYPE};

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

    #[test]
    fn test_reject_response_retains_bypass_and_stamps_http1_fields() {
        let mut ws = HeaderWorkspace::new(32);
        let req_id_str = ws.request_id().to_str().unwrap().to_string();
        ws.insert(http::header::HOST, HeaderValue::from_static("example.com"));
        ws.insert(http::header::COOKIE, HeaderValue::from_static("sid=1"));
        ws.insert_bypass_u64(
            http::header::HeaderName::from_static("x-ratelimit-remaining"),
            0,
        );

        let resp = reject_response(velda_core::RejectReason::TooManyRequests, ws);
        assert_eq!(resp.status, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(resp.version, Version::HTTP_11);
        assert!(!resp.headers.contains_key(http::header::HOST));
        assert!(!resp.headers.contains_key(http::header::COOKIE));
        assert_eq!(
            resp.headers.get(&HeaderWorkspace::X_REQUEST_ID).unwrap(),
            req_id_str.as_str()
        );
        assert_eq!(resp.headers.get("x-ratelimit-remaining").unwrap(), "0");
        assert_eq!(resp.headers.get(CACHE_CONTROL).unwrap(), "no-store");
        assert_eq!(
            resp.headers.get(CONTENT_TYPE).unwrap(),
            "text/plain; charset=utf-8"
        );
        assert_eq!(resp.headers.get(CONTENT_LENGTH).unwrap(), "22");
        assert_eq!(resp.headers.get(http::header::RETRY_AFTER).unwrap(), "1");
    }
}
