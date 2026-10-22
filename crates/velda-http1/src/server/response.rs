//! Ingress Downstream HTTP/1.1 Response Entity and Wire Serializer (RFC 9112).
//!
//! Architectural Perspective (Góc nhìn Ingress):
//! Handles serializing HTTP/1.1 responses and transmitting vectored I/O back to downstream clients.
//! Uses static fast-path status lines, zero-copy streaming, and RFC 9112 framing rules.

use bytes::{BufMut, BytesMut};
use http::header::{CONTENT_LENGTH, HeaderName, HeaderValue};
use http::{HeaderMap, StatusCode, Version};
use tokio::io::{AsyncWrite, AsyncWriteExt};
use velda_core::{Body, L7Response};

use crate::error::Http1Error;

/// HTTP/1.1 downstream response head metadata.
#[derive(Debug, Clone)]
pub struct Http1ServerResponseHead {
    /// HTTP status code.
    pub status: StatusCode,
    /// HTTP protocol version.
    pub version: Version,
    /// Response headers.
    pub headers: HeaderMap,
}

impl Http1ServerResponseHead {
    /// Creates a new HTTP/1.1 server response head.
    #[inline]
    pub fn new(status: StatusCode, version: Version, headers: HeaderMap) -> Self {
        Self {
            status,
            version,
            headers,
        }
    }
}

/// Downstream protocol-owned HTTP/1.1 response.
#[derive(Debug, Clone)]
pub struct Http1ServerResponse {
    /// Status code.
    pub status: StatusCode,
    /// Protocol version.
    pub version: Version,
    /// Response headers.
    pub headers: HeaderMap,
    /// Response body.
    pub body: Body,
}

impl Http1ServerResponse {
    /// Creates a new HTTP/1.1 server response.
    #[inline]
    pub fn new(status: StatusCode, version: Version, headers: HeaderMap, body: Body) -> Self {
        Self {
            status,
            version,
            headers,
            body,
        }
    }

    /// Fast constructor from raw status code and byte payload.
    #[inline]
    pub fn from_bytes(status: StatusCode, body: Vec<u8>) -> Self {
        let mut headers = HeaderMap::new();
        headers.insert(
            CONTENT_LENGTH,
            HeaderValue::from_str(&body.len().to_string()).unwrap(),
        );
        Self {
            status,
            version: Version::HTTP_11,
            headers,
            body: Body::Bytes(bytes::Bytes::from(body)),
        }
    }

    /// Appends a header using a fluent builder pattern.
    #[inline]
    pub fn with_header(mut self, name: HeaderName, value: HeaderValue) -> Self {
        self.headers.insert(name, value);
        self
    }

    /// Creates a response from head metadata and body.
    #[inline]
    pub fn from_parts(head: Http1ServerResponseHead, body: Body) -> Self {
        Self {
            status: head.status,
            version: head.version,
            headers: head.headers,
            body,
        }
    }

    /// Deconstructs response into head metadata and payload body.
    #[inline]
    pub fn into_parts(self) -> (Http1ServerResponseHead, Body) {
        (
            Http1ServerResponseHead {
                status: self.status,
                version: self.version,
                headers: self.headers,
            },
            self.body,
        )
    }

    /// Converts this protocol-owned response into canonical [`L7Response`].
    pub fn into_l7_response(self) -> L7Response {
        L7Response::new(self.status, self.version, self.headers, self.body)
    }

    /// Constructs an [`Http1ServerResponse`] from a canonical [`L7Response`].
    pub fn from_l7_response(resp: L7Response) -> Self {
        Self {
            status: resp.status,
            version: resp.version,
            headers: resp.headers,
            body: resp.body,
        }
    }
}

#[inline]
pub fn is_connection_close(headers: &HeaderMap) -> bool {
    headers
        .get(http::header::CONNECTION)
        .and_then(|h| h.to_str().ok())
        .is_some_and(|s| s.eq_ignore_ascii_case("close"))
}

/// Encodes an HTTP status line with fast-path static byte slices for common status codes.
pub fn encode_status_line(version: Version, status: StatusCode, dst: &mut BytesMut) {
    if version == Version::HTTP_11 {
        match status {
            StatusCode::OK => {
                dst.put_slice(b"HTTP/1.1 200 OK\r\n");
                return;
            }
            StatusCode::CREATED => {
                dst.put_slice(b"HTTP/1.1 201 Created\r\n");
                return;
            }
            StatusCode::ACCEPTED => {
                dst.put_slice(b"HTTP/1.1 202 Accepted\r\n");
                return;
            }
            StatusCode::NO_CONTENT => {
                dst.put_slice(b"HTTP/1.1 204 No Content\r\n");
                return;
            }
            StatusCode::MOVED_PERMANENTLY => {
                dst.put_slice(b"HTTP/1.1 301 Moved Permanently\r\n");
                return;
            }
            StatusCode::FOUND => {
                dst.put_slice(b"HTTP/1.1 302 Found\r\n");
                return;
            }
            StatusCode::NOT_MODIFIED => {
                dst.put_slice(b"HTTP/1.1 304 Not Modified\r\n");
                return;
            }
            StatusCode::BAD_REQUEST => {
                dst.put_slice(b"HTTP/1.1 400 Bad Request\r\n");
                return;
            }
            StatusCode::UNAUTHORIZED => {
                dst.put_slice(b"HTTP/1.1 401 Unauthorized\r\n");
                return;
            }
            StatusCode::FORBIDDEN => {
                dst.put_slice(b"HTTP/1.1 403 Forbidden\r\n");
                return;
            }
            StatusCode::NOT_FOUND => {
                dst.put_slice(b"HTTP/1.1 404 Not Found\r\n");
                return;
            }
            StatusCode::METHOD_NOT_ALLOWED => {
                dst.put_slice(b"HTTP/1.1 405 Method Not Allowed\r\n");
                return;
            }
            StatusCode::PAYLOAD_TOO_LARGE => {
                dst.put_slice(b"HTTP/1.1 413 Payload Too Large\r\n");
                return;
            }
            StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE => {
                dst.put_slice(b"HTTP/1.1 431 Request Header Fields Too Large\r\n");
                return;
            }
            StatusCode::INTERNAL_SERVER_ERROR => {
                dst.put_slice(b"HTTP/1.1 500 Internal Server Error\r\n");
                return;
            }
            StatusCode::BAD_GATEWAY => {
                dst.put_slice(b"HTTP/1.1 502 Bad Gateway\r\n");
                return;
            }
            StatusCode::SERVICE_UNAVAILABLE => {
                dst.put_slice(b"HTTP/1.1 503 Service Unavailable\r\n");
                return;
            }
            StatusCode::GATEWAY_TIMEOUT => {
                dst.put_slice(b"HTTP/1.1 504 Gateway Timeout\r\n");
                return;
            }
            _ => {}
        }
    }

    match version {
        Version::HTTP_10 => dst.put_slice(b"HTTP/1.0 "),
        _ => dst.put_slice(b"HTTP/1.1 "),
    }
    dst.put_slice(status.as_str().as_bytes());
    dst.put_slice(b" ");
    dst.put_slice(status.canonical_reason().unwrap_or("Unknown").as_bytes());
    dst.put_slice(b"\r\n");
}

/// Optional metadata and connection control directives applied during HTTP/1.1 response head serialization.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct HeadEncodeOptions<'a> {
    pub body_len: Option<usize>,
    pub force_close: bool,
    pub request_id: Option<&'a HeaderValue>,
}

impl<'a> HeadEncodeOptions<'a> {
    pub const fn new() -> Self {
        Self {
            body_len: None,
            force_close: false,
            request_id: None,
        }
    }

    pub const fn with_body_len(mut self, len: Option<usize>) -> Self {
        self.body_len = len;
        self
    }

    pub const fn with_force_close(mut self, force_close: bool) -> Self {
        self.force_close = force_close;
        self
    }

    pub const fn with_request_id(mut self, request_id: Option<&'a HeaderValue>) -> Self {
        self.request_id = request_id;
        self
    }
}

/// Serializes only the HTTP/1.1 response head with Content-Length or without body framing,
/// automatically enforcing the system-wide L7 `x-request-id` header invariant.
///
/// Invariant:
/// - If response headers already contain `x-request-id`, it is written as-is.
/// - If `request_id` is provided and `headers` does not already contain an `x-request-id`,
///   it is serialized directly to the wire buffer without any heap allocations.
pub fn encode_head(
    version: Version,
    status: StatusCode,
    headers: &HeaderMap,
    opts: HeadEncodeOptions<'_>,
    dst: &mut BytesMut,
) {
    let HeadEncodeOptions {
        body_len,
        force_close,
        request_id,
    } = opts;
    // Fast O(1) capacity estimation: avoids redundant pass over all response headers
    let extra_req_id_len = if request_id.is_some() { 64 } else { 0 };
    dst.reserve(128 + headers.len() * 48 + extra_req_id_len);

    encode_status_line(version, status, dst);
    let mut has_conn = false;
    let mut has_req_id = false;
    for (name, val) in headers {
        if name == http::header::CONNECTION {
            has_conn = true;
            if force_close {
                dst.put_slice(b"connection: close\r\n");
                continue;
            }
        } else if name == "x-request-id" {
            has_req_id = true;
        }
        dst.put_slice(name.as_str().as_bytes());
        dst.put_slice(b": ");
        dst.put_slice(val.as_bytes());
        dst.put_slice(b"\r\n");
    }

    if force_close && !has_conn {
        dst.put_slice(b"connection: close\r\n");
    }

    // Invariant: L7 Request ID is always serialized to downstream wire buffer
    if !has_req_id && let Some(req_id) = request_id {
        dst.put_slice(b"x-request-id: ");
        dst.put_slice(req_id.as_bytes());
        dst.put_slice(b"\r\n");
    }

    if let Some(len) = body_len
        && !headers.contains_key(CONTENT_LENGTH)
        && status != StatusCode::NO_CONTENT
        && status != StatusCode::NOT_MODIFIED
        && !status.is_informational()
    {
        dst.put_slice(b"content-length: ");
        let mut itoa_buf = itoa::Buffer::new();
        dst.put_slice(itoa_buf.format(len).as_bytes());
        dst.put_slice(b"\r\n");
    }

    dst.put_slice(b"\r\n");
}

/// Serializes only the HTTP/1.1 response head with `Transfer-Encoding: chunked`,
/// automatically enforcing the system-wide L7 `x-request-id` header invariant.
pub fn encode_chunked_head(
    version: Version,
    status: StatusCode,
    headers: &HeaderMap,
    opts: HeadEncodeOptions<'_>,
    dst: &mut BytesMut,
) {
    let HeadEncodeOptions {
        force_close,
        request_id,
        ..
    } = opts;
    let mut needed = 64;
    for (name, val) in headers {
        needed += name.as_str().len() + val.as_bytes().len() + 4;
    }
    if request_id.is_some() {
        needed += 64;
    }
    dst.reserve(needed);

    encode_status_line(version, status, dst);

    let mut has_te = false;
    let mut has_conn = false;
    let mut has_req_id = false;
    for (name, val) in headers {
        if name == http::header::CONTENT_LENGTH {
            continue;
        }
        if name == http::header::TRANSFER_ENCODING {
            has_te = true;
        }
        if name == http::header::CONNECTION {
            has_conn = true;
            if force_close {
                dst.put_slice(b"connection: close\r\n");
                continue;
            }
        }
        if name == "x-request-id" {
            has_req_id = true;
        }
        dst.put_slice(name.as_str().as_bytes());
        dst.put_slice(b": ");
        dst.put_slice(val.as_bytes());
        dst.put_slice(b"\r\n");
    }

    if force_close && !has_conn {
        dst.put_slice(b"connection: close\r\n");
    }

    // Invariant: L7 Request ID is always serialized to downstream wire buffer
    if !has_req_id && let Some(req_id) = request_id {
        dst.put_slice(b"x-request-id: ");
        dst.put_slice(req_id.as_bytes());
        dst.put_slice(b"\r\n");
    }

    if !has_te {
        dst.put_slice(b"transfer-encoding: chunked\r\n");
    }
    dst.put_slice(b"\r\n");
}

/// Serializes an entire HTTP/1.1 response (head + body) into the destination buffer without heap cloning.
pub fn encode_response(response: &Http1ServerResponse, dst: &mut BytesMut) {
    let body_len = match &response.body {
        Body::Empty => None,
        Body::Bytes(bytes) => Some(bytes.len()),
    };

    encode_head(
        response.version,
        response.status,
        &response.headers,
        HeadEncodeOptions {
            body_len,
            force_close: false,
            request_id: None,
        },
        dst,
    );

    let is_no_body = response.status.is_informational()
        || response.status == StatusCode::NO_CONTENT
        || response.status == StatusCode::NOT_MODIFIED;

    if !is_no_body && let Body::Bytes(bytes) = &response.body {
        dst.extend_from_slice(bytes);
    }
}

/// Serializes and writes an HTTP/1.1 response to downstream stream with request ID injection.
pub async fn send_response<W>(
    stream: &mut W,
    write_buf: &mut BytesMut,
    response: &Http1ServerResponse,
    force_close: bool,
    request_id: Option<&HeaderValue>,
) -> Result<bool, Http1Error>
where
    W: AsyncWrite + Unpin,
{
    write_buf.clear();
    encode_head(
        response.version,
        response.status,
        &response.headers,
        HeadEncodeOptions {
            body_len: Some(response.body.len()),
            force_close,
            request_id,
        },
        write_buf,
    );

    let is_no_body = response.status.is_informational()
        || response.status == StatusCode::NO_CONTENT
        || response.status == StatusCode::NOT_MODIFIED;

    if !is_no_body
        && let Body::Bytes(bytes) = &response.body
        && bytes.len() <= 65536
    {
        write_buf.extend_from_slice(bytes);
        stream.write_all(write_buf).await?;
        return Ok(force_close || is_connection_close(&response.headers));
    }

    stream.write_all(write_buf).await?;

    if !is_no_body && let Body::Bytes(bytes) = &response.body {
        stream.write_all(bytes).await?;
    }

    Ok(force_close || is_connection_close(&response.headers))
}

/// Serializes and writes HTTP/1.1 response parts with request ID injection.
pub async fn send_parts<W>(
    stream: &mut W,
    write_buf: &mut BytesMut,
    head: &Http1ServerResponseHead,
    body: &Body,
    force_close: bool,
    request_id: Option<&HeaderValue>,
) -> Result<bool, Http1Error>
where
    W: AsyncWrite + Unpin,
{
    write_buf.clear();
    encode_head(
        head.version,
        head.status,
        &head.headers,
        HeadEncodeOptions {
            body_len: Some(body.len()),
            force_close,
            request_id,
        },
        write_buf,
    );

    let is_no_body = head.status.is_informational()
        || head.status == StatusCode::NO_CONTENT
        || head.status == StatusCode::NOT_MODIFIED;

    if !is_no_body
        && let Body::Bytes(bytes) = body
        && bytes.len() <= 65536
    {
        write_buf.extend_from_slice(bytes);
        stream.write_all(write_buf).await?;
        return Ok(force_close || is_connection_close(&head.headers));
    }

    stream.write_all(write_buf).await?;

    if !is_no_body && let Body::Bytes(bytes) = body {
        stream.write_all(bytes).await?;
    }

    Ok(force_close || is_connection_close(&head.headers))
}

/// Serializes and writes only the HTTP/1.1 response head with `Transfer-Encoding: chunked`.
pub async fn send_chunked_head<W>(
    stream: &mut W,
    write_buf: &mut BytesMut,
    version: Version,
    status: StatusCode,
    headers: &HeaderMap,
    opts: HeadEncodeOptions<'_>,
) -> Result<bool, Http1Error>
where
    W: AsyncWrite + Unpin,
{
    write_buf.clear();
    let force_close = opts.force_close;
    encode_chunked_head(version, status, headers, opts, write_buf);
    stream.write_all(write_buf).await?;

    Ok(force_close || is_connection_close(headers))
}

/// Serializes and writes chunked response head coalescing initial body bytes in a single transmission.
pub async fn send_head_coalesced<W>(
    stream: &mut W,
    write_buf: &mut BytesMut,
    head: &Http1ServerResponseHead,
    initial_chunk: Option<&[u8]>,
    is_terminated: bool,
    opts: HeadEncodeOptions<'_>,
) -> Result<bool, Http1Error>
where
    W: AsyncWrite + Unpin,
{
    write_buf.clear();
    let force_close = opts.force_close;
    encode_chunked_head(head.version, head.status, &head.headers, opts, write_buf);

    if let Some(chunk) = initial_chunk
        && !chunk.is_empty()
    {
        let mut hex_buf = [0u8; 32];
        let hex_len = crate::wire::format_hex_crlf(chunk.len(), &mut hex_buf);
        let hex_slice = &hex_buf[..hex_len];
        write_buf.reserve(hex_slice.len() + chunk.len() + 2 + if is_terminated { 5 } else { 0 });
        write_buf.put_slice(hex_slice);
        write_buf.put_slice(chunk);
        write_buf.put_slice(b"\r\n");
    }

    if is_terminated {
        write_buf.put_slice(b"0\r\n\r\n");
    }

    stream.write_all(write_buf).await?;

    Ok(force_close || is_connection_close(&head.headers))
}

// ---------------------------------------------------------------------------
// Backward Compatibility Aliases
// ---------------------------------------------------------------------------
#[inline]
pub fn encode_response_head(
    version: Version,
    status: StatusCode,
    headers: &HeaderMap,
    body_len: Option<usize>,
    force_close: bool,
    dst: &mut BytesMut,
) {
    encode_head(
        version,
        status,
        headers,
        HeadEncodeOptions {
            body_len,
            force_close,
            ..Default::default()
        },
        dst,
    );
}

#[inline]
pub fn encode_response_head_chunked(
    version: Version,
    status: StatusCode,
    headers: &HeaderMap,
    force_close: bool,
    dst: &mut BytesMut,
) {
    encode_chunked_head(
        version,
        status,
        headers,
        HeadEncodeOptions {
            force_close,
            ..Default::default()
        },
        dst,
    );
}

#[inline]
pub async fn send_response_parts<W>(
    stream: &mut W,
    write_buf: &mut BytesMut,
    head: &Http1ServerResponseHead,
    body: &Body,
    force_close: bool,
) -> Result<bool, Http1Error>
where
    W: AsyncWrite + Unpin,
{
    send_parts(stream, write_buf, head, body, force_close, None).await
}

#[inline]
pub async fn send_response_head_chunked<W>(
    stream: &mut W,
    write_buf: &mut BytesMut,
    version: Version,
    status: StatusCode,
    headers: &HeaderMap,
    force_close: bool,
) -> Result<bool, Http1Error>
where
    W: AsyncWrite + Unpin,
{
    send_chunked_head(
        stream,
        write_buf,
        version,
        status,
        headers,
        HeadEncodeOptions {
            force_close,
            ..Default::default()
        },
    )
    .await
}

#[inline]
pub async fn send_response_head_chunked_coalesced<W>(
    stream: &mut W,
    write_buf: &mut BytesMut,
    head: &Http1ServerResponseHead,
    initial_chunk: Option<&[u8]>,
    is_terminated: bool,
    force_close: bool,
) -> Result<bool, Http1Error>
where
    W: AsyncWrite + Unpin,
{
    send_head_coalesced(
        stream,
        write_buf,
        head,
        initial_chunk,
        is_terminated,
        HeadEncodeOptions {
            force_close,
            ..Default::default()
        },
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_send_response_and_send_response_parts_consistency() {
        let mut headers = HeaderMap::new();
        headers.insert(
            http::header::CONTENT_TYPE,
            HeaderValue::from_static("text/plain"),
        );
        let body = Body::Bytes(bytes::Bytes::from_static(b"hello world"));

        let resp = Http1ServerResponse::new(
            StatusCode::OK,
            Version::HTTP_11,
            headers.clone(),
            body.clone(),
        );

        let mut write_buf1 = BytesMut::new();
        let mut stream1 = Vec::new();
        let close1 = send_response(&mut stream1, &mut write_buf1, &resp, false, None)
            .await
            .unwrap();

        let head = Http1ServerResponseHead::new(StatusCode::OK, Version::HTTP_11, headers);
        let mut write_buf2 = BytesMut::new();
        let mut stream2 = Vec::new();
        let close2 = send_response_parts(&mut stream2, &mut write_buf2, &head, &body, false)
            .await
            .unwrap();

        assert!(!close1);
        assert!(!close2);
        assert_eq!(stream1, stream2);
        let text = std::str::from_utf8(&stream1).unwrap();
        assert!(text.starts_with("HTTP/1.1 200 OK\r\n"));
        assert!(text.contains("content-length: 11\r\n"));
        assert!(text.ends_with("\r\nhello world"));
    }

    #[tokio::test]
    async fn test_send_response_head_chunked() {
        let headers = HeaderMap::new();
        let mut write_buf = BytesMut::new();
        let mut stream = Vec::new();

        let close = send_response_head_chunked(
            &mut stream,
            &mut write_buf,
            Version::HTTP_11,
            StatusCode::OK,
            &headers,
            false,
        )
        .await
        .unwrap();

        assert!(!close);
        let text = std::str::from_utf8(&stream).unwrap();
        assert!(text.starts_with("HTTP/1.1 200 OK\r\n"));
        assert!(text.contains("transfer-encoding: chunked\r\n"));
        assert!(text.ends_with("\r\n\r\n"));
    }

    #[test]
    fn test_encode_response_head_with_request_id_injected() {
        let headers = HeaderMap::new();
        let mut dst = BytesMut::new();
        let req_id = HeaderValue::from_static("test-w3c-trace-id-12345");

        encode_head(
            Version::HTTP_11,
            StatusCode::OK,
            &headers,
            HeadEncodeOptions {
                body_len: Some(42),
                force_close: false,
                request_id: Some(&req_id),
            },
            &mut dst,
        );

        let text = std::str::from_utf8(&dst).unwrap();
        assert!(text.starts_with("HTTP/1.1 200 OK\r\n"));
        assert!(text.contains("x-request-id: test-w3c-trace-id-12345\r\n"));
        assert!(text.contains("content-length: 42\r\n"));
        assert!(text.ends_with("\r\n\r\n"));
    }

    #[test]
    fn test_encode_response_head_with_request_id_no_duplicate() {
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("x-request-id"),
            HeaderValue::from_static("existing-req-id"),
        );
        let mut dst = BytesMut::new();
        let fallback_req_id = HeaderValue::from_static("fallback-req-id");

        encode_head(
            Version::HTTP_11,
            StatusCode::OK,
            &headers,
            HeadEncodeOptions {
                body_len: None,
                force_close: false,
                request_id: Some(&fallback_req_id),
            },
            &mut dst,
        );

        let text = std::str::from_utf8(&dst).unwrap();
        assert!(text.contains("x-request-id: existing-req-id\r\n"));
        assert!(!text.contains("fallback-req-id"));
        assert_eq!(text.matches("x-request-id:").count(), 1);
    }

    #[test]
    fn test_encode_response_head_chunked_with_request_id() {
        let headers = HeaderMap::new();
        let mut dst = BytesMut::new();
        let req_id = HeaderValue::from_static("trace-chunked-999");

        encode_chunked_head(
            Version::HTTP_11,
            StatusCode::CREATED,
            &headers,
            HeadEncodeOptions {
                body_len: None,
                force_close: false,
                request_id: Some(&req_id),
            },
            &mut dst,
        );

        let text = std::str::from_utf8(&dst).unwrap();
        assert!(text.starts_with("HTTP/1.1 201 Created\r\n"));
        assert!(text.contains("transfer-encoding: chunked\r\n"));
        assert!(text.contains("x-request-id: trace-chunked-999\r\n"));
        assert!(text.ends_with("\r\n\r\n"));
    }
}
