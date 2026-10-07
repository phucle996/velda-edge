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
pub use crate::wire::{
    encode_chunk, encode_chunked_end, encode_headers, send_chunk, send_chunked_end,
};

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

    /// Fast-path lookup for content-length.
    #[inline]
    pub fn content_length(&self) -> Option<usize> {
        self.headers
            .get(http::header::CONTENT_LENGTH)
            .and_then(|val| val.to_str().ok())
            .and_then(|s| s.parse().ok())
    }

    /// Returns `true` if response status indicates connection close or `Connection: close` is present.
    #[inline]
    pub fn is_close(&self) -> bool {
        self.headers
            .get(http::header::CONNECTION)
            .and_then(|h| h.to_str().ok())
            .is_some_and(|s| s.eq_ignore_ascii_case("close"))
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

    /// Serializes response head and body into the destination buffer.
    pub fn encode(&self, dst: &mut BytesMut) {
        encode_response(self, dst);
    }

    /// Writes this response directly to downstream async writer.
    pub async fn send_to<W>(
        &self,
        stream: &mut W,
        write_buf: &mut BytesMut,
        force_close: bool,
    ) -> Result<bool, Http1Error>
    where
        W: AsyncWrite + Unpin,
    {
        send_response(stream, write_buf, self, force_close).await
    }
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

/// Serializes HTTP/1.1 response head with optional forced close flag and zero heap allocations.
pub fn encode_response_head_ext(
    version: Version,
    status: StatusCode,
    headers: &HeaderMap,
    body_len: Option<usize>,
    force_close: bool,
    dst: &mut BytesMut,
) {
    let mut needed = 36;
    for (name, val) in headers {
        needed += name.as_str().len() + val.as_bytes().len() + 4;
    }
    if body_len.is_some() {
        needed += 36;
    }
    if force_close {
        needed += 24;
    }
    dst.reserve(needed);

    encode_status_line(version, status, dst);
    let mut has_conn = false;
    for (name, val) in headers {
        if name == http::header::CONNECTION {
            has_conn = true;
            if force_close {
                dst.put_slice(b"connection: close\r\n");
                continue;
            }
        }
        dst.put_slice(name.as_str().as_bytes());
        dst.put_slice(b": ");
        dst.put_slice(val.as_bytes());
        dst.put_slice(b"\r\n");
    }

    if force_close && !has_conn {
        dst.put_slice(b"connection: close\r\n");
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

/// Serializes HTTP/1.1 response head metadata into destination buffer.
#[inline]
pub fn encode_response_head(
    version: Version,
    status: StatusCode,
    headers: &HeaderMap,
    body_len: Option<usize>,
    dst: &mut BytesMut,
) {
    encode_response_head_ext(version, status, headers, body_len, false, dst);
}

/// Serializes an entire HTTP/1.1 response (head + body) into the destination buffer without heap cloning.
pub fn encode_response(response: &Http1ServerResponse, dst: &mut BytesMut) {
    let body_len = match &response.body {
        Body::Empty => None,
        Body::Bytes(bytes) => Some(bytes.len()),
    };

    encode_response_head(
        response.version,
        response.status,
        &response.headers,
        body_len,
        dst,
    );

    let is_no_body = response.status.is_informational()
        || response.status == StatusCode::NO_CONTENT
        || response.status == StatusCode::NOT_MODIFIED;

    if !is_no_body && let Body::Bytes(bytes) = &response.body {
        dst.extend_from_slice(bytes);
    }
}

/// Serializes and writes an HTTP/1.1 response to downstream stream using zero-copy body streaming.
pub async fn send_response<W>(
    stream: &mut W,
    write_buf: &mut BytesMut,
    response: &Http1ServerResponse,
    force_close: bool,
) -> Result<bool, Http1Error>
where
    W: AsyncWrite + Unpin,
{
    write_buf.clear();
    encode_response_head_ext(
        response.version,
        response.status,
        &response.headers,
        Some(response.body.len()),
        force_close,
        write_buf,
    );

    stream.write_all(write_buf).await?;

    let is_no_body = response.status.is_informational()
        || response.status == StatusCode::NO_CONTENT
        || response.status == StatusCode::NOT_MODIFIED;

    if !is_no_body && let Body::Bytes(bytes) = &response.body {
        stream.write_all(bytes).await?;
    }
    stream.flush().await?;

    let close = force_close
        || response
            .headers
            .get(http::header::CONNECTION)
            .and_then(|h| h.to_str().ok())
            .is_some_and(|s| s.eq_ignore_ascii_case("close"));

    Ok(close)
}

/// Serializes and writes HTTP/1.1 response parts without heap cloning.
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
    write_buf.clear();
    encode_response_head_ext(
        head.version,
        head.status,
        &head.headers,
        Some(body.len()),
        force_close,
        write_buf,
    );

    stream.write_all(write_buf).await?;

    let is_no_body = head.status.is_informational()
        || head.status == StatusCode::NO_CONTENT
        || head.status == StatusCode::NOT_MODIFIED;

    if !is_no_body && let Body::Bytes(bytes) = body {
        stream.write_all(bytes).await?;
    }
    stream.flush().await?;

    let close = force_close
        || head
            .headers
            .get(http::header::CONNECTION)
            .and_then(|h| h.to_str().ok())
            .is_some_and(|s| s.eq_ignore_ascii_case("close"));

    Ok(close)
}

/// Serializes and writes only the HTTP/1.1 response head with `Transfer-Encoding: chunked`.
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
    write_buf.clear();
    encode_status_line(version, status, write_buf);

    let mut has_te = false;
    let mut has_conn = false;
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
                write_buf.put_slice(b"connection: close\r\n");
                continue;
            }
        }
        write_buf.put_slice(name.as_str().as_bytes());
        write_buf.put_slice(b": ");
        write_buf.put_slice(val.as_bytes());
        write_buf.put_slice(b"\r\n");
    }

    if force_close && !has_conn {
        write_buf.put_slice(b"connection: close\r\n");
    }

    if !has_te {
        write_buf.put_slice(b"transfer-encoding: chunked\r\n");
    }
    write_buf.put_slice(b"\r\n");

    stream.write_all(write_buf).await?;
    stream.flush().await?;

    let close = force_close
        || headers
            .get(http::header::CONNECTION)
            .and_then(|h| h.to_str().ok())
            .is_some_and(|s| s.eq_ignore_ascii_case("close"));

    Ok(close)
}
