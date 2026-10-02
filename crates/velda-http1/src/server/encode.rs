//! Downstream HTTP/1.1 Wire Serializer.
//!
//! Architectural Perspective (Góc nhìn Ingress):
//! Handles serializing HTTP/1.1 responses and transmitting vectored I/O back to downstream clients.
//! Uses static fast-path status lines and zero-copy streaming.

use bytes::{BufMut, BytesMut};
use http::header::CONTENT_LENGTH;
use http::{HeaderMap, StatusCode, Version};
use tokio::io::{AsyncWrite, AsyncWriteExt};
use velda_core::Body;

use crate::client::response::{Http1Response, Http1ResponseHead};
use crate::error::Http1Error;

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

/// Encodes HTTP headers into the destination buffer with a single capacity reservation.
#[inline]
pub fn encode_headers(headers: &HeaderMap, dst: &mut BytesMut) {
    if headers.is_empty() {
        return;
    }
    let mut total_len = 0;
    for (name, val) in headers {
        total_len += name.as_str().len() + val.as_bytes().len() + 4;
    }
    dst.reserve(total_len);

    for (name, val) in headers {
        dst.put_slice(name.as_str().as_bytes());
        dst.put_slice(b": ");
        dst.put_slice(val.as_bytes());
        dst.put_slice(b"\r\n");
    }
}

/// Serializes HTTP/1.1 response head metadata into the destination buffer.
#[inline]
pub fn encode_response_head(
    version: Version,
    status: StatusCode,
    headers: &HeaderMap,
    body_len: Option<usize>,
    dst: &mut BytesMut,
) {
    encode_status_line(version, status, dst);
    encode_headers(headers, dst);

    if let Some(len) = body_len
        && !headers.contains_key(CONTENT_LENGTH)
        && status != StatusCode::NO_CONTENT
        && status != StatusCode::NOT_MODIFIED
    {
        dst.put_slice(b"content-length: ");
        let mut itoa_buf = itoa::Buffer::new();
        dst.put_slice(itoa_buf.format(len).as_bytes());
        dst.put_slice(b"\r\n");
    }

    dst.put_slice(b"\r\n");
}

/// Serializes an entire HTTP/1.1 response (head + body) into the destination buffer without heap cloning.
pub fn encode_response(response: &Http1Response, dst: &mut BytesMut) {
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

    if let Body::Bytes(bytes) = &response.body {
        dst.extend_from_slice(bytes);
    }
}

/// Serializes and writes an HTTP/1.1 response to downstream stream using zero-copy body streaming.
pub async fn send_response<W>(
    stream: &mut W,
    write_buf: &mut BytesMut,
    response: &Http1Response,
) -> Result<bool, Http1Error>
where
    W: AsyncWrite + Unpin,
{
    write_buf.clear();
    encode_response_head(
        response.version,
        response.status,
        &response.headers,
        Some(response.body.len()),
        write_buf,
    );

    stream.write_all(write_buf).await?;
    if let Body::Bytes(bytes) = &response.body {
        stream.write_all(bytes).await?;
    }
    stream.flush().await?;

    let close = response
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
    head: &Http1ResponseHead,
    body: &Body,
) -> Result<bool, Http1Error>
where
    W: AsyncWrite + Unpin,
{
    write_buf.clear();
    encode_response_head(
        head.version,
        head.status,
        &head.headers,
        Some(body.len()),
        write_buf,
    );

    stream.write_all(write_buf).await?;
    if let Body::Bytes(bytes) = body {
        stream.write_all(bytes).await?;
    }
    stream.flush().await?;

    let close = head
        .headers
        .get(http::header::CONNECTION)
        .and_then(|h| h.to_str().ok())
        .is_some_and(|s| s.eq_ignore_ascii_case("close"));

    Ok(close)
}

/// Encodes a single chunk into `dst` formatted as `<hex_len>\r\n<data>\r\n`.
#[inline]
pub fn encode_chunk(chunk: &[u8], dst: &mut BytesMut) {
    if chunk.is_empty() {
        return;
    }
    use std::io::Write;
    let mut hex_buf = [0u8; 16];
    let mut cursor = std::io::Cursor::new(&mut hex_buf[..]);
    let _ = write!(cursor, "{:X}\r\n", chunk.len());
    let len = cursor.position() as usize;
    dst.put_slice(&hex_buf[..len]);
    dst.extend_from_slice(chunk);
    dst.put_slice(b"\r\n");
}

/// Encodes the terminal zero chunk (`0\r\n\r\n`) into `dst`.
#[inline]
pub fn encode_chunked_end(dst: &mut BytesMut) {
    dst.put_slice(b"0\r\n\r\n");
}

/// Writes a chunk of body bytes formatted as chunked transfer coding to the stream.
pub async fn send_chunk<W>(stream: &mut W, chunk: &[u8]) -> Result<(), Http1Error>
where
    W: AsyncWrite + Unpin,
{
    if chunk.is_empty() {
        return Ok(());
    }
    use std::io::Write;
    let mut header = [0u8; 32];
    let mut cursor = std::io::Cursor::new(&mut header[..]);
    write!(cursor, "{:X}\r\n", chunk.len()).map_err(Http1Error::Io)?;
    let header_len = cursor.position() as usize;

    stream.write_all(&header[..header_len]).await?;
    stream.write_all(chunk).await?;
    stream.write_all(b"\r\n").await?;
    stream.flush().await?;
    Ok(())
}

/// Writes the terminal zero chunk (`0\r\n\r\n`) to terminate a chunked stream.
pub async fn send_chunked_end<W>(stream: &mut W) -> Result<(), Http1Error>
where
    W: AsyncWrite + Unpin,
{
    stream.write_all(b"0\r\n\r\n").await?;
    stream.flush().await?;
    Ok(())
}

/// Serializes and writes only the HTTP/1.1 response head with `Transfer-Encoding: chunked`.
pub async fn send_response_head_chunked<W>(
    stream: &mut W,
    write_buf: &mut BytesMut,
    version: Version,
    status: StatusCode,
    headers: &HeaderMap,
) -> Result<bool, Http1Error>
where
    W: AsyncWrite + Unpin,
{
    write_buf.clear();
    encode_status_line(version, status, write_buf);

    let mut has_te = false;
    for (name, val) in headers {
        if name == http::header::CONTENT_LENGTH {
            continue; // Chunked streaming supersedes Content-Length
        }
        if name == http::header::TRANSFER_ENCODING {
            has_te = true;
        }
        write_buf.put_slice(name.as_str().as_bytes());
        write_buf.put_slice(b": ");
        write_buf.put_slice(val.as_bytes());
        write_buf.put_slice(b"\r\n");
    }

    if !has_te {
        write_buf.put_slice(b"transfer-encoding: chunked\r\n");
    }
    write_buf.put_slice(b"\r\n");

    stream.write_all(write_buf).await?;
    stream.flush().await?;

    let close = headers
        .get(http::header::CONNECTION)
        .and_then(|h| h.to_str().ok())
        .is_some_and(|s| s.eq_ignore_ascii_case("close"));

    Ok(close)
}
