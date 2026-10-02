//! Upstream HTTP/1.1 Wire Codec Executor.
//!
//! Architectural Perspective (Góc nhìn Ingress):
//! Handles outbound HTTP/1.1 client communication over established streams (IO).
//! Encodes outgoing [`Http1Request`] and decodes incoming upstream [`Http1Response`].

use bytes::{Buf, BufMut, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use velda_core::{Body, IngressLimits};

use super::decode::{decode_response, decode_response_head};
use super::response::{Http1Response, Http1ResponseHead};
use crate::client::parse::parse_single_chunk;
use crate::config::Http1BufferConfig;
use crate::error::Http1Error;
use crate::server::request::{Http1BodyFraming, Http1Request, Http1RequestHead};

/// Forwards an HTTP/1.1 request over an already-connected stream.
///
/// Encodes the request onto the pre-established stream (TCP, TLS, or pooled connection)
/// and decodes the incoming response using the provided buffer config.
pub async fn forward_request<IO>(
    req: &Http1Request,
    stream: &mut IO,
    limits: &IngressLimits,
    buf_config: &Http1BufferConfig,
) -> Result<Http1Response, Http1Error>
where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    send_request(req, stream, buf_config).await?;

    let mut read_buf = BytesMut::with_capacity(buf_config.upstream_read_capacity);

    loop {
        let n = stream.read_buf(&mut read_buf).await?;
        if n == 0 {
            if let Some(resp) = decode_response(&mut read_buf, limits)? {
                return Ok(resp);
            }
            return Err(Http1Error::ConnectionClosed);
        }

        if let Some(resp) = decode_response(&mut read_buf, limits)? {
            return Ok(resp);
        }
    }
}

/// Serializes and writes an entire HTTP/1.1 request (head + body) to the upstream stream.
pub async fn send_request<IO>(
    req: &Http1Request,
    stream: &mut IO,
    buf_config: &Http1BufferConfig,
) -> Result<(), Http1Error>
where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    let mut write_buf = BytesMut::with_capacity(buf_config.upstream_write_base);
    super::encode::encode_request_head(
        &req.method,
        &req.uri,
        &req.headers,
        Some(req.body.len()),
        &mut write_buf,
    );

    stream.write_all(&write_buf).await?;
    if let Body::Bytes(bytes) = &req.body {
        stream.write_all(bytes).await?;
    }
    stream.flush().await?;
    Ok(())
}

/// Serializes and writes an HTTP/1.1 request head configured for chunked streaming to the upstream stream.
pub async fn send_request_head_chunked<IO>(
    head: &Http1RequestHead,
    stream: &mut IO,
    buf_config: &Http1BufferConfig,
) -> Result<(), Http1Error>
where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    let mut write_buf = BytesMut::with_capacity(buf_config.upstream_write_base);
    super::encode::encode_request_line(&head.method, &head.uri, &mut write_buf);

    let mut has_te = false;
    for (name, val) in &head.headers {
        if name == http::header::CONTENT_LENGTH {
            continue;
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

    stream.write_all(&write_buf).await?;
    stream.flush().await?;
    Ok(())
}

/// Reads and decodes the HTTP/1.1 response head from the upstream stream.
pub async fn read_response_head<IO>(
    stream: &mut IO,
    read_buf: &mut BytesMut,
    limits: &IngressLimits,
) -> Result<(Http1ResponseHead, Http1BodyFraming), Http1Error>
where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    loop {
        if let Some(parts) = decode_response_head(read_buf, limits)? {
            return Ok(parts);
        }

        let n = stream.read_buf(read_buf).await?;
        if n == 0 {
            if let Some(parts) = decode_response_head(read_buf, limits)? {
                return Ok(parts);
            }
            return Err(Http1Error::ConnectionClosed);
        }
    }
}

/// Reads the next progressive chunk from the upstream stream for chunked responses.
pub async fn read_next_chunk<IO>(
    stream: &mut IO,
    read_buf: &mut BytesMut,
) -> Result<Option<bytes::Bytes>, Http1Error>
where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    loop {
        if let Some((wire_len, payload, is_terminal)) = parse_single_chunk(read_buf)? {
            let bytes = if is_terminal || payload.is_empty() {
                None
            } else {
                Some(bytes::Bytes::copy_from_slice(payload))
            };
            read_buf.advance(wire_len);
            return Ok(bytes);
        }

        let n = stream.read_buf(read_buf).await?;
        if n == 0 {
            if read_buf.is_empty() {
                return Ok(None);
            } else {
                return Err(Http1Error::Parse(
                    "Unexpected EOF while reading upstream chunked body".into(),
                ));
            }
        }
    }
}

/// Reads up to `max_len` bytes from the upstream stream for Content-Length or raw streaming bodies.
pub async fn read_chunk_sized<IO>(
    stream: &mut IO,
    read_buf: &mut BytesMut,
    max_len: usize,
) -> Result<Option<bytes::Bytes>, Http1Error>
where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    if max_len == 0 {
        return Ok(None);
    }

    if !read_buf.is_empty() {
        let to_take = std::cmp::min(read_buf.len(), max_len);
        let bytes = read_buf.split_to(to_take).freeze();
        return Ok(Some(bytes));
    }

    let n = stream.read_buf(read_buf).await?;
    if n == 0 {
        return Ok(None);
    }
    let to_take = std::cmp::min(read_buf.len(), max_len);
    let bytes = read_buf.split_to(to_take).freeze();
    Ok(Some(bytes))
}
