//! Upstream HTTP/1.1 Request Entity and Wire Serializer (RFC 9112).
//!
//! Architectural Perspective (Góc nhìn Ingress):
//! Handles serializing outgoing HTTP/1.1 requests to upstream backend microservices.
//! Translates [`Http1ClientRequest`] into wire framing over pre-established stream connections.

use bytes::{BufMut, BytesMut};
use http::header::CONTENT_LENGTH;
use http::{HeaderMap, Method, Uri, Version};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use velda_core::{Body, L7Request};

use crate::client::response::{Http1ClientResponse, decode_response};
use crate::config::Http1Config;
use crate::error::Http1Error;
use crate::server::request::{Http1ServerRequest, Http1ServerRequestHead};

/// HTTP/1.1 client request head metadata.
#[derive(Debug, Clone)]
pub struct Http1ClientRequestHead {
    /// HTTP method.
    pub method: Method,
    /// Target URI / path and query.
    pub uri: Uri,
    /// Protocol version (default HTTP/1.1).
    pub version: Version,
    /// Outbound request headers.
    pub headers: HeaderMap,
}

impl Http1ClientRequestHead {
    /// Creates a new HTTP/1.1 client request head.
    #[inline]
    pub fn new(method: Method, uri: Uri, headers: HeaderMap) -> Self {
        Self {
            method,
            uri,
            version: Version::HTTP_11,
            headers,
        }
    }

    /// Constructs an upstream client request head from a downstream server request head.
    #[inline]
    pub fn from_server_head(head: &Http1ServerRequestHead) -> Self {
        Self {
            method: head.method.clone(),
            uri: head.uri.clone(),
            version: Version::HTTP_11,
            headers: head.headers.clone(),
        }
    }
}

/// Upstream protocol-owned HTTP/1.1 client request.
#[derive(Debug, Clone)]
pub struct Http1ClientRequest {
    /// HTTP method.
    pub method: Method,
    /// Request URI.
    pub uri: Uri,
    /// HTTP version (HTTP/1.1).
    pub version: Version,
    /// Outbound headers.
    pub headers: HeaderMap,
    /// Outbound payload body.
    pub body: Body,
}

impl Http1ClientRequest {
    /// Creates a new HTTP/1.1 client request.
    #[inline]
    pub fn new(method: Method, uri: Uri, version: Version, headers: HeaderMap, body: Body) -> Self {
        Self {
            method,
            uri,
            version,
            headers,
            body,
        }
    }

    /// Constructs an upstream client request from a downstream server request.
    #[inline]
    pub fn from_server_request(req: &Http1ServerRequest) -> Self {
        Self {
            method: req.method.clone(),
            uri: req.uri.clone(),
            version: Version::HTTP_11,
            headers: req.headers.clone(),
            body: req.body.clone(),
        }
    }

    /// Creates a client request from head metadata and body.
    #[inline]
    pub fn from_parts(head: Http1ClientRequestHead, body: Body) -> Self {
        Self {
            method: head.method,
            uri: head.uri,
            version: head.version,
            headers: head.headers,
            body,
        }
    }

    /// Deconstructs request into head and body components.
    #[inline]
    pub fn into_parts(self) -> (Http1ClientRequestHead, Body) {
        (
            Http1ClientRequestHead {
                method: self.method,
                uri: self.uri,
                version: self.version,
                headers: self.headers,
            },
            self.body,
        )
    }

    /// Converts into canonical [`L7Request`].
    pub fn into_l7_request(self) -> L7Request {
        L7Request::new(self.method, self.uri, self.version, self.headers, self.body)
    }

    /// Constructs from canonical [`L7Request`].
    pub fn from_l7_request(req: L7Request) -> Self {
        Self {
            method: req.method,
            uri: req.uri,
            version: req.version,
            headers: req.headers,
            body: req.body,
        }
    }

    /// Serializes entire request into the destination buffer without heap cloning.
    pub fn encode(&self, dst: &mut BytesMut) {
        encode_request_head(
            &self.method,
            &self.uri,
            &self.headers,
            Some(self.body.len()),
            dst,
        );
        if let Body::Bytes(bytes) = &self.body {
            dst.extend_from_slice(bytes);
        }
    }
}

/// Encodes an HTTP/1.1 request line (method, path and query, version).
pub fn encode_request_line(method: &Method, uri: &Uri, dst: &mut BytesMut) {
    dst.put_slice(method.as_str().as_bytes());
    dst.put_slice(b" ");
    let path_and_query = uri
        .path_and_query()
        .map(|pq| pq.as_str())
        .unwrap_or_else(|| uri.path());
    dst.put_slice(path_and_query.as_bytes());
    dst.put_slice(b" HTTP/1.1\r\n");
}

/// Serializes HTTP/1.1 request head metadata into the destination buffer.
#[inline]
pub fn encode_request_head(
    method: &Method,
    uri: &Uri,
    headers: &HeaderMap,
    body_len: Option<usize>,
    dst: &mut BytesMut,
) {
    // Fast O(1) capacity estimation: avoids redundant pass over all header keys & values
    dst.reserve(128 + headers.len() * 48);

    encode_request_line(method, uri, dst);
    for (name, val) in headers {
        dst.put_slice(name.as_str().as_bytes());
        dst.put_slice(b": ");
        dst.put_slice(val.as_bytes());
        dst.put_slice(b"\r\n");
    }

    if let Some(len) = body_len
        && !headers.contains_key(CONTENT_LENGTH)
    {
        dst.put_slice(b"content-length: ");
        let mut itoa_buf = itoa::Buffer::new();
        dst.put_slice(itoa_buf.format(len).as_bytes());
        dst.put_slice(b"\r\n");
    }

    dst.put_slice(b"\r\n");
}

/// Serializes an entire HTTP/1.1 request (head + body) into destination buffer.
pub fn encode_request(req: &Http1ServerRequest, dst: &mut BytesMut) {
    let body_len = match &req.body {
        Body::Empty => None,
        Body::Bytes(bytes) => Some(bytes.len()),
    };

    encode_request_head(&req.method, &req.uri, &req.headers, body_len, dst);

    if let Body::Bytes(bytes) = &req.body {
        dst.extend_from_slice(bytes);
    }
}

/// Serializes and writes request parts directly to upstream stream using vectored I/O.
///
/// Eliminates user-space copying of body payload into `write_buf` and coalesces headers + body into a single syscall.
pub async fn send_request_parts<IO>(
    method: &Method,
    uri: &Uri,
    headers: &HeaderMap,
    body: &Body,
    stream: &mut IO,
    write_buf: &mut BytesMut,
) -> Result<(), Http1Error>
where
    IO: AsyncWrite + Unpin,
{
    let body_len = match body {
        Body::Empty => None,
        Body::Bytes(b) => Some(b.len()),
    };
    write_buf.clear();
    encode_request_head(method, uri, headers, body_len, write_buf);

    match body {
        Body::Empty => {
            stream.write_all(write_buf).await?;
        }
        Body::Bytes(bytes) => {
            let mut h_slice = &write_buf[..];
            let mut b_slice = bytes.as_ref();
            while !h_slice.is_empty() || !b_slice.is_empty() {
                let mut io_slices = [
                    std::io::IoSlice::new(h_slice),
                    std::io::IoSlice::new(b_slice),
                ];
                let active = if h_slice.is_empty() {
                    &mut io_slices[1..]
                } else if b_slice.is_empty() {
                    &mut io_slices[..1]
                } else {
                    &mut io_slices[..]
                };
                let n = stream.write_vectored(active).await?;
                if n == 0 {
                    return Err(Http1Error::Io(std::io::Error::new(
                        std::io::ErrorKind::WriteZero,
                        "failed to write whole request",
                    )));
                }
                if n <= h_slice.len() {
                    h_slice = &h_slice[n..];
                } else {
                    let rem = n - h_slice.len();
                    h_slice = &[];
                    b_slice = &b_slice[rem..];
                }
            }
        }
    }
    Ok(())
}

/// Writes an RFC 9112 chunk to an HTTP/1.1 stream using vectored I/O without copying payload bytes.
pub async fn send_chunk_vectored<IO>(stream: &mut IO, chunk: &[u8]) -> Result<(), Http1Error>
where
    IO: AsyncWrite + Unpin,
{
    use std::io::Write;
    let mut header_buf = [0u8; 16];
    let mut cursor = std::io::Cursor::new(&mut header_buf[..]);
    write!(cursor, "{:x}\r\n", chunk.len())
        .map_err(|e| Http1Error::Io(std::io::Error::other(e)))?;
    let header_len = cursor.position() as usize;

    let mut h_slice = &header_buf[..header_len];
    let mut c_slice = chunk;
    let mut t_slice = &b"\r\n"[..];

    while !h_slice.is_empty() || !c_slice.is_empty() || !t_slice.is_empty() {
        let mut io_slices = [
            std::io::IoSlice::new(h_slice),
            std::io::IoSlice::new(c_slice),
            std::io::IoSlice::new(t_slice),
        ];
        let active = if !h_slice.is_empty() {
            &mut io_slices[..]
        } else if !c_slice.is_empty() {
            &mut io_slices[1..]
        } else {
            &mut io_slices[2..]
        };
        let n = stream.write_vectored(active).await?;
        if n == 0 {
            return Err(Http1Error::Io(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "failed to write whole chunk",
            )));
        }
        if n <= h_slice.len() {
            h_slice = &h_slice[n..];
        } else {
            let rem = n - h_slice.len();
            h_slice = &[];
            if rem <= c_slice.len() {
                c_slice = &c_slice[rem..];
            } else {
                let rem2 = rem - c_slice.len();
                c_slice = &[];
                t_slice = &t_slice[rem2..];
            }
        }
    }
    Ok(())
}

/// Serializes and writes an entire HTTP/1.1 request to the upstream stream using provided write buffer.
pub async fn send_request<IO>(
    req: &Http1ServerRequest,
    stream: &mut IO,
    write_buf: &mut BytesMut,
) -> Result<(), Http1Error>
where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    send_request_parts(
        &req.method,
        &req.uri,
        &req.headers,
        &req.body,
        stream,
        write_buf,
    )
    .await
}

/// Serializes an HTTP/1.1 request head configured for chunked streaming into provided write buffer.
#[inline]
pub fn encode_request_head_chunked(head: &Http1ServerRequestHead, write_buf: &mut BytesMut) {
    write_buf.clear();
    encode_request_line(&head.method, &head.uri, write_buf);

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
}

/// Serializes and writes an HTTP/1.1 request head configured for chunked streaming to upstream using provided write buffer.
pub async fn send_request_head_chunked<IO>(
    head: &Http1ServerRequestHead,
    stream: &mut IO,
    write_buf: &mut BytesMut,
) -> Result<(), Http1Error>
where
    IO: AsyncWrite + Unpin,
{
    encode_request_head_chunked(head, write_buf);
    stream.write_all(write_buf).await?;
    Ok(())
}

/// Forwards an HTTP/1.1 request over an already-connected stream and decodes the incoming response.
pub async fn forward_request<IO>(
    req: &Http1ServerRequest,
    stream: &mut IO,
    config: &Http1Config,
) -> Result<Http1ClientResponse, Http1Error>
where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    let mut write_buf = BytesMut::with_capacity(1024);
    send_request(req, stream, &mut write_buf).await?;

    let mut read_buf = BytesMut::with_capacity(65_536);

    loop {
        let n = stream.read_buf(&mut read_buf).await?;
        if n == 0 {
            if let Some(resp) = decode_response(&mut read_buf, config)? {
                return Ok(resp);
            }
            return Err(Http1Error::ConnectionClosed);
        }

        if let Some(resp) = decode_response(&mut read_buf, config)? {
            return Ok(resp);
        }
    }
}
