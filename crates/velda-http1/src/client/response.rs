//! Upstream HTTP/1.1 Response Entity and Wire Decoder (RFC 9112).
//!
//! Architectural Perspective (Góc nhìn Ingress):
//! The Gateway Client is the receiver and owner of the incoming upstream Response entity.
//! Defines native protocol-owned response models and head metadata, decoupled from generic L7 types.
//! Handles upstream wire parsing and stateful buffer consumption (&mut BytesMut) to decode backend responses.

use bytes::{Buf, BytesMut};
use http::header::{CONTENT_LENGTH, HeaderName, HeaderValue};
use http::{StatusCode, Version};
use tokio::io::{AsyncRead, AsyncReadExt};
use velda_core::{Body, HeaderWorkspace, L7Response};

use crate::config::Http1Config;
use crate::error::Http1Error;
use crate::server::request::Http1BodyFraming;
use crate::wire::{cold_parse_error, parse_chunked_body};

/// HTTP/1.1 upstream response head metadata (status code, version, and headers).
#[derive(Debug, Clone)]
pub struct Http1ClientResponseHead {
    /// HTTP status code.
    pub status: StatusCode,
    /// HTTP protocol version.
    pub version: Version,
    /// Response headers.
    pub headers: HeaderWorkspace,
}

impl Http1ClientResponseHead {
    /// Creates a new HTTP/1.1 client response head.
    #[inline]
    pub fn new(status: StatusCode, version: Version, headers: HeaderWorkspace) -> Self {
        Self {
            status,
            version,
            headers,
        }
    }
}

/// Upstream protocol-owned HTTP/1.1 response.
#[derive(Debug, Clone)]
pub struct Http1ClientResponse {
    /// Status code.
    pub status: StatusCode,
    /// Protocol version.
    pub version: Version,
    /// Response headers.
    pub headers: HeaderWorkspace,
    /// Response body.
    pub body: Body,
}

impl Http1ClientResponse {
    /// Creates a new HTTP/1.1 client response.
    #[inline]
    pub fn new(status: StatusCode, version: Version, headers: HeaderWorkspace, body: Body) -> Self {
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
        let mut headers = HeaderWorkspace::empty(4);
        let mut itoa_buf = itoa::Buffer::new();
        let len_str = itoa_buf.format(body.len());
        if let Ok(len_val) = HeaderValue::from_str(len_str) {
            headers.insert(CONTENT_LENGTH, len_val);
        }
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

    /// Converts into canonical [`L7Response`].
    pub fn into_l7_response(self) -> L7Response {
        L7Response::new(self.status, self.version, self.headers, self.body)
    }
}

/// Parses upstream HTTP/1.1 response head without mutating the input slice.
pub fn parse_response_head(
    data: &[u8],
    config: &Http1Config,
) -> Result<Option<(Http1ClientResponseHead, Http1BodyFraming, usize)>, Http1Error> {
    if data.is_empty() {
        return Ok(None);
    }

    let Some((crlf_pos, headers_offset)) = crate::simd::find_crlf_line_end(data)? else {
        if data.len() > config.max_header_size {
            return Err(Http1Error::HeaderTooLarge(data.len()));
        }
        return Ok(None);
    };

    if crlf_pos > config.max_header_size {
        return Err(Http1Error::HeaderTooLarge(crlf_pos));
    }

    let line = &data[..crlf_pos];
    let Some(sp1) = line.iter().position(|&b| b == b' ') else {
        return Err(cold_parse_error("Invalid HTTP response status line"));
    };

    let version = match &line[..sp1] {
        b"HTTP/1.1" => Version::HTTP_11,
        b"HTTP/1.0" => Version::HTTP_10,
        v => {
            if v.len() == 8 && v.starts_with(b"HTTP/1.") && v[7].is_ascii_digit() {
                let minor = v[7] - b'0';
                return Err(cold_parse_error(format!("Unsupported HTTP/1.{minor}")));
            }
            return Err(cold_parse_error("Invalid HTTP version in status line"));
        }
    };

    let after_sp = &line[sp1 + 1..];
    if after_sp.len() < 3
        || !after_sp[0].is_ascii_digit()
        || !after_sp[1].is_ascii_digit()
        || !after_sp[2].is_ascii_digit()
        || (after_sp.len() > 3 && after_sp[3] != b' ')
    {
        return Err(cold_parse_error(
            "Invalid or missing HTTP response status code",
        ));
    }

    if after_sp.len() > 4 && !crate::simd::is_valid_header_value_simd(&after_sp[4..]) {
        return Err(cold_parse_error(
            "Invalid control byte in HTTP status reason phrase",
        ));
    }

    let status_code = ((after_sp[0] - b'0') as u16) * 100
        + ((after_sp[1] - b'0') as u16) * 10
        + ((after_sp[2] - b'0') as u16);
    let status = StatusCode::from_u16(status_code)
        .map_err(|e| Http1Error::Parse(format!("Invalid status code {status_code}: {e}")))?;

    let Some(parsed) =
        crate::simd::parse_headers_into_workspace(data, headers_offset, config, true, None)?
    else {
        return Ok(None);
    };

    let is_no_body = status.is_informational()
        || status == StatusCode::NO_CONTENT
        || status == StatusCode::NOT_MODIFIED;

    let framing = if is_no_body {
        Http1BodyFraming::Empty
    } else if parsed.is_chunked {
        Http1BodyFraming::Chunked
    } else if let Some(len) = parsed.content_length {
        if len == 0 {
            Http1BodyFraming::Empty
        } else {
            Http1BodyFraming::ContentLength(len)
        }
    } else {
        Http1BodyFraming::Empty
    };

    let head = Http1ClientResponseHead::new(status, version, parsed.workspace);
    Ok(Some((head, framing, parsed.total_head_len)))
}

/// Decodes HTTP/1.1 response head from upstream read buffer and advances `buf` past headers.
pub fn decode_response_head(
    buf: &mut BytesMut,
    config: &Http1Config,
) -> Result<Option<(Http1ClientResponseHead, Http1BodyFraming)>, Http1Error> {
    loop {
        if let Some((head, framing, header_len)) = parse_response_head(buf.as_ref(), config)? {
            if head.status.is_informational() && head.status != StatusCode::SWITCHING_PROTOCOLS {
                buf.advance(header_len);
                continue;
            }
            buf.advance(header_len);
            return Ok(Some((head, framing)));
        } else {
            return Ok(None);
        }
    }
}

/// Decodes an entire HTTP/1.1 response from the upstream read buffer.
pub fn decode_response(
    buf: &mut BytesMut,
    config: &Http1Config,
) -> Result<Option<Http1ClientResponse>, Http1Error> {
    loop {
        if buf.is_empty() {
            return Ok(None);
        }
        let Some((head, framing, header_len)) = parse_response_head(buf.as_ref(), config)? else {
            return Ok(None);
        };

        if head.status.is_informational() && head.status != StatusCode::SWITCHING_PROTOCOLS {
            buf.advance(header_len);
            continue;
        }

        match framing {
            Http1BodyFraming::Empty => {
                buf.advance(header_len);
                return Ok(Some(Http1ClientResponse {
                    status: head.status,
                    version: head.version,
                    headers: head.headers,
                    body: Body::Empty,
                }));
            }
            Http1BodyFraming::ContentLength(body_len) => {
                if body_len > config.max_body_size {
                    return Err(Http1Error::PayloadTooLarge(body_len));
                }
                let total_len = header_len + body_len;
                if buf.len() < total_len {
                    return Ok(None);
                }
                buf.advance(header_len);
                let body = if body_len > 0 {
                    if body_len <= 4096 && buf.capacity() >= 16384 {
                        let b = bytes::Bytes::copy_from_slice(&buf[..body_len]);
                        buf.advance(body_len);
                        Body::Bytes(b)
                    } else {
                        Body::Bytes(buf.split_to(body_len).freeze())
                    }
                } else {
                    Body::Empty
                };
                return Ok(Some(Http1ClientResponse {
                    status: head.status,
                    version: head.version,
                    headers: head.headers,
                    body,
                }));
            }
            Http1BodyFraming::Chunked => {
                let chunked_slice = &buf[header_len..];
                match parse_chunked_body(chunked_slice, config.max_body_size)? {
                    Some((consumed_wire, body)) => {
                        buf.advance(header_len + consumed_wire);
                        return Ok(Some(Http1ClientResponse {
                            status: head.status,
                            version: head.version,
                            headers: head.headers,
                            body,
                        }));
                    }
                    None => return Ok(None),
                }
            }
        }
    }
}

/// Asynchronously reads and decodes the HTTP/1.1 response head from an upstream stream.
pub async fn read_response_head<IO>(
    stream: &mut IO,
    read_buf: &mut BytesMut,
    config: &Http1Config,
) -> Result<(Http1ClientResponseHead, Http1BodyFraming), Http1Error>
where
    IO: AsyncRead + Unpin,
{
    loop {
        if let Some(parts) = decode_response_head(read_buf, config)? {
            return Ok(parts);
        }

        let n = stream.read_buf(read_buf).await?;
        if n == 0 {
            if let Some(parts) = decode_response_head(read_buf, config)? {
                return Ok(parts);
            }
            return Err(Http1Error::ConnectionClosed);
        }
    }
}

/// Reads a fixed-size body slice of `needed` bytes from the upstream stream.
pub async fn read_chunk_sized<IO>(
    stream: &mut IO,
    read_buf: &mut BytesMut,
    needed: usize,
) -> Result<Option<bytes::Bytes>, Http1Error>
where
    IO: AsyncRead + Unpin,
{
    while read_buf.len() < needed {
        let remaining_needed = needed - read_buf.len();
        if read_buf.capacity() - read_buf.len() < remaining_needed {
            read_buf.reserve(remaining_needed.max(65_536));
        }
        let n = stream.read_buf(read_buf).await?;
        if n == 0 {
            if !read_buf.is_empty() {
                let available = read_buf.len().min(needed);
                let chunk = if available <= 4096 && read_buf.capacity() >= 16384 {
                    let b = bytes::Bytes::copy_from_slice(&read_buf[..available]);
                    read_buf.advance(available);
                    b
                } else {
                    read_buf.split_to(available).freeze()
                };
                return Ok(Some(chunk));
            }
            return Ok(None);
        }
    }

    let chunk = if needed <= 4096 && read_buf.capacity() >= 16384 {
        let b = bytes::Bytes::copy_from_slice(&read_buf[..needed]);
        read_buf.advance(needed);
        b
    } else {
        read_buf.split_to(needed).freeze()
    };
    Ok(Some(chunk))
}

/// Reads the next chunk from a chunked response stream.
pub async fn read_next_chunk<IO>(
    stream: &mut IO,
    read_buf: &mut BytesMut,
) -> Result<Option<bytes::Bytes>, Http1Error>
where
    IO: AsyncRead + Unpin,
{
    loop {
        if let Some(res) = crate::wire::decode_chunk(read_buf)? {
            return Ok(res);
        }

        if read_buf.is_empty() {
            if read_buf.capacity() < 8192 || read_buf.capacity() > 262144 {
                *read_buf = bytes::BytesMut::with_capacity(65536);
            }
        } else if read_buf.capacity() - read_buf.len() < 8192 {
            read_buf.reserve(65536);
        }
        let n = stream.read_buf(read_buf).await?;
        if n == 0 {
            return Err(Http1Error::ConnectionClosed);
        }
    }
}
