//! Upstream HTTP/1.1 Response Entity and Wire Decoder (RFC 9112).
//!
//! Architectural Perspective (Góc nhìn Ingress):
//! The Gateway Client is the receiver and owner of the incoming upstream Response entity.
//! Defines native protocol-owned response models and head metadata, decoupled from generic L7 types.
//! Handles upstream wire parsing and stateful buffer consumption (&mut BytesMut) to decode backend responses.

use bytes::{Buf, BytesMut};
use http::header::{CONTENT_LENGTH, HeaderName, HeaderValue};
use http::{HeaderMap, StatusCode, Version};
use tokio::io::{AsyncRead, AsyncReadExt};
pub use velda_core::{Body, L7Response};

use crate::config::Http1Config;
use crate::error::Http1Error;
use crate::server::request::Http1BodyFraming;
use crate::wire::{cold_parse_error, cold_smuggling_error, parse_ascii_digits, parse_chunked_body};

/// Fast length-partitioned header name matching for common HTTP/1.1 upstream response headers.
#[inline]
pub fn match_upstream_header_name(name_bytes: &[u8]) -> Result<HeaderName, Http1Error> {
    match name_bytes.len() {
        4 if name_bytes.eq_ignore_ascii_case(b"date") => Ok(http::header::DATE),
        6 if name_bytes.eq_ignore_ascii_case(b"server") => Ok(http::header::SERVER),
        7 if name_bytes.eq_ignore_ascii_case(b"upgrade") => Ok(http::header::UPGRADE),
        8 if name_bytes.eq_ignore_ascii_case(b"location") => Ok(http::header::LOCATION),
        10 => {
            if name_bytes.eq_ignore_ascii_case(b"connection") {
                Ok(http::header::CONNECTION)
            } else if name_bytes.eq_ignore_ascii_case(b"set-cookie") {
                Ok(http::header::SET_COOKIE)
            } else {
                HeaderName::from_bytes(name_bytes)
                    .map_err(|e| Http1Error::InvalidHeader(e.to_string()))
            }
        }
        12 if name_bytes.eq_ignore_ascii_case(b"content-type") => Ok(http::header::CONTENT_TYPE),
        13 if name_bytes.eq_ignore_ascii_case(b"cache-control") => Ok(http::header::CACHE_CONTROL),
        14 if name_bytes.eq_ignore_ascii_case(b"content-length") => Ok(CONTENT_LENGTH),
        17 if name_bytes.eq_ignore_ascii_case(b"transfer-encoding") => {
            Ok(http::header::TRANSFER_ENCODING)
        }
        _ => {
            HeaderName::from_bytes(name_bytes).map_err(|e| Http1Error::InvalidHeader(e.to_string()))
        }
    }
}

/// HTTP/1.1 upstream response head metadata (status code, version, and headers).
#[derive(Debug, Clone)]
pub struct Http1ClientResponseHead {
    /// HTTP status code.
    pub status: StatusCode,
    /// HTTP protocol version.
    pub version: Version,
    /// Response headers.
    pub headers: HeaderMap,
}

impl Http1ClientResponseHead {
    /// Creates a new HTTP/1.1 client response head.
    #[inline]
    pub fn new(status: StatusCode, version: Version, headers: HeaderMap) -> Self {
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
    pub headers: HeaderMap,
    /// Response body.
    pub body: Body,
}

impl Http1ClientResponse {
    /// Creates a new HTTP/1.1 client response.
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
    pub fn from_parts(head: Http1ClientResponseHead, body: Body) -> Self {
        Self {
            status: head.status,
            version: head.version,
            headers: head.headers,
            body,
        }
    }

    /// Deconstructs response into head metadata and payload body.
    #[inline]
    pub fn into_parts(self) -> (Http1ClientResponseHead, Body) {
        (
            Http1ClientResponseHead {
                status: self.status,
                version: self.version,
                headers: self.headers,
            },
            self.body,
        )
    }

    /// Converts into canonical [`L7Response`].
    pub fn into_l7_response(self) -> L7Response {
        L7Response::new(self.status, self.version, self.headers, self.body)
    }

    /// Constructs from canonical [`L7Response`].
    pub fn from_l7_response(resp: L7Response) -> Self {
        Self {
            status: resp.status,
            version: resp.version,
            headers: resp.headers,
            body: resp.body,
        }
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

    let max_headers = config.max_headers.max(1);
    let mut stack_headers = [httparse::EMPTY_HEADER; 128];
    let mut heap_headers: Vec<httparse::Header>;
    let header_slice: &mut [httparse::Header] = if max_headers <= 128 {
        &mut stack_headers[..max_headers]
    } else {
        heap_headers = vec![httparse::EMPTY_HEADER; max_headers];
        heap_headers.as_mut_slice()
    };
    let mut resp = httparse::Response::new(header_slice);

    let status = match resp.parse(data) {
        Ok(s) => s,
        Err(httparse::Error::TooManyHeaders) => {
            return Err(Http1Error::TooManyHeaders(config.max_headers));
        }
        Err(e) => return Err(Http1Error::Parse(e.to_string())),
    };

    let header_len = match status {
        httparse::Status::Complete(len) => {
            if len > config.max_header_size {
                return Err(Http1Error::HeaderTooLarge(len));
            }
            len
        }
        httparse::Status::Partial => {
            if data.len() > config.max_header_size {
                return Err(Http1Error::HeaderTooLarge(data.len()));
            }
            return Ok(None);
        }
    };

    let status_code = resp
        .code
        .ok_or_else(|| Http1Error::Parse("Missing HTTP response status code".into()))?;
    let status = StatusCode::from_u16(status_code)
        .map_err(|e| Http1Error::Parse(format!("Invalid status code {status_code}: {e}")))?;

    let version = match resp.version {
        Some(1) => Version::HTTP_11,
        Some(0) => Version::HTTP_10,
        Some(v) => return Err(cold_parse_error(format!("Unsupported HTTP/1.{v}"))),
        None => Version::HTTP_11,
    };

    let mut header_map = HeaderMap::with_capacity(resp.headers.len());
    let mut content_length_count = 0;
    let mut content_length: Option<usize> = None;
    let mut is_chunked = false;
    let mut has_transfer_encoding = false;

    for h in resp.headers.iter() {
        if h.name.is_empty() {
            continue;
        }
        let name = match_upstream_header_name(h.name.as_bytes())?;
        let value = HeaderValue::from_bytes(h.value)
            .map_err(|e| Http1Error::InvalidHeader(e.to_string()))?;

        if name == CONTENT_LENGTH {
            content_length_count += 1;
            let parsed_cl = parse_ascii_digits(h.value)?;
            if let Some(prev) = content_length
                && prev != parsed_cl
            {
                return Err(cold_smuggling_error(
                    "Multiple conflicting Content-Length headers in response",
                ));
            }
            content_length = Some(parsed_cl);
        } else if name == http::header::TRANSFER_ENCODING {
            has_transfer_encoding = true;
            let val_str = std::str::from_utf8(h.value)
                .map_err(|_| cold_parse_error("Invalid non-UTF8 Transfer-Encoding"))?;
            if val_str
                .split(',')
                .next_back()
                .map(str::trim)
                .is_some_and(|c| c.eq_ignore_ascii_case("chunked"))
            {
                is_chunked = true;
            } else {
                return Err(cold_parse_error(format!(
                    "Unsupported Transfer-Encoding coding in response: {val_str}"
                )));
            }
        }

        header_map.append(name, value);
    }

    if content_length_count > 0 && has_transfer_encoding {
        return Err(cold_smuggling_error(
            "Simultaneous Content-Length and Transfer-Encoding headers in response",
        ));
    }

    let is_no_body = status.is_informational()
        || status == StatusCode::NO_CONTENT
        || status == StatusCode::NOT_MODIFIED;

    let framing = if is_no_body {
        Http1BodyFraming::Empty
    } else if is_chunked {
        Http1BodyFraming::Chunked
    } else if let Some(len) = content_length {
        if len == 0 {
            Http1BodyFraming::Empty
        } else {
            Http1BodyFraming::ContentLength(len)
        }
    } else {
        Http1BodyFraming::Empty
    };

    let head = Http1ClientResponseHead::new(status, version, header_map);
    Ok(Some((head, framing, header_len)))
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
                return Ok(Some(Http1ClientResponse::from_parts(head, Body::Empty)));
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
                    Body::Bytes(buf.split_to(body_len).freeze())
                } else {
                    Body::Empty
                };
                return Ok(Some(Http1ClientResponse::from_parts(head, body)));
            }
            Http1BodyFraming::Chunked => {
                let chunked_slice = &buf[header_len..];
                match parse_chunked_body(chunked_slice, config.max_body_size)? {
                    Some((consumed_wire, body)) => {
                        buf.advance(header_len + consumed_wire);
                        return Ok(Some(Http1ClientResponse::from_parts(head, body)));
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
        let n = stream.read_buf(read_buf).await?;
        if n == 0 {
            if !read_buf.is_empty() {
                let available = read_buf.len().min(needed);
                return Ok(Some(read_buf.split_to(available).freeze()));
            }
            return Ok(None);
        }
    }

    Ok(Some(read_buf.split_to(needed).freeze()))
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

        if read_buf.capacity() - read_buf.len() < 65536 {
            read_buf.reserve(65536);
        }
        let n = stream.read_buf(read_buf).await?;
        if n == 0 {
            return Err(Http1Error::ConnectionClosed);
        }
    }
}
