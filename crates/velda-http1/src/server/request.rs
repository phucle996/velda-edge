//! Ingress Downstream HTTP/1.1 Request Entity and Wire Decoder (RFC 9112).
//!
//! Architectural Perspective (Góc nhìn Ingress):
//! The Gateway Server is the receiver and owner of the incoming downstream Request entity.
//! Handles downstream wire parsing and stateful buffer consumption (&mut BytesMut)
//! to decode client requests into domain [`Http1ServerRequest`].
//! Advances read buffers, extracts payloads, and enforces framing & smuggling security checks.

use bytes::{Buf, BytesMut};
use http::header::{CONTENT_LENGTH, HeaderValue};
use http::{HeaderMap, Method, Uri, Version};
use velda_core::{Body, L7Request};

use crate::config::Http1Config;
use crate::error::Http1Error;
pub use crate::wire::{
    ParsedChunk, find_crlf, parse_ascii_digits, parse_chunked_body, parse_hex_usize,
    parse_single_chunk,
};
use crate::wire::{cold_parse_error, cold_smuggling_error};

/// Message body framing mechanism for HTTP/1.1 request payloads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Http1BodyFraming {
    /// No body present in message.
    Empty,
    /// Fixed-length body defined by `Content-Length`.
    ContentLength(usize),
    /// Variable chunked body defined by `Transfer-Encoding: chunked`.
    Chunked,
}

/// Downstream HTTP/1.1 request head metadata (method, URI, version, and headers).
#[derive(Debug, Clone)]
pub struct Http1ServerRequestHead {
    /// HTTP method.
    pub method: Method,
    /// Request URI.
    pub uri: Uri,
    /// HTTP protocol version.
    pub version: Version,
    /// Request headers.
    pub headers: HeaderMap,
}

impl Http1ServerRequestHead {
    /// Creates a new HTTP/1.1 server request head.
    #[inline]
    pub fn new(method: Method, uri: Uri, version: Version, headers: HeaderMap) -> Self {
        Self {
            method,
            uri,
            version,
            headers,
        }
    }

    /// Fast-path lookup for the `Host` header.
    #[inline]
    pub fn host(&self) -> Option<&HeaderValue> {
        self.headers.get(http::header::HOST)
    }

    /// Fast-path lookup for the `Host` header as string.
    #[inline]
    pub fn host_str(&self) -> Option<&str> {
        self.headers
            .get(http::header::HOST)
            .and_then(|v| v.to_str().ok())
            .or_else(|| self.uri.host())
    }

    /// Fast-path lookup for request path.
    #[inline]
    pub fn path(&self) -> &str {
        self.uri.path()
    }

    /// Checks whether downstream specified `Expect: 100-continue` (RFC 9110 Section 10.1.1).
    #[inline]
    pub fn is_expect_100_continue(&self) -> bool {
        self.headers
            .get(http::header::EXPECT)
            .and_then(|val| val.to_str().ok())
            .is_some_and(|s| s.eq_ignore_ascii_case("100-continue"))
    }

    /// Normalizes and secures the HTTP/1.1 request URI path in-place (RFC 3986 & RFC 9112).
    #[inline]
    pub fn normalize_path(&mut self) -> Result<(), Http1Error> {
        super::path::normalize_path(&mut self.uri)
    }

    /// Enriches HTTP/1.1 request headers with RFC 7239 and standard proxy forwarding metadata.
    #[inline]
    pub fn enrich_forwarded_headers(
        &mut self,
        peer: std::net::SocketAddr,
        local_addr: std::net::SocketAddr,
        is_tls: bool,
    ) {
        super::header::enrich_headers(&mut self.headers, &self.uri, peer, local_addr, is_tls);
    }
}

/// Downstream protocol-owned HTTP/1.1 request.
#[derive(Debug, Clone)]
pub struct Http1ServerRequest {
    /// HTTP method.
    pub method: Method,
    /// Request URI.
    pub uri: Uri,
    /// HTTP protocol version.
    pub version: Version,
    /// Request headers.
    pub headers: HeaderMap,
    /// Request payload body.
    pub body: Body,
}

impl Http1ServerRequest {
    /// Creates a new HTTP/1.1 server request.
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

    /// Creates an HTTP/1.1 request from head and body components.
    #[inline]
    pub fn from_parts(head: Http1ServerRequestHead, body: Body) -> Self {
        Self {
            method: head.method,
            uri: head.uri,
            version: head.version,
            headers: head.headers,
            body,
        }
    }

    /// Deconstructs the request into head metadata and payload body.
    #[inline]
    pub fn into_parts(self) -> (Http1ServerRequestHead, Body) {
        (
            Http1ServerRequestHead {
                method: self.method,
                uri: self.uri,
                version: self.version,
                headers: self.headers,
            },
            self.body,
        )
    }

    /// Extracts request head metadata without consuming the payload body.
    #[inline]
    pub fn head(&self) -> Http1ServerRequestHead {
        Http1ServerRequestHead {
            method: self.method.clone(),
            uri: self.uri.clone(),
            version: self.version,
            headers: self.headers.clone(),
        }
    }

    /// Converts this protocol-owned request into canonical [`L7Request`].
    pub fn into_l7_request(self) -> L7Request {
        L7Request::new(self.method, self.uri, self.version, self.headers, self.body)
    }

    /// Constructs an [`Http1ServerRequest`] from a canonical [`L7Request`].
    pub fn from_l7_request(req: L7Request) -> Self {
        Self {
            method: req.method,
            uri: req.uri,
            version: req.version,
            headers: req.headers,
            body: req.body,
        }
    }

    /// Decodes an entire HTTP/1.1 request from the downstream read buffer.
    pub fn decode(buf: &mut BytesMut, config: &Http1Config) -> Result<Option<Self>, Http1Error> {
        decode_request(buf, config)
    }
}

/// Parses downstream HTTP/1.1 request head without mutating the input slice.
pub fn parse_request_head(
    data: &[u8],
    config: &Http1Config,
) -> Result<Option<(Http1ServerRequestHead, Http1BodyFraming, usize)>, Http1Error> {
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
    let mut req = httparse::Request::new(header_slice);

    let status = match req.parse(data) {
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

    let method_str = req
        .method
        .ok_or_else(|| Http1Error::Parse("Missing HTTP method".into()))?;
    let method = match method_str {
        "GET" => Method::GET,
        "POST" => Method::POST,
        "PUT" => Method::PUT,
        "DELETE" => Method::DELETE,
        "HEAD" => Method::HEAD,
        "OPTIONS" => Method::OPTIONS,
        "PATCH" => Method::PATCH,
        "CONNECT" => Method::CONNECT,
        "TRACE" => Method::TRACE,
        _ => Method::from_bytes(method_str.as_bytes())
            .map_err(|e| Http1Error::InvalidMethod(e.to_string()))?,
    };

    let path_str = req
        .path
        .ok_or_else(|| Http1Error::Parse("Missing HTTP path/URI".into()))?;
    let uri = match path_str {
        "/" => Uri::from_static("/"),
        "/health" => Uri::from_static("/health"),
        "/metrics" => Uri::from_static("/metrics"),
        "/ping" => Uri::from_static("/ping"),
        "/api" => Uri::from_static("/api"),
        "/favicon.ico" => Uri::from_static("/favicon.ico"),
        s => s
            .parse::<Uri>()
            .map_err(|e| Http1Error::InvalidUri(e.to_string()))?,
    };

    let version = match req.version {
        Some(1) => Version::HTTP_11,
        Some(0) => Version::HTTP_10,
        Some(v) => return Err(cold_parse_error(format!("Unsupported HTTP/1.{v}"))),
        None => Version::HTTP_11,
    };

    let mut header_map = HeaderMap::with_capacity(req.headers.len());
    let mut content_length_count = 0;
    let mut content_length: Option<usize> = None;
    let mut is_chunked = false;
    let mut has_transfer_encoding = false;
    let mut host_count = 0;

    for h in req.headers.iter() {
        if h.name.is_empty() {
            continue;
        }
        let name = super::header::match_header_name(h.name.as_bytes())?;
        let value = HeaderValue::from_bytes(h.value)
            .map_err(|e| Http1Error::InvalidHeader(e.to_string()))?;

        if name == http::header::HOST {
            host_count += 1;
            if host_count > 1 {
                return Err(cold_smuggling_error("Multiple Host headers in request"));
            }
        } else if name == CONTENT_LENGTH {
            content_length_count += 1;
            let parsed_cl = parse_ascii_digits(h.value)?;

            if let Some(prev) = content_length
                && prev != parsed_cl
            {
                return Err(cold_smuggling_error(
                    "Multiple conflicting Content-Length headers",
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
                    "Unsupported Transfer-Encoding coding: {val_str}"
                )));
            }
        }

        header_map.append(name, value);
    }

    if version == Version::HTTP_11 && host_count == 0 {
        return Err(cold_parse_error("Missing Host header in HTTP/1.1 request"));
    }

    if content_length_count > 0 && has_transfer_encoding {
        return Err(cold_smuggling_error(
            "Simultaneous Content-Length and Transfer-Encoding headers",
        ));
    }

    let framing = if is_chunked {
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

    let head = Http1ServerRequestHead::new(method, uri, version, header_map);
    Ok(Some((head, framing, header_len)))
}

/// Decodes HTTP/1.1 request head from downstream read buffer and advances `buf` past headers.
pub fn decode_request_head(
    buf: &mut BytesMut,
    config: &Http1Config,
) -> Result<Option<(Http1ServerRequestHead, Http1BodyFraming)>, Http1Error> {
    if let Some((head, framing, header_len)) = parse_request_head(buf.as_ref(), config)? {
        buf.advance(header_len);
        Ok(Some((head, framing)))
    } else {
        Ok(None)
    }
}

/// Decodes a message body from downstream buffer according to the given framing.
pub fn decode_body(
    buf: &mut BytesMut,
    framing: Http1BodyFraming,
    config: &Http1Config,
) -> Result<Option<Body>, Http1Error> {
    match framing {
        Http1BodyFraming::Empty => Ok(Some(Body::Empty)),
        Http1BodyFraming::ContentLength(body_len) => {
            if body_len > config.max_body_size {
                return Err(Http1Error::PayloadTooLarge(body_len));
            }
            if buf.len() < body_len {
                return Ok(None);
            }
            let body = if body_len > 0 {
                Body::Bytes(buf.split_to(body_len).freeze())
            } else {
                Body::Empty
            };
            Ok(Some(body))
        }
        Http1BodyFraming::Chunked => {
            match parse_chunked_body(buf.as_ref(), config.max_body_size)? {
                Some((consumed_wire, body)) => {
                    buf.advance(consumed_wire);
                    Ok(Some(body))
                }
                None => Ok(None),
            }
        }
    }
}

/// Decodes an entire HTTP/1.1 request from the downstream read buffer.
pub fn decode_request(
    buf: &mut BytesMut,
    config: &Http1Config,
) -> Result<Option<Http1ServerRequest>, Http1Error> {
    if buf.is_empty() {
        return Ok(None);
    }
    let Some((head, framing, header_len)) = parse_request_head(buf.as_ref(), config)? else {
        return Ok(None);
    };

    match framing {
        Http1BodyFraming::Empty => {
            buf.advance(header_len);
            Ok(Some(Http1ServerRequest::from_parts(head, Body::Empty)))
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
            Ok(Some(Http1ServerRequest::from_parts(head, body)))
        }
        Http1BodyFraming::Chunked => {
            let chunked_slice = &buf[header_len..];
            match parse_chunked_body(chunked_slice, config.max_body_size)? {
                Some((consumed_wire, body)) => {
                    buf.advance(header_len + consumed_wire);
                    Ok(Some(Http1ServerRequest::from_parts(head, body)))
                }
                None => Ok(None),
            }
        }
    }
}
