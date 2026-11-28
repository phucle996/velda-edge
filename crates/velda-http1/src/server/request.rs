//! Ingress Downstream HTTP/1.1 Request Entity and Wire Decoder (RFC 9112).
//!
//! Architectural Perspective (Góc nhìn Ingress):
//! The Gateway Server is the receiver and owner of the incoming downstream Request entity.
//! Handles downstream wire parsing and stateful buffer consumption (&mut BytesMut)
//! to decode client requests into domain [`Http1ServerRequest`].
//! Advances read buffers, extracts payloads, and enforces framing & smuggling security checks.

use bytes::{Buf, BytesMut};
use http::{Method, Uri, Version};
use velda_core::{Body, HeaderWorkspace};

use crate::config::Http1Config;
use crate::outcome::Http1Outcome;
use crate::wire::{cold_parse_error, parse_chunked_body};

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
    pub headers: HeaderWorkspace,
}

impl Http1ServerRequestHead {
    /// Creates a new HTTP/1.1 server request head.
    #[inline]
    pub fn new(method: Method, uri: Uri, version: Version, headers: HeaderWorkspace) -> Self {
        Self {
            method,
            uri,
            version,
            headers,
        }
    }

    /// Checks whether downstream specified `Expect: 100-continue` (RFC 9110 Section 10.1.1).
    #[inline]
    pub fn is_expect_100_continue(&self) -> bool {
        self.headers
            .get(http::header::EXPECT)
            .and_then(|val| val.to_str().ok())
            .is_some_and(|s| s.eq_ignore_ascii_case("100-continue"))
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
    pub headers: HeaderWorkspace,
    /// Request payload body.
    pub body: Body,
}

impl Http1ServerRequest {
    /// Creates a new HTTP/1.1 server request.
    #[inline]
    pub fn new(
        method: Method,
        uri: Uri,
        version: Version,
        headers: HeaderWorkspace,
        body: Body,
    ) -> Self {
        Self {
            method,
            uri,
            version,
            headers,
            body,
        }
    }
}

/// Parses downstream HTTP/1.1 request head without mutating the input slice.
pub fn parse_request_head(
    data: &[u8],
    config: &Http1Config,
    enrich: Option<crate::simd::H1IngressEnrich<'_>>,
) -> Result<Option<(Http1ServerRequestHead, Http1BodyFraming, usize)>, Http1Outcome> {
    if data.is_empty() {
        return Ok(None);
    }

    let Some((crlf_pos, headers_offset)) = crate::simd::find_crlf_line_end(data)? else {
        if data.len() > config.max_header_size {
            return Err(Http1Outcome::HeaderTooLarge(data.len()));
        }
        return Ok(None);
    };

    if crlf_pos > config.max_header_size {
        return Err(Http1Outcome::HeaderTooLarge(crlf_pos));
    }

    let line = &data[..crlf_pos];
    let Some(sp1) = line.iter().position(|&b| b == b' ') else {
        return Err(cold_parse_error("Invalid HTTP request line"));
    };
    let method_bytes = &line[..sp1];
    if method_bytes.is_empty() {
        return Err(cold_parse_error("Missing HTTP method"));
    }

    let rest = &line[sp1 + 1..];
    let Some(sp2_rel) = rest.iter().position(|&b| b == b' ') else {
        return Err(cold_parse_error("Invalid HTTP request line"));
    };
    let target_bytes = &rest[..sp2_rel];
    let version_bytes = &rest[sp2_rel + 1..];

    let method = match method_bytes {
        b"GET" => Method::GET,
        b"POST" => Method::POST,
        b"PUT" => Method::PUT,
        b"DELETE" => Method::DELETE,
        b"HEAD" => Method::HEAD,
        b"OPTIONS" => Method::OPTIONS,
        b"PATCH" => Method::PATCH,
        b"CONNECT" => Method::CONNECT,
        b"TRACE" => Method::TRACE,
        _ => Method::from_bytes(method_bytes)
            .map_err(|e| Http1Outcome::InvalidMethod(e.to_string()))?,
    };

    let path_str = crate::simd::validate_request_target_bytes(target_bytes)?;
    let uri = match path_str {
        "/" => Uri::from_static("/"),
        "/plain" => Uri::from_static("/plain"),
        "/tls12" => Uri::from_static("/tls12"),
        "/tls13" => Uri::from_static("/tls13"),
        "/h2c" => Uri::from_static("/h2c"),
        "/h2" => Uri::from_static("/h2"),
        "/h3" => Uri::from_static("/h3"),
        "/ping" => Uri::from_static("/ping"),
        "/health" => Uri::from_static("/health"),
        "/metrics" => Uri::from_static("/metrics"),
        "/upload" => Uri::from_static("/upload"),
        "/stream" => Uri::from_static("/stream"),
        "/api" => Uri::from_static("/api"),
        "/favicon.ico" => Uri::from_static("/favicon.ico"),
        s => s
            .parse::<Uri>()
            .map_err(|e| Http1Outcome::InvalidUri(e.to_string()))?,
    };

    let version = match version_bytes {
        b"HTTP/1.1" => Version::HTTP_11,
        b"HTTP/1.0" => Version::HTTP_10,
        _ => {
            if version_bytes.len() == 8
                && version_bytes.starts_with(b"HTTP/1.")
                && version_bytes[7].is_ascii_digit()
            {
                let minor = version_bytes[7] - b'0';
                return Err(cold_parse_error(format!("Unsupported HTTP/1.{minor}")));
            }
            return Err(cold_parse_error("Invalid HTTP version in request line"));
        }
    };

    let Some(mut parsed) =
        crate::simd::parse_headers_into_workspace(data, headers_offset, config, false, enrich)?
    else {
        return Ok(None);
    };

    if version == Version::HTTP_11 && parsed.host_count == 0 {
        return Err(cold_parse_error("Missing Host header in HTTP/1.1 request"));
    }

    if enrich.is_some()
        && let Some(hv) = parsed
            .host_header
            .take()
            .or_else(|| uri.host().and_then(|h| http::HeaderValue::from_str(h).ok()))
    {
        parsed.workspace.push_prehashed(
            crate::simd::KEY_X_FORWARDED_HOST,
            crate::simd::X_FORWARDED_HOST,
            hv,
        );
    }

    let framing = if parsed.is_chunked {
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

    let head = Http1ServerRequestHead::new(method, uri, version, parsed.workspace);
    Ok(Some((head, framing, parsed.total_head_len)))
}

/// Decodes HTTP/1.1 request head from downstream read buffer and advances `buf` past headers.
pub fn decode_request_head(
    buf: &mut BytesMut,
    config: &Http1Config,
    enrich: Option<crate::simd::H1IngressEnrich<'_>>,
) -> Result<Option<(Http1ServerRequestHead, Http1BodyFraming)>, Http1Outcome> {
    if let Some((head, framing, header_len)) = parse_request_head(buf.as_ref(), config, enrich)? {
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
) -> Result<Option<Body>, Http1Outcome> {
    match framing {
        Http1BodyFraming::Empty => Ok(Some(Body::Empty)),
        Http1BodyFraming::ContentLength(body_len) => {
            if body_len > config.max_body_size {
                return Err(Http1Outcome::PayloadTooLarge(body_len));
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
            if buf.len() > config.max_body_size.saturating_add(4096) {
                return Err(Http1Outcome::PayloadTooLarge(buf.len()));
            }
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
) -> Result<Option<Http1ServerRequest>, Http1Outcome> {
    if buf.is_empty() {
        return Ok(None);
    }
    let Some((head, framing, header_len)) = parse_request_head(buf.as_ref(), config, None)? else {
        return Ok(None);
    };

    match framing {
        Http1BodyFraming::Empty => {
            buf.advance(header_len);
            Ok(Some(Http1ServerRequest {
                method: head.method,
                uri: head.uri,
                version: head.version,
                headers: head.headers,
                body: Body::Empty,
            }))
        }
        Http1BodyFraming::ContentLength(body_len) => {
            if body_len > config.max_body_size {
                return Err(Http1Outcome::PayloadTooLarge(body_len));
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
            Ok(Some(Http1ServerRequest {
                method: head.method,
                uri: head.uri,
                version: head.version,
                headers: head.headers,
                body,
            }))
        }
        Http1BodyFraming::Chunked => {
            let chunked_slice = &buf[header_len..];
            if chunked_slice.len() > config.max_body_size.saturating_add(4096) {
                return Err(Http1Outcome::PayloadTooLarge(chunked_slice.len()));
            }
            match parse_chunked_body(chunked_slice, config.max_body_size)? {
                Some((consumed_wire, body)) => {
                    buf.advance(header_len + consumed_wire);
                    Ok(Some(Http1ServerRequest {
                        method: head.method,
                        uri: head.uri,
                        version: head.version,
                        headers: head.headers,
                        body,
                    }))
                }
                None => Ok(None),
            }
        }
    }
}
