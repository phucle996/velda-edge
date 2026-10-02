//! Downstream HTTP/1.1 Stream Decoder & Wire Parser (RFC 9112).
//!
//! Architectural Perspective (Góc nhìn Ingress):
//! Handles downstream wire parsing and stateful buffer consumption (&mut BytesMut)
//! to decode client requests into domain [`Http1Request`].
//! Advances read buffers, extracts payloads, and enforces framing & smuggling security checks.

use bytes::{Buf, BytesMut};
use http::header::{CONTENT_LENGTH, HeaderName, HeaderValue};
use http::{HeaderMap, Method, Uri, Version};
use velda_core::Body;

use super::request::{Http1BodyFraming, Http1Request, Http1RequestHead};
use crate::config::Http1Config;
use crate::error::Http1Error;
pub use crate::wire::{
    ParsedChunk, find_crlf, parse_ascii_digits, parse_chunked_body, parse_hex_usize,
    parse_single_chunk,
};
use crate::wire::{cold_parse_error, cold_smuggling_error};

/// Fast length-partitioned header name matching for common HTTP/1.1 downstream request headers.
#[inline]
fn match_downstream_header_name(name_bytes: &[u8]) -> Result<HeaderName, Http1Error> {
    match name_bytes.len() {
        4 => {
            if name_bytes.eq_ignore_ascii_case(b"host") {
                Ok(http::header::HOST)
            } else if name_bytes.eq_ignore_ascii_case(b"date") {
                Ok(http::header::DATE)
            } else {
                HeaderName::from_bytes(name_bytes)
                    .map_err(|e| Http1Error::InvalidHeader(e.to_string()))
            }
        }
        6 => {
            if name_bytes.eq_ignore_ascii_case(b"accept") {
                Ok(http::header::ACCEPT)
            } else if name_bytes.eq_ignore_ascii_case(b"cookie") {
                Ok(http::header::COOKIE)
            } else if name_bytes.eq_ignore_ascii_case(b"origin") {
                Ok(http::header::ORIGIN)
            } else if name_bytes.eq_ignore_ascii_case(b"server") {
                Ok(http::header::SERVER)
            } else {
                HeaderName::from_bytes(name_bytes)
                    .map_err(|e| Http1Error::InvalidHeader(e.to_string()))
            }
        }
        7 => {
            if name_bytes.eq_ignore_ascii_case(b"upgrade") {
                Ok(http::header::UPGRADE)
            } else if name_bytes.eq_ignore_ascii_case(b"referer") {
                Ok(http::header::REFERER)
            } else {
                HeaderName::from_bytes(name_bytes)
                    .map_err(|e| Http1Error::InvalidHeader(e.to_string()))
            }
        }
        10 => {
            if name_bytes.eq_ignore_ascii_case(b"connection") {
                Ok(http::header::CONNECTION)
            } else if name_bytes.eq_ignore_ascii_case(b"user-agent") {
                Ok(http::header::USER_AGENT)
            } else if name_bytes.eq_ignore_ascii_case(b"set-cookie") {
                Ok(http::header::SET_COOKIE)
            } else {
                HeaderName::from_bytes(name_bytes)
                    .map_err(|e| Http1Error::InvalidHeader(e.to_string()))
            }
        }
        12 if name_bytes.eq_ignore_ascii_case(b"content-type") => Ok(http::header::CONTENT_TYPE),
        13 => {
            if name_bytes.eq_ignore_ascii_case(b"authorization") {
                Ok(http::header::AUTHORIZATION)
            } else if name_bytes.eq_ignore_ascii_case(b"cache-control") {
                Ok(http::header::CACHE_CONTROL)
            } else if name_bytes.eq_ignore_ascii_case(b"if-none-match") {
                Ok(http::header::IF_NONE_MATCH)
            } else {
                HeaderName::from_bytes(name_bytes)
                    .map_err(|e| Http1Error::InvalidHeader(e.to_string()))
            }
        }
        14 if name_bytes.eq_ignore_ascii_case(b"content-length") => Ok(CONTENT_LENGTH),
        15 => {
            if name_bytes.eq_ignore_ascii_case(b"accept-encoding") {
                Ok(http::header::ACCEPT_ENCODING)
            } else if name_bytes.eq_ignore_ascii_case(b"x-forwarded-for") {
                static X_FORWARDED_FOR: HeaderName = HeaderName::from_static("x-forwarded-for");
                Ok(X_FORWARDED_FOR.clone())
            } else {
                HeaderName::from_bytes(name_bytes)
                    .map_err(|e| Http1Error::InvalidHeader(e.to_string()))
            }
        }
        17 if name_bytes.eq_ignore_ascii_case(b"transfer-encoding") => {
            Ok(http::header::TRANSFER_ENCODING)
        }
        _ => {
            HeaderName::from_bytes(name_bytes).map_err(|e| Http1Error::InvalidHeader(e.to_string()))
        }
    }
}

/// Parses downstream HTTP/1.1 request head without mutating the input slice.
///
/// Returns `Ok(Some((head, framing, header_len)))` if the request head is complete.
pub fn parse_request_head(
    data: &[u8],
    config: &Http1Config,
) -> Result<Option<(Http1RequestHead, Http1BodyFraming, usize)>, Http1Error> {
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
        httparse::Status::Complete(len) => len,
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

    for h in req.headers.iter() {
        if h.name.is_empty() {
            continue;
        }
        let name = match_downstream_header_name(h.name.as_bytes())?;
        let value = HeaderValue::from_bytes(h.value)
            .map_err(|e| Http1Error::InvalidHeader(e.to_string()))?;

        if name == CONTENT_LENGTH {
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

    // RFC 9112 Section 6.1: Simultaneous CL and TE is a smuggling vector -> fast fail reject
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

    let head = Http1RequestHead::new(method, uri, version, header_map);
    Ok(Some((head, framing, header_len)))
}

/// Decodes HTTP/1.1 request head from downstream read buffer and advances `buf` past headers.
pub fn decode_request_head(
    buf: &mut BytesMut,
    config: &Http1Config,
) -> Result<Option<(Http1RequestHead, Http1BodyFraming)>, Http1Error> {
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
) -> Result<Option<Http1Request>, Http1Error> {
    if buf.is_empty() {
        return Ok(None);
    }
    let Some((head, framing, header_len)) = parse_request_head(buf.as_ref(), config)? else {
        return Ok(None);
    };

    match framing {
        Http1BodyFraming::Empty => {
            buf.advance(header_len);
            Ok(Some(Http1Request::from_parts(head, Body::Empty)))
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
            Ok(Some(Http1Request::from_parts(head, body)))
        }
        Http1BodyFraming::Chunked => {
            let chunked_slice = &buf[header_len..];
            match parse_chunked_body(chunked_slice, config.max_body_size)? {
                Some((consumed_wire, body)) => {
                    buf.advance(header_len + consumed_wire);
                    Ok(Some(Http1Request::from_parts(head, body)))
                }
                None => Ok(None),
            }
        }
    }
}
