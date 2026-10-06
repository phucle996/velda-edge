//! Upstream HTTP/1.1 Stream Decoder & Wire Parser (RFC 9112).
//!
//! Architectural Perspective (Góc nhìn Ingress):
//! Handles upstream wire parsing and stateful buffer consumption (&mut BytesMut)
//! to decode backend responses into domain [`Http1Response`].
//! Advances read buffers, extracts payloads, and enforces framing & smuggling security checks.

use bytes::{Buf, BytesMut};
use http::header::{CONTENT_LENGTH, HeaderName, HeaderValue};
use http::{HeaderMap, StatusCode, Version};
use velda_core::Body;

use super::response::{Http1Response, Http1ResponseHead};
use crate::config::Http1Config;
use crate::error::Http1Error;
use crate::server::request::Http1BodyFraming;
pub use crate::wire::{
    ParsedChunk, find_crlf, parse_ascii_digits, parse_chunked_body, parse_hex_usize,
    parse_single_chunk,
};
use crate::wire::{cold_parse_error, cold_smuggling_error};

/// Fast length-partitioned header name matching for common HTTP/1.1 upstream response headers.
#[inline]
fn match_upstream_header_name(name_bytes: &[u8]) -> Result<HeaderName, Http1Error> {
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

/// Parses upstream HTTP/1.1 response head without mutating the input slice.
///
/// Returns `Ok(Some((head, framing, header_len)))` if the response head is complete.
pub fn parse_response_head(
    data: &[u8],
    config: &Http1Config,
) -> Result<Option<(Http1ResponseHead, Http1BodyFraming, usize)>, Http1Error> {
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
    let mut res = httparse::Response::new(header_slice);

    let status = match res.parse(data) {
        Ok(s) => s,
        Err(httparse::Error::TooManyHeaders) => {
            return Err(Http1Error::TooManyHeaders(config.max_headers));
        }
        Err(e) => return Err(cold_parse_error(e.to_string())),
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

    let code = res
        .code
        .ok_or_else(|| cold_parse_error("Missing HTTP status code"))?;
    let status_code = match code {
        200 => StatusCode::OK,
        201 => StatusCode::CREATED,
        202 => StatusCode::ACCEPTED,
        204 => StatusCode::NO_CONTENT,
        301 => StatusCode::MOVED_PERMANENTLY,
        302 => StatusCode::FOUND,
        304 => StatusCode::NOT_MODIFIED,
        400 => StatusCode::BAD_REQUEST,
        401 => StatusCode::UNAUTHORIZED,
        403 => StatusCode::FORBIDDEN,
        404 => StatusCode::NOT_FOUND,
        405 => StatusCode::METHOD_NOT_ALLOWED,
        413 => StatusCode::PAYLOAD_TOO_LARGE,
        500 => StatusCode::INTERNAL_SERVER_ERROR,
        502 => StatusCode::BAD_GATEWAY,
        503 => StatusCode::SERVICE_UNAVAILABLE,
        504 => StatusCode::GATEWAY_TIMEOUT,
        c => StatusCode::from_u16(c).map_err(|e| cold_parse_error(e.to_string()))?,
    };

    let version = match res.version {
        Some(1) => Version::HTTP_11,
        Some(0) => Version::HTTP_10,
        Some(v) => return Err(cold_parse_error(format!("Unsupported HTTP/1.{v}"))),
        None => Version::HTTP_11,
    };

    let mut header_map = HeaderMap::with_capacity(res.headers.len());
    let mut content_length_count = 0;
    let mut content_length: Option<usize> = None;
    let mut is_chunked = false;
    let mut has_transfer_encoding = false;

    for h in res.headers.iter() {
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
            }
        }

        header_map.append(name, value);
    }

    if content_length_count > 0 && has_transfer_encoding {
        return Err(cold_smuggling_error(
            "Simultaneous Content-Length and Transfer-Encoding in response",
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

    let head = Http1ResponseHead::new(status_code, version, header_map);
    Ok(Some((head, framing, header_len)))
}

/// Decodes HTTP/1.1 response head from upstream read buffer and advances `buf` past headers.
pub fn decode_response_head(
    buf: &mut BytesMut,
    config: &Http1Config,
) -> Result<Option<(Http1ResponseHead, Http1BodyFraming)>, Http1Error> {
    if let Some((head, framing, header_len)) = parse_response_head(buf.as_ref(), config)? {
        buf.advance(header_len);
        Ok(Some((head, framing)))
    } else {
        Ok(None)
    }
}

/// Decodes an entire HTTP/1.1 response from the upstream read buffer.
pub fn decode_response(
    buf: &mut BytesMut,
    config: &Http1Config,
) -> Result<Option<Http1Response>, Http1Error> {
    if buf.is_empty() {
        return Ok(None);
    }
    let Some((head, framing, header_len)) = parse_response_head(buf.as_ref(), config)? else {
        return Ok(None);
    };

    match framing {
        Http1BodyFraming::Empty => {
            buf.advance(header_len);
            Ok(Some(Http1Response::from_parts(head, Body::Empty)))
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
            Ok(Some(Http1Response::from_parts(head, body)))
        }
        Http1BodyFraming::Chunked => {
            let chunked_slice = &buf[header_len..];
            match parse_chunked_body(chunked_slice, config.max_body_size)? {
                Some((consumed_wire, body)) => {
                    buf.advance(header_len + consumed_wire);
                    Ok(Some(Http1Response::from_parts(head, body)))
                }
                None => Ok(None),
            }
        }
    }
}
