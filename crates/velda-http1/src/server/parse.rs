//! Downstream HTTP/1.1 Wire Parser (RFC 9112).
//!
//! Architectural Perspective (Góc nhìn Ingress):
//! Handles pure, non-destructive parsing of incoming client HTTP requests from immutable &[u8] slices.
//! Performs security checks (request smuggling detection, header length limits).

use bytes::BytesMut;
use http::header::{CONTENT_LENGTH, HeaderName, HeaderValue};
use http::{HeaderMap, Method, Uri, Version};
use velda_core::{Body, IngressLimits};

use super::request::{Http1BodyFraming, Http1RequestHead};
use crate::error::Http1Error;

/// Fast SIMD-accelerated search for CRLF (`\r\n`) in byte slice.
#[inline]
pub fn find_crlf(data: &[u8]) -> Option<usize> {
    memchr::memmem::find(data, b"\r\n")
}

/// Parses hex chunk size directly from ASCII byte slice without UTF-8 or String allocations.
#[inline]
pub fn parse_hex_usize(bytes: &[u8]) -> Result<usize, Http1Error> {
    let mut val: usize = 0;
    let mut empty = true;
    for &b in bytes {
        let digit = match b {
            b'0'..=b'9' => (b - b'0') as usize,
            b'a'..=b'f' => (b - b'a' + 10) as usize,
            b'A'..=b'F' => (b - b'A' + 10) as usize,
            b' ' | b'\t' => continue,
            _ => {
                return Err(Http1Error::InvalidChunkedEncoding(
                    "Invalid hex chunk size character".into(),
                ));
            }
        };
        empty = false;
        val = val
            .checked_shl(4)
            .and_then(|v| v.checked_add(digit))
            .ok_or_else(|| {
                Http1Error::InvalidChunkedEncoding("Chunk size integer overflow".into())
            })?;
    }
    if empty {
        return Err(Http1Error::InvalidChunkedEncoding(
            "Empty chunk size line".into(),
        ));
    }
    Ok(val)
}

/// Parses ASCII decimal digits directly from byte slice without UTF-8 validation or allocations.
#[inline]
pub fn parse_ascii_digits(bytes: &[u8]) -> Result<usize, Http1Error> {
    let mut val: usize = 0;
    let mut empty = true;
    for &b in bytes {
        if b == b' ' || b == b'\t' {
            continue;
        }
        if !b.is_ascii_digit() {
            return Err(Http1Error::Parse(
                "Invalid non-digit in Content-Length".into(),
            ));
        }
        empty = false;
        val = val
            .checked_mul(10)
            .and_then(|v| v.checked_add((b - b'0') as usize))
            .ok_or_else(|| Http1Error::Parse("Content-Length integer overflow".into()))?;
    }
    if empty {
        return Err(Http1Error::Parse("Empty Content-Length header".into()));
    }
    Ok(val)
}

/// Parses a chunked HTTP body from a byte slice with memoized chunk offsets.
pub fn parse_chunked_body(
    data: &[u8],
    max_body_size: usize,
) -> Result<Option<(usize, Body)>, Http1Error> {
    let mut offset = 0;
    let mut total_body_len = 0;
    let mut memoized_chunks = [(0usize, 0usize); 16];
    let mut chunk_count = 0;
    let mut overflowed_chunks = false;

    // First pass: validate framing, check limits, and memoize chunk ranges on stack
    loop {
        let remaining = &data[offset..];
        let Some(crlf_pos) = find_crlf(remaining) else {
            return Ok(None);
        };
        let line = &remaining[..crlf_pos];
        let size_part = match memchr::memchr(b';', line) {
            Some(semi) => &line[..semi],
            None => line,
        };
        let chunk_size = parse_hex_usize(size_part)?;
        offset += crlf_pos + 2;

        if chunk_size == 0 {
            let trailer_data = &data[offset..];
            if trailer_data.starts_with(b"\r\n") {
                offset += 2;
                break;
            }
            if let Some(pos) = trailer_data.windows(4).position(|w| w == b"\r\n\r\n") {
                offset += pos + 4;
                break;
            }
            return Ok(None);
        }

        total_body_len += chunk_size;
        if total_body_len > max_body_size {
            return Err(Http1Error::PayloadTooLarge(total_body_len));
        }

        if data[offset..].len() < chunk_size + 2 {
            return Ok(None);
        }

        if &data[offset + chunk_size..offset + chunk_size + 2] != b"\r\n" {
            return Err(Http1Error::InvalidChunkedEncoding(
                "Missing CRLF after chunk data".into(),
            ));
        }

        if chunk_count < 16 {
            memoized_chunks[chunk_count] = (offset, chunk_size);
            chunk_count += 1;
        } else {
            overflowed_chunks = true;
        }

        offset += chunk_size + 2;
    }

    if total_body_len == 0 {
        return Ok(Some((offset, Body::Empty)));
    }

    let mut body_bytes = BytesMut::with_capacity(total_body_len);
    if !overflowed_chunks {
        for &(start, len) in &memoized_chunks[..chunk_count] {
            body_bytes.extend_from_slice(&data[start..start + len]);
        }
    } else {
        let mut read_offset = 0;
        loop {
            let remaining = &data[read_offset..];
            let crlf_pos = find_crlf(remaining).unwrap();
            let line = &remaining[..crlf_pos];
            let size_part = match memchr::memchr(b';', line) {
                Some(semi) => &line[..semi],
                None => line,
            };
            let chunk_size = parse_hex_usize(size_part).unwrap();
            read_offset += crlf_pos + 2;
            if chunk_size == 0 {
                break;
            }
            body_bytes.extend_from_slice(&data[read_offset..read_offset + chunk_size]);
            read_offset += chunk_size + 2;
        }
    }

    Ok(Some((offset, Body::Bytes(body_bytes.freeze()))))
}

/// Parsed single progressive chunk item: `(total_wire_len, chunk_payload, is_terminal)`.
pub type ParsedChunk<'a> = (usize, &'a [u8], bool);

/// Parses a single progressive chunk from an immutable byte slice.
///
/// Returns:
/// - `Ok(Some((total_wire_len, chunk_payload, is_terminal)))`:
///   A complete chunk was parsed. `total_wire_len` is the number of wire bytes
///   consumed (including chunk-size line, CRLF, payload, and trailing CRLF).
///   If `is_terminal` is true (chunk size == 0), this is the terminal chunk.
/// - `Ok(None)`: Not enough bytes in `data` to form a complete chunk.
/// - `Err(Http1Error)`: Invalid chunked framing or encoding error.
pub fn parse_single_chunk(data: &[u8]) -> Result<Option<ParsedChunk<'_>>, Http1Error> {
    if data.is_empty() {
        return Ok(None);
    }
    let Some(crlf_pos) = find_crlf(data) else {
        return Ok(None);
    };
    let line = &data[..crlf_pos];
    let size_part = match memchr::memchr(b';', line) {
        Some(semi) => &line[..semi],
        None => line,
    };
    let chunk_size = parse_hex_usize(size_part)?;
    let header_len = crlf_pos + 2;

    if chunk_size == 0 {
        let trailer_data = &data[header_len..];
        if trailer_data.starts_with(b"\r\n") {
            return Ok(Some((header_len + 2, &[], true)));
        }
        if let Some(pos) = trailer_data.windows(4).position(|w| w == b"\r\n\r\n") {
            return Ok(Some((header_len + pos + 4, &[], true)));
        }
        return Ok(None);
    }

    let needed = header_len + chunk_size + 2;
    if data.len() < needed {
        return Ok(None);
    }

    if &data[header_len + chunk_size..needed] != b"\r\n" {
        return Err(Http1Error::InvalidChunkedEncoding(
            "Missing CRLF after chunk data".into(),
        ));
    }

    let payload = &data[header_len..header_len + chunk_size];
    Ok(Some((needed, payload, false)))
}

/// Parses downstream HTTP/1.1 request head without mutating the input slice.
///
/// Returns `Ok(Some((head, framing, header_len)))` if the request head is complete.
pub fn parse_request_head(
    data: &[u8],
    limits: &IngressLimits,
) -> Result<Option<(Http1RequestHead, Http1BodyFraming, usize)>, Http1Error> {
    if data.is_empty() {
        return Ok(None);
    }

    let max_headers = limits.max_headers.max(1);
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
            return Err(Http1Error::TooManyHeaders(limits.max_headers));
        }
        Err(e) => return Err(Http1Error::Parse(e.to_string())),
    };

    let header_len = match status {
        httparse::Status::Complete(len) => len,
        httparse::Status::Partial => {
            if data.len() > limits.max_header_size {
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
        s => s
            .parse::<Uri>()
            .map_err(|e| Http1Error::InvalidUri(e.to_string()))?,
    };

    let version = match req.version {
        Some(1) => Version::HTTP_11,
        Some(0) => Version::HTTP_10,
        Some(v) => return Err(Http1Error::Parse(format!("Unsupported HTTP/1.{v}"))),
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
        let name = match h.name {
            s if s.eq_ignore_ascii_case("host") => http::header::HOST,
            s if s.eq_ignore_ascii_case("content-length") => CONTENT_LENGTH,
            s if s.eq_ignore_ascii_case("connection") => http::header::CONNECTION,
            s if s.eq_ignore_ascii_case("content-type") => http::header::CONTENT_TYPE,
            s if s.eq_ignore_ascii_case("accept") => http::header::ACCEPT,
            s if s.eq_ignore_ascii_case("user-agent") => http::header::USER_AGENT,
            s if s.eq_ignore_ascii_case("transfer-encoding") => http::header::TRANSFER_ENCODING,
            s if s.eq_ignore_ascii_case("authorization") => http::header::AUTHORIZATION,
            _ => HeaderName::from_bytes(h.name.as_bytes())
                .map_err(|e| Http1Error::InvalidHeader(e.to_string()))?,
        };
        let value = HeaderValue::from_bytes(h.value)
            .map_err(|e| Http1Error::InvalidHeader(e.to_string()))?;

        if name == CONTENT_LENGTH {
            content_length_count += 1;
            let parsed_cl = parse_ascii_digits(h.value)?;

            if let Some(prev) = content_length
                && prev != parsed_cl
            {
                return Err(Http1Error::SmugglingDetected(
                    "Multiple conflicting Content-Length headers".into(),
                ));
            }
            content_length = Some(parsed_cl);
        } else if name == http::header::TRANSFER_ENCODING {
            has_transfer_encoding = true;
            let val_str = std::str::from_utf8(h.value)
                .map_err(|_| Http1Error::Parse("Invalid non-UTF8 Transfer-Encoding".into()))?;
            if val_str
                .split(',')
                .next_back()
                .map(str::trim)
                .is_some_and(|c| c.eq_ignore_ascii_case("chunked"))
            {
                is_chunked = true;
            } else {
                return Err(Http1Error::Parse(format!(
                    "Unsupported Transfer-Encoding coding: {val_str}"
                )));
            }
        }

        header_map.append(name, value);
    }

    // RFC 9112 Section 6.1: Simultaneous CL and TE is a smuggling vector -> fast fail reject
    if content_length_count > 0 && has_transfer_encoding {
        return Err(Http1Error::SmugglingDetected(
            "Simultaneous Content-Length and Transfer-Encoding headers".into(),
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
