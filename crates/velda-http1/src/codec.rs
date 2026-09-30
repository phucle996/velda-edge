//! HTTP/1.1 zero-copy request parser and response serializer.

use bytes::{Buf, BufMut, BytesMut};
use http::header::{CONTENT_LENGTH, HeaderName, HeaderValue};
use http::{HeaderMap, Method, Uri, Version};
use velda_core::{Body, L7Request, L7Response};

use crate::error::Http1Error;

/// Maximum number of headers supported per request.
pub const MAX_HEADERS: usize = 64;

/// Maximum allowed buffer capacity for HTTP headers (64 KB).
pub const MAX_HEADER_SIZE: usize = 64 * 1024;

/// Maximum allowed body payload buffer size (10 MB).
pub const MAX_BODY_BUFFER_SIZE: usize = 10 * 1024 * 1024;

/// Decodes a chunked HTTP body from a byte slice.
///
/// Returns:
/// - `Ok(Some((consumed_wire_bytes, body)))` if a complete chunked payload was decoded.
/// - `Ok(None)` if the chunked payload is incomplete (requires more bytes from the stream).
/// - `Err(Http1Error)` if chunk framing is corrupted, malformed, or exceeds `max_body_size`.
pub fn decode_chunked_body(
    data: &[u8],
    max_body_size: usize,
) -> Result<Option<(usize, Body)>, Http1Error> {
    let mut offset = 0;
    let mut total_body_len = 0;

    // First pass: validate framing, compute sizes, verify completeness (zero heap allocations)
    loop {
        let remaining = &data[offset..];
        let Some(crlf_pos) = remaining.windows(2).position(|w| w == b"\r\n") else {
            return Ok(None);
        };
        let line = &remaining[..crlf_pos];
        let size_part = match line.iter().position(|&b| b == b';') {
            Some(semi) => &line[..semi],
            None => line,
        };
        let size_str = match std::str::from_utf8(size_part) {
            Ok(s) => s.trim(),
            Err(_) => {
                return Err(Http1Error::InvalidChunkedEncoding(
                    "Non-UTF8 chunk size".into(),
                ));
            }
        };
        if size_str.is_empty() {
            return Err(Http1Error::InvalidChunkedEncoding(
                "Empty chunk size line".into(),
            ));
        }
        let chunk_size = usize::from_str_radix(size_str, 16).map_err(|e| {
            Http1Error::InvalidChunkedEncoding(format!("Invalid hex chunk size: {e}"))
        })?;

        offset += crlf_pos + 2;

        if chunk_size == 0 {
            // Terminal chunk. Check for trailer section ending with CRLF
            let trailer_data = &data[offset..];
            if trailer_data.starts_with(b"\r\n") {
                offset += 2;
                break;
            }
            if let Some(trailer_end) = trailer_data.windows(4).position(|w| w == b"\r\n\r\n") {
                offset += trailer_end + 4;
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

        offset += chunk_size + 2;
    }

    if total_body_len == 0 {
        return Ok(Some((offset, Body::Empty)));
    }

    // Second pass: extract body bytes
    let mut body_bytes = BytesMut::with_capacity(total_body_len);
    let mut read_offset = 0;
    loop {
        let remaining = &data[read_offset..];
        let crlf_pos = remaining.windows(2).position(|w| w == b"\r\n").unwrap();
        let line = &remaining[..crlf_pos];
        let size_part = match line.iter().position(|&b| b == b';') {
            Some(semi) => &line[..semi],
            None => line,
        };
        let size_str = std::str::from_utf8(size_part).unwrap().trim();
        let chunk_size = usize::from_str_radix(size_str, 16).unwrap();

        read_offset += crlf_pos + 2;

        if chunk_size == 0 {
            break;
        }

        body_bytes.put_slice(&data[read_offset..read_offset + chunk_size]);
        read_offset += chunk_size + 2;
    }

    Ok(Some((offset, Body::Bytes(body_bytes.freeze()))))
}

/// Decodes an HTTP/1.1 request from the read buffer.
pub fn decode_request(buf: &mut BytesMut) -> Result<Option<L7Request>, Http1Error> {
    if buf.is_empty() {
        return Ok(None);
    }

    let mut headers = [httparse::EMPTY_HEADER; MAX_HEADERS];
    let mut req = httparse::Request::new(&mut headers);

    let status = match req.parse(buf.as_ref()) {
        Ok(s) => s,
        Err(e) => return Err(Http1Error::Parse(e.to_string())),
    };

    let header_len = match status {
        httparse::Status::Complete(len) => len,
        httparse::Status::Partial => {
            if buf.len() > MAX_HEADER_SIZE {
                return Err(Http1Error::HeaderTooLarge(buf.len()));
            }
            return Ok(None);
        }
    };

    let method_str = req
        .method
        .ok_or_else(|| Http1Error::Parse("Missing HTTP method".into()))?;
    let method = Method::from_bytes(method_str.as_bytes())
        .map_err(|e| Http1Error::InvalidMethod(e.to_string()))?;

    let path_str = req
        .path
        .ok_or_else(|| Http1Error::Parse("Missing HTTP path/URI".into()))?;
    let uri = if path_str == "/" {
        Uri::from_static("/")
    } else {
        path_str
            .parse::<Uri>()
            .map_err(|e| Http1Error::InvalidUri(e.to_string()))?
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
        let name = HeaderName::from_bytes(h.name.as_bytes())
            .map_err(|e| Http1Error::InvalidHeader(e.to_string()))?;
        let value = HeaderValue::from_bytes(h.value)
            .map_err(|e| Http1Error::InvalidHeader(e.to_string()))?;

        if name == CONTENT_LENGTH {
            content_length_count += 1;
            let val_str = std::str::from_utf8(h.value)
                .map_err(|_| Http1Error::Parse("Invalid non-UTF8 Content-Length".into()))?;
            let parsed_cl = val_str
                .trim()
                .parse::<usize>()
                .map_err(|_| Http1Error::Parse("Invalid numeric Content-Length".into()))?;

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
            let codings: Vec<&str> = val_str.split(',').map(|s| s.trim()).collect();
            if codings
                .last()
                .is_some_and(|&c| c.eq_ignore_ascii_case("chunked"))
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

    if is_chunked {
        let chunked_slice = &buf[header_len..];
        match decode_chunked_body(chunked_slice, MAX_BODY_BUFFER_SIZE)? {
            Some((consumed_wire, body)) => {
                buf.advance(header_len + consumed_wire);
                return Ok(Some(L7Request::new(method, uri, version, header_map, body)));
            }
            None => return Ok(None),
        }
    }

    let body_len = content_length.unwrap_or(0);
    if body_len > MAX_BODY_BUFFER_SIZE {
        return Err(Http1Error::PayloadTooLarge(body_len));
    }

    let total_len = header_len + body_len;
    if buf.len() < total_len {
        return Ok(None);
    }

    buf.advance(header_len);

    let body = if body_len > 0 {
        let body_bytes = buf.split_to(body_len).freeze();
        Body::Bytes(body_bytes)
    } else {
        Body::Empty
    };

    Ok(Some(L7Request::new(method, uri, version, header_map, body)))
}

/// Encodes an HTTP/1.1 response into the destination buffer.
pub fn encode_response(res: &L7Response, dst: &mut BytesMut) {
    let status = res.status;
    let reason = status.canonical_reason().unwrap_or("Unknown");

    // Pre-reserve capacity to avoid micro-reallocations on new buffers
    dst.reserve(64 + res.headers.len() * 32 + res.body.len());

    match res.version {
        Version::HTTP_10 => dst.put_slice(b"HTTP/1.0 "),
        _ => dst.put_slice(b"HTTP/1.1 "),
    }
    dst.put_slice(status.as_str().as_bytes());
    dst.put_slice(b" ");
    dst.put_slice(reason.as_bytes());
    dst.put_slice(b"\r\n");

    let mut has_content_length = false;
    let mut has_transfer_encoding = false;
    for (name, val) in &res.headers {
        if name == CONTENT_LENGTH {
            has_content_length = true;
        }
        if name == http::header::TRANSFER_ENCODING {
            has_transfer_encoding = true;
        }
        dst.put_slice(name.as_str().as_bytes());
        dst.put_slice(b": ");
        dst.put_slice(val.as_bytes());
        dst.put_slice(b"\r\n");
    }

    if !has_content_length && !has_transfer_encoding {
        let body_len = res.body.len();
        dst.put_slice(b"content-length: ");
        let mut itoa_buf = itoa::Buffer::new();
        dst.put_slice(itoa_buf.format(body_len).as_bytes());
        dst.put_slice(b"\r\n");
    }

    dst.put_slice(b"\r\n");

    if let Body::Bytes(ref bytes) = res.body {
        dst.put_slice(bytes);
    }
}

/// Encodes an HTTP/1.1 request into the destination buffer.
pub fn encode_request(req: &L7Request, dst: &mut BytesMut) {
    // Pre-reserve capacity to avoid micro-reallocations on new buffers
    dst.reserve(64 + req.headers.len() * 32 + req.body.len());

    dst.put_slice(req.method.as_str().as_bytes());
    dst.put_slice(b" ");
    let path_and_query = req
        .uri
        .path_and_query()
        .map(|pq| pq.as_str())
        .unwrap_or_else(|| req.path());
    dst.put_slice(path_and_query.as_bytes());
    dst.put_slice(b" HTTP/1.1\r\n");

    let mut has_content_length = false;
    let mut has_host = false;
    for (name, val) in &req.headers {
        if name == CONTENT_LENGTH {
            has_content_length = true;
        }
        if name == http::header::HOST {
            has_host = true;
        }
        dst.put_slice(name.as_str().as_bytes());
        dst.put_slice(b": ");
        dst.put_slice(val.as_bytes());
        dst.put_slice(b"\r\n");
    }

    if !has_host && let Some(host) = req.uri.host() {
        dst.put_slice(b"host: ");
        dst.put_slice(host.as_bytes());
        if let Some(port) = req.uri.port_u16() {
            dst.put_slice(b":");
            let mut itoa_buf = itoa::Buffer::new();
            dst.put_slice(itoa_buf.format(port).as_bytes());
        }
        dst.put_slice(b"\r\n");
    }

    if !has_content_length && req.has_body() {
        let body_len = req.body.len();
        dst.put_slice(b"content-length: ");
        let mut itoa_buf = itoa::Buffer::new();
        dst.put_slice(itoa_buf.format(body_len).as_bytes());
        dst.put_slice(b"\r\n");
    }

    dst.put_slice(b"\r\n");

    if let Body::Bytes(ref bytes) = req.body {
        dst.put_slice(bytes);
    }
}

/// Decodes an HTTP/1.1 response from the read buffer.
pub fn decode_response(buf: &mut BytesMut) -> Result<Option<L7Response>, Http1Error> {
    if buf.is_empty() {
        return Ok(None);
    }

    let mut headers = [httparse::EMPTY_HEADER; MAX_HEADERS];
    let mut res = httparse::Response::new(&mut headers);

    let status = match res.parse(buf.as_ref()) {
        Ok(s) => s,
        Err(e) => return Err(Http1Error::Parse(e.to_string())),
    };

    let header_len = match status {
        httparse::Status::Complete(len) => len,
        httparse::Status::Partial => {
            if buf.len() > MAX_HEADER_SIZE {
                return Err(Http1Error::HeaderTooLarge(buf.len()));
            }
            return Ok(None);
        }
    };

    let code = res
        .code
        .ok_or_else(|| Http1Error::Parse("Missing HTTP status code".into()))?;
    let status_code =
        http::StatusCode::from_u16(code).map_err(|e| Http1Error::Parse(e.to_string()))?;

    let version = match res.version {
        Some(1) => Version::HTTP_11,
        Some(0) => Version::HTTP_10,
        Some(v) => return Err(Http1Error::Parse(format!("Unsupported HTTP/1.{v}"))),
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
        let name = HeaderName::from_bytes(h.name.as_bytes())
            .map_err(|e| Http1Error::InvalidHeader(e.to_string()))?;
        let value = HeaderValue::from_bytes(h.value)
            .map_err(|e| Http1Error::InvalidHeader(e.to_string()))?;

        if name == CONTENT_LENGTH {
            content_length_count += 1;
            let val_str = std::str::from_utf8(h.value)
                .map_err(|_| Http1Error::Parse("Invalid non-UTF8 Content-Length".into()))?;
            let parsed_cl = val_str
                .trim()
                .parse::<usize>()
                .map_err(|_| Http1Error::Parse("Invalid numeric Content-Length".into()))?;

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
            let codings: Vec<&str> = val_str.split(',').map(|s| s.trim()).collect();
            if codings
                .last()
                .is_some_and(|&c| c.eq_ignore_ascii_case("chunked"))
            {
                is_chunked = true;
            }
        }

        header_map.append(name, value);
    }

    if content_length_count > 0 && has_transfer_encoding {
        return Err(Http1Error::SmugglingDetected(
            "Simultaneous Content-Length and Transfer-Encoding in response".into(),
        ));
    }

    if is_chunked {
        let chunked_slice = &buf[header_len..];
        match decode_chunked_body(chunked_slice, MAX_BODY_BUFFER_SIZE)? {
            Some((consumed_wire, body)) => {
                buf.advance(header_len + consumed_wire);
                return Ok(Some(L7Response::new(
                    status_code,
                    version,
                    header_map,
                    body,
                )));
            }
            None => return Ok(None),
        }
    }

    let body_len = content_length.unwrap_or(0);
    if body_len > MAX_BODY_BUFFER_SIZE {
        return Err(Http1Error::PayloadTooLarge(body_len));
    }

    let total_len = header_len + body_len;
    if buf.len() < total_len {
        return Ok(None);
    }

    buf.advance(header_len);

    let body = if body_len > 0 {
        let body_bytes = buf.split_to(body_len).freeze();
        Body::Bytes(body_bytes)
    } else {
        Body::Empty
    };

    Ok(Some(L7Response::new(
        status_code,
        version,
        header_map,
        body,
    )))
}
