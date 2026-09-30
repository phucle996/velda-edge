//! HTTP/1.1 zero-copy request parser and response serializer.

use bytes::{Buf, BufMut, BytesMut};
use http::header::{CONTENT_LENGTH, HeaderName, HeaderValue};
use http::{HeaderMap, Method, Uri, Version};
use velda_core::{Body, L7Request, L7Response};

use crate::error::Http1Error;

/// Maximum number of headers supported per request.
const MAX_HEADERS: usize = 64;

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
        httparse::Status::Partial => return Ok(None),
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
    let mut content_length: Option<usize> = None;

    for h in req.headers.iter() {
        if h.name.is_empty() {
            continue;
        }
        let name = HeaderName::from_bytes(h.name.as_bytes())
            .map_err(|e| Http1Error::InvalidHeader(e.to_string()))?;
        let value = HeaderValue::from_bytes(h.value)
            .map_err(|e| Http1Error::InvalidHeader(e.to_string()))?;

        if name == CONTENT_LENGTH {
            content_length = std::str::from_utf8(h.value)
                .ok()
                .and_then(|s| s.trim().parse::<usize>().ok());
        }

        header_map.append(name, value);
    }

    let body_len = content_length.unwrap_or(0);
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

    dst.put_slice(b"HTTP/1.1 ");
    dst.put_slice(status.as_str().as_bytes());
    dst.put_slice(b" ");
    dst.put_slice(reason.as_bytes());
    dst.put_slice(b"\r\n");

    let mut has_content_length = false;
    for (name, val) in &res.headers {
        if name == CONTENT_LENGTH {
            has_content_length = true;
        }
        dst.put_slice(name.as_str().as_bytes());
        dst.put_slice(b": ");
        dst.put_slice(val.as_bytes());
        dst.put_slice(b"\r\n");
    }

    if !has_content_length {
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
        httparse::Status::Partial => return Ok(None),
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
    let mut content_length: Option<usize> = None;

    for h in res.headers.iter() {
        if h.name.is_empty() {
            continue;
        }
        let name = HeaderName::from_bytes(h.name.as_bytes())
            .map_err(|e| Http1Error::InvalidHeader(e.to_string()))?;
        let value = HeaderValue::from_bytes(h.value)
            .map_err(|e| Http1Error::InvalidHeader(e.to_string()))?;

        if name == CONTENT_LENGTH {
            content_length = std::str::from_utf8(h.value)
                .ok()
                .and_then(|s| s.trim().parse::<usize>().ok());
        }

        header_map.append(name, value);
    }

    let body_len = content_length.unwrap_or(0);
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
