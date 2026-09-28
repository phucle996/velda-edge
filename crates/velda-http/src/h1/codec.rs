//! HTTP/1.1 zero-copy request parser and response serializer.

use bytes::{Buf, BufMut, BytesMut};
use http::header::{CONTENT_LENGTH, HeaderName, HeaderValue};
use http::{HeaderMap, Method, Uri, Version};
use velda_core::{Body, L7Request, L7Response};

use crate::error::HttpError;

/// Maximum number of headers supported per request.
const MAX_HEADERS: usize = 64;

/// Decodes an HTTP/1.1 request from the read buffer.
///
/// Returns:
/// - `Ok(Some(request))` when a complete request (headers and body) is decoded.
/// - `Ok(None)` when more data is needed from the network stream.
/// - `Err(HttpError)` when the stream is malformed or invalid.
pub fn decode_request(buf: &mut BytesMut) -> Result<Option<L7Request>, HttpError> {
    if buf.is_empty() {
        return Ok(None);
    }

    let mut headers = [httparse::EMPTY_HEADER; MAX_HEADERS];
    let mut req = httparse::Request::new(&mut headers);

    let status = match req.parse(buf.as_ref()) {
        Ok(s) => s,
        Err(e) => return Err(HttpError::Parse(e.to_string())),
    };

    let header_len = match status {
        httparse::Status::Complete(len) => len,
        httparse::Status::Partial => return Ok(None),
    };

    let method_str = req
        .method
        .ok_or_else(|| HttpError::Parse("Missing HTTP method".into()))?;
    let method = Method::from_bytes(method_str.as_bytes())
        .map_err(|e| HttpError::InvalidMethod(e.to_string()))?;

    let path_str = req
        .path
        .ok_or_else(|| HttpError::Parse("Missing HTTP path/URI".into()))?;
    let uri = path_str
        .parse::<Uri>()
        .map_err(|e| HttpError::InvalidUri(e.to_string()))?;

    let version = match req.version {
        Some(1) => Version::HTTP_11,
        Some(0) => Version::HTTP_10,
        Some(v) => return Err(HttpError::UnsupportedVersion(format!("HTTP/1.{v}"))),
        None => Version::HTTP_11,
    };

    let mut header_map = HeaderMap::with_capacity(req.headers.len());
    let mut content_length: Option<usize> = None;

    for h in req.headers.iter() {
        if h.name.is_empty() {
            continue;
        }
        let name = HeaderName::from_bytes(h.name.as_bytes())
            .map_err(|e| HttpError::InvalidHeader(e.to_string()))?;
        let value = HeaderValue::from_bytes(h.value)
            .map_err(|e| HttpError::InvalidHeader(e.to_string()))?;

        if name == CONTENT_LENGTH {
            content_length = std::str::from_utf8(h.value)
                .ok()
                .and_then(|s| s.trim().parse::<usize>().ok());
        }

        header_map.append(name, value);
    }

    let body_len = content_length.unwrap_or(0);
    let total_len = header_len + body_len;

    // Verify whether entire body payload is available in buffer
    if buf.len() < total_len {
        return Ok(None);
    }

    // Advance past HTTP headers
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

    // 1. Status line
    dst.put_slice(b"HTTP/1.1 ");
    dst.put_slice(status.as_str().as_bytes());
    dst.put_slice(b" ");
    dst.put_slice(reason.as_bytes());
    dst.put_slice(b"\r\n");

    // 2. Headers
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

    // Auto-inject Content-Length if missing
    if !has_content_length {
        let body_len = res.body.len();
        dst.put_slice(b"content-length: ");
        dst.put_slice(body_len.to_string().as_bytes());
        dst.put_slice(b"\r\n");
    }

    // 3. Headers separator
    dst.put_slice(b"\r\n");

    // 4. Body payload
    if let Body::Bytes(ref bytes) = res.body {
        dst.put_slice(bytes);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::StatusCode;

    #[test]
    fn test_decode_complete_get_request() {
        let raw = b"GET /api/v1/users?page=2 HTTP/1.1\r\nHost: api.example.com\r\nAccept: application/json\r\n\r\n";
        let mut buf = BytesMut::from(&raw[..]);

        let req = decode_request(&mut buf).unwrap().unwrap();
        assert_eq!(req.method, Method::GET);
        assert_eq!(req.path(), "/api/v1/users");
        assert_eq!(req.query(), Some("page=2"));
        assert_eq!(req.version, Version::HTTP_11);
        assert_eq!(req.headers.get("host").unwrap(), "api.example.com");
        assert_eq!(req.headers.get("accept").unwrap(), "application/json");
        assert!(!req.has_body());
        assert!(buf.is_empty());
    }

    #[test]
    fn test_decode_post_request_with_body() {
        let raw =
            b"POST /submit HTTP/1.1\r\nHost: test.com\r\nContent-Length: 13\r\n\r\nHello, World!";
        let mut buf = BytesMut::from(&raw[..]);

        let req = decode_request(&mut buf).unwrap().unwrap();
        assert_eq!(req.method, Method::POST);
        assert_eq!(req.path(), "/submit");
        assert_eq!(req.body.len(), 13);
        match req.body {
            Body::Bytes(b) => assert_eq!(&b[..], b"Hello, World!"),
            Body::Empty => panic!("Expected body"),
        }
        assert!(buf.is_empty());
    }

    #[test]
    fn test_decode_partial_data_returns_none() {
        let partial_header = b"GET /index.html HTTP/1.1\r\nHost: test";
        let mut buf = BytesMut::from(&partial_header[..]);

        assert!(decode_request(&mut buf).unwrap().is_none());

        // Append remaining header bytes
        buf.extend_from_slice(b".com\r\n\r\n");
        let req = decode_request(&mut buf).unwrap().unwrap();
        assert_eq!(req.path(), "/index.html");
    }

    #[test]
    fn test_encode_response() {
        let res = L7Response::from_bytes(StatusCode::OK, b"{\"status\":\"ok\"}".to_vec());
        let mut buf = BytesMut::new();
        encode_response(&res, &mut buf);

        let s = String::from_utf8(buf.to_vec()).unwrap();
        assert!(s.starts_with("HTTP/1.1 200 OK\r\n"));
        assert!(s.contains("content-length: 15\r\n"));
        assert!(s.ends_with("\r\n\r\n{\"status\":\"ok\"}"));
    }
}
