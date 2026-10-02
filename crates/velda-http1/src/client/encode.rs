//! Upstream HTTP/1.1 Wire Serializer.
//!
//! Architectural Perspective (Góc nhìn Ingress):
//! Handles serializing outgoing HTTP/1.1 requests to upstream backend microservices.
//! Translates [`Http1Request`] into wire framing over stream connections.

use bytes::{BufMut, BytesMut};
use http::header::CONTENT_LENGTH;
use http::{HeaderMap, Method, Uri};
use velda_core::Body;

use crate::server::request::Http1Request;

/// Encodes an HTTP/1.1 request line (method, path and query, version).
pub fn encode_request_line(method: &Method, uri: &Uri, dst: &mut BytesMut) {
    dst.put_slice(method.as_str().as_bytes());
    dst.put_slice(b" ");
    let path_and_query = uri
        .path_and_query()
        .map(|pq| pq.as_str())
        .unwrap_or_else(|| uri.path());
    dst.put_slice(path_and_query.as_bytes());
    dst.put_slice(b" HTTP/1.1\r\n");
}

pub use crate::wire::encode_headers;

/// Serializes HTTP/1.1 request head metadata into the destination buffer.
#[inline]
pub fn encode_request_head(
    method: &Method,
    uri: &Uri,
    headers: &HeaderMap,
    body_len: Option<usize>,
    dst: &mut BytesMut,
) {
    let mut needed = method.as_str().len() + uri.path().len() + 36;
    for (name, val) in headers {
        needed += name.as_str().len() + val.as_bytes().len() + 4;
    }
    if body_len.is_some() {
        needed += 36;
    }
    dst.reserve(needed);

    encode_request_line(method, uri, dst);
    for (name, val) in headers {
        dst.put_slice(name.as_str().as_bytes());
        dst.put_slice(b": ");
        dst.put_slice(val.as_bytes());
        dst.put_slice(b"\r\n");
    }

    if let Some(len) = body_len
        && !headers.contains_key(CONTENT_LENGTH)
    {
        dst.put_slice(b"content-length: ");
        let mut itoa_buf = itoa::Buffer::new();
        dst.put_slice(itoa_buf.format(len).as_bytes());
        dst.put_slice(b"\r\n");
    }

    dst.put_slice(b"\r\n");
}

/// Serializes an entire HTTP/1.1 request (head + body) into the destination buffer without heap cloning.
pub fn encode_request(req: &Http1Request, dst: &mut BytesMut) {
    let body_len = match &req.body {
        Body::Empty => None,
        Body::Bytes(bytes) => Some(bytes.len()),
    };

    encode_request_head(&req.method, &req.uri, &req.headers, body_len, dst);

    if let Body::Bytes(bytes) = &req.body {
        dst.extend_from_slice(bytes);
    }
}
