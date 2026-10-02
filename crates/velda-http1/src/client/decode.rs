//! Upstream HTTP/1.1 Stream Decoder.
//!
//! Architectural Perspective (Góc nhìn Ingress):
//! Handles stateful buffer consumption (&mut BytesMut) to decode backend responses.
//! Advances read buffers and extracts payloads into domain [`Http1Response`].

use bytes::{Buf, BytesMut};
use velda_core::{Body, IngressLimits};

use super::parse::{parse_chunked_body, parse_response_head};
use super::response::{Http1Response, Http1ResponseHead};
use crate::error::Http1Error;
use crate::server::request::Http1BodyFraming;

/// Decodes HTTP/1.1 response head from upstream read buffer and advances `buf` past headers.
pub fn decode_response_head(
    buf: &mut BytesMut,
    limits: &IngressLimits,
) -> Result<Option<(Http1ResponseHead, Http1BodyFraming)>, Http1Error> {
    if let Some((head, framing, header_len)) = parse_response_head(buf.as_ref(), limits)? {
        buf.advance(header_len);
        Ok(Some((head, framing)))
    } else {
        Ok(None)
    }
}

/// Decodes an entire HTTP/1.1 response from the upstream read buffer.
pub fn decode_response(
    buf: &mut BytesMut,
    limits: &IngressLimits,
) -> Result<Option<Http1Response>, Http1Error> {
    if buf.is_empty() {
        return Ok(None);
    }
    let Some((head, framing, header_len)) = parse_response_head(buf.as_ref(), limits)? else {
        return Ok(None);
    };

    match framing {
        Http1BodyFraming::Empty => {
            buf.advance(header_len);
            Ok(Some(Http1Response::from_parts(head, Body::Empty)))
        }
        Http1BodyFraming::ContentLength(body_len) => {
            if body_len > limits.max_body_size {
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
            match parse_chunked_body(chunked_slice, limits.max_body_size)? {
                Some((consumed_wire, body)) => {
                    buf.advance(header_len + consumed_wire);
                    Ok(Some(Http1Response::from_parts(head, body)))
                }
                None => Ok(None),
            }
        }
    }
}
