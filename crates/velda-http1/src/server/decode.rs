//! Downstream HTTP/1.1 Stream Decoder.
//!
//! Architectural Perspective (Góc nhìn Ingress):
//! Handles stateful buffer consumption (&mut BytesMut) to decode client requests.
//! Advances read buffers and extracts payloads into domain [`Http1Request`].

use bytes::{Buf, BytesMut};
use velda_core::{Body, IngressLimits};

use super::parse::{parse_chunked_body, parse_request_head};
use super::request::{Http1BodyFraming, Http1Request, Http1RequestHead};
use crate::error::Http1Error;

/// Decodes a chunked HTTP body from a byte slice.
#[inline]
pub fn decode_chunked_body(
    data: &[u8],
    max_body_size: usize,
) -> Result<Option<(usize, Body)>, Http1Error> {
    parse_chunked_body(data, max_body_size)
}

/// Decodes HTTP/1.1 request head from downstream read buffer and advances `buf` past headers.
pub fn decode_request_head(
    buf: &mut BytesMut,
    limits: &IngressLimits,
) -> Result<Option<(Http1RequestHead, Http1BodyFraming)>, Http1Error> {
    if let Some((head, framing, header_len)) = parse_request_head(buf.as_ref(), limits)? {
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
    limits: &IngressLimits,
) -> Result<Option<Body>, Http1Error> {
    match framing {
        Http1BodyFraming::Empty => Ok(Some(Body::Empty)),
        Http1BodyFraming::ContentLength(body_len) => {
            if body_len > limits.max_body_size {
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
            match parse_chunked_body(buf.as_ref(), limits.max_body_size)? {
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
    limits: &IngressLimits,
) -> Result<Option<Http1Request>, Http1Error> {
    if buf.is_empty() {
        return Ok(None);
    }
    let Some((head, framing, header_len)) = parse_request_head(buf.as_ref(), limits)? else {
        return Ok(None);
    };

    match framing {
        Http1BodyFraming::Empty => {
            buf.advance(header_len);
            Ok(Some(Http1Request::from_parts(head, Body::Empty)))
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
            Ok(Some(Http1Request::from_parts(head, body)))
        }
        Http1BodyFraming::Chunked => {
            let chunked_slice = &buf[header_len..];
            match parse_chunked_body(chunked_slice, limits.max_body_size)? {
                Some((consumed_wire, body)) => {
                    buf.advance(header_len + consumed_wire);
                    Ok(Some(Http1Request::from_parts(head, body)))
                }
                None => Ok(None),
            }
        }
    }
}
