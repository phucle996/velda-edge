//! Downstream HTTP/2 Pre-Compiled Canonical Early Response Engine (RFC 9113 & RFC 7541).
//!
//! ```text
//! Server Early Error Fast Path (`send_early_error`):
//!   StatusCode ──▶ LazyLock Static Table (pre-encoded RFC 7541 Stateless HPACK slices)
//!                    ├─ :status (Static Index 12/13/14 or Literal Without Indexing)
//!                    ├─ content-type: text/plain; charset=utf-8   (Without Indexing)
//!                    ├─ cache-control: no-store                   (Without Indexing)
//!                    └─ retry-after: 1                            (429 only)
//!                        │
//!                        ▼
//!                  H2Connection::send_stateless_early_response(stream_id, ...)
//!                    ├─ Write 9-byte HEADERS slot directly into outbound_buf
//!                    ├─ Copy &'static [u8] HPACK slice
//!                    ├─ Append optional x-request-id / alt-svc (Without Indexing)
//!                    └─ Patch frame length & stream_id in-place (zero response HeaderWorkspace)
//! ```
//!
//! Because pre-encoded slices use RFC 7541 Static Indexing and Literal Without Indexing exclusively,
//! dispatching them on any stream leaves the connection's HPACK dynamic table untouched.

use std::sync::LazyLock;

use bytes::BytesMut;
use http::header::HeaderValue;
use http::{StatusCode, Version};
use velda_core::{Body, HeaderKey, HeaderWorkspace, L7Response};

use crate::proto::hpack::{encode_stateless_literal, encode_stateless_status};

struct EarlyErrorHpackTable {
    s400: Box<[u8]>,
    s401: Box<[u8]>,
    s403: Box<[u8]>,
    s404: Box<[u8]>,
    s405: Box<[u8]>,
    s408: Box<[u8]>,
    s409: Box<[u8]>,
    s413: Box<[u8]>,
    s414: Box<[u8]>,
    s415: Box<[u8]>,
    s425: Box<[u8]>,
    s429: Box<[u8]>,
    s431: Box<[u8]>,
    s499: Box<[u8]>,
    s500: Box<[u8]>,
    s502: Box<[u8]>,
    s503: Box<[u8]>,
    s504: Box<[u8]>,
}

fn compile_early_block(status: StatusCode) -> Box<[u8]> {
    let mut buf = BytesMut::with_capacity(64);
    encode_stateless_status(status, &mut buf);
    encode_stateless_literal(b"content-type", b"text/plain; charset=utf-8", &mut buf);
    encode_stateless_literal(b"cache-control", b"no-store", &mut buf);
    if status == StatusCode::TOO_MANY_REQUESTS {
        encode_stateless_literal(b"retry-after", b"1", &mut buf);
    }
    buf.to_vec().into_boxed_slice()
}

static EARLY_HPACK_TABLE: LazyLock<EarlyErrorHpackTable> = LazyLock::new(|| EarlyErrorHpackTable {
    s400: compile_early_block(StatusCode::BAD_REQUEST),
    s401: compile_early_block(StatusCode::UNAUTHORIZED),
    s403: compile_early_block(StatusCode::FORBIDDEN),
    s404: compile_early_block(StatusCode::NOT_FOUND),
    s405: compile_early_block(StatusCode::METHOD_NOT_ALLOWED),
    s408: compile_early_block(StatusCode::REQUEST_TIMEOUT),
    s409: compile_early_block(StatusCode::CONFLICT),
    s413: compile_early_block(StatusCode::PAYLOAD_TOO_LARGE),
    s414: compile_early_block(StatusCode::URI_TOO_LONG),
    s415: compile_early_block(StatusCode::UNSUPPORTED_MEDIA_TYPE),
    s425: compile_early_block(StatusCode::TOO_EARLY),
    s429: compile_early_block(StatusCode::TOO_MANY_REQUESTS),
    s431: compile_early_block(StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE),
    s499: compile_early_block(StatusCode::from_u16(499).unwrap_or(StatusCode::BAD_REQUEST)),
    s500: compile_early_block(StatusCode::INTERNAL_SERVER_ERROR),
    s502: compile_early_block(StatusCode::BAD_GATEWAY),
    s503: compile_early_block(StatusCode::SERVICE_UNAVAILABLE),
    s504: compile_early_block(StatusCode::GATEWAY_TIMEOUT),
});

/// Returns pre-compiled stateless RFC 7541 HPACK bytes for canonical edge error status codes.
///
/// The returned slice encodes `:status`, `content-type: text/plain; charset=utf-8`,
/// `cache-control: no-store`, and `retry-after: 1` (for `429`) without touching any dynamic table.
#[cold]
pub fn precompiled_early_hpack(status: StatusCode) -> Option<&'static [u8]> {
    let t = &*EARLY_HPACK_TABLE;
    match status.as_u16() {
        400 => Some(&t.s400),
        401 => Some(&t.s401),
        403 => Some(&t.s403),
        404 => Some(&t.s404),
        405 => Some(&t.s405),
        408 => Some(&t.s408),
        409 => Some(&t.s409),
        413 => Some(&t.s413),
        414 => Some(&t.s414),
        415 => Some(&t.s415),
        425 => Some(&t.s425),
        429 => Some(&t.s429),
        431 => Some(&t.s431),
        499 => Some(&t.s499),
        500 => Some(&t.s500),
        502 => Some(&t.s502),
        503 => Some(&t.s503),
        504 => Some(&t.s504),
        _ => None,
    }
}

/// Builds an HTTP/2 Zero-Body rejection [`L7Response`] in-place from a request [`HeaderWorkspace`],
/// preserving bypass headers (`x-request-id`, `x-ratelimit-*`, `retry-after`) while stamping
/// canonical HTTP/2 error headers.
#[cold]
pub fn reject_response(
    reason: velda_core::RejectReason,
    mut headers: HeaderWorkspace,
) -> L7Response {
    const KEY_CACHE_CTRL: HeaderKey = HeaderKey::from_static("cache-control");
    const KEY_RETRY_AFTER: HeaderKey = HeaderKey::from_static("retry-after");

    let meta = velda_core::lut::get_reject_meta(reason);
    headers.retain_bypass_only();
    headers.insert_prehashed(&KEY_CACHE_CTRL, HeaderValue::from_static("no-store"));
    if meta.status == StatusCode::TOO_MANY_REQUESTS && !headers.contains_prehashed(&KEY_RETRY_AFTER)
    {
        headers.insert_prehashed(&KEY_RETRY_AFTER, HeaderValue::from_static("1"));
    }

    L7Response {
        status: meta.status,
        version: Version::HTTP_2,
        headers,
        body: Body::Empty,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::hpack::HpackDecoder;
    use http::header::CACHE_CONTROL;

    #[test]
    fn test_precompiled_early_hpack_table_decodes_statelessly() {
        let codes = [
            400, 401, 403, 404, 405, 408, 409, 413, 414, 415, 425, 429, 431, 499, 500, 502, 503,
            504,
        ];
        let mut decoder = HpackDecoder::default();
        for code in codes {
            let status = StatusCode::from_u16(code).unwrap();
            let raw = precompiled_early_hpack(status).expect("canonical status precompiled");
            let decoded = decoder.decode(raw).unwrap();
            assert_eq!(decoded.status, Some(status));
            assert_eq!(
                decoded.headers.get("content-type").unwrap(),
                "text/plain; charset=utf-8"
            );
            assert_eq!(decoded.headers.get("cache-control").unwrap(), "no-store");
            if code == 429 {
                assert_eq!(decoded.headers.get("retry-after").unwrap(), "1");
            }
        }
        // Unknown status falls back cleanly to None (handled dynamically by H2Connection).
        assert!(precompiled_early_hpack(StatusCode::IM_A_TEAPOT).is_none());
    }

    #[test]
    fn test_reject_response_zero_body_and_preserves_plugin_retry_after() {
        let mut ws = HeaderWorkspace::new(32);
        ws.insert(http::header::HOST, HeaderValue::from_static("example.com"));
        ws.insert_bypass_u64(http::header::RETRY_AFTER, 42);

        let resp = reject_response(velda_core::RejectReason::TooManyRequests, ws);
        assert_eq!(resp.status, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(resp.version, Version::HTTP_2);
        assert!(resp.body.is_empty());
        assert!(!resp.headers.contains_key(http::header::HOST));
        assert_eq!(resp.headers.get(CACHE_CONTROL).unwrap(), "no-store");
        assert_eq!(resp.headers.get(http::header::RETRY_AFTER).unwrap(), "42");
    }
}
