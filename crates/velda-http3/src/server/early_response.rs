//! Downstream HTTP/3 Pre-Compiled Canonical Early Response Engine (RFC 9114).
//!
//! ```text
//!   StatusCode (Early 0-RTT / Connection Error) ──► precompiled_early_response(status, req_id)
//!   InternalStatus (Plugin / WAF / RateLimit)   ──► status_response(status, req.headers)
//!     └─► headers.retain_bypass_only() + HTTP_STATUS_METAS { status, content_len_str, body }
//! ```

use bytes::Bytes;
use http::header::HeaderValue;
use http::{StatusCode, Version};
use velda_core::{Body, HeaderKey, HeaderWorkspace, InternalStatus, L7Response};

#[derive(Debug, Clone, Copy)]
struct HttpStatusMeta {
    status: StatusCode,
    content_len_str: &'static str,
    body: &'static [u8],
}

const STATUS_499: StatusCode = match StatusCode::from_u16(499) {
    Ok(s) => s,
    Err(_) => panic!("Valid status code 499"),
};

/// Dense O(1) lookup table mapping `InternalStatus as usize` (0..41) to HTTP/3 wire metadata.
static HTTP_STATUS_METAS: [HttpStatusMeta; InternalStatus::COUNT] = [
    HttpStatusMeta {
        status: StatusCode::OK,
        content_len_str: "7",
        body: b"200 OK\n",
    },
    HttpStatusMeta {
        status: STATUS_499,
        content_len_str: "26",
        body: b"499 Client Closed Request\n",
    },
    HttpStatusMeta {
        status: StatusCode::BAD_GATEWAY,
        content_len_str: "16",
        body: b"502 Bad Gateway\n",
    },
    HttpStatusMeta {
        status: StatusCode::GATEWAY_TIMEOUT,
        content_len_str: "20",
        body: b"504 Gateway Timeout\n",
    },
    HttpStatusMeta {
        status: StatusCode::REQUEST_TIMEOUT,
        content_len_str: "20",
        body: b"408 Request Timeout\n",
    },
    HttpStatusMeta {
        status: StatusCode::NOT_FOUND,
        content_len_str: "14",
        body: b"404 Not Found\n",
    },
    HttpStatusMeta {
        status: StatusCode::INTERNAL_SERVER_ERROR,
        content_len_str: "26",
        body: b"500 Internal Server Error\n",
    },
    HttpStatusMeta {
        status: StatusCode::SERVICE_UNAVAILABLE,
        content_len_str: "24",
        body: b"503 Service Unavailable\n",
    },
    HttpStatusMeta {
        status: StatusCode::BAD_REQUEST,
        content_len_str: "16",
        body: b"400 Bad Request\n",
    },
    HttpStatusMeta {
        status: StatusCode::UNAUTHORIZED,
        content_len_str: "17",
        body: b"401 Unauthorized\n",
    },
    HttpStatusMeta {
        status: StatusCode::FORBIDDEN,
        content_len_str: "14",
        body: b"403 Forbidden\n",
    },
    HttpStatusMeta {
        status: StatusCode::METHOD_NOT_ALLOWED,
        content_len_str: "23",
        body: b"405 Method Not Allowed\n",
    },
    HttpStatusMeta {
        status: StatusCode::CONFLICT,
        content_len_str: "13",
        body: b"409 Conflict\n",
    },
    HttpStatusMeta {
        status: StatusCode::PAYLOAD_TOO_LARGE,
        content_len_str: "22",
        body: b"413 Payload Too Large\n",
    },
    HttpStatusMeta {
        status: StatusCode::URI_TOO_LONG,
        content_len_str: "17",
        body: b"414 URI Too Long\n",
    },
    HttpStatusMeta {
        status: StatusCode::UNSUPPORTED_MEDIA_TYPE,
        content_len_str: "27",
        body: b"415 Unsupported Media Type\n",
    },
    HttpStatusMeta {
        status: StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE,
        content_len_str: "36",
        body: b"431 Request Header Fields Too Large\n",
    },
    HttpStatusMeta {
        status: StatusCode::TOO_MANY_REQUESTS,
        content_len_str: "22",
        body: b"429 Too Many Requests\n",
    },
    HttpStatusMeta {
        status: StatusCode::FORBIDDEN,
        content_len_str: "14",
        body: b"403 Forbidden\n",
    },
    HttpStatusMeta {
        status: StatusCode::FORBIDDEN,
        content_len_str: "14",
        body: b"403 Forbidden\n",
    },
    HttpStatusMeta {
        status: StatusCode::FORBIDDEN,
        content_len_str: "14",
        body: b"403 Forbidden\n",
    },
    // 21: DownstreamIo
    HttpStatusMeta {
        status: StatusCode::BAD_REQUEST,
        content_len_str: "16",
        body: b"400 Bad Request\n",
    },
    // 22: DownstreamTls
    HttpStatusMeta {
        status: StatusCode::BAD_REQUEST,
        content_len_str: "16",
        body: b"400 Bad Request\n",
    },
    // 23: SmugglingDetected
    HttpStatusMeta {
        status: StatusCode::BAD_REQUEST,
        content_len_str: "16",
        body: b"400 Bad Request\n",
    },
    // 24: StreamingViolation
    HttpStatusMeta {
        status: StatusCode::BAD_REQUEST,
        content_len_str: "16",
        body: b"400 Bad Request\n",
    },
    // 25: FloodDetected
    HttpStatusMeta {
        status: StatusCode::TOO_MANY_REQUESTS,
        content_len_str: "22",
        body: b"429 Too Many Requests\n",
    },
    // 26: NoEndpointsAvailable
    HttpStatusMeta {
        status: StatusCode::SERVICE_UNAVAILABLE,
        content_len_str: "24",
        body: b"503 Service Unavailable\n",
    },
    // 27: PoolExhausted
    HttpStatusMeta {
        status: StatusCode::SERVICE_UNAVAILABLE,
        content_len_str: "24",
        body: b"503 Service Unavailable\n",
    },
    // 28: PoolAcquisitionTimeout
    HttpStatusMeta {
        status: StatusCode::SERVICE_UNAVAILABLE,
        content_len_str: "24",
        body: b"503 Service Unavailable\n",
    },
    // 29: UpstreamDnsFailed
    HttpStatusMeta {
        status: StatusCode::BAD_GATEWAY,
        content_len_str: "16",
        body: b"502 Bad Gateway\n",
    },
    // 30: UpstreamConnectFailed
    HttpStatusMeta {
        status: StatusCode::BAD_GATEWAY,
        content_len_str: "16",
        body: b"502 Bad Gateway\n",
    },
    // 31: UpstreamConnectTimeout
    HttpStatusMeta {
        status: StatusCode::GATEWAY_TIMEOUT,
        content_len_str: "20",
        body: b"504 Gateway Timeout\n",
    },
    // 32: UpstreamTlsFailed
    HttpStatusMeta {
        status: StatusCode::BAD_GATEWAY,
        content_len_str: "16",
        body: b"502 Bad Gateway\n",
    },
    // 33: UpstreamStale
    HttpStatusMeta {
        status: StatusCode::BAD_GATEWAY,
        content_len_str: "16",
        body: b"502 Bad Gateway\n",
    },
    // 34: UpstreamRefused
    HttpStatusMeta {
        status: StatusCode::BAD_GATEWAY,
        content_len_str: "16",
        body: b"502 Bad Gateway\n",
    },
    // 35: UpstreamIo
    HttpStatusMeta {
        status: StatusCode::BAD_GATEWAY,
        content_len_str: "16",
        body: b"502 Bad Gateway\n",
    },
    // 36: UpstreamProtocolError
    HttpStatusMeta {
        status: StatusCode::BAD_GATEWAY,
        content_len_str: "16",
        body: b"502 Bad Gateway\n",
    },
    // 37: InvalidRuntimeConfig
    HttpStatusMeta {
        status: StatusCode::INTERNAL_SERVER_ERROR,
        content_len_str: "26",
        body: b"500 Internal Server Error\n",
    },
    // 38: PoolKeep
    HttpStatusMeta {
        status: StatusCode::OK,
        content_len_str: "7",
        body: b"200 OK\n",
    },
    // 39: PoolStreamCanceled
    HttpStatusMeta {
        status: STATUS_499,
        content_len_str: "26",
        body: b"499 Client Closed Request\n",
    },
    // 40: PoolEvict
    HttpStatusMeta {
        status: StatusCode::SERVICE_UNAVAILABLE,
        content_len_str: "24",
        body: b"503 Service Unavailable\n",
    },
];

#[inline]
const fn status_payload(status: StatusCode) -> (StatusCode, &'static str, &'static [u8]) {
    let internal = match status {
        StatusCode::BAD_REQUEST => InternalStatus::BadRequest,
        StatusCode::UNAUTHORIZED => InternalStatus::Unauthorized,
        StatusCode::FORBIDDEN => InternalStatus::Forbidden,
        StatusCode::NOT_FOUND => InternalStatus::RouteNotFound,
        StatusCode::METHOD_NOT_ALLOWED => InternalStatus::MethodNotAllowed,
        StatusCode::REQUEST_TIMEOUT => InternalStatus::RequestTimeout,
        StatusCode::CONFLICT => InternalStatus::Conflict,
        StatusCode::PAYLOAD_TOO_LARGE => InternalStatus::PayloadTooLarge,
        StatusCode::URI_TOO_LONG => InternalStatus::UriTooLong,
        StatusCode::UNSUPPORTED_MEDIA_TYPE => InternalStatus::UnsupportedMediaType,
        StatusCode::TOO_EARLY => {
            return (
                StatusCode::TOO_EARLY,
                "47",
                b"425 Too Early: non-idempotent request in 0-RTT\n",
            );
        }
        StatusCode::TOO_MANY_REQUESTS => InternalStatus::RateLimited,
        StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE => InternalStatus::HeaderFieldsTooLarge,
        s if s.as_u16() == 499 => InternalStatus::ClientCanceled,
        StatusCode::BAD_GATEWAY => InternalStatus::UpstreamError,
        StatusCode::SERVICE_UNAVAILABLE => InternalStatus::ServiceUnavailable,
        StatusCode::GATEWAY_TIMEOUT => InternalStatus::UpstreamTimeout,
        _ => InternalStatus::InternalError,
    };
    let meta = &HTTP_STATUS_METAS[internal as usize];
    (meta.status, meta.content_len_str, meta.body)
}

/// Pre-compiles a canonical HTTP/3 early error [`L7Response`] with Zero-Copy static payload.
#[cold]
pub fn precompiled_early_response(
    status: StatusCode,
    request_id: Option<&HeaderValue>,
) -> L7Response {
    const KEY_CACHE_CTRL: HeaderKey = HeaderKey::from_static("cache-control");
    const KEY_CONTENT_TYPE: HeaderKey = HeaderKey::from_static("content-type");
    const KEY_CONTENT_LEN: HeaderKey = HeaderKey::from_static("content-length");
    const KEY_RETRY_AFTER: HeaderKey = HeaderKey::from_static("retry-after");

    let (status_code, content_len_str, body) = status_payload(status);
    let mut ws = match request_id {
        Some(req_id) => HeaderWorkspace::with_request_id(req_id.clone(), 5),
        None => HeaderWorkspace::new(5),
    };
    ws.insert_prehashed(&KEY_CACHE_CTRL, HeaderValue::from_static("no-store"));
    ws.insert_prehashed(
        &KEY_CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    ws.insert_prehashed(&KEY_CONTENT_LEN, HeaderValue::from_static(content_len_str));
    if status_code == StatusCode::TOO_MANY_REQUESTS && !ws.contains_prehashed(&KEY_RETRY_AFTER) {
        ws.insert_prehashed(&KEY_RETRY_AFTER, HeaderValue::from_static("1"));
    }

    L7Response {
        status: status_code,
        version: Version::HTTP_3,
        headers: ws,
        body: Body::Bytes(Bytes::from_static(body)),
    }
}

/// Builds an HTTP/3 [`L7Response`] in-place from an [`InternalStatus`] and request
/// [`HeaderWorkspace`], preserving Bit 1 bypass headers while stamping canonical HTTP/3 fields.
#[cold]
pub fn status_response(internal: InternalStatus, mut headers: HeaderWorkspace) -> L7Response {
    const KEY_CACHE_CTRL: HeaderKey = HeaderKey::from_static("cache-control");
    const KEY_CONTENT_TYPE: HeaderKey = HeaderKey::from_static("content-type");
    const KEY_CONTENT_LEN: HeaderKey = HeaderKey::from_static("content-length");
    const KEY_RETRY_AFTER: HeaderKey = HeaderKey::from_static("retry-after");

    let meta = &HTTP_STATUS_METAS[internal as usize];
    headers.retain_bypass_only();
    headers.insert_prehashed(&KEY_CACHE_CTRL, HeaderValue::from_static("no-store"));
    headers.insert_prehashed(
        &KEY_CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    headers.insert_prehashed(
        &KEY_CONTENT_LEN,
        HeaderValue::from_static(meta.content_len_str),
    );
    if meta.status == StatusCode::TOO_MANY_REQUESTS && !headers.contains_prehashed(&KEY_RETRY_AFTER)
    {
        headers.insert_prehashed(&KEY_RETRY_AFTER, HeaderValue::from_static("1"));
    }

    L7Response {
        status: meta.status,
        version: Version::HTTP_3,
        headers,
        body: Body::Bytes(Bytes::from_static(meta.body)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::header::{CACHE_CONTROL, CONTENT_LENGTH, CONTENT_TYPE};

    #[test]
    fn test_precompiled_early_response_http3() {
        let req_id = HeaderValue::from_static("0123456789abcdef0123456789abcdef");
        let resp = precompiled_early_response(StatusCode::TOO_EARLY, Some(&req_id));

        assert_eq!(resp.status, StatusCode::TOO_EARLY);
        assert_eq!(resp.version, Version::HTTP_3);
        assert_eq!(
            resp.headers.get(CONTENT_TYPE).unwrap(),
            "text/plain; charset=utf-8"
        );
        assert_eq!(resp.headers.get(CONTENT_LENGTH).unwrap(), "47");
        assert_eq!(
            resp.headers.get(&HeaderWorkspace::X_REQUEST_ID).unwrap(),
            "0123456789abcdef0123456789abcdef"
        );
        assert_eq!(
            resp.body.to_bytes(),
            Bytes::from_static(b"425 Too Early: non-idempotent request in 0-RTT\n")
        );
    }

    #[test]
    fn test_status_responses_keep_their_status_and_forbid_caching() {
        for (status, expected) in [
            (InternalStatus::UriTooLong, StatusCode::URI_TOO_LONG),
            (
                InternalStatus::HeaderFieldsTooLarge,
                StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE,
            ),
            (
                InternalStatus::PayloadTooLarge,
                StatusCode::PAYLOAD_TOO_LARGE,
            ),
            (
                InternalStatus::MethodNotAllowed,
                StatusCode::METHOD_NOT_ALLOWED,
            ),
            (InternalStatus::Forbidden, StatusCode::FORBIDDEN),
            (InternalStatus::BotBlocked, StatusCode::FORBIDDEN),
        ] {
            let resp = status_response(status, HeaderWorkspace::new(4));
            assert_eq!(resp.status, expected);
            assert_eq!(resp.headers.get(CACHE_CONTROL).unwrap(), "no-store");
        }
    }

    #[test]
    fn test_precompiled_early_response_http3_fallback() {
        let resp = precompiled_early_response(StatusCode::NOT_IMPLEMENTED, None);
        assert_eq!(resp.status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(resp.version, Version::HTTP_3);
        assert!(resp.headers.get(&HeaderWorkspace::X_REQUEST_ID).is_some());
    }

    #[test]
    fn test_status_response_http3_retains_bypass_and_sets_defaults() {
        let mut ws = HeaderWorkspace::new(32);
        ws.insert(http::header::COOKIE, HeaderValue::from_static("a=b"));
        let resp = status_response(InternalStatus::RateLimited, ws);
        assert_eq!(resp.status, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(resp.version, Version::HTTP_3);
        assert!(!resp.headers.contains_key(http::header::COOKIE));
        assert_eq!(resp.headers.get(CACHE_CONTROL).unwrap(), "no-store");
        assert_eq!(resp.headers.get(http::header::RETRY_AFTER).unwrap(), "1");
    }
}
