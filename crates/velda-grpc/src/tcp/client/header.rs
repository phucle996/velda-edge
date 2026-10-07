//! Upstream gRPC Client Header Processing and RFC 9113 Sanitization over TCP.

use http::header::{CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue};

use crate::wire::GrpcWire;

/// Hop-by-hop headers prohibited in upstream gRPC HTTP/2 client requests per RFC 9113.
pub const HOP_BY_HOP: [HeaderName; 5] = [
    http::header::CONNECTION,
    HeaderName::from_static("keep-alive"),
    HeaderName::from_static("proxy-connection"),
    http::header::TRANSFER_ENCODING,
    http::header::UPGRADE,
];

/// Sanitizes an outbound upstream gRPC header map according to RFC 9113 Section 8.2.2.
#[inline]
pub fn sanitize_headers(headers: &mut HeaderMap) {
    for name in &HOP_BY_HOP {
        headers.remove(name);
    }

    if let Some(te_val) = headers.get(http::header::TE) {
        let is_trailers = te_val
            .to_str()
            .is_ok_and(|s| s.eq_ignore_ascii_case("trailers"));
        if !is_trailers {
            headers.remove(http::header::TE);
        }
    }
}

/// Constructs a canonical baseline gRPC upstream client header map.
pub fn build_headers(authority: Option<&str>) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, GrpcWire::CONTENT_TYPE_VALUE);
    headers.insert(http::header::TE, HeaderValue::from_static("trailers"));

    if let Some(auth) = authority
        && let Ok(v) = auth.parse()
    {
        headers.insert(http::header::HOST, v);
    }

    headers
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sanitize_grpc_client_headers() {
        let mut headers = HeaderMap::new();
        headers.insert(http::header::CONNECTION, HeaderValue::from_static("close"));
        headers.insert(
            HeaderName::from_static("keep-alive"),
            HeaderValue::from_static("timeout=5"),
        );
        headers.insert(CONTENT_TYPE, GrpcWire::CONTENT_TYPE_VALUE);

        sanitize_headers(&mut headers);

        assert!(headers.get(http::header::CONNECTION).is_none());
        assert!(headers.get("keep-alive").is_none());
        assert_eq!(headers.get(CONTENT_TYPE).unwrap(), "application/grpc");
    }

    #[test]
    fn test_build_grpc_client_headers() {
        let headers = build_headers(Some("backend.internal:50051"));
        assert_eq!(headers.get(CONTENT_TYPE).unwrap(), "application/grpc");
        assert_eq!(headers.get(http::header::TE).unwrap(), "trailers");
        assert_eq!(
            headers.get(http::header::HOST).unwrap(),
            "backend.internal:50051"
        );
    }
}
