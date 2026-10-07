//! HTTP/3 header sanitization and validation per RFC 9114 Section 4.2.
//!
//! Enforces HTTP/3 protocol invariants:
//! - Prohibits connection-specific headers (`Connection`, `Keep-Alive`, `Proxy-Connection`,
//!   `Transfer-Encoding`, `Upgrade`).
//! - Validates `TE` header: if present, MUST only contain `"trailers"`.

use http::HeaderMap;
use http::header::HeaderName;

/// Sanitizes downstream or upstream headers in-place by removing prohibited
/// connection-specific header fields according to RFC 9114 Section 4.2.
#[inline]
pub fn sanitize_headers(headers: &mut HeaderMap) {
    static H3_PROHIBITED_NAMES: [HeaderName; 5] = [
        http::header::CONNECTION,
        HeaderName::from_static("keep-alive"),
        HeaderName::from_static("proxy-connection"),
        http::header::TRANSFER_ENCODING,
        http::header::UPGRADE,
    ];

    for name in &H3_PROHIBITED_NAMES {
        headers.remove(name);
    }

    // RFC 9114 §4.2: The only header field that is allowed to contain the name
    // of another header field is the TE header field, which may only contain "trailers".
    if let Some(te_val) = headers.get(http::header::TE) {
        let is_trailers = te_val
            .to_str()
            .is_ok_and(|s| s.trim().eq_ignore_ascii_case("trailers"));
        if !is_trailers {
            headers.remove(http::header::TE);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderValue;

    #[test]
    fn test_removes_prohibited_h3_headers() {
        let mut headers = HeaderMap::new();
        headers.insert(
            http::header::CONNECTION,
            HeaderValue::from_static("keep-alive"),
        );
        headers.insert(
            HeaderName::from_static("keep-alive"),
            HeaderValue::from_static("timeout=5"),
        );
        headers.insert(
            HeaderName::from_static("proxy-connection"),
            HeaderValue::from_static("close"),
        );
        headers.insert(
            http::header::TRANSFER_ENCODING,
            HeaderValue::from_static("chunked"),
        );
        headers.insert(http::header::UPGRADE, HeaderValue::from_static("websocket"));
        headers.insert(
            http::header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );

        sanitize_headers(&mut headers);

        assert!(headers.get(http::header::CONNECTION).is_none());
        assert!(headers.get("keep-alive").is_none());
        assert!(headers.get("proxy-connection").is_none());
        assert!(headers.get(http::header::TRANSFER_ENCODING).is_none());
        assert!(headers.get(http::header::UPGRADE).is_none());
        assert_eq!(
            headers.get(http::header::CONTENT_TYPE).unwrap(),
            "application/json"
        );
    }

    #[test]
    fn test_te_trailers_preserved() {
        let mut headers = HeaderMap::new();
        headers.insert(http::header::TE, HeaderValue::from_static("trailers"));
        sanitize_headers(&mut headers);
        assert_eq!(headers.get(http::header::TE).unwrap(), "trailers");

        let mut invalid_headers = HeaderMap::new();
        invalid_headers.insert(http::header::TE, HeaderValue::from_static("gzip"));
        sanitize_headers(&mut invalid_headers);
        assert!(invalid_headers.get(http::header::TE).is_none());
    }
}
