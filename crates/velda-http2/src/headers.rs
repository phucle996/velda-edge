//! RFC 9113 Header Sanitization for HTTP/2.
//!
//! Architectural Perspective (Góc nhìn Ingress):
//! Section 8.2.2 mandates that connection-specific header fields MUST NOT appear
//! in HTTP/2 requests or responses:
//! - `connection`
//! - `keep-alive`
//! - `proxy-connection`
//! - `transfer-encoding`
//! - `upgrade`
//!
//! The `TE` header field MAY be present only if its value is "trailers".
//! Pseudo-headers (starting with ':') must also be excluded from generic header maps.

use http::HeaderMap;
use http::header::HeaderName;

static H2_HOP_BY_HOP_NAMES: [HeaderName; 5] = [
    http::header::CONNECTION,
    HeaderName::from_static("keep-alive"),
    HeaderName::from_static("proxy-connection"),
    http::header::TRANSFER_ENCODING,
    http::header::UPGRADE,
];

/// Returns true if a header field is prohibited under RFC 9113 Section 8.2.2.
///
/// Invariants:
/// - Pseudo-headers (starting with ':') MUST NOT appear in generic header maps.
/// - Connection-specific hop-by-hop headers (`connection`, `keep-alive`,
///   `proxy-connection`, `transfer-encoding`, `upgrade`) MUST NOT appear in HTTP/2.
/// - `te` is permitted ONLY when its value is exactly "trailers" (case-insensitive).
#[inline]
pub fn is_disallowed_h2_header(name: &HeaderName, value: &http::HeaderValue) -> bool {
    let s = name.as_str();
    if s.starts_with(':') {
        return true;
    }
    if s.eq_ignore_ascii_case("connection")
        || s.eq_ignore_ascii_case("keep-alive")
        || s.eq_ignore_ascii_case("proxy-connection")
        || s.eq_ignore_ascii_case("transfer-encoding")
        || s.eq_ignore_ascii_case("upgrade")
    {
        return true;
    }
    if s.eq_ignore_ascii_case("te")
        && !value
            .to_str()
            .is_ok_and(|v| v.eq_ignore_ascii_case("trailers"))
    {
        return true;
    }
    false
}

/// Sanitizes an HTTP header map in-place according to RFC 9113 Section 8.2.2.
///
/// Invariants:
/// - Strips `connection`, `keep-alive`, `proxy-connection`, `transfer-encoding`, `upgrade`.
/// - Retains `te` only if value is "trailers", strips otherwise.
/// - Strips any pseudo-headers (starting with ':').
#[inline]
pub fn sanitize_h2_headers(headers: &mut HeaderMap) {
    // 1. Remove standard RFC 9113 hop-by-hop headers
    for name in &H2_HOP_BY_HOP_NAMES {
        headers.remove(name);
    }

    // 2. Validate TE header (RFC 9113 Section 8.2.2: only "trailers" is permitted)
    if let Some(te_val) = headers.get(http::header::TE) {
        let is_trailers = te_val
            .to_str()
            .is_ok_and(|s| s.eq_ignore_ascii_case("trailers"));
        if !is_trailers {
            headers.remove(http::header::TE);
        }
    }

    // 3. Strip pseudo-headers (starting with ':') unconditionally
    if headers.keys().any(|name| name.as_str().starts_with(':')) {
        let pseudo_keys: Vec<HeaderName> = headers
            .keys()
            .filter(|name| name.as_str().starts_with(':'))
            .cloned()
            .collect();
        for name in &pseudo_keys {
            headers.remove(name);
        }
    }
}

/// Returns a clean copy of `headers` with RFC 9113 hop-by-hop and pseudo-headers stripped.
#[inline]
pub fn filter_h2_headers(headers: &HeaderMap) -> HeaderMap {
    let mut clean = headers.clone();
    sanitize_h2_headers(&mut clean);
    clean
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::header::{CONNECTION, CONTENT_TYPE, HeaderValue, TE, TRANSFER_ENCODING, UPGRADE};

    #[test]
    fn test_removes_hop_by_hop() {
        let mut map = HeaderMap::new();
        map.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        map.insert(CONNECTION, HeaderValue::from_static("keep-alive"));
        map.insert(TRANSFER_ENCODING, HeaderValue::from_static("chunked"));
        map.insert(UPGRADE, HeaderValue::from_static("websocket"));
        map.insert("proxy-connection", HeaderValue::from_static("keep-alive"));

        let clean = filter_h2_headers(&map);
        assert_eq!(clean.len(), 1);
        assert_eq!(clean.get(CONTENT_TYPE).unwrap(), "application/json");
        assert!(clean.get(CONNECTION).is_none());
        assert!(clean.get(TRANSFER_ENCODING).is_none());
        assert!(clean.get(UPGRADE).is_none());
    }

    #[test]
    fn test_sanitize_h2_headers_in_place() {
        let mut map = HeaderMap::new();
        map.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        map.insert(CONNECTION, HeaderValue::from_static("keep-alive"));
        map.insert(TRANSFER_ENCODING, HeaderValue::from_static("chunked"));

        sanitize_h2_headers(&mut map);
        assert_eq!(map.len(), 1);
        assert_eq!(map.get(CONTENT_TYPE).unwrap(), "application/json");
        assert!(map.get(CONNECTION).is_none());
        assert!(map.get(TRANSFER_ENCODING).is_none());
    }

    #[test]
    fn test_te_trailers_preserved() {
        let mut map = HeaderMap::new();
        map.insert(TE, HeaderValue::from_static("trailers"));
        let clean = filter_h2_headers(&map);
        assert_eq!(clean.get(TE).unwrap(), "trailers");

        let mut bad_te = HeaderMap::new();
        bad_te.insert(TE, HeaderValue::from_static("gzip"));
        let clean_bad = filter_h2_headers(&bad_te);
        assert!(clean_bad.get(TE).is_none());
    }
}
