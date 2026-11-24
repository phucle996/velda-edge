//! Downstream HTTP/3 Host Extraction Helper (RFC 9114).
//!
//! Proxy forwarding enrichment and RFC 9114 §4.2 anti-spoofing sanitization run
//! in a single wire decode pass inside [`crate::proto::qpack::decode_qpack_ingress`].

use http::Uri;
use velda_core::HeaderWorkspace;

/// Extracts effective Host or authority string from request headers or URI.
#[inline]
pub fn extract_host<'a>(headers: &'a HeaderWorkspace, uri: &'a Uri) -> Option<&'a str> {
    headers
        .get(http::header::HOST)
        .and_then(|v| v.to_str().ok())
        .or_else(|| uri.authority().map(|a| a.as_str()))
        .or_else(|| uri.host())
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::header::HeaderValue;

    #[test]
    fn test_extract_host_precedence() {
        let mut ws = HeaderWorkspace::empty(2);
        ws.insert(
            http::header::HOST,
            HeaderValue::from_static("hdr.example.com"),
        );
        let uri: Uri = "https://uri.example.com/api".parse().unwrap();
        assert_eq!(extract_host(&ws, &uri), Some("hdr.example.com"));

        let empty = HeaderWorkspace::empty(0);
        assert_eq!(extract_host(&empty, &uri), Some("uri.example.com"));
    }
}
