//! Downstream gRPC Authority Extraction Helper over UDP (RFC 9114).
//!
//! Proxy forwarding enrichment and RFC 9114/9113 hop-by-hop anti-spoofing sanitization run
//! in a single wire decode pass inside [`crate::udp::wire::decode_qpack_ingress`].

use http::Uri;
use velda_core::HeaderWorkspace;

/// Extracts effective authority from request headers or URI authority.
#[inline]
pub fn extract_authority<'a>(headers: &'a HeaderWorkspace, uri: &'a Uri) -> Option<&'a str> {
    headers
        .get(http::header::HOST)
        .and_then(|v| v.to_str().ok())
        .or_else(|| uri.authority().map(|a| a.as_str()).or_else(|| uri.host()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::header::HeaderValue;

    #[test]
    fn test_extract_authority_precedence() {
        let mut ws = HeaderWorkspace::empty(2);
        ws.insert(
            http::header::HOST,
            HeaderValue::from_static("udp-grpc.internal"),
        );
        let uri: Uri = "https://uri.example.com/pkg.Svc/Call".parse().unwrap();
        assert_eq!(extract_authority(&ws, &uri), Some("udp-grpc.internal"));

        let empty = HeaderWorkspace::empty(0);
        assert_eq!(extract_authority(&empty, &uri), Some("uri.example.com"));
    }
}
