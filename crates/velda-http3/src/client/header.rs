//! Upstream HTTP/3 Client Header Processing and RFC 9114 Sanitization.

use std::net::SocketAddr;

pub use crate::headers::sanitize_headers;
use velda_core::L7Request;

/// Strips the port component from a host string (e.g., `example.com:8443` -> `example.com`,
/// `[::1]:8443` -> `::1`).
#[inline]
pub fn strip_port(host: &str) -> &str {
    if let Some(stripped) = host.strip_prefix('[')
        && let Some(end_bracket) = stripped.find(']')
    {
        return &stripped[..end_bracket];
    }

    if let Some(colon_idx) = host.rfind(':') {
        if host[..colon_idx].contains(':') {
            host
        } else {
            &host[..colon_idx]
        }
    } else {
        host
    }
}

/// Extracts effective SNI / Host from an outbound request or fallback target IP.
#[inline]
pub fn extract_sni<'a>(req: &'a L7Request, target: &SocketAddr) -> std::borrow::Cow<'a, str> {
    if let Some(host) = req.uri.host() {
        return std::borrow::Cow::Borrowed(host);
    }

    if let Some(host_hdr) = req
        .headers
        .get(http::header::HOST)
        .and_then(|v| v.to_str().ok())
    {
        return std::borrow::Cow::Borrowed(strip_port(host_hdr));
    }

    std::borrow::Cow::Owned(target.ip().to_string())
}

/// Resolves the upstream TLS SNI / ServerName for an HTTP/3 outbound connection.
///
/// Precedence:
/// 1. Explicit upstream configuration `explicit_sni` (e.g. from upstream TLS target_sni)
/// 2. Request URI host (`req.uri.host()`)
/// 3. Request `Host` header (stripping `:port` if present)
/// 4. Target IP address string (`target.ip().to_string()`)
pub fn resolve_sni<'a>(
    req: &'a L7Request,
    target: &SocketAddr,
    explicit_sni: Option<&'a str>,
) -> std::borrow::Cow<'a, str> {
    if let Some(sni) = explicit_sni.filter(|s| !s.trim().is_empty()) {
        return std::borrow::Cow::Borrowed(sni.trim());
    }
    extract_sni(req, target)
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{HeaderMap, Method, Uri, Version};
    use velda_core::Body;

    #[test]
    fn test_strip_port() {
        assert_eq!(strip_port("example.com"), "example.com");
        assert_eq!(strip_port("example.com:8443"), "example.com");
        assert_eq!(strip_port("192.168.1.100"), "192.168.1.100");
        assert_eq!(strip_port("192.168.1.100:443"), "192.168.1.100");
        assert_eq!(strip_port("[::1]:8443"), "::1");
        assert_eq!(strip_port("[2001:db8::1]:443"), "2001:db8::1");
        assert_eq!(strip_port("::1"), "::1");
    }

    #[test]
    fn test_resolve_sni_precedence() {
        let target: SocketAddr = "10.0.0.1:4433".parse().unwrap();

        // 1. Explicit SNI overrides everything
        let req1 = L7Request::new(
            Method::GET,
            "https://uri-host.internal/api".parse::<Uri>().unwrap(),
            Version::HTTP_3,
            {
                let mut h = HeaderMap::new();
                h.insert(http::header::HOST, "header-host.com:8443".parse().unwrap());
                h
            },
            Body::Empty,
        );
        assert_eq!(
            resolve_sni(&req1, &target, Some("explicit-backend.internal")),
            "explicit-backend.internal"
        );

        // 2. URI host takes precedence when explicit SNI is None
        assert_eq!(resolve_sni(&req1, &target, None), "uri-host.internal");

        // 3. Host header with port stripped takes precedence when URI host is absent
        let req2 = L7Request::new(
            Method::GET,
            "/api/v1/orders".parse::<Uri>().unwrap(),
            Version::HTTP_3,
            {
                let mut h = HeaderMap::new();
                h.insert(
                    http::header::HOST,
                    "my-service.internal:9443".parse().unwrap(),
                );
                h
            },
            Body::Empty,
        );
        assert_eq!(resolve_sni(&req2, &target, None), "my-service.internal");

        // 4. Target IP fallback when no SNI, URI host, or Host header exists
        let req3 = L7Request::new(
            Method::GET,
            "/health".parse::<Uri>().unwrap(),
            Version::HTTP_3,
            HeaderMap::new(),
            Body::Empty,
        );
        assert_eq!(resolve_sni(&req3, &target, None), "10.0.0.1");
    }
}
