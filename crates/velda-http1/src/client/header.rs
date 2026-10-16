//! Upstream HTTP/1.1 Header Sanitization, Hop-by-Hop Stripping, and SNI Resolution.
//!
//! Enforces:
//! - RFC 9112 Section 7.6.1 hop-by-hop header removal.
//! - Upstream target SNI / Host extraction with proper port stripping.

use std::borrow::Cow;
use std::net::SocketAddr;

use http::HeaderMap;
use http::header::HeaderName;

/// Standard hop-by-hop headers that must be stripped by intermediaries (RFC 9112 Section 7.6.1).
pub const HOP_BY_HOP: &[&str] = &[
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "proxy-connection",
    "te",
    "trailer",
    "trailers",
    "transfer-encoding",
    "upgrade",
];

#[inline(always)]
fn is_hop_by_hop(name: &HeaderName) -> bool {
    name == http::header::CONNECTION
        || name == http::header::TRANSFER_ENCODING
        || name == http::header::UPGRADE
        || name == http::header::TE
        || name == http::header::TRAILER
        || name == http::header::PROXY_AUTHENTICATE
        || name == http::header::PROXY_AUTHORIZATION
        || name.as_str().eq_ignore_ascii_case("keep-alive")
        || name.as_str().eq_ignore_ascii_case("proxy-connection")
        || name.as_str().eq_ignore_ascii_case("trailers")
}

/// Strips RFC 9112 hop-by-hop headers and any header nominated in the `Connection` header value.
pub fn sanitize_headers(headers: &mut HeaderMap) {
    if headers.is_empty() {
        return;
    }

    let mut custom_stack: [Option<HeaderName>; 16] = [const { None }; 16];
    let mut custom_count = 0;
    let mut custom_heap: Option<Vec<HeaderName>> = None;

    // Iterate across all Connection header entries (RFC 9110 Section 7.6.1)
    if headers.contains_key(http::header::CONNECTION) {
        for conn in headers.get_all(http::header::CONNECTION) {
            if let Ok(conn_str) = conn.to_str() {
                for part in conn_str.split(',') {
                    let trimmed = part.trim();
                    if !trimmed.is_empty()
                        && let Ok(name) = HeaderName::from_bytes(trimmed.as_bytes())
                    {
                        if custom_count < custom_stack.len() {
                            custom_stack[custom_count] = Some(name);
                            custom_count += 1;
                        } else {
                            custom_heap.get_or_insert_with(Vec::new).push(name);
                        }
                    }
                }
            }
        }
    }

    // Fast-path: Only inspect headers that are actually present in the map
    // instead of blindly invoking 10 hash table lookup probes!
    let mut to_remove_stack: [Option<HeaderName>; 8] = [const { None }; 8];
    let mut to_remove_count = 0;
    for (name, _) in headers.iter() {
        if is_hop_by_hop(name) && to_remove_count < to_remove_stack.len() {
            to_remove_stack[to_remove_count] = Some(name.clone());
            to_remove_count += 1;
        }
    }
    for name in to_remove_stack[..to_remove_count].iter().flatten() {
        headers.remove(name);
    }

    // Remove custom headers nominated by Connection header
    for name in custom_stack[..custom_count].iter().flatten() {
        headers.remove(name);
    }
    if let Some(heap) = custom_heap {
        for name in heap {
            headers.remove(name);
        }
    }
}

/// Strips the port component from a host string (e.g. `example.com:8080` -> `example.com`).
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

/// Extracts upstream SNI / host string from headers or URI.
pub fn extract_sni<'a>(
    headers: &'a HeaderMap,
    uri: &'a http::Uri,
    target: &SocketAddr,
) -> Cow<'a, str> {
    if let Some(host) = uri.host().filter(|h| !h.trim().is_empty()) {
        return Cow::Borrowed(host.trim());
    }

    if let Some(host_hdr) = headers
        .get(http::header::HOST)
        .and_then(|h| h.to_str().ok())
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
    {
        return Cow::Borrowed(strip_port(host_hdr));
    }

    Cow::Owned(target.ip().to_string())
}

/// Resolves the upstream TLS SNI / ServerName for an outbound connection.
pub fn resolve_sni<'a>(
    headers: &'a HeaderMap,
    uri: &'a http::Uri,
    target: &SocketAddr,
    explicit_sni: Option<&'a str>,
) -> Cow<'a, str> {
    if let Some(sni) = explicit_sni.filter(|s| !s.trim().is_empty()) {
        return Cow::Borrowed(sni.trim());
    }
    extract_sni(headers, uri, target)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sanitize_headers() {
        let mut headers = HeaderMap::new();
        headers.insert(http::header::CONNECTION, "x-foo, close".parse().unwrap());
        headers.insert(http::header::UPGRADE, "websocket".parse().unwrap());
        headers.insert(HeaderName::from_static("x-foo"), "bar".parse().unwrap());
        headers.insert(http::header::HOST, "example.com".parse().unwrap());

        sanitize_headers(&mut headers);

        assert!(!headers.contains_key(http::header::CONNECTION));
        assert!(!headers.contains_key(http::header::UPGRADE));
        assert!(!headers.contains_key("x-foo"));
        assert!(headers.contains_key(http::header::HOST));
    }

    #[test]
    fn test_strip_port() {
        assert_eq!(strip_port("example.com:80"), "example.com");
        assert_eq!(strip_port("example.com"), "example.com");
        assert_eq!(strip_port("[::1]:8080"), "::1");
        assert_eq!(strip_port("192.168.1.1:443"), "192.168.1.1");
    }
}
