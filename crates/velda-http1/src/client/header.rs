//! Upstream HTTP/1.1 Header Sanitization and Hop-by-Hop Stripping.
//!
//! Enforces:
//! - RFC 9112 Section 7.6.1 hop-by-hop header removal.

use http::header::HeaderName;
use velda_core::HeaderWorkspace;

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

static KEEP_ALIVE: HeaderName = HeaderName::from_static("keep-alive");
static PROXY_CONNECTION: HeaderName = HeaderName::from_static("proxy-connection");
static TRAILERS: HeaderName = HeaderName::from_static("trailers");

#[inline(always)]
fn is_hop_by_hop(name: &HeaderName) -> bool {
    // Fast O(1) integer length branch: Hop-by-hop headers only exist at specific byte lengths:
    // 2 (te), 7 (upgrade, trailer), 8 (trailers), 10 (connection, keep-alive), 16 (proxy-connection),
    // 17 (transfer-encoding), 18 (proxy-authenticate), 19 (proxy-authorization).
    // All other lengths (e.g. host: 4, accept: 6, content-type: 12, authorization: 13,
    // content-length: 14, x-user-id: 9, x-forwarded-for: 15) immediately bail out via a single CPU branch!
    match name.as_str().len() {
        2 => name == http::header::TE,
        7 => name == http::header::UPGRADE || name == http::header::TRAILER,
        8 => name == TRAILERS,
        10 => name == http::header::CONNECTION || name == KEEP_ALIVE,
        16 => name == PROXY_CONNECTION,
        17 => name == http::header::TRANSFER_ENCODING,
        18 => name == http::header::PROXY_AUTHENTICATE,
        19 => name == http::header::PROXY_AUTHORIZATION,
        _ => false,
    }
}

/// Strips RFC 9112 hop-by-hop headers and any header nominated in the `Connection` header value.
pub fn sanitize_headers(headers: &mut HeaderWorkspace) {
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
                    if trimmed.is_empty()
                        || trimmed.eq_ignore_ascii_case("keep-alive")
                        || trimmed.eq_ignore_ascii_case("close")
                        || trimmed.eq_ignore_ascii_case("upgrade")
                    {
                        // Standard connection tokens: handled by hop-by-hop stripping, skip custom header parsing
                        continue;
                    }
                    if let Ok(name) = HeaderName::from_bytes(trimmed.as_bytes()) {
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

    // Fast-path: Collect up to 8 hop-by-hop headers on stack per pass; loop only if > 8 are present.
    loop {
        let mut to_remove_stack: [Option<HeaderName>; 8] = [const { None }; 8];
        let mut to_remove_count = 0usize;
        let mut hit_cap = false;
        for (name, _) in headers.iter() {
            if is_hop_by_hop(name) {
                if to_remove_count < to_remove_stack.len() {
                    to_remove_stack[to_remove_count] = Some(name.clone());
                    to_remove_count += 1;
                } else {
                    hit_cap = true;
                    break;
                }
            }
        }
        for name in to_remove_stack[..to_remove_count].iter().flatten() {
            headers.remove(name.as_str());
        }
        if !hit_cap {
            break;
        }
    }

    // Remove custom headers nominated by Connection header
    for name in custom_stack[..custom_count].iter().flatten() {
        headers.remove(name.as_str());
    }
    if let Some(heap) = custom_heap {
        for name in heap {
            headers.remove(name.as_str());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sanitize_headers() {
        let mut headers = HeaderWorkspace::empty(8);
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
    fn test_sanitize_all_10_hop_by_hop_headers() {
        let mut headers = HeaderWorkspace::empty(16);
        for &name in HOP_BY_HOP {
            headers.insert(
                HeaderName::from_static(name),
                http::HeaderValue::from_static("1"),
            );
        }
        headers.insert(
            http::header::HOST,
            http::HeaderValue::from_static("example.com"),
        );

        sanitize_headers(&mut headers);

        for &name in HOP_BY_HOP {
            assert!(
                !headers.contains_key(name),
                "hop-by-hop header {name} must be stripped"
            );
        }
        assert!(headers.contains_key(http::header::HOST));
    }
}
