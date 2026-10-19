//! RFC 9110 and RFC 7230 Hop-by-Hop Header Stripping Engine.
//!
//! Enforces edge hygiene by stripping connection-specific headers before
//! proxying requests downstream or upstream.

use http::HeaderMap;
use http::header::HeaderName;

/// Strips RFC 9110 / RFC 7230 hop-by-hop headers from a mutable headers map.
#[inline]
pub fn strip_hop_by_hop_headers(headers: &mut HeaderMap) {
    // Collect custom connection-token headers if specified in Connection header
    if let Some(raw) = headers
        .get(http::header::CONNECTION)
        .and_then(|v| v.to_str().ok())
    {
        let custom_tokens: Vec<String> = raw
            .split(',')
            .map(|token| token.trim().to_ascii_lowercase())
            .filter(|token| !token.is_empty() && token != "close" && token != "keep-alive")
            .collect();

        for token in custom_tokens {
            if let Ok(name) = HeaderName::from_bytes(token.as_bytes()) {
                headers.remove(name);
            }
        }
    }

    // Standard RFC Hop-by-Hop headers
    headers.remove(http::header::CONNECTION);
    headers.remove("keep-alive");
    headers.remove(http::header::PROXY_AUTHENTICATE);
    headers.remove(http::header::PROXY_AUTHORIZATION);
    headers.remove(http::header::TE);
    headers.remove(http::header::TRAILER);
    headers.remove(http::header::TRANSFER_ENCODING);
    headers.remove(http::header::UPGRADE);
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderValue;

    #[test]
    fn test_strip_standard_hop_by_hop() {
        let mut headers = HeaderMap::new();
        headers.insert(
            http::header::CONNECTION,
            HeaderValue::from_static("keep-alive"),
        );
        headers.insert(http::header::UPGRADE, HeaderValue::from_static("websocket"));
        headers.insert("keep-alive", HeaderValue::from_static("timeout=5"));
        headers.insert("x-custom-header", HeaderValue::from_static("preserve-me"));

        strip_hop_by_hop_headers(&mut headers);

        assert!(!headers.contains_key(http::header::CONNECTION));
        assert!(!headers.contains_key(http::header::UPGRADE));
        assert!(!headers.contains_key("keep-alive"));
        assert_eq!(headers.get("x-custom-header").unwrap(), "preserve-me");
    }

    #[test]
    fn test_strip_custom_connection_tokens() {
        let mut headers = HeaderMap::new();
        headers.insert(
            http::header::CONNECTION,
            HeaderValue::from_static("close, X-Foo, X-Bar"),
        );
        headers.insert("x-foo", HeaderValue::from_static("value1"));
        headers.insert("x-bar", HeaderValue::from_static("value2"));
        headers.insert("x-baz", HeaderValue::from_static("value3"));

        strip_hop_by_hop_headers(&mut headers);

        assert!(!headers.contains_key(http::header::CONNECTION));
        assert!(!headers.contains_key("x-foo"));
        assert!(!headers.contains_key("x-bar"));
        assert_eq!(headers.get("x-baz").unwrap(), "value3");
    }
}
