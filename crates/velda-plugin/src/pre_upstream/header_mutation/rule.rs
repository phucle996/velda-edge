//! RFC 9110 and RFC 7230 Hop-by-Hop Header Stripping Engine.
//!
//! Enforces edge hygiene by stripping connection-specific headers before
//! proxying requests downstream or upstream.

use http::header::HeaderName;
use velda_core::HeaderWorkspace;

/// Strips RFC 9110 / RFC 7230 hop-by-hop headers from a mutable headers map.
#[inline]
pub fn strip_hop_by_hop_headers(headers: &mut HeaderWorkspace) {
    // Collect custom connection-token headers if specified in Connection header
    if let Some(raw) = headers
        .get(http::header::CONNECTION)
        .and_then(|v| v.to_str().ok())
    {
        // Optimization: Zero-allocation stack buffer [Option<HeaderName>; 8] to avoid heap allocation
        // while safely separating the immutable string slice borrow from the mutable headers map modification.
        let mut custom_tokens = [const { None }; 8];
        let mut count = 0;
        for token in raw.split(',') {
            let trimmed = token.trim();
            if !trimmed.is_empty()
                && !trimmed.eq_ignore_ascii_case("close")
                && !trimmed.eq_ignore_ascii_case("keep-alive")
                && let Ok(name) = HeaderName::from_bytes(trimmed.as_bytes())
                && count < custom_tokens.len()
            {
                custom_tokens[count] = Some(name);
                count += 1;
            }
        }

        for name in custom_tokens.into_iter().flatten() {
            headers.remove(name);
        }
    }

    // Optimization: Pre-parsed static HeaderName to avoid dynamic header parsing per request
    const KEEP_ALIVE: HeaderName = HeaderName::from_static("keep-alive");

    // Standard RFC Hop-by-Hop headers
    headers.remove(http::header::CONNECTION);
    headers.remove(&KEEP_ALIVE);
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
    use velda_core::HeaderWorkspace;

    #[test]
    fn test_strip_standard_hop_by_hop() {
        let mut headers = HeaderWorkspace::default();
        headers.insert(
            http::header::CONNECTION,
            HeaderValue::from_static("keep-alive"),
        );
        headers.insert(http::header::UPGRADE, HeaderValue::from_static("websocket"));
        headers.insert(
            HeaderName::from_static("keep-alive"),
            HeaderValue::from_static("timeout=5"),
        );
        headers.insert(
            HeaderName::from_static("x-custom-header"),
            HeaderValue::from_static("preserve-me"),
        );

        strip_hop_by_hop_headers(&mut headers);

        assert!(!headers.contains_key(http::header::CONNECTION));
        assert!(!headers.contains_key(http::header::UPGRADE));
        assert!(!headers.contains_key("keep-alive"));
        assert_eq!(headers.get("x-custom-header").unwrap(), "preserve-me");
    }

    #[test]
    fn test_strip_custom_connection_tokens() {
        let mut headers = HeaderWorkspace::default();
        headers.insert(
            http::header::CONNECTION,
            HeaderValue::from_static("close, X-Foo, X-Bar"),
        );
        headers.insert(
            HeaderName::from_static("x-foo"),
            HeaderValue::from_static("value1"),
        );
        headers.insert(
            HeaderName::from_static("x-bar"),
            HeaderValue::from_static("value2"),
        );
        headers.insert(
            HeaderName::from_static("x-baz"),
            HeaderValue::from_static("value3"),
        );

        strip_hop_by_hop_headers(&mut headers);

        assert!(!headers.contains_key(http::header::CONNECTION));
        assert!(!headers.contains_key("x-foo"));
        assert!(!headers.contains_key("x-bar"));
        assert_eq!(headers.get("x-baz").unwrap(), "value3");
    }
}
