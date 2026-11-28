//! RFC 9113 Header Sanitization over [`HeaderWorkspace`] for HTTP/2.
//!
//! Section 8.2.2 mandates that connection-specific header fields MUST NOT appear
//! in HTTP/2 requests or responses:
//! - `connection`
//! - `keep-alive`
//! - `proxy-connection`
//! - `transfer-encoding`
//! - `upgrade`
//!
//! The `TE` header field MAY be present only if its value is `"trailers"`.
//! Pseudo-headers (starting with `':'`) must also be excluded from regular header maps.

use http::header::{HeaderName, HeaderValue};
use velda_core::buffer::header::HeaderWorkspace;

/// Returns true if a header field is prohibited under RFC 9113 Section 8.2.2.
#[inline]
pub fn is_disallowed_h2_header(name: &HeaderName, value: &HeaderValue) -> bool {
    let s = name.as_str();
    if s.starts_with(':') {
        return true;
    }
    match s.len() {
        2 if name == http::header::TE => !value
            .to_str()
            .is_ok_and(|v| v.trim().eq_ignore_ascii_case("trailers")),
        7 => name == http::header::UPGRADE,
        10 => name == http::header::CONNECTION || s == "keep-alive",
        16 => s == "proxy-connection",
        17 => name == http::header::TRANSFER_ENCODING,
        _ => false,
    }
}

/// Copies all RFC 9113 compliant headers from `headers` into a new sanitized [`HeaderWorkspace`],
/// preserving duplicate multi-value fields (e.g. `set-cookie`, `cookie`, `via`).
#[inline]
pub fn sanitize_workspace_headers(headers: &HeaderWorkspace) -> HeaderWorkspace {
    let mut out = HeaderWorkspace::empty(headers.len().max(1));
    for (name, val) in headers.iter() {
        if !is_disallowed_h2_header(name, val) {
            out.append(name.clone(), val.clone());
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::header::{CONNECTION, CONTENT_TYPE, TE, TRANSFER_ENCODING, UPGRADE};

    #[test]
    fn test_removes_hop_by_hop_and_keeps_te_trailers() {
        let mut ws = HeaderWorkspace::empty(8);
        ws.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        ws.insert(CONNECTION, HeaderValue::from_static("keep-alive"));
        ws.insert(
            HeaderName::from_static("keep-alive"),
            HeaderValue::from_static("timeout=5"),
        );
        ws.insert(
            HeaderName::from_static("proxy-connection"),
            HeaderValue::from_static("keep-alive"),
        );
        ws.insert(TRANSFER_ENCODING, HeaderValue::from_static("chunked"));
        ws.insert(UPGRADE, HeaderValue::from_static("websocket"));
        ws.insert(TE, HeaderValue::from_static("trailers"));

        let clean = sanitize_workspace_headers(&ws);
        assert_eq!(clean.get(CONTENT_TYPE).unwrap(), "application/json");
        assert_eq!(clean.get(TE).unwrap(), "trailers");
        assert!(clean.get(CONNECTION).is_none());
        assert!(clean.get("keep-alive").is_none());
        assert!(clean.get("proxy-connection").is_none());
        assert!(clean.get(TRANSFER_ENCODING).is_none());
        assert!(clean.get(UPGRADE).is_none());

        let mut bad_te = HeaderWorkspace::empty(2);
        bad_te.insert(TE, HeaderValue::from_static("gzip, deflate"));
        assert!(sanitize_workspace_headers(&bad_te).get(TE).is_none());

        let mut multi = HeaderWorkspace::empty(4);
        multi.append(
            http::header::SET_COOKIE,
            HeaderValue::from_static("a=1; Path=/"),
        );
        multi.append(
            http::header::SET_COOKIE,
            HeaderValue::from_static("b=2; Path=/"),
        );
        let sanitized_multi = sanitize_workspace_headers(&multi);
        let cookies: Vec<_> = sanitized_multi.get_all(http::header::SET_COOKIE).collect();
        assert_eq!(cookies.len(), 2);
    }
}
