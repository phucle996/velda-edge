//! Downstream HTTP/3 Header Processing, Anti-Spoofing, and Proxy Enrichment (RFC 9114).

use http::Uri;
use http::header::{HeaderMap, HeaderName, HeaderValue};
use velda_core::HeaderWorkspace;

/// Enriches headers in-place with authoritative proxy forwarding headers
/// and sanitizes RFC 9114 prohibited hop-by-hop headers.
///
/// Overwrites standard proxy forwarding headers (`x-forwarded-*`, `x-real-ip`, `forwarded`) directly with authoritative values.
pub fn enrich_headers(
    headers: &mut HeaderWorkspace,
    uri: &Uri,
    peer_ip: u128,
    local_ip: u128,
    local_port_be: u16,
) {
    // 1. Strip prohibited connection-specific headers per RFC 9114 Section 4.2
    crate::headers::sanitize_header_workspace(headers);

    // 2. Inject authoritative client IP (directly overwriting spoofed headers)
    // HTTP/3 QUIC is always TLS 1.3 encrypted
    let proto = "https";
    let (ip_buf, ip_len) = velda_core::lut::ip::format_ip_bytes(peer_ip);
    let client_ip_bytes = &ip_buf[..ip_len];
    let peer_is_v4 = velda_core::lut::ip::is_v4_u128(peer_ip);

    if let Ok(val) = HeaderValue::from_bytes(client_ip_bytes) {
        headers.insert(HeaderName::from_static("x-forwarded-for"), val.clone());
        headers.insert(HeaderName::from_static("x-real-ip"), val);
    }

    headers.insert(
        HeaderName::from_static("x-forwarded-proto"),
        HeaderValue::from_static(proto),
    );

    headers.insert(
        HeaderName::from_static("x-forwarded-port"),
        HeaderValue::from(u16::from_be(local_port_be)),
    );

    let host_hdr = headers.get(http::header::HOST).cloned();
    let host_str = host_hdr
        .as_ref()
        .and_then(|v| v.to_str().ok())
        .or_else(|| uri.authority().map(|a| a.as_str()))
        .or_else(|| uri.host());

    if let Some(h) = host_str
        && let Ok(val) = HeaderValue::from_str(h)
    {
        headers.insert(HeaderName::from_static("x-forwarded-host"), val.clone());
        if !headers.contains_key(http::header::HOST) {
            headers.insert(http::header::HOST, val);
        }
    }

    // RFC 7239 Forwarded
    let mut fwd_buf = [0u8; 256];
    let fwd_len = {
        use std::io::Write;
        let mut cursor = std::io::Cursor::new(&mut fwd_buf[..]);
        let client_str = std::str::from_utf8(client_ip_bytes).unwrap_or("");
        if peer_is_v4 {
            let _ = write!(cursor, "for={client_str}");
        } else {
            let _ = write!(cursor, "for=\"[{client_str}]\"");
        }
        let _ = write!(cursor, ";proto={proto};by=");
        let (local_buf, local_len) = velda_core::lut::ip::format_ip_bytes(local_ip);
        let local_str = std::str::from_utf8(&local_buf[..local_len]).unwrap_or("");
        if velda_core::lut::ip::is_v4_u128(local_ip) {
            let _ = write!(cursor, "{local_str}");
        } else {
            let _ = write!(cursor, "\"[{local_str}]\"");
        }
        if let Some(h) = host_str {
            let _ = write!(cursor, ";host=\"{h}\"");
        }
        cursor.position() as usize
    };
    let fwd_bytes = &fwd_buf[..fwd_len];
    if let Ok(val) = HeaderValue::from_bytes(fwd_bytes) {
        headers.insert(HeaderName::from_static("forwarded"), val);
    }
}

/// Extracts effective Host or authority string from request headers or URI.
#[inline]
pub fn extract_host<'a>(headers: &'a HeaderMap, uri: &'a Uri) -> Option<&'a str> {
    headers
        .get(http::header::HOST)
        .and_then(|v| v.to_str().ok())
        .or_else(|| uri.authority().map(|a| a.as_str()))
        .or_else(|| uri.host())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_enrich_h3_forwarded_headers() {
        let mut headers = HeaderWorkspace::default();
        headers.insert(
            HeaderName::from_static("x-forwarded-for"),
            "203.0.113.195".parse().unwrap(),
        );
        headers.insert(
            HeaderName::from_static("x-real-ip"),
            "203.0.113.195".parse().unwrap(),
        );
        headers.insert(
            HeaderName::from_static("x-forwarded-host"),
            "evil.attacker.com".parse().unwrap(),
        );
        headers.insert(
            HeaderName::from_static("x-forwarded-proto"),
            "http".parse().unwrap(),
        );
        headers.insert(
            HeaderName::from_static("x-forwarded-ssl"),
            "off".parse().unwrap(),
        );
        headers.insert(
            HeaderName::from_static("forwarded"),
            "for=203.0.113.195;proto=http".parse().unwrap(),
        );
        // RFC 9114 connection header to strip
        headers.insert(http::header::CONNECTION, "keep-alive".parse().unwrap());

        let uri: Uri = "https://secure.example.com/api".parse().unwrap();
        let peer_ip = velda_core::lut::ip::v4_to_u128(u32::from_be_bytes([192, 0, 2, 20]));
        let local_ip = velda_core::lut::ip::v4_to_u128(u32::from_be_bytes([10, 0, 0, 1]));

        enrich_headers(&mut headers, &uri, peer_ip, local_ip, 443u16.to_be());

        assert_eq!(headers.get("x-forwarded-for").unwrap(), "192.0.2.20");
        assert_eq!(headers.get("x-real-ip").unwrap(), "192.0.2.20");
        assert_eq!(headers.get("x-forwarded-proto").unwrap(), "https");
        assert_eq!(headers.get("x-forwarded-port").unwrap(), "443");
        assert_eq!(
            headers.get("x-forwarded-host").unwrap(),
            "secure.example.com"
        );
        assert_eq!(
            headers.get("forwarded").unwrap(),
            "for=192.0.2.20;proto=https;by=10.0.0.1;host=\"secure.example.com\""
        );
        assert!(headers.get("connection").is_none());
    }
}
