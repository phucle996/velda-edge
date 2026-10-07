//! Downstream HTTP/2 Header Processing, Anti-Spoofing, and Proxy Enrichment (RFC 9113).

use std::net::SocketAddr;

use http::Uri;
use http::header::{HeaderMap, HeaderName, HeaderValue};

/// List of standard client-supplied untrusted forwarding headers to strip upon ingress.
pub const UNTRUSTED_HEADERS: [HeaderName; 6] = [
    HeaderName::from_static("x-forwarded-for"),
    HeaderName::from_static("x-forwarded-proto"),
    HeaderName::from_static("x-forwarded-host"),
    HeaderName::from_static("x-forwarded-port"),
    HeaderName::from_static("x-real-ip"),
    HeaderName::from_static("forwarded"),
];

/// Enriches HTTP/2 request headers with RFC 7239 and standard proxy forwarding metadata.
pub fn enrich_headers(
    headers: &mut HeaderMap,
    uri: &Uri,
    peer: SocketAddr,
    local_addr: SocketAddr,
    is_tls: bool,
) {
    // 1. Strip prohibited connection-specific headers per RFC 9113 Section 8.2.2
    crate::headers::sanitize_h2_headers(headers);

    // 2. Strip standard client-supplied untrusted forwarding headers
    for name in &UNTRUSTED_HEADERS {
        headers.remove(name);
    }

    // 3. Defensive sweep for custom "x-forwarded-*"
    let mut custom_to_remove: [Option<HeaderName>; 8] = [const { None }; 8];
    let mut count = 0;
    for key in headers.keys() {
        if key.as_str().starts_with("x-forwarded-") && count < custom_to_remove.len() {
            custom_to_remove[count] = Some(key.clone());
            count += 1;
        }
    }
    for name in custom_to_remove[..count].iter().flatten() {
        headers.remove(name);
    }

    let client_ip = peer.ip();
    let proto = if is_tls { "https" } else { "http" };

    let mut ip_buf = [0u8; 64];
    let ip_len = {
        use std::io::Write;
        let mut cursor = std::io::Cursor::new(&mut ip_buf[..]);
        let _ = write!(cursor, "{}", client_ip);
        cursor.position() as usize
    };
    let client_ip_bytes = &ip_buf[..ip_len];

    if let Ok(val) = HeaderValue::from_bytes(client_ip_bytes) {
        headers.insert(HeaderName::from_static("x-forwarded-for"), val.clone());
        headers.insert(HeaderName::from_static("x-real-ip"), val);
    }

    headers.insert(
        HeaderName::from_static("x-forwarded-proto"),
        HeaderValue::from_static(proto),
    );

    let mut port_buf = [0u8; 8];
    let port_len = {
        use std::io::Write;
        let mut cursor = std::io::Cursor::new(&mut port_buf[..]);
        let _ = write!(cursor, "{}", local_addr.port());
        cursor.position() as usize
    };
    if let Ok(val) = HeaderValue::from_bytes(&port_buf[..port_len]) {
        headers.insert(HeaderName::from_static("x-forwarded-port"), val);
    }

    let host_hdr = headers.get(http::header::HOST).cloned();
    let host_str = host_hdr
        .as_ref()
        .and_then(|v| v.to_str().ok())
        .or_else(|| uri.authority().map(|a| a.as_str()))
        .or_else(|| uri.host());

    if let Some(h) = host_str
        && let Ok(val) = HeaderValue::from_str(h)
    {
        headers.insert(HeaderName::from_static("x-forwarded-host"), val);
    }

    // RFC 7239 Forwarded
    let mut fwd_buf = [0u8; 256];
    let fwd_len = {
        use std::io::Write;
        let mut cursor = std::io::Cursor::new(&mut fwd_buf[..]);
        match client_ip {
            std::net::IpAddr::V4(v4) => {
                let _ = write!(cursor, "for={v4}");
            }
            std::net::IpAddr::V6(v6) => {
                let _ = write!(cursor, "for=\"[{v6}]\"");
            }
        }
        let _ = write!(cursor, ";proto={proto};by=");
        match local_addr.ip() {
            std::net::IpAddr::V4(v4) => {
                let _ = write!(cursor, "{v4}");
            }
            std::net::IpAddr::V6(v6) => {
                let _ = write!(cursor, "\"[{v6}]\"");
            }
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

/// Extracts effective Host or authority string from request headers (Host) or URI.
#[inline]
pub fn extract_host<'a>(headers: &'a HeaderMap, uri: &'a Uri) -> Option<&'a str> {
    headers
        .get(http::header::HOST)
        .and_then(|v| v.to_str().ok())
        .or_else(|| uri.authority().map(|a| a.as_str()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_enrich_forwarded_headers() {
        let mut headers = HeaderMap::new();
        headers.insert(http::header::HOST, "api.example.com".parse().unwrap());
        headers.insert("connection", "keep-alive".parse().unwrap()); // Prohibited in H2
        headers.insert("x-forwarded-for", "1.2.3.4".parse().unwrap());

        let uri: Uri = "/".parse().unwrap();
        let peer: SocketAddr = "192.168.1.1:1000".parse().unwrap();
        let local: SocketAddr = "10.0.0.1:443".parse().unwrap();

        enrich_headers(&mut headers, &uri, peer, local, true);

        assert_eq!(
            headers.get("x-forwarded-for").unwrap().to_str().unwrap(),
            "192.168.1.1"
        );
        assert_eq!(
            headers.get("x-forwarded-proto").unwrap().to_str().unwrap(),
            "https"
        );
        assert_eq!(
            headers.get("x-forwarded-host").unwrap().to_str().unwrap(),
            "api.example.com"
        );
        assert!(headers.get("connection").is_none());
    }
}
