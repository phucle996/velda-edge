//! Downstream HTTP/2 Header Processing, Anti-Spoofing, and Proxy Enrichment (RFC 9113).

use std::net::SocketAddr;

use http::Uri;
use http::header::{HeaderMap, HeaderName, HeaderValue};

/// Enriches HTTP/2 request headers with authoritative proxy forwarding metadata.
///
/// Overwrites standard proxy forwarding headers (`x-forwarded-*`, `x-real-ip`) directly with authoritative values.
pub fn enrich_headers(
    headers: &mut HeaderMap,
    uri: &Uri,
    peer: SocketAddr,
    local_addr: SocketAddr,
    is_tls: bool,
) {
    // 1. Strip prohibited connection-specific headers per RFC 9113 Section 8.2.2
    crate::headers::sanitize_h2_headers(headers);

    // 2. Inject authoritative client IP (directly overwriting any client-supplied values)
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

    headers.insert(
        HeaderName::from_static("x-forwarded-port"),
        HeaderValue::from(local_addr.port()),
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

/// Enriches HTTP/2 request headers with pre-computed connection-level metadata.
///
/// Completely eliminates per-stream IP/port string formatting and buffer writes.
#[inline]
pub fn enrich_headers_precomputed(
    headers: &mut HeaderMap,
    uri: &Uri,
    client_ip: &HeaderValue,
    client_port: &HeaderValue,
    is_tls: bool,
) {
    // 1. Strip prohibited connection-specific headers per RFC 9113 Section 8.2.2
    crate::headers::sanitize_h2_headers(headers);

    // 2. Inject authoritative pre-computed forwarding headers (zero formatting)
    let proto = if is_tls { "https" } else { "http" };

    headers.insert(
        HeaderName::from_static("x-forwarded-for"),
        client_ip.clone(),
    );
    headers.insert(HeaderName::from_static("x-real-ip"), client_ip.clone());
    headers.insert(
        HeaderName::from_static("x-forwarded-proto"),
        HeaderValue::from_static(proto),
    );
    headers.insert(
        HeaderName::from_static("x-forwarded-port"),
        client_port.clone(),
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
        headers.insert(HeaderName::from_static("x-forwarded-host"), val);
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
