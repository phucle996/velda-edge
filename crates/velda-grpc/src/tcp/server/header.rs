//! Downstream gRPC Header Processing and Proxy Enrichment over TCP (RFC 9113 / RFC 7239).

use std::net::SocketAddr;

use http::Uri;
use http::header::{HeaderMap, HeaderName, HeaderValue};

/// Standard client-supplied untrusted forwarding headers to strip upon ingress.
pub const UNTRUSTED_HEADERS: [HeaderName; 6] = [
    HeaderName::from_static("x-forwarded-for"),
    HeaderName::from_static("x-forwarded-proto"),
    HeaderName::from_static("x-forwarded-host"),
    HeaderName::from_static("x-forwarded-port"),
    HeaderName::from_static("x-real-ip"),
    HeaderName::from_static("forwarded"),
];

/// Prohibited connection-specific hop-by-hop headers per RFC 9113 Section 8.2.2.
pub const HOP_BY_HOP: [HeaderName; 5] = [
    http::header::CONNECTION,
    HeaderName::from_static("keep-alive"),
    HeaderName::from_static("proxy-connection"),
    http::header::TRANSFER_ENCODING,
    http::header::UPGRADE,
];

/// Enriches gRPC headers with authoritative proxy forwarding headers and strips
/// untrusted client-supplied headers and RFC 9113 hop-by-hop headers.
pub fn enrich_headers(
    headers: &mut HeaderMap,
    peer: SocketAddr,
    local_addr: SocketAddr,
    is_tls: bool,
    authority: Option<&str>,
) {
    // 1. Strip all client-supplied untrusted forwarding headers
    for name in &UNTRUSTED_HEADERS {
        headers.remove(name);
    }

    // Defensive sweep for custom "x-forwarded-*"
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

    // 2. Strip RFC 9113 connection-specific hop-by-hop headers
    for name in &HOP_BY_HOP {
        headers.remove(name);
    }
    if let Some(te_val) = headers.get(http::header::TE) {
        let is_trailers = te_val
            .to_str()
            .is_ok_and(|s| s.eq_ignore_ascii_case("trailers"));
        if !is_trailers {
            headers.remove(http::header::TE);
        }
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

    if let Some(auth) = authority
        && let Ok(val) = HeaderValue::from_str(auth)
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
        if let Some(auth) = authority {
            let _ = write!(cursor, ";host=\"{auth}\"");
        }
        cursor.position() as usize
    };
    let fwd_bytes = &fwd_buf[..fwd_len];
    if let Ok(val) = HeaderValue::from_bytes(fwd_bytes) {
        headers.insert(HeaderName::from_static("forwarded"), val);
    }
}

/// Extracts effective authority from request headers or URI authority.
#[inline]
pub fn extract_authority<'a>(headers: &'a HeaderMap, uri: &'a Uri) -> Option<&'a str> {
    headers
        .get(http::header::HOST)
        .and_then(|v| v.to_str().ok())
        .or_else(|| uri.authority().map(|a| a.as_str()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_enrich_grpc_forwarded_headers() {
        let mut headers = HeaderMap::new();
        headers.insert("connection", "keep-alive".parse().unwrap());
        headers.insert("x-forwarded-for", "1.2.3.4".parse().unwrap());

        let peer: SocketAddr = "192.168.1.1:1000".parse().unwrap();
        let local: SocketAddr = "10.0.0.1:443".parse().unwrap();

        enrich_headers(
            &mut headers,
            peer,
            local,
            true,
            Some("grpc.service.internal"),
        );

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
            "grpc.service.internal"
        );
        assert!(headers.get("connection").is_none());
    }
}
