//! Downstream HTTP/1.1 Header Processing, Anti-Spoofing, and Proxy Enrichment.
//!
//! Enforces:
//! - Stripping of untrusted downstream forwarding headers (`X-Forwarded-*`, `Forwarded`, `X-Real-IP`).
//! - Injection of authoritative proxy forwarding headers (RFC 7239 + `X-Forwarded-*`).
//! - Validation and canonical lookup of downstream header names.
//! - Authoritative extraction of Host / :authority.

use std::net::SocketAddr;

use http::header::{CONTENT_LENGTH, HeaderName, HeaderValue};
use http::{HeaderMap, Uri};

use crate::error::Http1Error;

/// Enriches HTTP/1.1 request headers with authoritative proxy forwarding metadata.
///
/// Overwrites standard proxy forwarding headers (`x-forwarded-*`, `x-real-ip`) directly with authoritative values.
pub fn enrich_headers(
    headers: &mut HeaderMap,
    uri: &Uri,
    peer: SocketAddr,
    local_addr: SocketAddr,
    is_tls: bool,
) {
    // 1. Inject authoritative client IP (directly overwriting any client-supplied spoofed values)
    let client_ip = peer.ip();
    let proto = if is_tls { "https" } else { "http" };

    let mut ip_buf = [0u8; 64];
    let client_ip_bytes: &[u8] = match client_ip {
        std::net::IpAddr::V4(v4) => {
            let octets = v4.octets();
            let mut cursor = 0;
            for (idx, &octet) in octets.iter().enumerate() {
                if idx > 0 {
                    ip_buf[cursor] = b'.';
                    cursor += 1;
                }
                if octet >= 100 {
                    ip_buf[cursor] = b'0' + octet / 100;
                    cursor += 1;
                    ip_buf[cursor] = b'0' + (octet / 10) % 10;
                    cursor += 1;
                    ip_buf[cursor] = b'0' + octet % 10;
                    cursor += 1;
                } else if octet >= 10 {
                    ip_buf[cursor] = b'0' + octet / 10;
                    cursor += 1;
                    ip_buf[cursor] = b'0' + octet % 10;
                    cursor += 1;
                } else {
                    ip_buf[cursor] = b'0' + octet;
                    cursor += 1;
                }
            }
            &ip_buf[..cursor]
        }
        std::net::IpAddr::V6(v6) => {
            use std::io::Write;
            let mut cursor = std::io::Cursor::new(&mut ip_buf[..]);
            let _ = write!(cursor, "{}", v6);
            let len = cursor.position() as usize;
            &ip_buf[..len]
        }
    };

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
        .or_else(|| uri.host());

    if let Some(h) = host_str
        && let Ok(val) = HeaderValue::from_str(h)
    {
        headers.insert(HeaderName::from_static("x-forwarded-host"), val);
    }

    // Build RFC 7239 Forwarded header without dynamic trait formatting overhead
    let mut fwd_buf = [0u8; 256];
    let mut cursor = 0;

    fwd_buf[cursor..cursor + 4].copy_from_slice(b"for=");
    cursor += 4;
    match client_ip {
        std::net::IpAddr::V4(_) => {
            fwd_buf[cursor..cursor + client_ip_bytes.len()].copy_from_slice(client_ip_bytes);
            cursor += client_ip_bytes.len();
        }
        std::net::IpAddr::V6(_) => {
            fwd_buf[cursor..cursor + 2].copy_from_slice(b"\"[");
            cursor += 2;
            fwd_buf[cursor..cursor + client_ip_bytes.len()].copy_from_slice(client_ip_bytes);
            cursor += client_ip_bytes.len();
            fwd_buf[cursor..cursor + 2].copy_from_slice(b"]\"");
            cursor += 2;
        }
    }

    fwd_buf[cursor..cursor + 7].copy_from_slice(b";proto=");
    cursor += 7;
    let proto_bytes = proto.as_bytes();
    fwd_buf[cursor..cursor + proto_bytes.len()].copy_from_slice(proto_bytes);
    cursor += proto_bytes.len();

    fwd_buf[cursor..cursor + 4].copy_from_slice(b";by=");
    cursor += 4;
    match local_addr.ip() {
        std::net::IpAddr::V4(v4) => {
            let octets = v4.octets();
            for (idx, &octet) in octets.iter().enumerate() {
                if idx > 0 {
                    fwd_buf[cursor] = b'.';
                    cursor += 1;
                }
                if octet >= 100 {
                    fwd_buf[cursor] = b'0' + octet / 100;
                    cursor += 1;
                    fwd_buf[cursor] = b'0' + (octet / 10) % 10;
                    cursor += 1;
                    fwd_buf[cursor] = b'0' + octet % 10;
                    cursor += 1;
                } else if octet >= 10 {
                    fwd_buf[cursor] = b'0' + octet / 10;
                    cursor += 1;
                    fwd_buf[cursor] = b'0' + octet % 10;
                    cursor += 1;
                } else {
                    fwd_buf[cursor] = b'0' + octet;
                    cursor += 1;
                }
            }
        }
        std::net::IpAddr::V6(v6) => {
            use std::io::Write;
            let mut c = std::io::Cursor::new(&mut fwd_buf[cursor..]);
            let _ = write!(c, "\"[{v6}]\"");
            cursor += c.position() as usize;
        }
    }

    if let Some(h) = host_str {
        fwd_buf[cursor..cursor + 7].copy_from_slice(b";host=\"");
        cursor += 7;
        let h_bytes = h.as_bytes();
        if cursor + h_bytes.len() < fwd_buf.len() {
            fwd_buf[cursor..cursor + h_bytes.len()].copy_from_slice(h_bytes);
            cursor += h_bytes.len();
            fwd_buf[cursor] = b'"';
            cursor += 1;
        }
    }

    if let Ok(val) = HeaderValue::from_bytes(&fwd_buf[..cursor]) {
        headers.insert(HeaderName::from_static("forwarded"), val);
    }
}

/// Extracts effective Host or authority string from request headers or URI.
#[inline]
pub fn extract_host<'a>(headers: &'a HeaderMap, uri: &'a Uri) -> Option<&'a str> {
    headers
        .get(http::header::HOST)
        .and_then(|v| v.to_str().ok())
        .or_else(|| uri.host())
}

/// Matches downstream header names against common well-known ASCII tokens to avoid dynamic allocations.
pub fn match_header_name(name_bytes: &[u8]) -> Result<HeaderName, Http1Error> {
    match name_bytes.len() {
        4 if name_bytes.eq_ignore_ascii_case(b"host") => Ok(http::header::HOST),
        6 => {
            if name_bytes.eq_ignore_ascii_case(b"accept") {
                Ok(http::header::ACCEPT)
            } else if name_bytes.eq_ignore_ascii_case(b"cookie") {
                Ok(http::header::COOKIE)
            } else if name_bytes.eq_ignore_ascii_case(b"origin") {
                Ok(http::header::ORIGIN)
            } else {
                HeaderName::from_bytes(name_bytes)
                    .map_err(|e| Http1Error::InvalidHeader(e.to_string()))
            }
        }
        7 if name_bytes.eq_ignore_ascii_case(b"referer") => Ok(http::header::REFERER),
        10 => {
            if name_bytes.eq_ignore_ascii_case(b"connection") {
                Ok(http::header::CONNECTION)
            } else if name_bytes.eq_ignore_ascii_case(b"user-agent") {
                Ok(http::header::USER_AGENT)
            } else {
                HeaderName::from_bytes(name_bytes)
                    .map_err(|e| Http1Error::InvalidHeader(e.to_string()))
            }
        }
        12 if name_bytes.eq_ignore_ascii_case(b"content-type") => Ok(http::header::CONTENT_TYPE),
        13 => {
            if name_bytes.eq_ignore_ascii_case(b"authorization") {
                Ok(http::header::AUTHORIZATION)
            } else if name_bytes.eq_ignore_ascii_case(b"if-none-match") {
                Ok(http::header::IF_NONE_MATCH)
            } else {
                HeaderName::from_bytes(name_bytes)
                    .map_err(|e| Http1Error::InvalidHeader(e.to_string()))
            }
        }
        14 if name_bytes.eq_ignore_ascii_case(b"content-length") => Ok(CONTENT_LENGTH),
        15 => {
            if name_bytes.eq_ignore_ascii_case(b"accept-encoding") {
                Ok(http::header::ACCEPT_ENCODING)
            } else if name_bytes.eq_ignore_ascii_case(b"accept-language") {
                Ok(http::header::ACCEPT_LANGUAGE)
            } else {
                HeaderName::from_bytes(name_bytes)
                    .map_err(|e| Http1Error::InvalidHeader(e.to_string()))
            }
        }
        17 => {
            if name_bytes.eq_ignore_ascii_case(b"transfer-encoding") {
                Ok(http::header::TRANSFER_ENCODING)
            } else if name_bytes.eq_ignore_ascii_case(b"if-modified-since") {
                Ok(http::header::IF_MODIFIED_SINCE)
            } else {
                HeaderName::from_bytes(name_bytes)
                    .map_err(|e| Http1Error::InvalidHeader(e.to_string()))
            }
        }
        25 if name_bytes.eq_ignore_ascii_case(b"upgrade-insecure-requests") => {
            Ok(http::header::UPGRADE_INSECURE_REQUESTS)
        }
        _ => {
            HeaderName::from_bytes(name_bytes).map_err(|e| Http1Error::InvalidHeader(e.to_string()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_enrich_forwarded_headers() {
        let mut headers = HeaderMap::new();
        headers.insert(http::header::HOST, "example.com".parse().unwrap());
        headers.insert(
            HeaderName::from_static("x-forwarded-for"),
            "1.2.3.4".parse().unwrap(),
        );

        let uri: Uri = "/test".parse().unwrap();
        let peer: SocketAddr = "192.168.1.50:12345".parse().unwrap();
        let local: SocketAddr = "10.0.0.1:80".parse().unwrap();

        enrich_headers(&mut headers, &uri, peer, local, false);

        assert_eq!(
            headers.get("x-forwarded-for").unwrap().to_str().unwrap(),
            "192.168.1.50"
        );
        assert_eq!(
            headers.get("x-forwarded-proto").unwrap().to_str().unwrap(),
            "http"
        );
        assert_eq!(
            headers.get("x-forwarded-host").unwrap().to_str().unwrap(),
            "example.com"
        );
    }
}
