//! Downstream gRPC Header Processing and Proxy Enrichment over TCP (RFC 9113 / RFC 7239).

use http::Uri;
use http::header::{HeaderMap, HeaderName, HeaderValue};

/// Prohibited connection-specific hop-by-hop headers per RFC 9113 Section 8.2.2.
pub const HOP_BY_HOP: [HeaderName; 5] = [
    http::header::CONNECTION,
    HeaderName::from_static("keep-alive"),
    HeaderName::from_static("proxy-connection"),
    http::header::TRANSFER_ENCODING,
    http::header::UPGRADE,
];

/// Enriches gRPC headers with authoritative proxy forwarding headers and strips
/// RFC 9113 hop-by-hop headers.
///
/// Overwrites standard proxy forwarding headers (`x-forwarded-*`, `x-real-ip`) directly with authoritative values.
pub fn enrich_headers(
    headers: &mut HeaderMap,
    peer_ip: u128,
    local_ip: u128,
    local_port_be: u16,
    is_tls: bool,
    authority: Option<&str>,
) {
    // 1. Strip RFC 9113 connection-specific hop-by-hop headers
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

    // 2. Inject authoritative client IP (directly overwriting spoofed headers)
    let proto = if is_tls { "https" } else { "http" };

    let (ip_buf, ip_len) = velda_core::lut::ip::format_ip_bytes(peer_ip);
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
        HeaderValue::from(u16::from_be(local_port_be)),
    );

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
        let client_str = std::str::from_utf8(client_ip_bytes).unwrap_or("");
        if velda_core::lut::ip::is_v4_u128(peer_ip) {
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

        let peer_ip = velda_core::lut::ip::v4_to_u128(0xC0A8_0101);
        let local_ip = velda_core::lut::ip::v4_to_u128(0x0A00_0001);
        let local_port_be = 443u16.to_be();

        enrich_headers(
            &mut headers,
            peer_ip,
            local_ip,
            local_port_be,
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
