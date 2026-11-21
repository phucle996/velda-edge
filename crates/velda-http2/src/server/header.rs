//! Downstream HTTP/2 Header Processing, Anti-Spoofing, and Proxy Enrichment (RFC 9113).

use http::Uri;
use http::header::{HeaderMap, HeaderName, HeaderValue};

/// Enriches HTTP/2 request headers with authoritative proxy forwarding metadata.
///
/// Overwrites standard proxy forwarding headers (`x-forwarded-*`, `x-real-ip`) directly with authoritative values.
pub fn enrich_headers(
    headers: &mut HeaderMap,
    uri: &Uri,
    peer_ip: u128,
    local_ip: u128,
    local_port_be: u16,
    is_tls: bool,
) {
    // 1. Strip prohibited connection-specific headers per RFC 9113 Section 8.2.2
    crate::headers::sanitize_h2_headers(headers);

    // 2. Inject authoritative client IP (directly overwriting any client-supplied values)
    let proto = if is_tls { "https" } else { "http" };
    let (ip_buf, ip_len) = velda_core::lut::ip::format_ip_bytes(peer_ip);
    let client_ip_bytes = &ip_buf[..ip_len];
    let peer_is_v4 = velda_core::lut::ip::is_v4_u128(peer_ip);

    if let Ok(val) = HeaderValue::from_bytes(client_ip_bytes) {
        if let Some(existing) = headers.get(HeaderName::from_static("x-forwarded-for")) {
            let mut combined =
                Vec::with_capacity(existing.as_bytes().len() + 2 + val.as_bytes().len());
            combined.extend_from_slice(existing.as_bytes());
            combined.extend_from_slice(b", ");
            combined.extend_from_slice(val.as_bytes());
            if let Ok(comb_val) = HeaderValue::from_maybe_shared(bytes::Bytes::from(combined)) {
                headers.insert(HeaderName::from_static("x-forwarded-for"), comb_val);
            }
        } else {
            headers.insert(HeaderName::from_static("x-forwarded-for"), val.clone());
        }

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
        headers.insert(HeaderName::from_static("x-forwarded-host"), val);
    }

    // RFC 7239 Forwarded
    let mut fwd_buf = [0u8; 256];
    let fwd_len = {
        use std::io::Write;
        let mut cursor = std::io::Cursor::new(&mut fwd_buf[..]);
        if peer_is_v4 {
            let _ = write!(
                cursor,
                "for={}",
                std::str::from_utf8(client_ip_bytes).unwrap_or("")
            );
        } else {
            let _ = write!(
                cursor,
                "for=\"[{}]\"",
                std::str::from_utf8(client_ip_bytes).unwrap_or("")
            );
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

/// Enriches HTTP/2 request headers with pre-computed connection-level metadata.
///
/// Completely eliminates per-stream IP/port string formatting and buffer writes.
#[inline]
pub fn enrich_headers_precomputed(
    headers: &mut HeaderMap,
    uri: &Uri,
    client_ip: &HeaderValue,
    listener_port: &HeaderValue,
    is_tls: bool,
) {
    // 1. Strip prohibited connection-specific headers per RFC 9113 Section 8.2.2
    crate::headers::sanitize_h2_headers(headers);

    // 2. Inject authoritative pre-computed forwarding headers (zero formatting)
    let proto = if is_tls { "https" } else { "http" };

    if let Some(existing) = headers.get(HeaderName::from_static("x-forwarded-for")) {
        let mut combined =
            Vec::with_capacity(existing.as_bytes().len() + 2 + client_ip.as_bytes().len());
        combined.extend_from_slice(existing.as_bytes());
        combined.extend_from_slice(b", ");
        combined.extend_from_slice(client_ip.as_bytes());
        if let Ok(comb_val) = HeaderValue::from_maybe_shared(bytes::Bytes::from(combined)) {
            headers.insert(HeaderName::from_static("x-forwarded-for"), comb_val);
        }
    } else {
        headers.insert(
            HeaderName::from_static("x-forwarded-for"),
            client_ip.clone(),
        );
    }

    if !headers.contains_key("x-real-ip") {
        headers.insert(HeaderName::from_static("x-real-ip"), client_ip.clone());
    }
    headers.insert(
        HeaderName::from_static("x-forwarded-proto"),
        HeaderValue::from_static(proto),
    );
    headers.insert(
        HeaderName::from_static("x-forwarded-port"),
        listener_port.clone(),
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
        let peer_ip = velda_core::lut::ip::v4_to_u128(u32::from_be_bytes([192, 168, 1, 1]));
        let local_ip = velda_core::lut::ip::v4_to_u128(u32::from_be_bytes([10, 0, 0, 1]));

        enrich_headers(&mut headers, &uri, peer_ip, local_ip, 443u16.to_be(), true);

        assert_eq!(
            headers.get("x-forwarded-for").unwrap().to_str().unwrap(),
            "1.2.3.4, 192.168.1.1"
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
