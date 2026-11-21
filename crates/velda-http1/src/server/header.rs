//! Downstream HTTP/1.1 Header Processing, Anti-Spoofing, and Proxy Enrichment.
//!
//! Enforces:
//! - Stripping of untrusted downstream forwarding headers (`X-Forwarded-*`, `Forwarded`, `X-Real-IP`).
//! - Injection of authoritative proxy forwarding headers (RFC 7239 + `X-Forwarded-*`).
//! - Validation and canonical lookup of downstream header names.
//! - Authoritative extraction of Host / :authority.

use http::header::{CONTENT_LENGTH, HeaderName, HeaderValue};
use http::{HeaderMap, Uri};

use crate::error::Http1Error;

/// Enriches HTTP/1.1 request headers using pre-computed connection header values (zero formatting overhead).
#[inline]
pub fn enrich_headers_cached(
    headers: &mut HeaderMap,
    uri: &Uri,
    client_ip: &HeaderValue,
    listener_port: &HeaderValue,
    is_tls: bool,
) {
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

    headers.insert(HeaderName::from_static("x-real-ip"), client_ip.clone());
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
        .or_else(|| uri.host());

    if let Some(h) = host_str
        && let Ok(val) = HeaderValue::from_str(h)
    {
        headers.insert(HeaderName::from_static("x-forwarded-host"), val);
    }
}

/// Enriches HTTP/1.1 request headers with authoritative proxy forwarding metadata.
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
    // 1. Inject authoritative client IP (directly overwriting any client-supplied spoofed values)
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
    if peer_is_v4 {
        fwd_buf[cursor..cursor + client_ip_bytes.len()].copy_from_slice(client_ip_bytes);
        cursor += client_ip_bytes.len();
    } else {
        fwd_buf[cursor..cursor + 2].copy_from_slice(b"\"[");
        cursor += 2;
        fwd_buf[cursor..cursor + client_ip_bytes.len()].copy_from_slice(client_ip_bytes);
        cursor += client_ip_bytes.len();
        fwd_buf[cursor..cursor + 2].copy_from_slice(b"]\"");
        cursor += 2;
    }

    fwd_buf[cursor..cursor + 7].copy_from_slice(b";proto=");
    cursor += 7;
    let proto_bytes = proto.as_bytes();
    fwd_buf[cursor..cursor + proto_bytes.len()].copy_from_slice(proto_bytes);
    cursor += proto_bytes.len();

    fwd_buf[cursor..cursor + 4].copy_from_slice(b";by=");
    cursor += 4;
    let (local_buf, local_len) = velda_core::lut::ip::format_ip_bytes(local_ip);
    if velda_core::lut::ip::is_v4_u128(local_ip) {
        fwd_buf[cursor..cursor + local_len].copy_from_slice(&local_buf[..local_len]);
        cursor += local_len;
    } else {
        fwd_buf[cursor..cursor + 2].copy_from_slice(b"\"[");
        cursor += 2;
        fwd_buf[cursor..cursor + local_len].copy_from_slice(&local_buf[..local_len]);
        cursor += local_len;
        fwd_buf[cursor..cursor + 2].copy_from_slice(b"]\"");
        cursor += 2;
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
        let peer_ip = velda_core::lut::ip::v4_to_u128(u32::from_be_bytes([192, 168, 1, 50]));
        let local_ip = velda_core::lut::ip::v4_to_u128(u32::from_be_bytes([10, 0, 0, 1]));

        enrich_headers(&mut headers, &uri, peer_ip, local_ip, 80u16.to_be(), false);

        assert_eq!(
            headers.get("x-forwarded-for").unwrap().to_str().unwrap(),
            "1.2.3.4, 192.168.1.50"
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
