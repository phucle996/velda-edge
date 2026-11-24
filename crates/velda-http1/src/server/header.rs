//! Downstream HTTP/1.1 Header Processing and Proxy Enrichment.
//!
//! Enforces:
//! - Appending peer IP to `X-Forwarded-For` and overwriting `X-Real-IP` / `X-Forwarded-Proto` /
//!   `X-Forwarded-Port` / `X-Forwarded-Host` / `Forwarded` with authoritative socket metadata.
//! - Validation and canonical lookup of downstream header names.
//! - Authoritative extraction of Host / :authority.

use http::Uri;
use http::header::{HeaderName, HeaderValue};
use velda_core::HeaderWorkspace;

const X_FORWARDED_FOR: HeaderName = HeaderName::from_static("x-forwarded-for");
const X_REAL_IP: HeaderName = HeaderName::from_static("x-real-ip");
const X_FORWARDED_PROTO: HeaderName = HeaderName::from_static("x-forwarded-proto");
const X_FORWARDED_PORT: HeaderName = HeaderName::from_static("x-forwarded-port");
const X_FORWARDED_HOST: HeaderName = HeaderName::from_static("x-forwarded-host");
const FORWARDED: HeaderName = HeaderName::from_static("forwarded");
const PROTO_HTTPS_VAL: HeaderValue = HeaderValue::from_static("https");
const PROTO_HTTP_VAL: HeaderValue = HeaderValue::from_static("http");

/// Enriches HTTP/1.1 request headers with authoritative proxy forwarding metadata.
///
/// Appends the peer IP to `x-forwarded-for` and sets authoritative `x-real-ip`, `x-forwarded-proto`,
/// `x-forwarded-port`, `x-forwarded-host`, and RFC 7239 `forwarded` headers.
pub fn enrich_headers(
    headers: &mut HeaderWorkspace,
    uri: &Uri,
    peer_ip: u128,
    local_ip: u128,
    local_port_be: u16,
    is_tls: bool,
) {
    // 1. Inject authoritative client IP and forwarding chain
    let (proto, proto_val) = if is_tls {
        ("https", PROTO_HTTPS_VAL)
    } else {
        ("http", PROTO_HTTP_VAL)
    };
    let (ip_buf, ip_len) = velda_core::lut::ip::format_ip_bytes(peer_ip);
    let client_ip_bytes = &ip_buf[..ip_len];
    let peer_is_v4 = velda_core::lut::ip::is_v4_u128(peer_ip);

    if let Ok(val) = HeaderValue::from_bytes(client_ip_bytes) {
        if let Some(existing) = headers.get(&X_FORWARDED_FOR) {
            let mut combined =
                Vec::with_capacity(existing.as_bytes().len() + 2 + val.as_bytes().len());
            combined.extend_from_slice(existing.as_bytes());
            combined.extend_from_slice(b", ");
            combined.extend_from_slice(val.as_bytes());
            if let Ok(comb_val) = HeaderValue::from_maybe_shared(bytes::Bytes::from(combined)) {
                headers.insert(X_FORWARDED_FOR, comb_val);
            }
        } else {
            headers.insert(X_FORWARDED_FOR, val.clone());
        }

        headers.insert(X_REAL_IP, val);
    }

    headers.insert(X_FORWARDED_PROTO, proto_val);

    headers.insert(
        X_FORWARDED_PORT,
        velda_core::buffer::header::resolve_u64_header_value(u16::from_be(local_port_be) as u64),
    );

    let host_hdr = headers.get(http::header::HOST).cloned();
    let host_str = host_hdr
        .as_ref()
        .and_then(|v| v.to_str().ok())
        .or_else(|| uri.host());

    if let Some(h) = host_str
        && let Ok(val) = HeaderValue::from_str(h)
    {
        headers.insert(X_FORWARDED_HOST, val);
    }

    // Build RFC 7239 Forwarded header into fixed stack buffer via checked slice splits (zero panic branches).
    let mut fwd_buf = [0u8; 256];
    let mut rem = &mut fwd_buf[..];
    let push = |dst: &mut &mut [u8], src: &[u8]| {
        if let Some((slot, rest)) = std::mem::take(dst).split_at_mut_checked(src.len()) {
            slot.copy_from_slice(src);
            *dst = rest;
        }
    };

    push(&mut rem, b"for=");
    if peer_is_v4 {
        push(&mut rem, client_ip_bytes);
    } else {
        push(&mut rem, b"\"[");
        push(&mut rem, client_ip_bytes);
        push(&mut rem, b"]\"");
    }

    push(&mut rem, b";proto=");
    push(&mut rem, proto.as_bytes());

    push(&mut rem, b";by=");
    let (local_buf, local_len) = velda_core::lut::ip::format_ip_bytes(local_ip);
    if let Some(local_bytes) = local_buf.get(..local_len) {
        if velda_core::lut::ip::is_v4_u128(local_ip) {
            push(&mut rem, local_bytes);
        } else {
            push(&mut rem, b"\"[");
            push(&mut rem, local_bytes);
            push(&mut rem, b"]\"");
        }
    }

    if let Some(h) = host_str {
        let h_bytes = h.as_bytes();
        if rem.len() >= 8 + h_bytes.len() {
            push(&mut rem, b";host=\"");
            push(&mut rem, h_bytes);
            push(&mut rem, b"\"");
        }
    }

    let written = 256 - rem.len();
    if let Some(fwd_bytes) = fwd_buf.get(..written)
        && let Ok(val) = HeaderValue::from_bytes(fwd_bytes)
    {
        headers.insert(FORWARDED, val);
    }
}

/// Extracts effective Host or authority string from request headers or URI.
#[inline]
pub fn extract_host<'a>(headers: &'a HeaderWorkspace, uri: &'a Uri) -> Option<&'a str> {
    headers
        .get(http::header::HOST)
        .and_then(|v| v.to_str().ok())
        .or_else(|| uri.host())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_enrich_forwarded_headers() {
        let mut headers = HeaderWorkspace::empty(4);
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
