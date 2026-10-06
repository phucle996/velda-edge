//! Dedicated wire protocol framing and QPACK header codec for Layer 7 gRPC over UDP.
//!
//! Provides zero-copy variable-length integer encoding/decoding, RFC 9114 HTTP/3
//! frame serialization, and RFC 9204 QPACK static-table header encoding/decoding
//! without borrowing any code or dependencies from HTTP crates.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use http::header::{HeaderMap, HeaderName, HeaderValue};
use http::{Method, StatusCode, Uri};

use crate::error::GrpcError;

/// RFC 9204 Appendix A Static Table entries relevant for gRPC and HTTP/3.
#[rustfmt::skip]
pub static STATIC_TABLE: [(&str, &str); 99] = [
    (":authority", ""),                              // 0
    (":path", "/"),                                  // 1
    ("age", "0"),                                    // 2
    ("content-disposition", ""),                     // 3
    ("content-length", "0"),                         // 4
    ("cookie", ""),                                  // 5
    ("date", ""),                                    // 6
    ("etag", ""),                                    // 7
    ("if-modified-since", ""),                       // 8
    ("if-none-match", ""),                           // 9
    ("last-modified", ""),                           // 10
    ("link", ""),                                    // 11
    ("location", ""),                                // 12
    ("referer", ""),                                 // 13
    ("set-cookie", ""),                              // 14
    (":method", "CONNECT"),                          // 15
    (":method", "DELETE"),                           // 16
    (":method", "GET"),                              // 17
    (":method", "HEAD"),                             // 18
    (":method", "OPTIONS"),                          // 19
    (":method", "POST"),                             // 20
    (":method", "PUT"),                              // 21
    (":scheme", "http"),                             // 22
    (":scheme", "https"),                            // 23
    (":status", "103"),                              // 24
    (":status", "200"),                              // 25
    (":status", "304"),                              // 26
    (":status", "404"),                              // 27
    (":status", "503"),                              // 28
    ("accept", "*/*"),                               // 29
    ("accept", "application/dns-message"),           // 30
    ("accept-encoding", "gzip, deflate, br"),        // 31
    ("accept-ranges", "bytes"),                      // 32
    ("access-control-allow-headers", "cache-control, content-type"), // 33
    ("access-control-allow-headers", "content-type"),// 34
    ("access-control-allow-origin", "*"),            // 35
    ("cache-control", "max-age=0"),                  // 36
    ("cache-control", "max-age=2592000"),            // 37
    ("cache-control", "max-age=604800"),             // 38
    ("cache-control", "no-cache"),                   // 39
    ("cache-control", "no-store"),                   // 40
    ("cache-control", "public, max-age=31536000"),   // 41
    ("content-encoding", "br"),                      // 42
    ("content-encoding", "gzip"),                    // 43
    ("content-type", "application/dns-message"),     // 44
    ("content-type", "application/javascript"),      // 45
    ("content-type", "application/json"),            // 46
    ("content-type", "application/x-www-form-urlencoded"), // 47
    ("content-type", "image/gif"),                   // 48
    ("content-type", "image/jpeg"),                  // 49
    ("content-type", "image/png"),                   // 50
    ("content-type", "text/css"),                    // 51
    ("content-type", "text/html; charset=utf-8"),    // 52
    ("content-type", "text/plain"),                  // 53
    ("content-type", "text/plain;charset=utf-8"),    // 54
    ("range", "bytes=0-"),                           // 55
    ("strict-transport-security", "max-age=31536000"), // 56
    ("strict-transport-security", "max-age=31536000; includesubdomains; preload"), // 57
    ("strict-transport-security", "max-age=31536000; includesubdomains"), // 58
    ("vary", "accept-encoding"),                     // 59
    ("vary", "origin"),                              // 60
    (":status", "100"),                              // 61
    (":status", "204"),                              // 62
    (":status", "206"),                              // 63
    (":status", "302"),                              // 64
    (":status", "400"),                              // 65
    (":status", "403"),                              // 66
    (":status", "421"),                              // 67
    (":status", "425"),                              // 68
    (":status", "500"),                              // 69
    ("accept-language", ""),                         // 70
    ("access-control-allow-credentials", "FALSE"),   // 71
    ("access-control-allow-credentials", "TRUE"),    // 72
    ("access-control-allow-headers", "*"),           // 73
    ("access-control-allow-methods", "get"),         // 74
    ("access-control-allow-methods", "get, post, options"), // 75
    ("access-control-allow-methods", "options"),     // 76
    ("access-control-expose-headers", "content-length"), // 77
    ("access-control-request-headers", "content-type"), // 78
    ("access-control-request-method", "get"),        // 79
    ("access-control-request-method", "post"),       // 80
    ("alt-svc", "clear"),                            // 81
    ("authorization", ""),                           // 82
    ("content-security-policy", "script-src 'none'; object-src 'none'; base-uri 'none'"), // 83
    ("early-data", "1"),                             // 84
    ("expect-ct", ""),                               // 85
    ("forwarded", ""),                               // 86
    ("if-range", ""),                                // 87
    ("origin", ""),                                  // 88
    ("purpose", "prefetch"),                         // 89
    ("server", ""),                                  // 90
    ("timing-allow-origin", "*"),                    // 91
    ("upgrade-insecure-requests", "1"),              // 92
    ("user-agent", ""),                              // 93
    ("x-content-type-options", "nosniff"),           // 94
    ("x-frame-options", "deny"),                     // 95
    ("x-frame-options", "sameorigin"),               // 96
    ("x-xss-protection", "1; mode=block"),           // 97
    (":status", "502"),                              // 98
];

/// Encodes a variable-length integer (RFC 9000 Section 16).
#[inline]
pub fn encode_varint(val: u64, dst: &mut BytesMut) {
    if val < 64 {
        dst.put_u8(val as u8);
    } else if val < 16384 {
        dst.put_u16((val as u16) | 0x4000);
    } else if val < 1073741824 {
        dst.put_u32((val as u32) | 0x8000_0000);
    } else {
        dst.put_u64(val | 0xc000_0000_0000_0000);
    }
}

/// Decodes a variable-length integer (RFC 9000 Section 16).
#[inline]
pub fn decode_varint(buf: &mut BytesMut) -> Option<u64> {
    if buf.is_empty() {
        return None;
    }
    let first = buf[0];
    let prefix = first >> 6;
    let len = 1 << prefix;
    if buf.len() < len {
        return None;
    }
    match prefix {
        0 => {
            buf.advance(1);
            Some(first as u64)
        }
        1 => {
            let v = buf.get_u16();
            Some((v & 0x3fff) as u64)
        }
        2 => {
            let v = buf.get_u32();
            Some((v & 0x3fff_ffff) as u64)
        }
        3 => {
            let v = buf.get_u64();
            Some(v & 0x3fff_ffff_ffff_ffff)
        }
        _ => unreachable!(),
    }
}

/// Encodes an integer with an N-bit prefix (RFC 7541 / RFC 9204 Section 4.1.1).
#[inline]
pub fn encode_prefixed_int(val: u64, prefix_bits: u8, prefix_mask: u8, dst: &mut BytesMut) {
    let max_prefix = (1u64 << prefix_bits) - 1;
    if val < max_prefix {
        dst.put_u8(prefix_mask | (val as u8));
    } else {
        dst.put_u8(prefix_mask | (max_prefix as u8));
        let mut rem = val - max_prefix;
        while rem >= 128 {
            dst.put_u8(((rem & 0x7f) as u8) | 0x80);
            rem >>= 7;
        }
        dst.put_u8(rem as u8);
    }
}

/// Decodes an integer with an N-bit prefix.
#[inline]
pub fn decode_prefixed_int(slice: &[u8], prefix_bits: u8) -> Option<(u64, usize)> {
    if slice.is_empty() {
        return None;
    }
    let max_prefix = (1u64 << prefix_bits) - 1;
    let first = (slice[0] as u64) & max_prefix;
    if first < max_prefix {
        return Some((first, 1));
    }
    let mut val = first;
    let mut shift = 0;
    let mut consumed = 1;
    while consumed < slice.len() {
        let b = slice[consumed];
        consumed += 1;
        val += ((b & 0x7f) as u64) << shift;
        shift += 7;
        if (b & 0x80) == 0 {
            return Some((val, consumed));
        }
        if shift > 62 {
            return None;
        }
    }
    None
}

/// Encodes a raw string literal with a 7-bit prefix.
#[inline]
pub fn encode_string_literal(bytes: &[u8], dst: &mut BytesMut) {
    encode_prefixed_int(bytes.len() as u64, 7, 0x00, dst);
    dst.put_slice(bytes);
}

/// Decodes a string literal (RFC 9204 Section 4.1.2) with a 7-bit prefix.
pub fn decode_string_literal(slice: &[u8]) -> Option<(String, usize)> {
    if slice.is_empty() {
        return None;
    }
    let (len, consumed) = decode_prefixed_int(slice, 7)?;
    let start = consumed;
    let end = start + len as usize;
    if slice.len() < end {
        return None;
    }
    let s = String::from_utf8(slice[start..end].to_vec()).ok()?;
    Some((s, end))
}

/// Decodes a string literal with a custom N-bit prefix.
pub fn decode_string_literal_prefixed(slice: &[u8], prefix_bits: u8) -> Option<(String, usize)> {
    if slice.is_empty() {
        return None;
    }
    let (len, consumed) = decode_prefixed_int(slice, prefix_bits)?;
    let start = consumed;
    let end = start + len as usize;
    if slice.len() < end {
        return None;
    }
    let s = String::from_utf8(slice[start..end].to_vec()).ok()?;
    Some((s, end))
}

/// Wire frame types for UDP gRPC streaming.
#[derive(Debug, Clone)]
pub enum UdpFrame {
    /// RFC 9114 DATA frame (Type 0x00).
    Data(Bytes),
    /// RFC 9114 HEADERS frame (Type 0x01).
    Headers(Bytes),
    /// RFC 9114 SETTINGS frame (Type 0x04).
    Settings(Vec<(u64, u64)>),
    /// Other frame.
    Other(u64, Bytes),
}

/// Decodes the next HTTP/3 frame from the buffer.
pub fn decode_udp_frame(buf: &mut BytesMut) -> Result<Option<UdpFrame>, GrpcError> {
    if buf.is_empty() {
        return Ok(None);
    }
    let mut peek = buf.clone();
    let frame_type = match decode_varint(&mut peek) {
        Some(t) => t,
        None => return Ok(None),
    };
    let length = match decode_varint(&mut peek) {
        Some(l) => l,
        None => return Ok(None),
    };
    let header_len = buf.len() - peek.len();
    if buf.len() < header_len + length as usize {
        return Ok(None);
    }
    buf.advance(header_len);
    let payload = buf.split_to(length as usize).freeze();

    let frame = match frame_type {
        0x00 => UdpFrame::Data(payload),
        0x01 => UdpFrame::Headers(payload),
        0x04 => {
            let mut p = BytesMut::from(&payload[..]);
            let mut settings = Vec::new();
            while !p.is_empty() {
                if let (Some(id), Some(val)) = (decode_varint(&mut p), decode_varint(&mut p)) {
                    settings.push((id, val));
                } else {
                    break;
                }
            }
            UdpFrame::Settings(settings)
        }
        t => UdpFrame::Other(t, payload),
    };
    Ok(Some(frame))
}

/// Encodes a UDP frame into the destination buffer.
pub fn encode_udp_frame(frame: &UdpFrame, dst: &mut BytesMut) {
    match frame {
        UdpFrame::Data(payload) => {
            encode_varint(0x00, dst);
            encode_varint(payload.len() as u64, dst);
            dst.put_slice(payload);
        }
        UdpFrame::Headers(payload) => {
            encode_varint(0x01, dst);
            encode_varint(payload.len() as u64, dst);
            dst.put_slice(payload);
        }
        UdpFrame::Settings(settings) => {
            let mut payload = BytesMut::new();
            for (id, val) in settings {
                encode_varint(*id, &mut payload);
                encode_varint(*val, &mut payload);
            }
            encode_varint(0x04, dst);
            encode_varint(payload.len() as u64, dst);
            dst.extend_from_slice(&payload);
        }
        UdpFrame::Other(t, payload) => {
            encode_varint(*t, dst);
            encode_varint(payload.len() as u64, dst);
            dst.put_slice(payload);
        }
    }
}

/// Decoded request or response headers from QPACK field section.
#[derive(Debug, Default)]
pub struct DecodedHeaders {
    pub method: Option<Method>,
    pub uri: Option<Uri>,
    pub status: Option<StatusCode>,
    pub headers: HeaderMap,
}

/// Decodes QPACK encoded headers for incoming gRPC over UDP requests.
pub fn decode_qpack_headers(mut slice: &[u8]) -> Result<DecodedHeaders, GrpcError> {
    if slice.len() < 2 {
        return Err(GrpcError::Internal(
            "QPACK field section too short for prefix".into(),
        ));
    }
    // 1. Skip Required Insert Count (RIC) & Base
    let (_ric, ric_consumed) = decode_prefixed_int(slice, 8)
        .ok_or_else(|| GrpcError::Internal("malformed QPACK RIC".into()))?;
    slice = &slice[ric_consumed..];

    let (_base, base_consumed) = decode_prefixed_int(slice, 7)
        .ok_or_else(|| GrpcError::Internal("malformed QPACK Base".into()))?;
    slice = &slice[base_consumed..];

    let mut decoded = DecodedHeaders::default();
    let mut path_str = String::new();

    // 2. Process field lines
    while !slice.is_empty() {
        let first = slice[0];
        if (first & 0x80) != 0 {
            // Indexed Field Line (RFC 9204 §4.5.2)
            let is_static = (first & 0x40) != 0;
            let (idx, consumed) = decode_prefixed_int(slice, 6)
                .ok_or_else(|| GrpcError::Internal("malformed QPACK index".into()))?;
            slice = &slice[consumed..];

            if is_static && (idx as usize) < STATIC_TABLE.len() {
                let (name, val) = STATIC_TABLE[idx as usize];
                apply_header_pair(&mut decoded, &mut path_str, name, val)?;
            }
        } else if (first & 0x40) != 0 {
            // Literal Field Line with Name Reference (RFC 9204 §4.5.4)
            let is_static = (first & 0x10) != 0;
            let (name_idx, consumed) = decode_prefixed_int(slice, 4)
                .ok_or_else(|| GrpcError::Internal("malformed QPACK name index".into()))?;
            slice = &slice[consumed..];

            let (val, val_consumed) = decode_string_literal(slice)
                .ok_or_else(|| GrpcError::Internal("malformed QPACK value string".into()))?;
            slice = &slice[val_consumed..];

            if is_static && (name_idx as usize) < STATIC_TABLE.len() {
                let (name, _) = STATIC_TABLE[name_idx as usize];
                apply_header_pair(&mut decoded, &mut path_str, name, &val)?;
            }
        } else if (first & 0x20) != 0 {
            // Literal Field Line with Literal Name (RFC 9204 §4.5.6)
            let (name, name_consumed) = decode_string_literal_prefixed(slice, 3)
                .ok_or_else(|| GrpcError::Internal("malformed QPACK literal name".into()))?;
            slice = &slice[name_consumed..];

            let (val, val_consumed) = decode_string_literal(slice)
                .ok_or_else(|| GrpcError::Internal("malformed QPACK literal value".into()))?;
            slice = &slice[val_consumed..];

            apply_header_pair(&mut decoded, &mut path_str, &name, &val)?;
        } else {
            // Unhandled or reserved QPACK instruction
            break;
        }
    }

    if !path_str.is_empty() {
        decoded.uri = path_str.parse().ok();
    }
    if decoded.method.is_none() {
        decoded.method = Some(Method::POST);
    }

    Ok(decoded)
}

fn apply_header_pair(
    decoded: &mut DecodedHeaders,
    path_str: &mut String,
    name: &str,
    val: &str,
) -> Result<(), GrpcError> {
    match name {
        ":method" => {
            if let Ok(m) = val.parse::<Method>() {
                decoded.method = Some(m);
            }
        }
        ":path" => {
            *path_str = val.to_string();
        }
        ":authority" => {
            if let Ok(v) = HeaderValue::from_str(val) {
                decoded
                    .headers
                    .insert(HeaderName::from_static(":authority"), v);
            }
        }
        ":scheme" => {
            if let Ok(v) = HeaderValue::from_str(val) {
                decoded
                    .headers
                    .insert(HeaderName::from_static(":scheme"), v);
            }
        }
        ":status" => {
            if let Ok(code) = val.parse::<u16>()
                && let Ok(sc) = StatusCode::from_u16(code)
            {
                decoded.status = Some(sc);
            }
        }
        other => {
            if let (Ok(n), Ok(v)) = (
                HeaderName::from_bytes(other.as_bytes()),
                HeaderValue::from_str(val),
            ) {
                decoded.headers.append(n, v);
            }
        }
    }
    Ok(())
}

/// Encodes QPACK response headers into the destination buffer.
pub fn encode_qpack_response(status: StatusCode, headers: &HeaderMap, dst: &mut BytesMut) {
    // 1. Required Insert Count (RIC) = 0, Base = 0
    dst.put_u8(0x00);
    dst.put_u8(0x00);

    // 2. Encode :status
    if status == StatusCode::OK {
        // Static table index 25: ":status", "200"
        dst.put_u8(0xc0 | 25);
    } else {
        // Literal status
        encode_prefixed_int(24, 4, 0x50, dst); // Name reference index 24 (:status)
        let s_str = status.as_str();
        encode_string_literal(s_str.as_bytes(), dst);
    }

    // 3. Encode additional headers (e.g. content-type, grpc-status, grpc-message)
    for (name, val) in headers {
        let name_str = name.as_str();
        encode_prefixed_int(name_str.len() as u64, 3, 0x20, dst);
        dst.put_slice(name_str.as_bytes());
        encode_string_literal(val.as_bytes(), dst);
    }
}

/// Encodes QPACK request headers into the destination buffer (RFC 9204).
pub fn encode_qpack_request(method: &Method, uri: &Uri, headers: &HeaderMap, dst: &mut BytesMut) {
    // 1. Field Section Prefix: RIC = 0, Base = 0
    dst.put_u8(0x00);
    dst.put_u8(0x00);

    // 2. :method
    let method_idx = match *method {
        Method::POST => Some(20),
        Method::GET => Some(17),
        Method::PUT => Some(21),
        Method::DELETE => Some(16),
        _ => None,
    };
    if let Some(idx) = method_idx {
        encode_prefixed_int(idx, 6, 0xc0, dst);
    } else {
        encode_prefixed_int(17, 4, 0x50, dst);
        encode_string_literal(method.as_str().as_bytes(), dst);
    }

    // 3. :scheme
    let scheme = uri.scheme_str().unwrap_or("https");
    if scheme == "https" {
        encode_prefixed_int(23, 6, 0xc0, dst);
    } else if scheme == "http" {
        encode_prefixed_int(22, 6, 0xc0, dst);
    } else {
        encode_prefixed_int(23, 4, 0x50, dst);
        encode_string_literal(scheme.as_bytes(), dst);
    }

    // 4. :path
    let path = uri.path_and_query().map(|pq| pq.as_str()).unwrap_or("/");
    if path == "/" {
        encode_prefixed_int(1, 6, 0xc0, dst);
    } else {
        encode_prefixed_int(1, 4, 0x50, dst);
        encode_string_literal(path.as_bytes(), dst);
    }

    // 5. :authority
    if let Some(auth) = uri.authority() {
        encode_prefixed_int(0, 4, 0x50, dst);
        encode_string_literal(auth.as_str().as_bytes(), dst);
    } else if let Some(host) = headers
        .get(http::header::HOST)
        .and_then(|h| h.to_str().ok())
    {
        encode_prefixed_int(0, 4, 0x50, dst);
        encode_string_literal(host.as_bytes(), dst);
    }

    // 6. Additional headers (content-type, te, grpc-timeout, etc.)
    for (name, val) in headers {
        if name == http::header::HOST {
            continue;
        }
        let name_str = name.as_str();
        encode_prefixed_int(name_str.len() as u64, 3, 0x20, dst);
        dst.put_slice(name_str.as_bytes());
        encode_string_literal(val.as_bytes(), dst);
    }
}
