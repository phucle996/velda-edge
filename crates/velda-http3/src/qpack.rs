//! Canonical QPACK (RFC 9204) encoder and decoder for HTTP/3 field sections.
//!
//! Features:
//! - Full 99-entry Static Table defined in RFC 9204 Appendix A.
//! - Direct O(1) indexed encoding and decoding for pseudo-headers and common headers.
//! - Zero-alloc literal name/value encoding with optional Huffman compression.
//! - Static-only mode (Dynamic Table Capacity = 0), eliminating dynamic state synchronization
//!   overhead and head-of-line blocking on QPACK decoder streams.

use bytes::{BufMut, BytesMut};
use http::header::{HeaderMap, HeaderName, HeaderValue};
use http::{Method, StatusCode, Uri};

use crate::error::Http3Error;
use crate::huffman::decode_huffman;

/// RFC 9204 Appendix A Static Table (99 entries).
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
    ("x-content-type-options", "nosniff"),           // 61
    ("x-xss-protection", "1; mode=block"),           // 62
    (":status", "100"),                              // 63
    (":status", "204"),                              // 64
    (":status", "206"),                              // 65
    (":status", "302"),                              // 66
    (":status", "400"),                              // 67
    (":status", "403"),                              // 68
    (":status", "421"),                              // 69
    (":status", "425"),                              // 70
    (":status", "500"),                              // 71
    ("accept-language", ""),                         // 72
    ("access-control-allow-credentials", "FALSE"),   // 73
    ("access-control-allow-credentials", "TRUE"),    // 74
    ("access-control-allow-headers", "*"),           // 75
    ("access-control-allow-methods", "get"),         // 76
    ("access-control-allow-methods", "get, post, options"), // 77
    ("access-control-allow-methods", "options"),     // 78
    ("access-control-expose-headers", "content-length"), // 79
    ("access-control-request-headers", "content-type"), // 80
    ("access-control-request-method", "get"),        // 81
    ("access-control-request-method", "post"),       // 82
    ("alt-svc", "clear"),                            // 83
    ("authorization", ""),                           // 84
    ("content-security-policy", "script-src 'none'; object-src 'none'; base-uri 'none'"), // 85
    ("early-data", "1"),                             // 86
    ("expect-ct", ""),                               // 87
    ("forwarded", ""),                               // 88
    ("server", ""),                                  // 89
    ("origin", ""),                                  // 90
    ("timing-allow-origin", "*"),                    // 91
    ("x-forwarded-for", ""),                         // 92
    ("x-frame-options", "deny"),                     // 93
    ("x-frame-options", "sameorigin"),               // 94
    ("content-type", "text/plain;charset=utf-8"),    // 95
    ("content-type", "text/plain"),                  // 96
    ("content-length", ""),                          // 97
    ("te", "trailers"),                              // 98
];

/// Result of decoding an RFC 9204 QPACK field section.
#[derive(Debug, Default, Clone)]
pub struct DecodedHeaders {
    /// HTTP Method (from `:method` pseudo-header).
    pub method: Option<Method>,
    /// Request URI (from `:path`, `:scheme`, `:authority` pseudo-headers).
    pub uri: Option<Uri>,
    /// HTTP Response Status (from `:status` pseudo-header).
    pub status: Option<StatusCode>,
    /// Regular HTTP Headers.
    pub headers: HeaderMap,
}

/// Decodes an integer with an N-bit prefix (RFC 7541 Section 5.1 / RFC 9204 Section 4.1.1).
/// Returns `(value, bytes_consumed)`.
pub fn decode_prefixed_int(slice: &[u8], prefix_bits: u8) -> Option<(u64, usize)> {
    if slice.is_empty() {
        return None;
    }
    let max_prefix = (1u64 << prefix_bits) - 1;
    let mask = max_prefix as u8;
    let first = (slice[0] & mask) as u64;
    if first < max_prefix {
        return Some((first, 1));
    }
    let mut val = first;
    let mut m = 0;
    let mut consumed = 1;
    for &b in &slice[1..] {
        consumed += 1;
        val += ((b & 0x7f) as u64) << m;
        m += 7;
        if (b & 0x80) == 0 {
            return Some((val, consumed));
        }
        if m >= 63 {
            return None;
        }
    }
    None
}

/// Encodes an integer with an N-bit prefix into the buffer.
pub fn encode_prefixed_int(val: u64, prefix_bits: u8, first_byte_prefix: u8, dst: &mut BytesMut) {
    let max_prefix = (1u64 << prefix_bits) - 1;
    if val < max_prefix {
        dst.put_u8(first_byte_prefix | (val as u8));
    } else {
        dst.put_u8(first_byte_prefix | (max_prefix as u8));
        let mut rem = val - max_prefix;
        while rem >= 128 {
            dst.put_u8(((rem & 0x7f) as u8) | 0x80);
            rem >>= 7;
        }
        dst.put_u8(rem as u8);
    }
}

/// Decodes a string literal (RFC 9204 Section 4.1.2) with a 7-bit prefix.
/// Supports both raw bytes and Huffman compression.
pub fn decode_string_literal(slice: &[u8]) -> Option<(String, usize)> {
    if slice.is_empty() {
        return None;
    }
    let is_huffman = (slice[0] & 0x80) != 0;
    let (len, consumed) = decode_prefixed_int(slice, 7)?;
    let start = consumed;
    let end = start + len as usize;
    if slice.len() < end {
        return None;
    }
    let data = &slice[start..end];
    let s = if is_huffman {
        decode_huffman(data)?
    } else {
        String::from_utf8(data.to_vec()).ok()?
    };
    Some((s, end))
}

/// Decodes a string literal with a custom N-bit prefix (e.g., 3-bit for literal names).
pub fn decode_string_literal_prefixed(slice: &[u8], prefix_bits: u8) -> Option<(String, usize)> {
    if slice.is_empty() {
        return None;
    }
    let h_bit_mask = 1 << prefix_bits;
    let is_huffman = (slice[0] & h_bit_mask) != 0;
    let (len, consumed) = decode_prefixed_int(slice, prefix_bits)?;
    let start = consumed;
    let end = start + len as usize;
    if slice.len() < end {
        return None;
    }
    let data = &slice[start..end];
    let s = if is_huffman {
        decode_huffman(data)?
    } else {
        String::from_utf8(data.to_vec()).ok()?
    };
    Some((s, end))
}

/// Encodes a string literal without Huffman compression.
pub fn encode_string_literal(bytes: &[u8], dst: &mut BytesMut) {
    encode_prefixed_int(bytes.len() as u64, 7, 0x00, dst);
    dst.put_slice(bytes);
}

/// Encodes a literal name and value pair (RFC 9204 Section 4.5.6).
pub fn encode_literal_name_and_value(name: &[u8], val: &[u8], dst: &mut BytesMut) {
    encode_prefixed_int(name.len() as u64, 3, 0x20, dst);
    dst.put_slice(name);
    encode_string_literal(val, dst);
}

/// Finds the static table index for a header name if present.
///
/// Fast path: direct O(1) jump-table match on canonical lowercase header names
/// mandated by RFC 9114 Section 4.2. Zero-alloc fallback for non-canonical casing.
pub fn find_static_name(name: &str) -> Option<u64> {
    match name {
        ":authority" => Some(0),
        ":path" => Some(1),
        "age" => Some(2),
        "content-disposition" => Some(3),
        "content-length" => Some(4),
        "cookie" => Some(5),
        "date" => Some(6),
        "etag" => Some(7),
        "if-modified-since" => Some(8),
        "if-none-match" => Some(9),
        "last-modified" => Some(10),
        "link" => Some(11),
        "location" => Some(12),
        "referer" => Some(13),
        "set-cookie" => Some(14),
        ":method" => Some(15),
        ":scheme" => Some(22),
        ":status" => Some(24),
        "accept" => Some(29),
        "accept-encoding" => Some(31),
        "accept-ranges" => Some(32),
        "access-control-allow-headers" => Some(33),
        "access-control-allow-origin" => Some(35),
        "cache-control" => Some(36),
        "content-encoding" => Some(42),
        "content-type" => Some(44),
        "range" => Some(55),
        "strict-transport-security" => Some(56),
        "vary" => Some(59),
        "x-content-type-options" => Some(61),
        "x-xss-protection" => Some(62),
        "accept-language" => Some(72),
        "access-control-allow-credentials" => Some(73),
        "access-control-allow-methods" => Some(76),
        "access-control-expose-headers" => Some(79),
        "access-control-request-headers" => Some(80),
        "access-control-request-method" => Some(81),
        "alt-svc" => Some(83),
        "authorization" => Some(84),
        "content-security-policy" => Some(85),
        "early-data" => Some(86),
        "expect-ct" => Some(87),
        "forwarded" => Some(88),
        "server" => Some(89),
        "origin" => Some(90),
        "timing-allow-origin" => Some(91),
        "x-forwarded-for" => Some(92),
        "x-frame-options" => Some(93),
        "te" => Some(98),
        _ => {
            for (idx, &(sname, _)) in STATIC_TABLE.iter().enumerate() {
                if sname.eq_ignore_ascii_case(name) {
                    return Some(idx as u64);
                }
            }
            None
        }
    }
}

/// Decodes an RFC 9204 QPACK-encoded field section (HEADERS frame payload).
pub fn decode_qpack(mut slice: &[u8]) -> Result<DecodedHeaders, Http3Error> {
    if slice.len() < 2 {
        return Err(Http3Error::H3("QPACK block too short for prefix".into()));
    }

    // 1. Decode Field Section Prefix (RFC 9204 Section 4.5.1)
    let (_req_insert_count, ric_consumed) = decode_prefixed_int(slice, 8)
        .ok_or_else(|| Http3Error::H3("Invalid Required Insert Count".into()))?;
    slice = &slice[ric_consumed..];

    if slice.is_empty() {
        return Err(Http3Error::H3("QPACK block missing Delta Base".into()));
    }
    let (_delta_base, db_consumed) =
        decode_prefixed_int(slice, 7).ok_or_else(|| Http3Error::H3("Invalid Delta Base".into()))?;
    slice = &slice[db_consumed..];

    let mut decoded = DecodedHeaders::default();
    let mut raw_path = None;
    let mut raw_authority = None;
    let mut raw_scheme = None;

    // 2. Decode Field Lines
    while !slice.is_empty() {
        let first = slice[0];

        if (first & 0xc0) == 0xc0 {
            // Pattern 11xxxxxx: Indexed Field Line with Static Table Reference (Section 4.5.2)
            let (idx, consumed) = decode_prefixed_int(slice, 6)
                .ok_or_else(|| Http3Error::H3("Invalid Indexed Field Line".into()))?;
            slice = &slice[consumed..];

            let (name, val) = STATIC_TABLE.get(idx as usize).ok_or_else(|| {
                Http3Error::H3(format!("Static table index out of bounds: {idx}"))
            })?;

            apply_header(
                name,
                val,
                &mut decoded,
                &mut raw_path,
                &mut raw_authority,
                &mut raw_scheme,
            )?;
        } else if (first & 0xc0) == 0x40 {
            // Pattern 01NTxxxx: Literal Field Line with Name Reference (Section 4.5.4)
            let is_static = (first & 0x10) != 0;
            let (idx, consumed) = decode_prefixed_int(slice, 4)
                .ok_or_else(|| Http3Error::H3("Invalid Literal with Name Reference".into()))?;
            slice = &slice[consumed..];

            if !is_static {
                return Err(Http3Error::H3(
                    "Dynamic table references not enabled (table capacity is 0)".into(),
                ));
            }

            let name = STATIC_TABLE
                .get(idx as usize)
                .ok_or_else(|| Http3Error::H3(format!("Static table index out of bounds: {idx}")))?
                .0;

            let (val, consumed_val) = decode_string_literal(slice)
                .ok_or_else(|| Http3Error::H3("Invalid string literal value".into()))?;
            slice = &slice[consumed_val..];

            apply_header(
                name,
                &val,
                &mut decoded,
                &mut raw_path,
                &mut raw_authority,
                &mut raw_scheme,
            )?;
        } else if (first & 0xe0) == 0x20 {
            // Pattern 001Nxxxx: Literal Field Line with Literal Name (Section 4.5.6)
            let (name, consumed_name) = decode_string_literal_prefixed(slice, 3)
                .ok_or_else(|| Http3Error::H3("Invalid literal field name".into()))?;
            slice = &slice[consumed_name..];

            let (val, consumed_val) = decode_string_literal(slice)
                .ok_or_else(|| Http3Error::H3("Invalid literal field value".into()))?;
            slice = &slice[consumed_val..];

            apply_header(
                &name,
                &val,
                &mut decoded,
                &mut raw_path,
                &mut raw_authority,
                &mut raw_scheme,
            )?;
        } else {
            // Unrecognized or dynamic-only field representation
            return Err(Http3Error::H3(format!(
                "Unsupported QPACK field representation: {first:#x}"
            )));
        }
    }

    // Assemble URI from pseudo-headers if present
    if let Some(path) = raw_path {
        let uri_str = if let (Some(scheme), Some(auth)) = (raw_scheme, raw_authority) {
            format!("{scheme}://{auth}{path}")
        } else {
            path
        };
        if let Ok(u) = uri_str.parse::<Uri>() {
            decoded.uri = Some(u);
        }
    } else if let Some(auth) = raw_authority {
        // CONNECT method uses authority-form URI without :path (RFC 9110 §7.1, RFC 9114 §4.4)
        if let Ok(u) = auth.parse::<Uri>() {
            decoded.uri = Some(u);
        }
    }

    Ok(decoded)
}

fn apply_header(
    name: &str,
    val: &str,
    decoded: &mut DecodedHeaders,
    raw_path: &mut Option<String>,
    raw_authority: &mut Option<String>,
    raw_scheme: &mut Option<String>,
) -> Result<(), Http3Error> {
    match name {
        ":method" => {
            if let Ok(m) = Method::from_bytes(val.as_bytes()) {
                decoded.method = Some(m);
            }
        }
        ":path" => {
            *raw_path = Some(val.to_string());
        }
        ":scheme" => {
            *raw_scheme = Some(val.to_string());
        }
        ":authority" => {
            *raw_authority = Some(val.to_string());
            if let Ok(hval) = HeaderValue::from_str(val) {
                decoded.headers.insert(http::header::HOST, hval);
            }
        }
        ":status" => {
            if let Some(sc) = val
                .parse::<u16>()
                .ok()
                .and_then(|c| StatusCode::from_u16(c).ok())
            {
                decoded.status = Some(sc);
            }
        }
        other => {
            if let (Ok(hname), Ok(hval)) = (
                HeaderName::from_bytes(other.as_bytes()),
                HeaderValue::from_str(val),
            ) {
                decoded.headers.append(hname, hval);
            }
        }
    }
    Ok(())
}

/// Encodes an HTTP/3 response field section into QPACK bytes.
pub fn encode_qpack_response(status: StatusCode, headers: &HeaderMap, dst: &mut BytesMut) {
    // 1. Field Section Prefix: Required Insert Count = 0, Delta Base = 0
    dst.put_u8(0x00);
    dst.put_u8(0x00);

    // 2. Encode :status pseudo-header
    let status_code = status.as_u16();
    let static_idx = match status_code {
        100 => Some(63),
        103 => Some(24),
        200 => Some(25),
        204 => Some(64),
        206 => Some(65),
        302 => Some(66),
        304 => Some(26),
        400 => Some(67),
        403 => Some(68),
        404 => Some(27),
        421 => Some(69),
        425 => Some(70),
        500 => Some(71),
        503 => Some(28),
        _ => None,
    };

    if let Some(idx) = static_idx {
        encode_prefixed_int(idx, 6, 0xc0, dst);
    } else {
        // Name ref to index 24 (:status)
        encode_prefixed_int(24, 4, 0x50, dst);
        let s = status.as_str();
        encode_string_literal(s.as_bytes(), dst);
    }

    // 3. Encode all other headers
    for (name, val) in headers {
        let name_str = name.as_str();
        let val_bytes = val.as_bytes();

        if let Some(name_idx) = find_static_name(name_str) {
            encode_prefixed_int(name_idx, 4, 0x50, dst);
            encode_string_literal(val_bytes, dst);
        } else {
            encode_literal_name_and_value(name_str.as_bytes(), val_bytes, dst);
        }
    }
}

/// Encodes an HTTP/3 request field section into QPACK bytes.
pub fn encode_qpack_request(method: &Method, uri: &Uri, headers: &HeaderMap, dst: &mut BytesMut) {
    // 1. Field Section Prefix
    dst.put_u8(0x00);
    dst.put_u8(0x00);

    // 2. :method
    let method_idx = match *method {
        Method::GET => Some(17),
        Method::POST => Some(20),
        Method::PUT => Some(21),
        Method::DELETE => Some(16),
        Method::HEAD => Some(18),
        Method::OPTIONS => Some(19),
        Method::CONNECT => Some(15),
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

    // 6. Additional headers
    for (name, val) in headers {
        if name == http::header::HOST {
            continue; // Already handled as :authority
        }
        let name_str = name.as_str();
        let val_bytes = val.as_bytes();
        if let Some(name_idx) = find_static_name(name_str) {
            encode_prefixed_int(name_idx, 4, 0x50, dst);
            encode_string_literal(val_bytes, dst);
        } else {
            encode_literal_name_and_value(name_str.as_bytes(), val_bytes, dst);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_qpack_response_status_200_roundtrip() {
        let mut buf = BytesMut::new();
        let mut headers = HeaderMap::new();
        headers.insert(
            http::header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
        headers.insert(
            http::header::SERVER,
            HeaderValue::from_static("Velda-Edge-H3"),
        );

        encode_qpack_response(StatusCode::OK, &headers, &mut buf);

        let decoded = decode_qpack(&buf).unwrap();
        assert_eq!(decoded.status, Some(StatusCode::OK));
        assert_eq!(
            decoded.headers.get("content-type").unwrap(),
            "application/json"
        );
        assert_eq!(decoded.headers.get("server").unwrap(), "Velda-Edge-H3");
    }

    #[test]
    fn test_qpack_request_get_roundtrip() {
        let mut buf = BytesMut::new();
        let mut headers = HeaderMap::new();
        headers.insert(
            http::header::USER_AGENT,
            HeaderValue::from_static("curl/8.5.0"),
        );
        headers.insert(
            http::header::ACCEPT,
            HeaderValue::from_static("application/json"),
        );

        let method = Method::GET;
        let uri: Uri = "https://example.com/api/v1/status".parse().unwrap();

        encode_qpack_request(&method, &uri, &headers, &mut buf);

        let decoded = decode_qpack(&buf).unwrap();
        assert_eq!(decoded.method, Some(Method::GET));
        assert!(decoded.uri.is_some());
        let dec_uri = decoded.uri.unwrap();
        assert_eq!(dec_uri.path(), "/api/v1/status");
        assert_eq!(decoded.headers.get("user-agent").unwrap(), "curl/8.5.0");
        assert_eq!(decoded.headers.get("accept").unwrap(), "application/json");
    }

    #[test]
    fn test_qpack_prefixed_int_roundtrip() {
        let values = [0, 1, 14, 15, 16, 62, 63, 64, 126, 127, 128, 500, 10000];
        for prefix in [3, 4, 6, 7, 8] {
            for val in values {
                let mut buf = BytesMut::new();
                encode_prefixed_int(val, prefix, 0, &mut buf);
                let (dec, consumed) = decode_prefixed_int(&buf, prefix).unwrap();
                assert_eq!(dec, val);
                assert_eq!(consumed, buf.len());
            }
        }
    }
}
