//! Canonical QPACK (RFC 9204) encoder and decoder for HTTP/3 field sections.
//!
//! Features:
//! - Full 99-entry Static Table defined in RFC 9204 Appendix A.
//! - Direct O(1) indexed encoding and decoding for pseudo-headers and common headers.
//! - Zero-alloc literal name/value encoding with optional Huffman compression.
//! - Static-only mode (Dynamic Table Capacity = 0), eliminating dynamic state synchronization
//!   overhead and head-of-line blocking on QPACK decoder streams.

use std::borrow::Cow;

use bytes::{BufMut, Bytes, BytesMut};
use http::header::{HeaderName, HeaderValue};
use http::{Method, StatusCode, Uri};
use velda_core::buffer::header::{
    DEFAULT_HEADER_CAPACITY, HeaderWorkspace, MAX_WIRE_HEADERS, X_REQUEST_ID, header_key,
};

use crate::error::Http3Error;
use crate::proto::huffman::decode_huffman_bytes;

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

/// Compile-time 63-bit FNV-1a hashes for all 99 RFC 9204 static table header names.
const STATIC_TABLE_KEYS: [u64; 99] = {
    let mut keys = [0u64; 99];
    let mut i = 0;
    while i < 99 {
        keys[i] = header_key(STATIC_TABLE[i].0.as_bytes());
        i += 1;
    }
    keys
};

/// Pre-built compile-time [`HeaderName`] LUT for non-pseudo RFC 9204 static table entries.
/// Eliminates runtime `HeaderName::from_static` byte-validation loops on hot QPACK paths.
static STATIC_HEADER_NAMES: [Option<HeaderName>; 99] = {
    use std::mem::MaybeUninit;
    let mut arr: [MaybeUninit<Option<HeaderName>>; 99] = [const { MaybeUninit::uninit() }; 99];
    let mut i = 0;
    while i < 99 {
        let name = STATIC_TABLE[i].0;
        let val = if name.as_bytes()[0] == b':' {
            None
        } else {
            Some(HeaderName::from_static(name))
        };
        arr[i].write(val);
        i += 1;
    }
    // SAFETY: All 99 elements in [0..99] are initialized above prior to transmuting.
    unsafe {
        std::mem::transmute::<[MaybeUninit<Option<HeaderName>>; 99], [Option<HeaderName>; 99]>(arr)
    }
};

/// Pre-built compile-time [`HeaderValue`] LUT for all 99 RFC 9204 static table entries.
/// Eliminates runtime `HeaderValue::from_static` validation loops on hot QPACK paths.
static STATIC_HEADER_VALUES: [HeaderValue; 99] = {
    use std::mem::MaybeUninit;
    let mut arr: [MaybeUninit<HeaderValue>; 99] = [const { MaybeUninit::uninit() }; 99];
    let mut i = 0;
    while i < 99 {
        arr[i].write(HeaderValue::from_static(STATIC_TABLE[i].1));
        i += 1;
    }
    // SAFETY: All 99 elements in [0..99] are initialized above prior to transmuting.
    unsafe { std::mem::transmute::<[MaybeUninit<HeaderValue>; 99], [HeaderValue; 99]>(arr) }
};

const KEY_X_REQUEST_ID: u64 = header_key(b"x-request-id");
const KEY_HOST: u64 = header_key(b"host");
const KEY_X_REAL_IP: u64 = header_key(b"x-real-ip");
const KEY_X_FORWARDED_FOR: u64 = header_key(b"x-forwarded-for");
const KEY_X_FORWARDED_PROTO: u64 = header_key(b"x-forwarded-proto");
const KEY_X_FORWARDED_PORT: u64 = header_key(b"x-forwarded-port");
const KEY_X_FORWARDED_HOST: u64 = header_key(b"x-forwarded-host");
const KEY_FORWARDED: u64 = header_key(b"forwarded");
const KEY_CONNECTION: u64 = header_key(b"connection");
const KEY_KEEP_ALIVE: u64 = header_key(b"keep-alive");
const KEY_PROXY_CONNECTION: u64 = header_key(b"proxy-connection");
const KEY_TRANSFER_ENCODING: u64 = header_key(b"transfer-encoding");
const KEY_UPGRADE: u64 = header_key(b"upgrade");
const KEY_TE: u64 = header_key(b"te");

const X_REAL_IP: HeaderName = HeaderName::from_static("x-real-ip");
const X_FORWARDED_FOR: HeaderName = HeaderName::from_static("x-forwarded-for");
const X_FORWARDED_PROTO: HeaderName = HeaderName::from_static("x-forwarded-proto");
const X_FORWARDED_PORT: HeaderName = HeaderName::from_static("x-forwarded-port");
const X_FORWARDED_HOST: HeaderName = HeaderName::from_static("x-forwarded-host");
const FORWARDED: HeaderName = HeaderName::from_static("forwarded");

/// Network address bundle for 1-pass proxy forwarding enrichment during QPACK ingress decoding.
#[derive(Debug, Clone, Copy)]
pub struct H3ForwardingAddrs {
    pub peer_ip: u128,
    pub local_ip: u128,
    pub local_port_be: u16,
}

/// Parameters for 1-pass QPACK ingress decoding with Bit-63 `x-request-id` seeding
/// and optional proxy forwarding header injection (`x-forwarded-*`, `x-real-ip`, `forwarded`).
#[derive(Debug, Clone)]
pub struct H3IngressEnrich {
    pub request_id: HeaderValue,
    pub forwarding: Option<H3ForwardingAddrs>,
}

/// Resolves a pre-built static [`HeaderName`] for non-pseudo RFC 9204 static table entries (O(1)).
#[inline]
fn static_header_name(idx: usize) -> Option<HeaderName> {
    STATIC_HEADER_NAMES.get(idx)?.clone()
}

/// Result of decoding an RFC 9204 QPACK field section directly into [`HeaderWorkspace`].
#[derive(Debug, Clone)]
pub struct DecodedHeaders {
    /// HTTP Method (from `:method` pseudo-header).
    pub method: Option<Method>,
    /// Request URI (from `:path`, `:scheme`, `:authority` pseudo-headers).
    pub uri: Option<Uri>,
    /// HTTP Response Status (from `:status` pseudo-header).
    pub status: Option<StatusCode>,
    /// Regular HTTP Headers stored in flat `[tags | slots]`.
    pub headers: HeaderWorkspace,
}

impl Default for DecodedHeaders {
    #[inline]
    fn default() -> Self {
        Self {
            method: None,
            uri: None,
            status: None,
            headers: HeaderWorkspace::empty(DEFAULT_HEADER_CAPACITY),
        }
    }
}

/// Decodes an integer with an N-bit prefix (RFC 7541 Section 5.1 / RFC 9204 Section 4.1.1).
/// Returns `(value, bytes_consumed)`.
pub fn decode_prefixed_int(slice: &[u8], prefix_bits: u8) -> Option<(u64, usize)> {
    if slice.is_empty() || !(1..=8).contains(&prefix_bits) {
        return None;
    }
    let max_prefix = if prefix_bits == 8 {
        u8::MAX as u64
    } else {
        (1u64 << prefix_bits) - 1
    };
    let mask = max_prefix as u8;
    let first = (slice[0] & mask) as u64;
    if first < max_prefix {
        return Some((first, 1));
    }
    let mut val = first;
    let mut m: u32 = 0;
    let mut consumed = 1;
    for &b in &slice[1..] {
        consumed += 1;
        let added = ((b & 0x7f) as u64).checked_shl(m)?;
        val = val.checked_add(added)?;
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

/// Decodes a string literal with an N-bit length prefix into raw bytes without UTF-8/String overhead.
#[inline]
fn decode_string_literal_bytes(slice: &[u8], prefix_bits: u8) -> Option<(Cow<'_, [u8]>, usize)> {
    if slice.is_empty() || prefix_bits >= 8 {
        return None;
    }
    let h_bit_mask = 1u8 << prefix_bits;
    let is_huffman = (slice[0] & h_bit_mask) != 0;
    let (len, consumed) = decode_prefixed_int(slice, prefix_bits)?;
    let len_usize = usize::try_from(len).ok()?;
    let start = consumed;
    let end = start.checked_add(len_usize)?;
    let data = slice.get(start..end)?;
    if is_huffman {
        decode_huffman_bytes(data).map(|v| (Cow::Owned(v), end))
    } else {
        Some((Cow::Borrowed(data), end))
    }
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

/// Decodes an RFC 9204 QPACK-encoded field section directly into [`HeaderWorkspace`].
pub fn decode_qpack(slice: &[u8]) -> Result<DecodedHeaders, Http3Error> {
    decode_qpack_internal(slice, None)
}

/// Single-pass ingress QPACK decode pre-seeding `x-request-id` (Bit-63 bypass) and proxy
/// forwarding headers (`x-forwarded-*`, `x-real-ip`, `forwarded`) while skipping spoofed client
/// forwarding headers and RFC 9114 §4.2 prohibited headers.
pub fn decode_qpack_ingress(
    slice: &[u8],
    enrich: H3IngressEnrich,
) -> Result<DecodedHeaders, Http3Error> {
    decode_qpack_internal(slice, Some(enrich))
}

#[inline]
fn bytes_to_header_value(raw: Cow<'_, [u8]>) -> Option<HeaderValue> {
    match raw {
        Cow::Borrowed(b) => HeaderValue::from_bytes(b).ok(),
        Cow::Owned(v) => HeaderValue::from_maybe_shared(Bytes::from(v)).ok(),
    }
}

fn decode_qpack_internal(
    mut slice: &[u8],
    enrich: Option<H3IngressEnrich>,
) -> Result<DecodedHeaders, Http3Error> {
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
    let is_ingress = enrich.is_some();
    let forwarding = enrich.as_ref().and_then(|e| e.forwarding);
    let has_forwarding = forwarding.is_some();
    let mut rem_slots = MAX_WIRE_HEADERS;

    // Pre-formatted peer IP cache reused between `x-real-ip`/`x-forwarded-for` and `forwarded`.
    let mut peer_ip_buf = [0u8; 48];
    let mut peer_ip_len = 0usize;

    if let Some(meta) = enrich {
        decoded
            .headers
            .push_prehashed_bypass(KEY_X_REQUEST_ID, X_REQUEST_ID, meta.request_id);
        if let Some(fwd) = meta.forwarding {
            let (ip_buf, ip_len) = velda_core::lut::ip::format_ip_bytes(fwd.peer_ip);
            let len = ip_len.min(peer_ip_buf.len());
            peer_ip_buf[..len].copy_from_slice(&ip_buf[..len]);
            peer_ip_len = len;

            if let Ok(ip_val) = HeaderValue::from_bytes(&peer_ip_buf[..len]) {
                decoded
                    .headers
                    .push_prehashed(KEY_X_REAL_IP, X_REAL_IP, ip_val.clone());
                decoded
                    .headers
                    .push_prehashed(KEY_X_FORWARDED_FOR, X_FORWARDED_FOR, ip_val);
            }
            decoded.headers.push_prehashed(
                KEY_X_FORWARDED_PROTO,
                X_FORWARDED_PROTO,
                HeaderValue::from_static("https"),
            );
            decoded.headers.push_prehashed(
                KEY_X_FORWARDED_PORT,
                X_FORWARDED_PORT,
                HeaderValue::from(u16::from_be(fwd.local_port_be)),
            );
            // Reserve 1 (`x-request-id`) + 4 pre-seeded + 2 (`x-forwarded-host`, `forwarded`).
            rem_slots -= 7;
        } else {
            rem_slots -= 1;
        }
    }

    let mut raw_path: Option<String> = None;
    let mut raw_authority: Option<String> = None;
    let mut raw_scheme: Option<String> = None;

    // 2. Decode Field Lines directly into flat HeaderWorkspace
    while !slice.is_empty() {
        let first = slice[0];

        if (first & 0xc0) == 0xc0 {
            // Pattern 11xxxxxx: Indexed Field Line with Static Table Reference (Section 4.5.2)
            let (idx, consumed) = decode_prefixed_int(slice, 6)
                .ok_or_else(|| Http3Error::H3("Invalid Indexed Field Line".into()))?;
            slice = &slice[consumed..];

            let idx_usize = usize::try_from(idx)
                .map_err(|_| Http3Error::H3(format!("Static table index out of bounds: {idx}")))?;
            let (name, val) = STATIC_TABLE.get(idx_usize).ok_or_else(|| {
                Http3Error::H3(format!("Static table index out of bounds: {idx}"))
            })?;

            apply_header_bytes(
                name.as_bytes(),
                Cow::Borrowed(val.as_bytes()),
                Some(idx_usize),
                true,
                is_ingress,
                has_forwarding,
                &mut rem_slots,
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

            let idx_usize = usize::try_from(idx)
                .map_err(|_| Http3Error::H3(format!("Static table index out of bounds: {idx}")))?;
            let name = STATIC_TABLE
                .get(idx_usize)
                .ok_or_else(|| Http3Error::H3(format!("Static table index out of bounds: {idx}")))?
                .0;

            let (val, consumed_val) = decode_string_literal_bytes(slice, 7)
                .ok_or_else(|| Http3Error::H3("Invalid string literal value".into()))?;
            slice = &slice[consumed_val..];

            apply_header_bytes(
                name.as_bytes(),
                val,
                Some(idx_usize),
                false,
                is_ingress,
                has_forwarding,
                &mut rem_slots,
                &mut decoded,
                &mut raw_path,
                &mut raw_authority,
                &mut raw_scheme,
            )?;
        } else if (first & 0xe0) == 0x20 {
            // Pattern 001Nxxxx: Literal Field Line with Literal Name (Section 4.5.6)
            let (name, consumed_name) = decode_string_literal_bytes(slice, 3)
                .ok_or_else(|| Http3Error::H3("Invalid literal field name".into()))?;
            slice = &slice[consumed_name..];

            let (val, consumed_val) = decode_string_literal_bytes(slice, 7)
                .ok_or_else(|| Http3Error::H3("Invalid literal field value".into()))?;
            slice = &slice[consumed_val..];

            apply_header_bytes(
                &name,
                val,
                None,
                false,
                is_ingress,
                has_forwarding,
                &mut rem_slots,
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
            Cow::Owned(format!("{scheme}://{auth}{path}"))
        } else {
            Cow::Borrowed(path.as_str())
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

    // Inject host-dependent `x-forwarded-host` and RFC 7239 `forwarded` directly in the same decode pass.
    if let Some(fwd) = forwarding {
        let host_hdr = decoded.headers.get(http::header::HOST).cloned();
        let host_str = host_hdr.as_ref().and_then(|v| v.to_str().ok()).or_else(|| {
            decoded
                .uri
                .as_ref()
                .and_then(|u| u.authority().map(|a| a.as_str()).or_else(|| u.host()))
        });

        if let Some(h) = host_str
            && let Ok(hv) = HeaderValue::from_str(h)
        {
            decoded
                .headers
                .push_prehashed(KEY_X_FORWARDED_HOST, X_FORWARDED_HOST, hv);
        }

        let mut fwd_buf = [0u8; 256];
        let mut rem = &mut fwd_buf[..];
        let push = |dst: &mut &mut [u8], src: &[u8]| {
            if let Some((slot, rest)) = std::mem::take(dst).split_at_mut_checked(src.len()) {
                slot.copy_from_slice(src);
                *dst = rest;
            }
        };

        let peer_bytes = &peer_ip_buf[..peer_ip_len];
        push(&mut rem, b"for=");
        if velda_core::lut::ip::is_v4_u128(fwd.peer_ip) {
            push(&mut rem, peer_bytes);
        } else {
            push(&mut rem, b"\"[");
            push(&mut rem, peer_bytes);
            push(&mut rem, b"]\"");
        }

        push(&mut rem, b";proto=https;by=");
        let (local_buf, local_len) = velda_core::lut::ip::format_ip_bytes(fwd.local_ip);
        if let Some(local_bytes) = local_buf.get(..local_len) {
            if velda_core::lut::ip::is_v4_u128(fwd.local_ip) {
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
            decoded
                .headers
                .push_prehashed(KEY_FORWARDED, FORWARDED, val);
        }
    }

    Ok(decoded)
}

#[allow(clippy::too_many_arguments)]
#[inline]
fn apply_header_bytes(
    name: &[u8],
    val: Cow<'_, [u8]>,
    static_idx: Option<usize>,
    is_static_val: bool,
    is_ingress: bool,
    has_forwarding: bool,
    rem_slots: &mut usize,
    decoded: &mut DecodedHeaders,
    raw_path: &mut Option<String>,
    raw_authority: &mut Option<String>,
    raw_scheme: &mut Option<String>,
) -> Result<(), Http3Error> {
    if name.starts_with(b":") {
        match name {
            b":method" => {
                if let Ok(m) = Method::from_bytes(&val) {
                    decoded.method = Some(m);
                }
            }
            b":path" => {
                if let Ok(s) = std::str::from_utf8(&val) {
                    *raw_path = Some(s.to_owned());
                }
            }
            b":scheme" => {
                if let Ok(s) = std::str::from_utf8(&val) {
                    *raw_scheme = Some(s.to_owned());
                }
            }
            b":authority" => {
                if let Ok(s) = std::str::from_utf8(&val) {
                    *raw_authority = Some(s.to_owned());
                }
                if let Some(hval) = bytes_to_header_value(val)
                    && !decoded.headers.contains_key(http::header::HOST)
                {
                    let Some(next) = rem_slots.checked_sub(1) else {
                        return Err(Http3Error::H3(
                            "Header count exceeds MAX_WIRE_HEADERS".into(),
                        ));
                    };
                    *rem_slots = next;
                    decoded
                        .headers
                        .push_prehashed(KEY_HOST, http::header::HOST, hval);
                }
            }
            b":status" => {
                if let Ok(s) = std::str::from_utf8(&val)
                    && let Some(sc) = s
                        .parse::<u16>()
                        .ok()
                        .and_then(|c| StatusCode::from_u16(c).ok())
                {
                    decoded.status = Some(sc);
                }
            }
            _ => {}
        }
        return Ok(());
    }

    let Some(next) = rem_slots.checked_sub(1) else {
        return Err(Http3Error::H3(
            "Header count exceeds MAX_WIRE_HEADERS".into(),
        ));
    };
    *rem_slots = next;

    let (key_hash, hname) = if let Some(idx) = static_idx
        && let Some(static_name) = static_header_name(idx)
    {
        (STATIC_TABLE_KEYS[idx], static_name)
    } else if let Ok(parsed_name) = HeaderName::from_bytes(name) {
        (header_key(parsed_name.as_str().as_bytes()), parsed_name)
    } else {
        return Ok(());
    };

    if is_ingress {
        if key_hash == KEY_X_REQUEST_ID && hname == X_REQUEST_ID {
            return Ok(());
        }
        if has_forwarding
            && ((key_hash == KEY_CONNECTION && hname == http::header::CONNECTION)
                || (key_hash == KEY_KEEP_ALIVE && hname.as_str() == "keep-alive")
                || (key_hash == KEY_PROXY_CONNECTION && hname.as_str() == "proxy-connection")
                || (key_hash == KEY_TRANSFER_ENCODING && hname == http::header::TRANSFER_ENCODING)
                || (key_hash == KEY_UPGRADE && hname == http::header::UPGRADE)
                || (key_hash == KEY_X_FORWARDED_FOR && hname == X_FORWARDED_FOR)
                || (key_hash == KEY_X_REAL_IP && hname == X_REAL_IP)
                || (key_hash == KEY_X_FORWARDED_PROTO && hname == X_FORWARDED_PROTO)
                || (key_hash == KEY_X_FORWARDED_PORT && hname == X_FORWARDED_PORT)
                || (key_hash == KEY_X_FORWARDED_HOST && hname == X_FORWARDED_HOST)
                || (key_hash == KEY_FORWARDED && hname == FORWARDED))
        {
            return Ok(());
        }
    }

    let hval = if is_static_val
        && let Some(idx) = static_idx
        && let Some(static_val) = STATIC_HEADER_VALUES.get(idx)
    {
        static_val.clone()
    } else if let Some(parsed_val) = bytes_to_header_value(val) {
        parsed_val
    } else {
        return Ok(());
    };

    if is_ingress
        && has_forwarding
        && key_hash == KEY_TE
        && hname == http::header::TE
        && !hval
            .to_str()
            .is_ok_and(|s| s.trim().eq_ignore_ascii_case("trailers"))
    {
        return Ok(());
    }

    decoded.headers.push_prehashed(key_hash, hname, hval);
    Ok(())
}

/// Encodes an HTTP/3 response field section into QPACK bytes directly from [`HeaderWorkspace`].
pub fn encode_qpack_response(status: StatusCode, headers: &HeaderWorkspace, dst: &mut BytesMut) {
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
    for (name, val) in headers.iter() {
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

/// Encodes an HTTP/3 request field section into QPACK bytes directly from [`HeaderWorkspace`].
pub fn encode_qpack_request(
    method: &Method,
    uri: &Uri,
    headers: &HeaderWorkspace,
    dst: &mut BytesMut,
) {
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

    // 6. Additional headers (RFC 9114 §4.2: connection-specific headers MUST NOT be sent)
    for (name, val) in headers.iter() {
        // :authority already encoded from URI / HOST above
        if name == http::header::HOST {
            continue;
        }
        // Drop connection-specific headers that are forbidden in HTTP/3
        if name == http::header::CONNECTION
            || name == http::header::TRANSFER_ENCODING
            || name == http::header::UPGRADE
            || name.as_str().eq_ignore_ascii_case("keep-alive")
            || name.as_str().eq_ignore_ascii_case("proxy-connection")
        {
            continue;
        }
        // RFC 9114 §4.2: TE, if present, MUST only contain "trailers"
        if name == http::header::TE {
            let is_trailers = val
                .to_str()
                .is_ok_and(|s| s.trim().eq_ignore_ascii_case("trailers"));
            if !is_trailers {
                continue;
            }
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
        let mut headers = HeaderWorkspace::empty(4);
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
        let mut headers = HeaderWorkspace::empty(4);
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

    #[test]
    fn test_qpack_request_strips_rfc9114_prohibited_headers() {
        let mut buf = BytesMut::new();
        let mut headers = HeaderWorkspace::empty(8);
        // These MUST be dropped by the encoder per RFC 9114 §4.2
        headers.insert(
            http::header::TRANSFER_ENCODING,
            HeaderValue::from_static("chunked"),
        );
        headers.insert(
            http::header::CONNECTION,
            HeaderValue::from_static("keep-alive"),
        );
        headers.insert(http::header::UPGRADE, HeaderValue::from_static("websocket"));
        headers.insert(
            HeaderName::from_static("keep-alive"),
            HeaderValue::from_static("timeout=5"),
        );
        headers.insert(
            HeaderName::from_static("proxy-connection"),
            HeaderValue::from_static("close"),
        );
        // TE with non-"trailers" value must also be dropped
        headers.insert(http::header::TE, HeaderValue::from_static("gzip"));
        // This one must pass through
        headers.insert(
            http::header::CONTENT_TYPE,
            HeaderValue::from_static("application/octet-stream"),
        );

        let method = Method::POST;
        let uri: Uri = "https://upstream.example.com/upload".parse().unwrap();
        encode_qpack_request(&method, &uri, &headers, &mut buf);

        let decoded = decode_qpack(&buf).unwrap();
        // All prohibited headers must be absent from encoded output
        assert!(
            decoded
                .headers
                .get(http::header::TRANSFER_ENCODING)
                .is_none()
        );
        assert!(decoded.headers.get(http::header::CONNECTION).is_none());
        assert!(decoded.headers.get(http::header::UPGRADE).is_none());
        assert!(decoded.headers.get("keep-alive").is_none());
        assert!(decoded.headers.get("proxy-connection").is_none());
        assert!(decoded.headers.get(http::header::TE).is_none());
        // Allowed header must survive encoding
        assert_eq!(
            decoded.headers.get(http::header::CONTENT_TYPE).unwrap(),
            "application/octet-stream"
        );
    }

    #[test]
    fn test_debug_print_encoded() {
        let mut headers = HeaderWorkspace::empty(4);
        headers.insert(
            http::header::TRANSFER_ENCODING,
            HeaderValue::from_static("chunked"),
        );
        headers.insert(
            http::header::CONTENT_TYPE,
            HeaderValue::from_static("application/octet-stream"),
        );
        let mut buf = BytesMut::new();
        let method = Method::POST;
        let uri: Uri = "/upload-h3/5mb".parse().unwrap();
        encode_qpack_request(&method, &uri, &headers, &mut buf);
        let dec = decode_qpack(&buf).unwrap();
        assert!(dec.headers.get(http::header::TRANSFER_ENCODING).is_none());
    }

    #[test]
    fn test_decode_qpack_ingress_preseed_bypass_and_limit() {
        let mut headers = HeaderWorkspace::empty(4);
        headers.insert(X_REQUEST_ID, HeaderValue::from_static("spoofed-id"));
        headers.insert(
            http::header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
        let mut buf = BytesMut::new();
        let uri: Uri = "https://example.com/v1".parse().unwrap();
        encode_qpack_request(&Method::GET, &uri, &headers, &mut buf);

        let dec = decode_qpack_ingress(
            &buf,
            H3IngressEnrich {
                request_id: HeaderValue::from_static("auth-req-id"),
                forwarding: None,
            },
        )
        .unwrap();
        assert_eq!(dec.headers.get("x-request-id").unwrap(), "auth-req-id");
        assert!(
            dec.headers
                .bypass_headers()
                .any(|(n, v)| n == X_REQUEST_ID && v == "auth-req-id")
        );
        assert_eq!(dec.headers.get("content-type").unwrap(), "application/json");
    }

    #[test]
    fn test_decode_qpack_ingress_single_pass_enrichment_and_anti_spoof() {
        // Encode a raw request containing client-spoofed forwarding headers directly using low-level QPACK literals
        let mut buf = BytesMut::new();
        buf.put_u8(0x00);
        buf.put_u8(0x00);
        encode_prefixed_int(17, 6, 0xc0, &mut buf); // :method: GET
        encode_prefixed_int(23, 6, 0xc0, &mut buf); // :scheme: https
        encode_prefixed_int(0, 4, 0x50, &mut buf); // :authority
        encode_string_literal(b"quic.example.com", &mut buf);
        encode_prefixed_int(1, 4, 0x50, &mut buf); // :path
        encode_string_literal(b"/test", &mut buf);
        encode_literal_name_and_value(b"x-forwarded-for", b"203.0.113.195", &mut buf);
        encode_literal_name_and_value(b"x-real-ip", b"203.0.113.195", &mut buf);
        encode_literal_name_and_value(b"x-forwarded-host", b"evil.attacker.com", &mut buf);
        encode_literal_name_and_value(b"x-forwarded-proto", b"http", &mut buf);
        encode_literal_name_and_value(b"connection", b"close", &mut buf);
        encode_literal_name_and_value(b"keep-alive", b"timeout=5", &mut buf);
        encode_literal_name_and_value(b"forwarded", b"for=203.0.113.195;proto=http", &mut buf);
        encode_literal_name_and_value(b"x-custom", b"ok", &mut buf);

        let peer_ip = velda_core::lut::ip::v4_to_u128(0xC000_021E);
        let local_ip = velda_core::lut::ip::v4_to_u128(0x0A00_0001);
        let dec = decode_qpack_ingress(
            &buf,
            H3IngressEnrich {
                request_id: HeaderValue::from_static("auth-req-id"),
                forwarding: Some(H3ForwardingAddrs {
                    peer_ip,
                    local_ip,
                    local_port_be: 443u16.to_be(),
                }),
            },
        )
        .unwrap();

        assert_eq!(dec.headers.get("x-forwarded-for").unwrap(), "192.0.2.30");
        assert_eq!(dec.headers.get("x-real-ip").unwrap(), "192.0.2.30");
        assert_eq!(dec.headers.get("x-forwarded-proto").unwrap(), "https");
        assert_eq!(dec.headers.get("x-forwarded-port").unwrap(), "443");
        assert_eq!(
            dec.headers.get("x-forwarded-host").unwrap(),
            "quic.example.com"
        );
        assert_eq!(
            dec.headers.get("forwarded").unwrap(),
            "for=192.0.2.30;proto=https;by=10.0.0.1;host=\"quic.example.com\""
        );
        assert!(dec.headers.get("connection").is_none());
        assert!(dec.headers.get("keep-alive").is_none());
        assert_eq!(dec.headers.get("x-custom").unwrap(), "ok");
    }

    #[test]
    fn test_decode_prefixed_int_bounds_and_static_lut() {
        // 8-bit prefix boundary must not overflow `1u64 << 8` or mask arithmetic
        assert_eq!(decode_prefixed_int(&[0xff, 0x00], 8), Some((255, 2)));
        assert_eq!(decode_prefixed_int(&[0xfe], 8), Some((254, 1)));
        assert_eq!(decode_prefixed_int(&[0x00], 0), None);
        assert_eq!(decode_prefixed_int(&[0x00], 9), None);

        // Overflowing 64-bit continuation must return None instead of wrapping
        let overflow_payload = [
            0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01,
        ];
        assert_eq!(decode_prefixed_int(&overflow_payload, 8), None);

        // Verify compile-time LUT matches STATIC_TABLE verbatim
        for (idx, &(name, val)) in STATIC_TABLE.iter().enumerate() {
            if name.starts_with(':') {
                assert!(static_header_name(idx).is_none());
            } else {
                assert_eq!(static_header_name(idx).unwrap().as_str(), name);
            }
            assert_eq!(STATIC_HEADER_VALUES[idx].to_str().unwrap(), val);
        }
    }
}
