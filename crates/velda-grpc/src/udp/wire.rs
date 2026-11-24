//! Dedicated wire protocol framing and QPACK header codec for Layer 7 gRPC over UDP.
//!
//! Provides zero-copy variable-length integer encoding/decoding, RFC 9114 HTTP/3
//! frame serialization, and RFC 9204 QPACK static-table header encoding/decoding
//! without borrowing any code or dependencies from HTTP crates.

use std::borrow::Cow;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use http::header::{HeaderName, HeaderValue};
use http::{Method, StatusCode, Uri};
use velda_core::buffer::header::{
    DEFAULT_HEADER_CAPACITY, HeaderWorkspace, MAX_WIRE_HEADERS, X_REQUEST_ID, header_key,
};

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

/// Pre-built compile-time [`HeaderName`] LUT for non-pseudo RFC 9204 static table entries.
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
    if slice.is_empty() || !(1..=8).contains(&prefix_bits) {
        return None;
    }
    let max_prefix = if prefix_bits == 8 {
        u8::MAX as u64
    } else {
        (1u64 << prefix_bits) - 1
    };
    let first = (slice[0] as u64) & max_prefix;
    if first < max_prefix {
        return Some((first, 1));
    }
    let mut val = first;
    let mut shift: u32 = 0;
    let mut consumed = 1;
    while consumed < slice.len() {
        let b = slice[consumed];
        consumed += 1;
        let added = ((b & 0x7f) as u64).checked_shl(shift)?;
        val = val.checked_add(added)?;
        shift += 7;
        if (b & 0x80) == 0 {
            return Some((val, consumed));
        }
        if shift >= 63 {
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

/// Decodes a string literal (RFC 9204 Section 4.1.2) into raw bytes with an N-bit length prefix.
#[inline]
fn decode_string_literal_bytes(slice: &[u8], prefix_bits: u8) -> Option<(Cow<'_, [u8]>, usize)> {
    if slice.is_empty() || prefix_bits >= 8 {
        return None;
    }
    let (len, consumed) = decode_prefixed_int(slice, prefix_bits)?;
    let len_usize = usize::try_from(len).ok()?;
    let start = consumed;
    let end = start.checked_add(len_usize)?;
    let data = slice.get(start..end)?;
    Some((Cow::Borrowed(data), end))
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
    let Ok(payload_len) = usize::try_from(length) else {
        return Err(GrpcError::Protocol(
            "HTTP/3 gRPC frame payload length exceeds address space".into(),
        ));
    };
    let Some(total_len) = header_len.checked_add(payload_len) else {
        return Err(GrpcError::Protocol(
            "HTTP/3 gRPC frame total length overflows address space".into(),
        ));
    };
    if buf.len() < total_len {
        return Ok(None);
    }
    buf.advance(header_len);
    let payload = buf.split_to(payload_len).freeze();

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

/// Compile-time 63-bit FNV-1a hashes for all 99 gRPC UDP static table header names.
const STATIC_TABLE_KEYS: [u64; 99] = {
    let mut keys = [0u64; 99];
    let mut i = 0;
    while i < 99 {
        keys[i] = header_key(STATIC_TABLE[i].0.as_bytes());
        i += 1;
    }
    keys
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
const PROTO_HTTPS_VAL: HeaderValue = HeaderValue::from_static("https");

/// Network address bundle for 1-pass proxy forwarding enrichment during gRPC UDP QPACK decode.
#[derive(Debug, Clone, Copy)]
pub struct GrpcUdpForwardingAddrs {
    pub peer_ip: u128,
    pub local_ip: u128,
    pub local_port_be: u16,
}

/// Parameters for 1-pass QPACK ingress decoding with Bit-63 `x-request-id` seeding
/// and optional proxy forwarding header injection (`x-forwarded-*`, `x-real-ip`, `forwarded`).
#[derive(Debug, Clone)]
pub struct GrpcUdpIngressEnrich {
    pub request_id: HeaderValue,
    pub forwarding: Option<GrpcUdpForwardingAddrs>,
}

/// Decoded request or response headers from QPACK field section directly into [`HeaderWorkspace`].
#[derive(Debug)]
pub struct DecodedHeaders {
    pub method: Option<Method>,
    pub uri: Option<Uri>,
    pub status: Option<StatusCode>,
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

/// Decodes QPACK encoded headers directly into [`HeaderWorkspace`].
pub fn decode_qpack(slice: &[u8]) -> Result<DecodedHeaders, GrpcError> {
    decode_qpack_internal(slice, None)
}

/// Single-pass ingress QPACK decode pre-seeding `x-request-id` (Bit-63 bypass) and proxy
/// forwarding headers (`x-forwarded-*`, `x-real-ip`, `forwarded`) while skipping spoofed client
/// forwarding headers and RFC 9114/9113 hop-by-hop headers.
pub fn decode_qpack_ingress(
    slice: &[u8],
    enrich: GrpcUdpIngressEnrich,
) -> Result<DecodedHeaders, GrpcError> {
    decode_qpack_internal(slice, Some(enrich))
}

fn decode_qpack_internal(
    mut slice: &[u8],
    enrich: Option<GrpcUdpIngressEnrich>,
) -> Result<DecodedHeaders, GrpcError> {
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
    let is_ingress = enrich.is_some();
    let forwarding = enrich.as_ref().and_then(|e| e.forwarding);
    let has_forwarding = forwarding.is_some();
    let mut rem_slots = MAX_WIRE_HEADERS;

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
                PROTO_HTTPS_VAL,
            );
            decoded.headers.push_prehashed(
                KEY_X_FORWARDED_PORT,
                X_FORWARDED_PORT,
                velda_core::buffer::header::resolve_u64_header_value(
                    u16::from_be(fwd.local_port_be) as u64,
                ),
            );
            rem_slots -= 7;
        } else {
            rem_slots -= 1;
        }
    }
    let mut raw_path: Option<String> = None;
    let mut raw_authority: Option<String> = None;
    let mut raw_scheme: Option<String> = None;

    // 2. Process field lines
    while !slice.is_empty() {
        let first = slice[0];
        if (first & 0x80) != 0 {
            // Indexed Field Line (RFC 9204 §4.5.2)
            let is_static = (first & 0x40) != 0;
            let (idx, consumed) = decode_prefixed_int(slice, 6)
                .ok_or_else(|| GrpcError::Internal("malformed QPACK index".into()))?;
            slice = &slice[consumed..];

            if is_static
                && let Ok(idx_usize) = usize::try_from(idx)
                && let Some(&(name, val)) = STATIC_TABLE.get(idx_usize)
            {
                apply_header_pair_bytes(
                    name.as_bytes(),
                    val.as_bytes(),
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
            }
        } else if (first & 0x40) != 0 {
            // Literal Field Line with Name Reference (RFC 9204 §4.5.4)
            let is_static = (first & 0x10) != 0;
            let (name_idx, consumed) = decode_prefixed_int(slice, 4)
                .ok_or_else(|| GrpcError::Internal("malformed QPACK name index".into()))?;
            slice = &slice[consumed..];

            let (val, val_consumed) = decode_string_literal_bytes(slice, 7)
                .ok_or_else(|| GrpcError::Internal("malformed QPACK value string".into()))?;
            slice = &slice[val_consumed..];

            if is_static
                && let Ok(idx_usize) = usize::try_from(name_idx)
                && let Some(&(name, _)) = STATIC_TABLE.get(idx_usize)
            {
                apply_header_pair_bytes(
                    name.as_bytes(),
                    &val,
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
            }
        } else if (first & 0x20) != 0 {
            // Literal Field Line with Literal Name (RFC 9204 §4.5.6)
            let (name, name_consumed) = decode_string_literal_bytes(slice, 3)
                .ok_or_else(|| GrpcError::Internal("malformed QPACK literal name".into()))?;
            slice = &slice[name_consumed..];

            let (val, val_consumed) = decode_string_literal_bytes(slice, 7)
                .ok_or_else(|| GrpcError::Internal("malformed QPACK literal value".into()))?;
            slice = &slice[val_consumed..];

            apply_header_pair_bytes(
                &name,
                &val,
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
            // Unhandled or reserved QPACK instruction
            break;
        }
    }

    if let Some(path) = raw_path {
        let uri_str = if let (Some(scheme), Some(auth)) = (raw_scheme, raw_authority) {
            std::borrow::Cow::Owned(format!("{scheme}://{auth}{path}"))
        } else {
            std::borrow::Cow::Borrowed(path.as_str())
        };
        if let Ok(u) = uri_str.parse::<Uri>() {
            decoded.uri = Some(u);
        }
    } else if let Some(auth) = raw_authority
        && let Ok(u) = auth.parse::<Uri>()
    {
        decoded.uri = Some(u);
    }
    if decoded.method.is_none() {
        decoded.method = Some(Method::POST);
    }

    if let Some(fwd) = forwarding {
        let auth_hdr = decoded.headers.get(http::header::HOST).cloned();
        let auth_str = auth_hdr.as_ref().and_then(|v| v.to_str().ok()).or_else(|| {
            decoded
                .uri
                .as_ref()
                .and_then(|u| u.authority().map(|a| a.as_str()).or_else(|| u.host()))
        });

        if let Some(auth) = auth_str
            && let Ok(val) = HeaderValue::from_str(auth)
        {
            decoded
                .headers
                .push_prehashed(KEY_X_FORWARDED_HOST, X_FORWARDED_HOST, val);
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

        if let Some(auth) = auth_str {
            let auth_bytes = auth.as_bytes();
            if rem.len() >= 8 + auth_bytes.len() {
                push(&mut rem, b";host=\"");
                push(&mut rem, auth_bytes);
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
fn apply_header_pair_bytes(
    name: &[u8],
    val: &[u8],
    static_idx: Option<usize>,
    is_static_val: bool,
    is_ingress: bool,
    has_forwarding: bool,
    rem_slots: &mut usize,
    decoded: &mut DecodedHeaders,
    raw_path: &mut Option<String>,
    raw_authority: &mut Option<String>,
    raw_scheme: &mut Option<String>,
) -> Result<(), GrpcError> {
    if name.starts_with(b":") {
        match name {
            b":method" => {
                if let Ok(m) = Method::from_bytes(val) {
                    decoded.method = Some(m);
                }
            }
            b":path" => {
                if let Ok(s) = std::str::from_utf8(val) {
                    *raw_path = Some(s.to_owned());
                }
            }
            b":authority" => {
                if let Ok(s) = std::str::from_utf8(val) {
                    *raw_authority = Some(s.to_owned());
                }
                if let Ok(v) = HeaderValue::from_bytes(val)
                    && !decoded.headers.contains_key(http::header::HOST)
                {
                    let Some(next) = rem_slots.checked_sub(1) else {
                        return Err(GrpcError::Protocol("header count exceeds limit".into()));
                    };
                    *rem_slots = next;
                    decoded
                        .headers
                        .push_prehashed(KEY_HOST, http::header::HOST, v);
                }
            }
            b":scheme" => {
                if let Ok(s) = std::str::from_utf8(val) {
                    *raw_scheme = Some(s.to_owned());
                }
            }
            b":status" => {
                if let Ok(s) = std::str::from_utf8(val)
                    && let Ok(code) = s.parse::<u16>()
                    && let Ok(sc) = StatusCode::from_u16(code)
                {
                    decoded.status = Some(sc);
                }
            }
            _ => {}
        }
        return Ok(());
    }

    let Some(next) = rem_slots.checked_sub(1) else {
        return Err(GrpcError::Protocol("header count exceeds limit".into()));
    };
    *rem_slots = next;

    let (key_hash, hname) = if let Some(idx) = static_idx
        && let Some(Some(static_hname)) = STATIC_HEADER_NAMES.get(idx)
    {
        (STATIC_TABLE_KEYS[idx], static_hname.clone())
    } else if let Ok(parsed) = HeaderName::from_bytes(name) {
        (header_key(parsed.as_str().as_bytes()), parsed)
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
    } else if let Ok(v) = HeaderValue::from_bytes(val) {
        v
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

/// Encodes QPACK response headers directly from [`HeaderWorkspace`] into the destination buffer.
pub fn encode_qpack_response(status: StatusCode, headers: &HeaderWorkspace, dst: &mut BytesMut) {
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
    for (name, val) in headers.iter() {
        let name_str = name.as_str();
        encode_prefixed_int(name_str.len() as u64, 3, 0x20, dst);
        dst.put_slice(name_str.as_bytes());
        encode_string_literal(val.as_bytes(), dst);
    }
}

/// Encodes QPACK request headers directly from [`HeaderWorkspace`] into the destination buffer (RFC 9204).
pub fn encode_qpack_request(
    method: &Method,
    uri: &Uri,
    headers: &HeaderWorkspace,
    dst: &mut BytesMut,
) {
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
    for (name, val) in headers.iter() {
        if name == http::header::HOST {
            continue;
        }
        let name_str = name.as_str();
        encode_prefixed_int(name_str.len() as u64, 3, 0x20, dst);
        dst.put_slice(name_str.as_bytes());
        encode_string_literal(val.as_bytes(), dst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_qpack_ingress_single_pass_enrichment_and_anti_spoof() {
        let mut buf = BytesMut::new();
        buf.put_u8(0x00);
        buf.put_u8(0x00);
        encode_prefixed_int(20, 6, 0xc0, &mut buf); // :method: POST
        encode_prefixed_int(23, 6, 0xc0, &mut buf); // :scheme: https
        encode_prefixed_int(0, 4, 0x50, &mut buf); // :authority
        encode_string_literal(b"udp-grpc.internal", &mut buf);
        encode_prefixed_int(1, 4, 0x50, &mut buf); // :path
        encode_string_literal(b"/pkg.Svc/Call", &mut buf);

        let push_lit = |dst: &mut BytesMut, k: &[u8], v: &[u8]| {
            encode_prefixed_int(k.len() as u64, 3, 0x20, dst);
            dst.put_slice(k);
            encode_string_literal(v, dst);
        };
        push_lit(&mut buf, b"x-request-id", b"spoofed-id");
        push_lit(&mut buf, b"x-forwarded-for", b"10.0.0.99");
        push_lit(&mut buf, b"connection", b"keep-alive");
        push_lit(&mut buf, b"te", b"trailers");
        push_lit(&mut buf, b"content-type", b"application/grpc");

        let dec = decode_qpack_ingress(
            &buf,
            GrpcUdpIngressEnrich {
                request_id: HeaderValue::from_static("auth-grpc-id"),
                forwarding: Some(GrpcUdpForwardingAddrs {
                    peer_ip: velda_core::lut::ip::v4_to_u128(0xC0A8_0101),
                    local_ip: velda_core::lut::ip::v4_to_u128(0x0A00_0001),
                    local_port_be: 443u16.to_be(),
                }),
            },
        )
        .unwrap();

        assert_eq!(dec.headers.get("x-request-id").unwrap(), "auth-grpc-id");
        assert!(
            dec.headers
                .bypass_headers()
                .any(|(n, v)| n == X_REQUEST_ID && v == "auth-grpc-id")
        );
        assert_eq!(dec.headers.get("x-forwarded-for").unwrap(), "192.168.1.1");
        assert_eq!(dec.headers.get("x-forwarded-proto").unwrap(), "https");
        assert_eq!(
            dec.headers.get("x-forwarded-host").unwrap(),
            "udp-grpc.internal"
        );
        assert_eq!(
            dec.headers.get("forwarded").unwrap(),
            "for=192.168.1.1;proto=https;by=10.0.0.1;host=\"udp-grpc.internal\""
        );
        assert!(dec.headers.get("connection").is_none());
        assert_eq!(dec.headers.get("te").unwrap(), "trailers");
        assert_eq!(dec.headers.get("content-type").unwrap(), "application/grpc");
    }

    #[test]
    fn test_udp_qpack_prefixed_int_bounds_and_static_lut() {
        assert_eq!(decode_prefixed_int(&[0xff, 0x00], 8), Some((255, 2)));
        assert_eq!(decode_prefixed_int(&[0xfe], 8), Some((254, 1)));
        assert_eq!(decode_prefixed_int(&[0x00], 0), None);
        assert_eq!(decode_prefixed_int(&[0x00], 9), None);

        let overflow = [
            0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01,
        ];
        assert_eq!(decode_prefixed_int(&overflow, 8), None);

        for (idx, &(name, val)) in STATIC_TABLE.iter().enumerate() {
            if name.starts_with(':') {
                assert!(STATIC_HEADER_NAMES[idx].is_none());
            } else {
                assert_eq!(STATIC_HEADER_NAMES[idx].as_ref().unwrap().as_str(), name);
            }
            assert_eq!(STATIC_HEADER_VALUES[idx].to_str().unwrap(), val);
        }
    }
}
