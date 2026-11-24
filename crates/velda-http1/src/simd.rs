//! Multi-Target SIMD HTTP/1.1 Wire Header Parser into [`velda_core::buffer::header::HeaderWorkspace`].
//!
//! ```text
//!   Wire Slice `data: &[u8]` (after Request/Status Line `\r\n`)
//!     ├─► memchr::memchr(b'\n')     : SIMD line split, bare-LF & obs-fold defense
//!     ├─► memchr::memchr(b':')      : Split `raw_name : raw_value`, reject empty/spaced names
//!     ├─► TCHAR_LOWER_LUT + FNV-1a  : Well-known static HeaderName fast-path OR 1-pass lowercase + 63-bit hash
//!     ├─► is_valid_header_value_simd: 32B AVX2 / 16B SSE2 / 16B NEON / 8B SWAR field-value validation
//!     └─► HeaderWorkspace::push_prehashed(hash63, name, value)  [Single flat `[tags | slots]` block]
//! ```

use bytes::Bytes;
use http::header::{CONTENT_LENGTH, HeaderName, HeaderValue, TRANSFER_ENCODING};
use memchr::{memchr, memchr2};
use velda_core::buffer::header::{
    DEFAULT_HEADER_CAPACITY, HeaderWorkspace, MAX_WIRE_HEADERS, header_key,
};

use crate::config::Http1Config;
use crate::error::Http1Error;
use crate::wire::{cold_parse_error, cold_smuggling_error, parse_ascii_digits};

/// Mask ensuring header name hash fits in lower 63 bits of `u64` tag.
const TAG_HASH_MASK: u64 = (1u64 << 63) - 1;
const FNV1A_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV1A_PRIME: u64 = 0x0100_0000_01b3;

/// Maximum header name length permitted on wire stack lowercase buffer.
const MAX_HEADER_NAME_LEN: usize = 256;

/// 256-byte RFC 9110 HTTP token (`tchar`) lookup + ASCII lowercase table.
/// Valid token bytes map to lowercase ASCII; invalid bytes (spaces, controls, delimiters) map to `0xFF`.
const TCHAR_LOWER_LUT: [u8; 256] = {
    let mut t = [0xFFu8; 256];
    let mut i = 0u8;
    while i < 128 {
        let lower = if i >= b'A' && i <= b'Z' { i + 32 } else { i };
        let valid = (i >= b'0' && i <= b'9')
            || (i >= b'a' && i <= b'z')
            || (i >= b'A' && i <= b'Z')
            || matches!(
                i,
                b'!' | b'#'
                    | b'$'
                    | b'%'
                    | b'&'
                    | b'\''
                    | b'*'
                    | b'+'
                    | b'-'
                    | b'.'
                    | b'^'
                    | b'_'
                    | b'`'
                    | b'|'
                    | b'~'
            );
        if valid {
            t[i as usize] = lower;
        }
        i += 1;
    }
    t
};

// Precomputed 63-bit hashes for fast-path well-known headers (zero hash loop on hot path).
const KEY_HOST: u64 = header_key(b"host");
const KEY_DATE: u64 = header_key(b"date");
const KEY_ETAG: u64 = header_key(b"etag");
const KEY_VARY: u64 = header_key(b"vary");
const KEY_ACCEPT: u64 = header_key(b"accept");
const KEY_COOKIE: u64 = header_key(b"cookie");
const KEY_ORIGIN: u64 = header_key(b"origin");
const KEY_SERVER: u64 = header_key(b"server");
const KEY_EXPECT: u64 = header_key(b"expect");
const KEY_REFERER: u64 = header_key(b"referer");
const KEY_UPGRADE: u64 = header_key(b"upgrade");
const KEY_LOCATION: u64 = header_key(b"location");
const KEY_CONNECTION: u64 = header_key(b"connection");
const KEY_USER_AGENT: u64 = header_key(b"user-agent");
const KEY_SET_COOKIE: u64 = header_key(b"set-cookie");
const KEY_CONTENT_TYPE: u64 = header_key(b"content-type");
const KEY_AUTHORIZATION: u64 = header_key(b"authorization");
const KEY_CACHE_CONTROL: u64 = header_key(b"cache-control");
const KEY_IF_NONE_MATCH: u64 = header_key(b"if-none-match");
const KEY_LAST_MODIFIED: u64 = header_key(b"last-modified");
const KEY_CONTENT_LENGTH: u64 = header_key(b"content-length");
const KEY_ACCEPT_ENCODING: u64 = header_key(b"accept-encoding");
const KEY_ACCEPT_LANGUAGE: u64 = header_key(b"accept-language");
const KEY_CONTENT_ENCODING: u64 = header_key(b"content-encoding");
const KEY_TRANSFER_ENCODING: u64 = header_key(b"transfer-encoding");
const KEY_IF_MODIFIED_SINCE: u64 = header_key(b"if-modified-since");
pub const KEY_X_REQUEST_ID: u64 = header_key(b"x-request-id");
const KEY_X_REAL_IP: u64 = header_key(b"x-real-ip");
const KEY_X_FORWARDED_FOR: u64 = header_key(b"x-forwarded-for");
const KEY_X_FORWARDED_PROTO: u64 = header_key(b"x-forwarded-proto");
const KEY_X_FORWARDED_PORT: u64 = header_key(b"x-forwarded-port");
pub const KEY_X_FORWARDED_HOST: u64 = header_key(b"x-forwarded-host");
const KEY_UPGRADE_INSECURE_REQUESTS: u64 = header_key(b"upgrade-insecure-requests");
const KEY_STRICT_TRANSPORT_SECURITY: u64 = header_key(b"strict-transport-security");
const KEY_ACCESS_CONTROL_ALLOW_ORIGIN: u64 = header_key(b"access-control-allow-origin");

const X_REAL_IP: HeaderName = HeaderName::from_static("x-real-ip");
const X_FORWARDED_FOR: HeaderName = HeaderName::from_static("x-forwarded-for");
const X_FORWARDED_PROTO: HeaderName = HeaderName::from_static("x-forwarded-proto");
const X_FORWARDED_PORT: HeaderName = HeaderName::from_static("x-forwarded-port");
pub const X_FORWARDED_HOST: HeaderName = HeaderName::from_static("x-forwarded-host");

/// Authoritative connection metadata pre-seeded into [`HeaderWorkspace`] during HTTP/1.1 wire decode.
#[derive(Debug, Clone, Copy)]
pub struct H1IngressEnrich<'a> {
    pub client_ip: &'a HeaderValue,
    pub listener_port: &'a HeaderValue,
    pub is_tls: bool,
}

/// Validates RFC 9110 `field-value` bytes: `(b >= 0x20 && b != 0x7F) || b == b'\t'`.
///
/// Compile-time multi-target dispatch:
/// - `x86_64` + `avx2`: 32 bytes/cycle (`_mm256_max_epu8` + `_mm256_movemask_epi8`)
/// - `x86_64` baseline: 16 bytes/cycle SSE2 (`_mm_max_epu8` + `_mm_movemask_epi8`)
/// - `aarch64`: 16 bytes/cycle NEON (`vcgtq_u8` / `vceqq_u8` + `vmaxvq_u8`)
/// - portable: 8-byte SWAR ALU fallback
#[inline(always)]
pub fn is_valid_header_value_simd(bytes: &[u8]) -> bool {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    {
        is_valid_header_value_avx2(bytes)
    }
    #[cfg(all(target_arch = "x86_64", not(target_feature = "avx2")))]
    {
        is_valid_header_value_sse2(bytes)
    }
    #[cfg(target_arch = "aarch64")]
    {
        is_valid_header_value_neon(bytes)
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        is_valid_header_value_swar(bytes)
    }
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[inline(always)]
fn is_valid_header_value_avx2(bytes: &[u8]) -> bool {
    use core::arch::x86_64::*;

    let len = bytes.len();
    let ptr = bytes.as_ptr();
    let mut i = 0usize;

    unsafe {
        let v1f = _mm256_set1_epi8(0x1F);
        let vtab = _mm256_set1_epi8(b'\t' as i8);
        let v7f = _mm256_set1_epi8(0x7F);

        while i + 32 <= len {
            let chunk = _mm256_loadu_si256(ptr.add(i) as *const __m256i);
            let le_1f = _mm256_cmpeq_epi8(_mm256_max_epu8(chunk, v1f), v1f);
            let is_tab = _mm256_cmpeq_epi8(chunk, vtab);
            let bad_ctrl = _mm256_andnot_si256(is_tab, le_1f);
            let is_del = _mm256_cmpeq_epi8(chunk, v7f);
            if _mm256_movemask_epi8(_mm256_or_si256(bad_ctrl, is_del)) != 0 {
                return false;
            }
            i += 32;
        }

        let v1f_128 = _mm_set1_epi8(0x1F);
        let vtab_128 = _mm_set1_epi8(b'\t' as i8);
        let v7f_128 = _mm_set1_epi8(0x7F);
        while i + 16 <= len {
            let chunk = _mm_loadu_si128(ptr.add(i) as *const __m128i);
            let le_1f = _mm_cmpeq_epi8(_mm_max_epu8(chunk, v1f_128), v1f_128);
            let is_tab = _mm_cmpeq_epi8(chunk, vtab_128);
            let bad_ctrl = _mm_andnot_si128(is_tab, le_1f);
            let is_del = _mm_cmpeq_epi8(chunk, v7f_128);
            if _mm_movemask_epi8(_mm_or_si128(bad_ctrl, is_del)) != 0 {
                return false;
            }
            i += 16;
        }
    }

    is_valid_header_value_scalar(&bytes[i..])
}

#[cfg(all(target_arch = "x86_64", not(target_feature = "avx2")))]
#[inline(always)]
fn is_valid_header_value_sse2(bytes: &[u8]) -> bool {
    use core::arch::x86_64::*;

    let len = bytes.len();
    let ptr = bytes.as_ptr();
    let mut i = 0usize;

    unsafe {
        let v1f = _mm_set1_epi8(0x1F);
        let vtab = _mm_set1_epi8(b'\t' as i8);
        let v7f = _mm_set1_epi8(0x7F);

        while i + 16 <= len {
            let chunk = _mm_loadu_si128(ptr.add(i) as *const __m128i);
            let le_1f = _mm_cmpeq_epi8(_mm_max_epu8(chunk, v1f), v1f);
            let is_tab = _mm_cmpeq_epi8(chunk, vtab);
            let bad_ctrl = _mm_andnot_si128(is_tab, le_1f);
            let is_del = _mm_cmpeq_epi8(chunk, v7f);
            if _mm_movemask_epi8(_mm_or_si128(bad_ctrl, is_del)) != 0 {
                return false;
            }
            i += 16;
        }
    }

    is_valid_header_value_scalar(&bytes[i..])
}

#[cfg(target_arch = "aarch64")]
#[inline(always)]
fn is_valid_header_value_neon(bytes: &[u8]) -> bool {
    use core::arch::aarch64::*;

    let len = bytes.len();
    let ptr = bytes.as_ptr();
    let mut i = 0usize;

    unsafe {
        let v1f = vdupq_n_u8(0x1F);
        let vtab = vdupq_n_u8(b'\t');
        let v7f = vdupq_n_u8(0x7F);

        while i + 16 <= len {
            let chunk = vld1q_u8(ptr.add(i));
            let le_1f = vcleq_u8(chunk, v1f);
            let is_tab = vceqq_u8(chunk, vtab);
            let bad_ctrl = vbicq_u8(le_1f, is_tab);
            let is_del = vceqq_u8(chunk, v7f);
            if vmaxvq_u8(vorrq_u8(bad_ctrl, is_del)) != 0 {
                return false;
            }
            i += 16;
        }
    }

    is_valid_header_value_scalar(&bytes[i..])
}

#[cfg(any(test, not(any(target_arch = "x86_64", target_arch = "aarch64"))))]
#[inline(always)]
fn is_valid_header_value_swar(bytes: &[u8]) -> bool {
    let mut i = 0usize;
    while i + 8 <= bytes.len() {
        let mut arr = [0u8; 8];
        arr.copy_from_slice(&bytes[i..i + 8]);
        let word = u64::from_ne_bytes(arr);
        // Fast gate: when all 8 bytes have bit7 == 0 and none is < 0x20 or == 0x7F, verify scalar only on slow bytes.
        // Detect any byte < 0x20 (`0x00..=0x1F`) or == 0x7F via SWAR zero-byte test.
        let low7 = word & 0x7F7F_7F7F_7F7F_7F7F;
        let has_lt_20 = (low7.wrapping_sub(0x2020_2020_2020_2020)) & !low7 & 0x8080_8080_8080_8080;
        let xor_7f = word ^ 0x7F7F_7F7F_7F7F_7F7F;
        let has_7f = (xor_7f.wrapping_sub(0x0101_0101_0101_0101)) & !xor_7f & 0x8080_8080_8080_8080;
        if (has_lt_20 | has_7f) != 0 && !is_valid_header_value_scalar(&bytes[i..i + 8]) {
            return false;
        }
        i += 8;
    }
    is_valid_header_value_scalar(&bytes[i..])
}

#[inline(always)]
fn is_valid_header_value_scalar(bytes: &[u8]) -> bool {
    let mut bad = false;
    for &b in bytes {
        bad |= !((b >= 0x20 && b != 0x7F) || b == b'\t');
    }
    !bad
}

/// Validates and constructs a [`HeaderValue`] using explicit multi-target SIMD validation.
#[inline(always)]
pub fn parse_header_value_simd(raw: &[u8]) -> Result<HeaderValue, Http1Error> {
    let mut trimmed = raw;
    while let Some((&b, rest)) = trimmed.split_first() {
        if b == b' ' || b == b'\t' {
            trimmed = rest;
        } else {
            break;
        }
    }
    while let Some((&b, rest)) = trimmed.split_last() {
        if b == b' ' || b == b'\t' {
            trimmed = rest;
        } else {
            break;
        }
    }
    if !is_valid_header_value_simd(trimmed) {
        return Err(Http1Error::InvalidHeader(
            "Invalid control or DEL byte in header value".into(),
        ));
    }
    // SAFETY: `is_valid_header_value_simd` just verified the exact `http::HeaderValue::is_valid`
    // predicate (`(b >= 0x20 && b != 0x7F) || b == b'\t'`) across every byte in `trimmed`.
    Ok(unsafe { HeaderValue::from_maybe_shared_unchecked(Bytes::copy_from_slice(trimmed)) })
}

/// Fixed-length case-insensitive ASCII literal comparison compiled into unrolled SWAR ALU checks.
///
/// Only lowercase letters `'a'..='z'` mask bit `0x20`; hyphens and digits compare exact so control
/// bytes (e.g. `\r | 0x20 == '-'`) cannot false-match.
#[inline(always)]
fn eq_ascii_lower_fixed<const N: usize>(raw: &[u8], lower: &[u8; N]) -> bool {
    let Ok(arr): Result<&[u8; N], _> = raw.try_into() else {
        return false;
    };
    let mut diff = 0u8;
    let mut i = 0;
    while i < N {
        let b = arr[i];
        let lc = lower[i];
        let masked = if lc.is_ascii_lowercase() { b | 0x20 } else { b };
        diff |= masked ^ lc;
        i += 1;
    }
    diff == 0
}

/// Length-partitioned well-known header name + precomputed 63-bit tag hash resolver,
/// falling back to single-pass LUT `tchar` validation, lowercasing, and FNV-1a hashing.
#[inline]
pub fn resolve_header_name_and_hash(raw_name: &[u8]) -> Result<(u64, HeaderName), Http1Error> {
    if raw_name.is_empty() {
        return Err(Http1Error::InvalidHeader("Empty header name".into()));
    }

    let fast = match raw_name.len() {
        4 => {
            if raw_name.eq_ignore_ascii_case(b"host") {
                Some((KEY_HOST, http::header::HOST))
            } else if raw_name.eq_ignore_ascii_case(b"date") {
                Some((KEY_DATE, http::header::DATE))
            } else if raw_name.eq_ignore_ascii_case(b"etag") {
                Some((KEY_ETAG, http::header::ETAG))
            } else if raw_name.eq_ignore_ascii_case(b"vary") {
                Some((KEY_VARY, http::header::VARY))
            } else {
                None
            }
        }
        6 => {
            if raw_name.eq_ignore_ascii_case(b"accept") {
                Some((KEY_ACCEPT, http::header::ACCEPT))
            } else if raw_name.eq_ignore_ascii_case(b"cookie") {
                Some((KEY_COOKIE, http::header::COOKIE))
            } else if raw_name.eq_ignore_ascii_case(b"origin") {
                Some((KEY_ORIGIN, http::header::ORIGIN))
            } else if raw_name.eq_ignore_ascii_case(b"server") {
                Some((KEY_SERVER, http::header::SERVER))
            } else if raw_name.eq_ignore_ascii_case(b"expect") {
                Some((KEY_EXPECT, http::header::EXPECT))
            } else {
                None
            }
        }
        7 => {
            if raw_name.eq_ignore_ascii_case(b"referer") {
                Some((KEY_REFERER, http::header::REFERER))
            } else if raw_name.eq_ignore_ascii_case(b"upgrade") {
                Some((KEY_UPGRADE, http::header::UPGRADE))
            } else {
                None
            }
        }
        8 if raw_name.eq_ignore_ascii_case(b"location") => {
            Some((KEY_LOCATION, http::header::LOCATION))
        }
        9 if eq_ascii_lower_fixed(raw_name, b"x-real-ip") => Some((KEY_X_REAL_IP, X_REAL_IP)),
        10 => {
            if raw_name.eq_ignore_ascii_case(b"connection") {
                Some((KEY_CONNECTION, http::header::CONNECTION))
            } else if raw_name.eq_ignore_ascii_case(b"user-agent") {
                Some((KEY_USER_AGENT, http::header::USER_AGENT))
            } else if raw_name.eq_ignore_ascii_case(b"set-cookie") {
                Some((KEY_SET_COOKIE, http::header::SET_COOKIE))
            } else {
                None
            }
        }
        12 => {
            if raw_name.eq_ignore_ascii_case(b"content-type") {
                Some((KEY_CONTENT_TYPE, http::header::CONTENT_TYPE))
            } else if raw_name.eq_ignore_ascii_case(b"x-request-id") {
                Some((KEY_X_REQUEST_ID, velda_core::buffer::header::X_REQUEST_ID))
            } else {
                None
            }
        }
        13 => {
            if raw_name.eq_ignore_ascii_case(b"authorization") {
                Some((KEY_AUTHORIZATION, http::header::AUTHORIZATION))
            } else if raw_name.eq_ignore_ascii_case(b"cache-control") {
                Some((KEY_CACHE_CONTROL, http::header::CACHE_CONTROL))
            } else if raw_name.eq_ignore_ascii_case(b"if-none-match") {
                Some((KEY_IF_NONE_MATCH, http::header::IF_NONE_MATCH))
            } else if raw_name.eq_ignore_ascii_case(b"last-modified") {
                Some((KEY_LAST_MODIFIED, http::header::LAST_MODIFIED))
            } else {
                None
            }
        }
        14 if raw_name.eq_ignore_ascii_case(b"content-length") => {
            Some((KEY_CONTENT_LENGTH, CONTENT_LENGTH))
        }
        15 => {
            if raw_name.eq_ignore_ascii_case(b"accept-encoding") {
                Some((KEY_ACCEPT_ENCODING, http::header::ACCEPT_ENCODING))
            } else if raw_name.eq_ignore_ascii_case(b"accept-language") {
                Some((KEY_ACCEPT_LANGUAGE, http::header::ACCEPT_LANGUAGE))
            } else if eq_ascii_lower_fixed(raw_name, b"x-forwarded-for") {
                Some((KEY_X_FORWARDED_FOR, X_FORWARDED_FOR))
            } else {
                None
            }
        }
        16 => {
            if eq_ascii_lower_fixed(raw_name, b"content-encoding") {
                Some((KEY_CONTENT_ENCODING, http::header::CONTENT_ENCODING))
            } else if eq_ascii_lower_fixed(raw_name, b"x-forwarded-port") {
                Some((KEY_X_FORWARDED_PORT, X_FORWARDED_PORT))
            } else if eq_ascii_lower_fixed(raw_name, b"x-forwarded-host") {
                Some((KEY_X_FORWARDED_HOST, X_FORWARDED_HOST))
            } else {
                None
            }
        }
        17 => {
            if eq_ascii_lower_fixed(raw_name, b"transfer-encoding") {
                Some((KEY_TRANSFER_ENCODING, TRANSFER_ENCODING))
            } else if eq_ascii_lower_fixed(raw_name, b"if-modified-since") {
                Some((KEY_IF_MODIFIED_SINCE, http::header::IF_MODIFIED_SINCE))
            } else if eq_ascii_lower_fixed(raw_name, b"x-forwarded-proto") {
                Some((KEY_X_FORWARDED_PROTO, X_FORWARDED_PROTO))
            } else {
                None
            }
        }
        25 => {
            if eq_ascii_lower_fixed(raw_name, b"upgrade-insecure-requests") {
                Some((
                    KEY_UPGRADE_INSECURE_REQUESTS,
                    http::header::UPGRADE_INSECURE_REQUESTS,
                ))
            } else if eq_ascii_lower_fixed(raw_name, b"strict-transport-security") {
                Some((
                    KEY_STRICT_TRANSPORT_SECURITY,
                    http::header::STRICT_TRANSPORT_SECURITY,
                ))
            } else {
                None
            }
        }
        27 if eq_ascii_lower_fixed(raw_name, b"access-control-allow-origin") => Some((
            KEY_ACCESS_CONTROL_ALLOW_ORIGIN,
            http::header::ACCESS_CONTROL_ALLOW_ORIGIN,
        )),
        _ => None,
    };

    if let Some(hit) = fast {
        return Ok(hit);
    }

    if raw_name.len() > MAX_HEADER_NAME_LEN {
        return Err(Http1Error::InvalidHeader(
            "Header name exceeds 256-byte limit".into(),
        ));
    }

    let mut lower = [0u8; MAX_HEADER_NAME_LEN];
    let mut hash = FNV1A_OFFSET;
    let mut bad = false;
    for (i, &b) in raw_name.iter().enumerate() {
        let lc = TCHAR_LOWER_LUT[b as usize];
        bad |= lc == 0xFF;
        lower[i] = lc;
        hash ^= lc as u64;
        hash = hash.wrapping_mul(FNV1A_PRIME);
    }
    if bad {
        return Err(Http1Error::InvalidHeader(
            "Invalid character in header name".into(),
        ));
    }

    let name = HeaderName::from_bytes(&lower[..raw_name.len()])
        .map_err(|e| Http1Error::InvalidHeader(e.to_string()))?;
    let hash63 = (hash & TAG_HASH_MASK).max(1);
    Ok((hash63, name))
}

/// Finds the end index (`lf - 1`, pointing at `\r`) of a `\r\n`-delimited HTTP/1.1 line.
/// Returns `Ok(None)` if no `\n` is present yet, or `Err(Parse)` on bare `\n`.
#[inline(always)]
pub fn find_crlf_line_end(data: &[u8]) -> Result<Option<(usize, usize)>, Http1Error> {
    let Some(lf) = memchr(b'\n', data) else {
        return Ok(None);
    };
    if lf == 0 || data[lf - 1] != b'\r' {
        return Err(cold_parse_error("Bare LF in HTTP/1.1 message"));
    }
    // Check for stray CR inside line
    if memchr(b'\r', &data[..lf - 1]).is_some() {
        return Err(cold_parse_error("Stray CR inside HTTP/1.1 line"));
    }
    Ok(Some((lf - 1, lf + 1)))
}

/// Result of SIMD parsing the HTTP/1.1 header section straight into a flat [`HeaderWorkspace`].
pub struct ParsedHeaderSection {
    pub workspace: HeaderWorkspace,
    pub host_header: Option<HeaderValue>,
    pub host_count: usize,
    pub content_length_count: usize,
    pub content_length: Option<usize>,
    pub has_transfer_encoding: bool,
    pub is_chunked: bool,
    pub total_head_len: usize,
}

/// Parses HTTP/1.1 header lines starting at `headers_offset` directly into a flat [`HeaderWorkspace`].
///
/// Pre-locates header block boundaries via SIMD `memchr` and validates field names/values
/// directly into `[tags | slots]` without allocating intermediate `httparse` arrays or `HeaderMap`.
///
/// When `enrich` is `Some`, authoritative connection headers (`x-real-ip`, `x-forwarded-proto`,
/// `x-forwarded-port`, `x-forwarded-for`) are pre-seeded first; client-spoofed overrides
/// (including `x-request-id` and `x-forwarded-host`) are validated on the wire and skipped
/// without allocating slots, and incoming `x-forwarded-for` values are chained in place.
pub fn parse_headers_into_workspace(
    data: &[u8],
    headers_offset: usize,
    config: &Http1Config,
    is_response: bool,
    enrich: Option<H1IngressEnrich<'_>>,
) -> Result<Option<ParsedHeaderSection>, Http1Error> {
    let mut offset = headers_offset;
    let mut ws = HeaderWorkspace::empty(DEFAULT_HEADER_CAPACITY);
    let mut xff_slot_idx: Option<usize> = None;
    let mut xff_chain: Vec<u8> = Vec::new();
    let mut host_header: Option<HeaderValue> = None;
    let mut rem_slots = MAX_WIRE_HEADERS;

    if let Some(meta) = enrich {
        let proto = if meta.is_tls {
            HeaderValue::from_static("https")
        } else {
            HeaderValue::from_static("http")
        };
        ws.push_prehashed(KEY_X_REAL_IP, X_REAL_IP, meta.client_ip.clone());
        ws.push_prehashed(KEY_X_FORWARDED_PROTO, X_FORWARDED_PROTO, proto);
        ws.push_prehashed(
            KEY_X_FORWARDED_PORT,
            X_FORWARDED_PORT,
            meta.listener_port.clone(),
        );
        xff_slot_idx = Some(ws.len());
        ws.push_prehashed(KEY_X_FORWARDED_FOR, X_FORWARDED_FOR, meta.client_ip.clone());
        // 4 pre-seeded slots + 2 reserved slots (`x-forwarded-host`, `x-request-id`)
        rem_slots -= 6;
    }

    let mut rem = config.max_headers.clamp(1, rem_slots);
    let mut host_count = 0usize;
    let mut content_length_count = 0usize;
    let mut content_length: Option<usize> = None;
    let mut has_transfer_encoding = false;
    let mut is_chunked = false;

    loop {
        let Some(remaining) = data.get(offset..) else {
            return Err(cold_parse_error("Header offset exceeds input length"));
        };
        let Some((line_end_rel, next_rel)) = find_crlf_line_end(remaining)? else {
            if data.len() > config.max_header_size {
                return Err(Http1Error::HeaderTooLarge(data.len()));
            }
            return Ok(None);
        };

        let next_offset = offset + next_rel;
        if next_offset > config.max_header_size {
            return Err(Http1Error::HeaderTooLarge(next_offset));
        }

        // Empty `\r\n` terminates the header section
        if line_end_rel == 0 {
            if content_length_count > 0 && has_transfer_encoding {
                return Err(cold_smuggling_error(if is_response {
                    "Simultaneous Content-Length and Transfer-Encoding headers in response"
                } else {
                    "Simultaneous Content-Length and Transfer-Encoding headers"
                }));
            }
            if let (Some(meta), Some(idx)) = (enrich, xff_slot_idx)
                && !xff_chain.is_empty()
            {
                xff_chain.extend_from_slice(b", ");
                xff_chain.extend_from_slice(meta.client_ip.as_bytes());
                if let Ok(comb_val) = HeaderValue::from_maybe_shared(Bytes::from(xff_chain)) {
                    ws.set_value_at(idx, comb_val);
                }
            }
            return Ok(Some(ParsedHeaderSection {
                workspace: ws,
                host_header,
                host_count,
                content_length_count,
                content_length,
                has_transfer_encoding,
                is_chunked,
                total_head_len: next_offset,
            }));
        }

        let Some(line) = remaining.get(..line_end_rel) else {
            return Err(cold_parse_error("Invalid header line bounds"));
        };
        if line[0] == b' ' || line[0] == b'\t' {
            return Err(Http1Error::InvalidHeader(
                "Obsolete header line folding rejected".into(),
            ));
        }

        let Some(next_rem) = rem.checked_sub(1) else {
            return Err(Http1Error::TooManyHeaders(config.max_headers));
        };
        rem = next_rem;

        let colon = memchr(b':', line)
            .ok_or_else(|| cold_parse_error("Missing colon delimiter in header line"))?;
        let raw_name = &line[..colon];
        let raw_val = &line[colon + 1..];

        let (key_hash, name) = resolve_header_name_and_hash(raw_name)?;
        let value = parse_header_value_simd(raw_val)?;

        if !is_response && key_hash == KEY_HOST && name == http::header::HOST {
            host_count += 1;
            if host_count > 1 {
                return Err(cold_smuggling_error("Multiple Host headers in request"));
            }
            if enrich.is_some() {
                host_header = Some(value.clone());
            }
        } else if key_hash == KEY_CONTENT_LENGTH && name == CONTENT_LENGTH {
            content_length_count += 1;
            let parsed_cl = parse_ascii_digits(value.as_bytes())?;
            if let Some(prev) = content_length
                && prev != parsed_cl
            {
                return Err(cold_smuggling_error(if is_response {
                    "Multiple conflicting Content-Length headers in response"
                } else {
                    "Multiple conflicting Content-Length headers"
                }));
            }
            content_length = Some(parsed_cl);
        } else if key_hash == KEY_TRANSFER_ENCODING && name == TRANSFER_ENCODING {
            has_transfer_encoding = true;
            let val_str = std::str::from_utf8(value.as_bytes())
                .map_err(|_| cold_parse_error("Invalid non-UTF8 Transfer-Encoding"))?;
            if val_str
                .split(',')
                .next_back()
                .map(str::trim)
                .is_some_and(|c| c.eq_ignore_ascii_case("chunked"))
            {
                is_chunked = true;
            } else {
                return Err(cold_parse_error(format!(
                    "Unsupported Transfer-Encoding coding: {val_str}"
                )));
            }
        } else if enrich.is_some() {
            if (key_hash == KEY_X_REQUEST_ID && name == velda_core::buffer::header::X_REQUEST_ID)
                || (key_hash == KEY_X_REAL_IP && name == X_REAL_IP)
                || (key_hash == KEY_X_FORWARDED_PROTO && name == X_FORWARDED_PROTO)
                || (key_hash == KEY_X_FORWARDED_PORT && name == X_FORWARDED_PORT)
                || (key_hash == KEY_X_FORWARDED_HOST && name == X_FORWARDED_HOST)
            {
                offset = next_offset;
                continue;
            }
            if key_hash == KEY_X_FORWARDED_FOR && name == X_FORWARDED_FOR {
                if !xff_chain.is_empty() {
                    xff_chain.extend_from_slice(b", ");
                } else if let Some(meta) = enrich {
                    xff_chain.reserve(value.as_bytes().len() + 2 + meta.client_ip.as_bytes().len());
                }
                xff_chain.extend_from_slice(value.as_bytes());
                offset = next_offset;
                continue;
            }
        }

        ws.push_prehashed(key_hash, name, value);
        offset = next_offset;
    }
}

/// Validates that a request target byte slice contains no spaces or control characters via SIMD/memchr.
#[inline]
pub fn validate_request_target_bytes(target: &[u8]) -> Result<&str, Http1Error> {
    if target.is_empty() {
        return Err(Http1Error::InvalidUri("Empty request target".into()));
    }
    if memchr2(b' ', b'\t', target).is_some() || !is_valid_header_value_simd(target) {
        return Err(Http1Error::InvalidUri(
            "Invalid whitespace or control byte in request target".into(),
        ));
    }
    std::str::from_utf8(target).map_err(|e| Http1Error::InvalidUri(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simd_header_value_validation_exhaustive_bytes() {
        for b in 0u8..=255u8 {
            let expected = (b >= 0x20 && b != 0x7F) || b == b'\t';
            let buf = [b; 1];
            assert_eq!(
                is_valid_header_value_simd(&buf),
                expected,
                "1-byte mismatch for 0x{b:02X}"
            );

            // Place test byte at offset 31 inside a 64-byte slice to hit AVX2 + SSE2 + scalar paths
            let mut wide = [b'a'; 64];
            wide[31] = b;
            assert_eq!(
                is_valid_header_value_simd(&wide),
                expected,
                "64-byte mismatch at offset 31 for 0x{b:02X}"
            );

            wide[31] = b'a';
            wide[63] = b;
            assert_eq!(
                is_valid_header_value_simd(&wide),
                expected,
                "64-byte mismatch at tail for 0x{b:02X}"
            );

            assert_eq!(is_valid_header_value_swar(&wide), expected);
        }
    }

    #[test]
    fn test_parse_headers_into_workspace_oob_offset_returns_err() {
        let cfg = Http1Config::from_hardware(4, 4 * 1024 * 1024 * 1024);
        let res = parse_headers_into_workspace(b"GET / HTTP/1.1\r\n", 999, &cfg, false, None);
        assert!(res.is_err());
    }

    #[test]
    fn test_parse_headers_into_workspace_preseed_no_overwrite_and_xff_chaining() {
        let cfg = Http1Config::from_hardware(4, 4 * 1024 * 1024 * 1024);
        let raw = b"GET / HTTP/1.1\r\n\
Host: api.example.com\r\n\
X-Request-Id: spoofed-id\r\n\
X-Real-IP: 9.9.9.9\r\n\
X-Forwarded-Proto: http\r\n\
X-Forwarded-Port: 80\r\n\
X-Forwarded-Host: evil.example\r\n\
X-Forwarded-For: 1.1.1.1\r\n\
X-Forwarded-For: 2.2.2.2\r\n\
Content-Type: application/json\r\n\r\n";
        let headers_offset = b"GET / HTTP/1.1\r\n".len();
        let client_ip = HeaderValue::from_static("203.0.113.10");
        let port = HeaderValue::from_static("443");
        let parsed = parse_headers_into_workspace(
            raw,
            headers_offset,
            &cfg,
            false,
            Some(H1IngressEnrich {
                client_ip: &client_ip,
                listener_port: &port,
                is_tls: true,
            }),
        )
        .unwrap()
        .unwrap();

        let ws = parsed.workspace;
        assert!(ws.get("x-request-id").is_none());
        assert_eq!(ws.get("x-real-ip").unwrap(), "203.0.113.10");
        assert_eq!(ws.get("x-forwarded-proto").unwrap(), "https");
        assert_eq!(ws.get("x-forwarded-port").unwrap(), "443");
        assert!(ws.get("x-forwarded-host").is_none());
        assert_eq!(
            ws.get("x-forwarded-for").unwrap(),
            "1.1.1.1, 2.2.2.2, 203.0.113.10"
        );
        assert_eq!(ws.get("content-type").unwrap(), "application/json");
        assert_eq!(parsed.host_header.unwrap(), "api.example.com");
    }

    #[test]
    fn test_parse_headers_into_workspace_slot_countdown_to_zero() {
        let mut cfg = Http1Config::from_hardware(4, 4 * 1024 * 1024 * 1024);
        cfg.max_headers = 2;
        let headers_offset = b"GET / HTTP/1.1\r\n".len();

        let ok_raw = b"GET / HTTP/1.1\r\nHost: example.com\r\nX-A: 1\r\n\r\n";
        assert!(parse_headers_into_workspace(ok_raw, headers_offset, &cfg, false, None).is_ok());

        let err_raw = b"GET / HTTP/1.1\r\nHost: example.com\r\nX-A: 1\r\nX-B: 2\r\n\r\n";
        let err = parse_headers_into_workspace(err_raw, headers_offset, &cfg, false, None)
            .err()
            .expect("3rd header must fail when 2 slots count down to 0");
        assert!(matches!(err, Http1Error::TooManyHeaders(2)));
    }
}
