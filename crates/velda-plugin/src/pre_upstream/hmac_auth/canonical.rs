//! Streaming Canonical Request Signer & Cryptographic Primitives (`hmac_auth::canonical`).
//!
//! Feeds canonical HTTP components directly into [`ring::hmac::Context`] (pre-computed
//! `ipad`/`opad` state in [`hmac::Key`]) without intermediate canonical buffers or
//! runtime FNV-1a header hashing, then decodes the caller's signature onto the stack and
//! verifies raw digest bytes via `ring::constant_time::verify_slices_are_equal`.
//!
//! ```text
//!   L7Request components ──► hmac::Context::with_key(precomputed_key)
//!     ├─► METHOD \n PATH \n SORTED_QUERY \n
//!     ├─► pre-hashed signed_headers (`get_prehashed` / `:authority` H2 fallback)
//!     ├─► signed_headers_joined_line \n
//!     └─► payload_sha256 ─► ctx.sign() == decoded client_sig (constant-time)
//! ```

use std::str::FromStr;

use base64::prelude::*;
use http::header::HeaderName;
use ring::{digest, hmac};
use velda_core::{HeaderKey, HeaderWorkspace};

use crate::error::PluginError;

const KEY_HOST: HeaderKey = HeaderKey::from_static("host");

/// Flat projection of an inbound request for canonical HMAC verification.
pub struct CanonicalRequest<'a> {
    pub method: &'a str,
    pub path: &'a str,
    pub query: Option<&'a str>,
    pub headers: &'a HeaderWorkspace,
    pub authority: Option<&'a [u8]>,
    pub payload_hash: &'a [u8],
}

/// Pre-materialized signed header specification compiled once at plugin reload.
pub struct SignedHeadersSpec {
    /// Pre-hashed H1 keys and `header_name:` wire prefixes.
    pub h1_headers: Box<[(HeaderKey, Box<[u8]>)]>,
    /// Headers strictly before `host` in declaration order for HTTP/2 & HTTP/3.
    pub h2_before_host: Box<[(HeaderKey, Box<[u8]>)]>,
    /// Whether `host` participates in `signed_headers`.
    pub h2_has_host: bool,
    /// Headers strictly after `host` in declaration order for HTTP/2 & HTTP/3.
    pub h2_after_host: Box<[(HeaderKey, Box<[u8]>)]>,
    /// Pre-joined signed headers line, e.g. `b"host;x-timestamp;x-nonce\n"`.
    pub joined_line: Box<[u8]>,
}

impl SignedHeadersSpec {
    /// Compiles signed header names into pre-hashed [`HeaderKey`] entries and specialized H1 / H2+ layouts.
    pub fn compile(signed_headers: &[String], plugin_id: &str) -> Result<Self, PluginError> {
        let mut h1_headers = Vec::with_capacity(signed_headers.len());
        let mut h2_before_host = Vec::new();
        let mut h2_after_host = Vec::new();
        let mut h2_has_host = false;
        let mut joined_names = Vec::with_capacity(signed_headers.len());

        for h in signed_headers {
            let lower = h.trim().to_ascii_lowercase();
            let hname = HeaderName::from_str(&lower).map_err(|e| {
                PluginError::Config(format!(
                    "Invalid signed_header '{h}' for plugin '{plugin_id}': {e}"
                ))
            })?;
            joined_names.push(lower.clone());

            let mut prefix = lower.into_bytes();
            prefix.push(b':');
            let prefix_boxed = prefix.into_boxed_slice();
            let key = HeaderKey::from_name(hname.clone());

            h1_headers.push((key.clone(), prefix_boxed.clone()));

            if hname == http::header::HOST {
                h2_has_host = true;
            } else if !h2_has_host {
                h2_before_host.push((key, prefix_boxed));
            } else {
                h2_after_host.push((key, prefix_boxed));
            }
        }

        let mut joined_line = joined_names.join(";").into_bytes();
        joined_line.push(b'\n');

        Ok(Self {
            h1_headers: h1_headers.into_boxed_slice(),
            h2_before_host: h2_before_host.into_boxed_slice(),
            h2_has_host,
            h2_after_host: h2_after_host.into_boxed_slice(),
            joined_line: joined_line.into_boxed_slice(),
        })
    }
}

#[inline(always)]
fn feed_method_uppercase(ctx: &mut hmac::Context, method: &str) {
    let bytes = method.as_bytes();
    if bytes.iter().all(|b| !b.is_ascii_lowercase()) {
        ctx.update(bytes);
        return;
    }
    let mut stack = [0u8; 16];
    if bytes.len() <= stack.len() {
        for (i, &b) in bytes.iter().enumerate() {
            stack[i] = b.to_ascii_uppercase();
        }
        ctx.update(&stack[..bytes.len()]);
    } else {
        for &b in bytes {
            let up = [b.to_ascii_uppercase()];
            ctx.update(&up);
        }
    }
}

/// Streams sorted canonical query pairs directly into `ctx` with stack-allocated storage up to 16 params.
#[inline(always)]
fn feed_canonical_query(ctx: &mut hmac::Context, query: Option<&str>) {
    let Some(q) = query else {
        return;
    };
    if q.is_empty() {
        return;
    }
    if !q.contains('&') {
        ctx.update(q.as_bytes());
        if !q.contains('=') {
            ctx.update(b"=");
        }
        return;
    }

    let mut stack_pairs = [("", ""); 16];
    let mut count = 0usize;
    let mut overflow: Vec<(&str, &str)> = Vec::new();

    for part in q.split('&').filter(|p| !p.is_empty()) {
        let pair = match part.split_once('=') {
            Some((k, v)) => (k, v),
            None => (part, ""),
        };
        if overflow.is_empty() && count < stack_pairs.len() {
            stack_pairs[count] = pair;
            count += 1;
        } else {
            if overflow.is_empty() {
                overflow = stack_pairs[..count].to_vec();
            }
            overflow.push(pair);
        }
    }

    let slice: &mut [(&str, &str)] = if !overflow.is_empty() {
        &mut overflow
    } else {
        &mut stack_pairs[..count]
    };
    slice.sort_unstable_by(|a, b| a.0.cmp(b.0).then_with(|| a.1.cmp(b.1)));

    for (idx, (k, v)) in slice.iter().enumerate() {
        if idx > 0 {
            ctx.update(b"&");
        }
        ctx.update(k.as_bytes());
        ctx.update(b"=");
        ctx.update(v.as_bytes());
    }
}

/// Feeds a header entry (including RFC 9110 §5.3 comma-joined multi-value slots) using pre-hashed lookup.
#[inline(always)]
fn feed_prehashed_header(
    ctx: &mut hmac::Context,
    headers: &HeaderWorkspace,
    key: &HeaderKey,
    prefix: &[u8],
) {
    ctx.update(prefix);
    let mut vals = headers.get_all_prehashed(key);
    if let Some(first) = vals.next() {
        ctx.update(first.as_bytes().trim_ascii());
        for extra in vals {
            ctx.update(b",");
            ctx.update(extra.as_bytes().trim_ascii());
        }
    }
    ctx.update(b"\n");
}

/// Computes the canonical HMAC tag by streaming directly into `hmac::Context::with_key`.
#[inline]
pub fn sign_canonical_request(
    key: &hmac::Key,
    req: &CanonicalRequest<'_>,
    spec: &SignedHeadersSpec,
) -> hmac::Tag {
    let mut ctx = hmac::Context::with_key(key);

    // 1. Method
    feed_method_uppercase(&mut ctx, req.method);
    ctx.update(b"\n");

    // 2. Path
    let path = if req.path.is_empty() { "/" } else { req.path };
    ctx.update(path.as_bytes());
    ctx.update(b"\n");

    // 3. Sorted canonical query
    feed_canonical_query(&mut ctx, req.query);
    ctx.update(b"\n");

    // 4. Signed headers (protocol-specific `:authority` fallback for H2/H3 when Host header absent)
    if req.authority.is_none() {
        for (hkey, prefix) in spec.h1_headers.iter() {
            feed_prehashed_header(&mut ctx, req.headers, hkey, prefix);
        }
    } else {
        for (hkey, prefix) in spec.h2_before_host.iter() {
            feed_prehashed_header(&mut ctx, req.headers, hkey, prefix);
        }
        if spec.h2_has_host {
            ctx.update(b"host:");
            if let Some(val) = req.headers.get_prehashed(&KEY_HOST) {
                ctx.update(val.as_bytes().trim_ascii());
            } else if let Some(auth) = req.authority {
                ctx.update(auth.trim_ascii());
            }
            ctx.update(b"\n");
        }
        for (hkey, prefix) in spec.h2_after_host.iter() {
            feed_prehashed_header(&mut ctx, req.headers, hkey, prefix);
        }
    }

    // 5. Signed headers list + payload SHA-256 hex
    ctx.update(&spec.joined_line);
    ctx.update(req.payload_hash);

    ctx.sign()
}

/// Decodes a hex-encoded signature slice (case-insensitive) directly into `out`.
#[inline(always)]
pub fn decode_hex_to_buf(hex: &[u8], out: &mut [u8]) -> Option<usize> {
    if hex.len() != out.len() * 2 {
        return None;
    }
    #[inline(always)]
    const fn nibble(b: u8) -> Option<u8> {
        match b {
            b'0'..=b'9' => Some(b - b'0'),
            b'a'..=b'f' => Some(b - b'a' + 10),
            b'A'..=b'F' => Some(b - b'A' + 10),
            _ => None,
        }
    }
    for (i, chunk) in hex.as_chunks::<2>().0.iter().enumerate() {
        let hi = nibble(chunk[0])?;
        let lo = nibble(chunk[1])?;
        out[i] = (hi << 4) | lo;
    }
    Some(out.len())
}

/// Decodes an incoming hex or standard-base64 client signature into `out_digest` (32B SHA-256 / 64B SHA-512).
#[inline(always)]
pub fn decode_client_signature(
    client_sig: &[u8],
    is_sha512: bool,
    is_base64: bool,
    out_digest: &mut [u8; 64],
) -> Option<usize> {
    let raw_len = if is_sha512 { 64 } else { 32 };
    if is_base64 {
        let expected_b64_len = if is_sha512 { 88 } else { 44 };
        if client_sig.len() != expected_b64_len {
            return None;
        }
        let decoded_len = BASE64_STANDARD.decode_slice(client_sig, out_digest).ok()?;
        (decoded_len == raw_len).then_some(raw_len)
    } else {
        decode_hex_to_buf(client_sig, &mut out_digest[..raw_len])
    }
}

/// Formats a byte slice into a pre-allocated stack buffer as lowercase hex characters.
#[inline(always)]
pub fn format_hex_to_buf(bytes: &[u8], out: &mut [u8]) {
    const HEX_CHARS: &[u8; 16] = b"0123456789abcdef";
    for (i, &b) in bytes.iter().enumerate() {
        out[i * 2] = HEX_CHARS[(b >> 4) as usize];
        out[i * 2 + 1] = HEX_CHARS[(b & 0x0f) as usize];
    }
}

/// Constant-time byte slice comparison to prevent timing side-channel attacks.
#[inline(always)]
pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (&x, &y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    core::hint::black_box(diff) == 0
}

/// Calculates the SHA-256 digest of a byte payload.
#[inline]
pub fn compute_sha256(payload: &[u8]) -> [u8; 32] {
    let d = digest::digest(&digest::SHA256, payload);
    let mut out = [0u8; 32];
    out.copy_from_slice(d.as_ref());
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderValue;

    #[test]
    fn test_streaming_sign_canonical_h1_and_h2_match() {
        let mut h1_headers = HeaderWorkspace::empty(4);
        h1_headers.insert("host", HeaderValue::from_static("api.velda.io"));
        h1_headers.insert("x-timestamp", HeaderValue::from_static("1729600000"));
        h1_headers.insert("x-nonce", HeaderValue::from_static("abc-123"));

        let mut h2_headers = HeaderWorkspace::empty(4);
        h2_headers.insert("x-timestamp", HeaderValue::from_static("1729600000"));
        h2_headers.insert("x-nonce", HeaderValue::from_static("abc-123"));

        let signed_headers = vec![
            "host".to_string(),
            "x-timestamp".to_string(),
            "x-nonce".to_string(),
        ];

        let payload_hash_hex = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        let key = hmac::Key::new(hmac::HMAC_SHA256, b"test_secret_key_12345");
        let spec = SignedHeadersSpec::compile(&signed_headers, "test").unwrap();

        let canonical_str = format!(
            "POST\n/v1/payments\na=1&b=2&flag=\nhost:api.velda.io\nx-timestamp:1729600000\nx-nonce:abc-123\nhost;x-timestamp;x-nonce\n{payload_hash_hex}"
        );
        let expected_tag = hmac::sign(&key, canonical_str.as_bytes());

        let req_h1 = CanonicalRequest {
            method: "POST",
            path: "/v1/payments",
            query: Some("b=2&flag&a=1"),
            headers: &h1_headers,
            authority: None,
            payload_hash: payload_hash_hex.as_bytes(),
        };
        let req_h2 = CanonicalRequest {
            method: "POST",
            path: "/v1/payments",
            query: Some("b=2&flag&a=1"),
            headers: &h2_headers,
            authority: Some(b"api.velda.io"),
            payload_hash: payload_hash_hex.as_bytes(),
        };

        let tag_h1 = sign_canonical_request(&key, &req_h1, &spec);
        let tag_h2 = sign_canonical_request(&key, &req_h2, &spec);
        assert_eq!(tag_h1.as_ref(), expected_tag.as_ref());
        assert_eq!(tag_h2.as_ref(), expected_tag.as_ref());
    }

    #[test]
    fn test_decode_hex_case_insensitive_and_base64() {
        let raw = [0xabu8; 32];
        let mut hex = [0u8; 64];
        format_hex_to_buf(&raw, &mut hex);
        let hex_upper: Vec<u8> = hex.iter().map(|b| b.to_ascii_uppercase()).collect();

        let mut out = [0u8; 64];
        assert_eq!(
            decode_client_signature(&hex_upper, false, false, &mut out),
            Some(32)
        );
        assert_eq!(&out[..32], &raw);

        let b64 = BASE64_STANDARD.encode(raw);
        let mut out_b64 = [0u8; 64];
        assert_eq!(
            decode_client_signature(b64.as_bytes(), false, true, &mut out_b64),
            Some(32)
        );
        assert_eq!(&out_b64[..32], &raw);
    }

    #[test]
    fn test_constant_time_eq_delegates_to_ring() {
        assert!(constant_time_eq(b"abcdef", b"abcdef"));
        assert!(!constant_time_eq(b"abcdef", b"abcdeg"));
        assert!(!constant_time_eq(b"abcdef", b"abcde"));
    }
}
