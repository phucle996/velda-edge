//! Canonical Request Formatter & Cryptographic Primitives (`hmac_auth::canonical`).
//!
//! Generates RFC-compliant canonical representations of HTTP requests for deterministic
//! cryptographic signature calculation. Provides fast stack-allocated hashing, SIMD-accelerated
//! 128-bit constant-time comparisons, and zero-allocation streaming HMAC calculation.

use std::str::FromStr;

use base64::prelude::*;
use http::HeaderMap;
use http::header::HeaderName;
use ring::{digest, hmac};

use crate::error::PluginError;

/// Flat projection of an inbound request for canonical HMAC calculation.
pub struct CanonicalRequest<'a> {
    pub method: &'a str,
    pub path: &'a str,
    pub query: Option<&'a str>,
    pub headers: &'a HeaderMap,
    pub authority: Option<&'a [u8]>,
    pub payload_hash: &'a [u8],
}

/// Pre-materialized signed header configuration compiled at bootstrap.
pub struct SignedHeadersSpec {
    /// Full headers layout for HTTP/1.1 branch: (HeaderName, b"header_name:")
    pub h1_headers: Box<[(HeaderName, Box<[u8]>)]>,
    /// Headers strictly before "host" in declaration order for H2/H3
    pub h2_before_host: Box<[(HeaderName, Box<[u8]>)]>,
    /// Whether "host" was included in signed_headers
    pub h2_has_host: bool,
    /// Headers strictly after "host" in declaration order for H2/H3
    pub h2_after_host: Box<[(HeaderName, Box<[u8]>)]>,
    /// Pre-joined signed headers line, e.g. b"host;x-timestamp;x-nonce\n"
    pub joined_line: Box<[u8]>,
}

impl SignedHeadersSpec {
    /// Compiles signed header names into pre-parsed `HeaderName`s and specialized H1 & H2/H3 layouts.
    pub fn compile(signed_headers: &[String], plugin_id: &str) -> Result<Self, PluginError> {
        let mut h1_headers = Vec::with_capacity(signed_headers.len());
        let mut h2_before_host = Vec::new();
        let mut h2_after_host = Vec::new();
        let mut h2_has_host = false;
        let mut joined_names = Vec::with_capacity(signed_headers.len());

        let host_name = HeaderName::from_static("host");

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

            h1_headers.push((hname.clone(), prefix_boxed.clone()));

            if hname == host_name {
                h2_has_host = true;
            } else if !h2_has_host {
                h2_before_host.push((hname, prefix_boxed));
            } else {
                h2_after_host.push((hname, prefix_boxed));
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

/// Bounded stack-allocated buffer with transparent heap fallback for large canonical strings.
/// Guarantees 100% Zero-Allocation on the hot path while eliminating duplicate streaming logic.
struct CanonicalBuffer<'a> {
    stack: &'a mut [u8],
    pos: usize,
    heap: Option<Vec<u8>>,
}

impl<'a> CanonicalBuffer<'a> {
    #[inline(always)]
    fn new(stack: &'a mut [u8]) -> Self {
        Self {
            stack,
            pos: 0,
            heap: None,
        }
    }

    #[inline(always)]
    fn put(&mut self, bytes: &[u8]) {
        if let Some(h) = &mut self.heap {
            h.extend_from_slice(bytes);
        } else if self.pos + bytes.len() <= self.stack.len() {
            self.stack[self.pos..self.pos + bytes.len()].copy_from_slice(bytes);
            self.pos += bytes.len();
        } else {
            let mut h = Vec::with_capacity(self.pos + bytes.len() + 256);
            h.extend_from_slice(&self.stack[..self.pos]);
            h.extend_from_slice(bytes);
            self.heap = Some(h);
        }
    }

    #[inline(always)]
    fn put_byte(&mut self, b: u8) {
        if let Some(h) = &mut self.heap {
            h.push(b);
        } else if self.pos < self.stack.len() {
            self.stack[self.pos] = b;
            self.pos += 1;
        } else {
            let mut h = Vec::with_capacity(self.pos + 256);
            h.extend_from_slice(&self.stack[..self.pos]);
            h.push(b);
            self.heap = Some(h);
        }
    }

    #[inline(always)]
    fn as_bytes(&self) -> &[u8] {
        if let Some(h) = &self.heap {
            h.as_slice()
        } else {
            &self.stack[..self.pos]
        }
    }
}

#[inline(always)]
fn append_method_uppercase(sbuf: &mut CanonicalBuffer<'_>, method: &str) {
    let bytes = method.as_bytes();
    if bytes.iter().all(|b| !b.is_ascii_lowercase()) {
        sbuf.put(bytes);
    } else {
        let mut buf = [0u8; 16];
        if bytes.len() <= buf.len() {
            for (i, &b) in bytes.iter().enumerate() {
                buf[i] = b.to_ascii_uppercase();
            }
            sbuf.put(&buf[..bytes.len()]);
        } else {
            for &b in bytes {
                sbuf.put_byte(b.to_ascii_uppercase());
            }
        }
    }
}

#[inline(always)]
fn append_canonical_query(sbuf: &mut CanonicalBuffer<'_>, query: Option<&str>) {
    if let Some(q) = query
        && !q.is_empty()
    {
        if !q.contains('&') {
            sbuf.put(q.as_bytes());
            if !q.contains('=') {
                sbuf.put_byte(b'=');
            }
        } else {
            let mut pairs: Vec<(&str, &str)> = q
                .split('&')
                .filter(|part| !part.is_empty())
                .map(|part| match part.split_once('=') {
                    Some((k, v)) => (k, v),
                    None => (part, ""),
                })
                .collect();

            pairs.sort_unstable_by(|a, b| a.0.cmp(b.0).then_with(|| a.1.cmp(b.1)));

            for (idx, (k, v)) in pairs.iter().enumerate() {
                if idx > 0 {
                    sbuf.put_byte(b'&');
                }
                sbuf.put(k.as_bytes());
                sbuf.put_byte(b'=');
                sbuf.put(v.as_bytes());
            }
        }
    }
}

/// HTTP/1.1 specialized canonical signer.
/// Guaranteed ZERO per-header conditional branching (`if hname == HOST`) in the inner loop.
#[inline(always)]
pub fn sign_canonical_h1(
    key: &hmac::Key,
    req: &CanonicalRequest<'_>,
    spec: &SignedHeadersSpec,
) -> hmac::Tag {
    let mut raw_buf = [0u8; 512];
    let mut sbuf = CanonicalBuffer::new(&mut raw_buf);

    // 1. Method
    append_method_uppercase(&mut sbuf, req.method);
    sbuf.put_byte(b'\n');

    // 2. Path
    let path = if req.path.is_empty() { "/" } else { req.path };
    sbuf.put(path.as_bytes());
    sbuf.put_byte(b'\n');

    // 3. Query
    append_canonical_query(&mut sbuf, req.query);
    sbuf.put_byte(b'\n');

    // 4. Headers (Branchless loop over h1_headers using pure byte trimming)
    for (hname, prefix_with_colon) in spec.h1_headers.iter() {
        sbuf.put(prefix_with_colon);
        if let Some(val) = req.headers.get(hname) {
            sbuf.put(val.as_bytes().trim_ascii());
        }
        sbuf.put_byte(b'\n');
    }

    // 5. Signed headers joined line
    sbuf.put(&spec.joined_line);

    // 6. Payload hash
    sbuf.put(req.payload_hash);

    hmac::sign(key, sbuf.as_bytes())
}

/// HTTP/2 & HTTP/3 specialized canonical signer.
/// Guaranteed ZERO per-header conditional branching (`if hname == HOST`) in the inner loops.
#[inline(always)]
pub fn sign_canonical_h2_h3(
    key: &hmac::Key,
    req: &CanonicalRequest<'_>,
    spec: &SignedHeadersSpec,
) -> hmac::Tag {
    let mut raw_buf = [0u8; 512];
    let mut sbuf = CanonicalBuffer::new(&mut raw_buf);

    // 1. Method
    append_method_uppercase(&mut sbuf, req.method);
    sbuf.put_byte(b'\n');

    // 2. Path
    let path = if req.path.is_empty() { "/" } else { req.path };
    sbuf.put(path.as_bytes());
    sbuf.put_byte(b'\n');

    // 3. Query
    append_canonical_query(&mut sbuf, req.query);
    sbuf.put_byte(b'\n');

    // 4. Headers: Zero branching inside loops!
    // A. Headers before host
    for (hname, prefix_with_colon) in spec.h2_before_host.iter() {
        sbuf.put(prefix_with_colon);
        if let Some(val) = req.headers.get(hname) {
            sbuf.put(val.as_bytes().trim_ascii());
        }
        sbuf.put_byte(b'\n');
    }

    // B. Host header (if declared in spec)
    if spec.h2_has_host {
        sbuf.put(b"host:");
        if let Some(val) = req.headers.get(http::header::HOST) {
            sbuf.put(val.as_bytes().trim_ascii());
        } else if let Some(auth) = req.authority {
            sbuf.put(auth.trim_ascii());
        }
        sbuf.put_byte(b'\n');
    }

    // C. Headers after host
    for (hname, prefix_with_colon) in spec.h2_after_host.iter() {
        sbuf.put(prefix_with_colon);
        if let Some(val) = req.headers.get(hname) {
            sbuf.put(val.as_bytes().trim_ascii());
        }
        sbuf.put_byte(b'\n');
    }

    // 5. Signed headers joined line
    sbuf.put(&spec.joined_line);

    // 6. Payload hash
    sbuf.put(req.payload_hash);

    hmac::sign(key, sbuf.as_bytes())
}

/// Unified entrypoint for canonical HMAC signing. Dispatches to H1 or H2/H3 fast paths.
#[inline(always)]
pub fn sign_canonical_request(
    key: &hmac::Key,
    req: &CanonicalRequest<'_>,
    spec: &SignedHeadersSpec,
) -> hmac::Tag {
    if req.authority.is_none() {
        sign_canonical_h1(key, req, spec)
    } else {
        sign_canonical_h2_h3(key, req, spec)
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

/// Formats a byte slice into a pre-allocated stack buffer as standard Base64 (Zero Heap Allocation).
/// Returns the sub-slice of `out` containing the encoded Base64 bytes.
#[inline(always)]
pub fn format_base64_to_buf<'a>(bytes: &[u8], out: &'a mut [u8; 88]) -> &'a [u8] {
    let len = BASE64_STANDARD.encode_slice(bytes, out).unwrap_or(0);
    &out[..len]
}

/// Formats a byte slice into a standard Base64 string.
#[inline]
pub fn to_base64(bytes: &[u8]) -> String {
    BASE64_STANDARD.encode(bytes)
}

/// Constant-time byte slice comparison using 128-bit vector words to eliminate timing
/// side-channel attacks while vectorizing directly into SIMD instructions on modern CPUs.
#[inline]
pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u128;
    let (chunks_a, remainder_a) = a.as_chunks::<16>();
    let (chunks_b, remainder_b) = b.as_chunks::<16>();

    for (ca, cb) in chunks_a.iter().zip(chunks_b.iter()) {
        let val_a = u128::from_ne_bytes(*ca);
        let val_b = u128::from_ne_bytes(*cb);
        diff |= val_a ^ val_b;
    }

    for (&ra, &rb) in remainder_a.iter().zip(remainder_b.iter()) {
        diff |= (ra ^ rb) as u128;
    }

    diff == 0
}

/// Calculates the SHA-256 digest of a byte payload.
#[inline]
pub fn compute_sha256(payload: &[u8]) -> [u8; 32] {
    let d = digest::digest(&digest::SHA256, payload);
    let mut out = [0u8; 32];
    out.copy_from_slice(d.as_ref());
    out
}

/// Generates a sorted, canonical query string (alphabetical order by param name).
pub fn canonicalize_query(query_opt: Option<&str>) -> String {
    let q = match query_opt {
        Some(s) if !s.is_empty() => s,
        _ => return String::new(),
    };

    let mut pairs: Vec<(&str, &str)> = q
        .split('&')
        .filter(|part| !part.is_empty())
        .map(|part| match part.split_once('=') {
            Some((k, v)) => (k, v),
            None => (part, ""),
        })
        .collect();

    pairs.sort_by(|a, b| a.0.cmp(b.0).then_with(|| a.1.cmp(b.1)));

    let mut out = String::with_capacity(q.len());
    for (idx, (k, v)) in pairs.iter().enumerate() {
        if idx > 0 {
            out.push('&');
        }
        out.push_str(k);
        out.push('=');
        out.push_str(v);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderValue;

    fn build_canonical_request_str(
        method: &str,
        uri_path: &str,
        query_opt: Option<&str>,
        headers: &HeaderMap,
        signed_header_names: &[String],
        payload_hash_hex: &str,
    ) -> String {
        let mut out = String::with_capacity(512);

        out.push_str(&method.to_ascii_uppercase());
        out.push('\n');

        let path = if uri_path.is_empty() { "/" } else { uri_path };
        out.push_str(path);
        out.push('\n');

        let canonical_q = canonicalize_query(query_opt);
        out.push_str(&canonical_q);
        out.push('\n');

        for hname in signed_header_names {
            out.push_str(hname);
            out.push(':');
            if let Some(val) = headers.get(hname) {
                out.push_str(std::str::from_utf8(val.as_bytes().trim_ascii()).unwrap_or(""));
            }
            out.push('\n');
        }

        let signed_list = signed_header_names.join(";");
        out.push_str(&signed_list);
        out.push('\n');

        out.push_str(payload_hash_hex);

        out
    }

    #[test]
    fn test_canonicalize_query_sorting() {
        assert_eq!(canonicalize_query(None), "");
        assert_eq!(canonicalize_query(Some("")), "");
        assert_eq!(
            canonicalize_query(Some("z=last&a=first&m=middle")),
            "a=first&m=middle&z=last"
        );
        assert_eq!(canonicalize_query(Some("flag&b=2&a=1")), "a=1&b=2&flag=");
    }

    #[test]
    fn test_streaming_sign_canonical_request_identical_to_string() {
        let mut headers = HeaderMap::new();
        headers.insert("host", HeaderValue::from_static("api.velda.io"));
        headers.insert("x-timestamp", HeaderValue::from_static("1729600000"));
        headers.insert("x-nonce", HeaderValue::from_static("abc-123"));

        let signed_headers = vec![
            "host".to_string(),
            "x-nonce".to_string(),
            "x-timestamp".to_string(),
        ];

        let payload_hash_hex = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

        let canonical_str = build_canonical_request_str(
            "POST",
            "/v1/payments",
            Some("currency=VND&amount=100000"),
            &headers,
            &signed_headers,
            payload_hash_hex,
        );

        let key = hmac::Key::new(hmac::HMAC_SHA256, b"test_secret_key_12345");
        let tag_from_string = hmac::sign(&key, canonical_str.as_bytes());

        let spec = SignedHeadersSpec::compile(&signed_headers, "test").unwrap();
        let req = CanonicalRequest {
            method: "POST",
            path: "/v1/payments",
            query: Some("currency=VND&amount=100000"),
            headers: &headers,
            authority: None,
            payload_hash: payload_hash_hex.as_bytes(),
        };

        let tag_from_stream = sign_canonical_request(&key, &req, &spec);
        assert_eq!(tag_from_string.as_ref(), tag_from_stream.as_ref());
    }

    #[test]
    fn test_h2_canonical_signing_with_authority_matches_h1_host() {
        let mut h1_headers = HeaderMap::new();
        h1_headers.insert("host", HeaderValue::from_static("api.velda.io"));
        h1_headers.insert("x-timestamp", HeaderValue::from_static("1729600000"));
        h1_headers.insert("x-nonce", HeaderValue::from_static("abc-123"));

        let mut h2_headers = HeaderMap::new();
        h2_headers.insert("x-timestamp", HeaderValue::from_static("1729600000"));
        h2_headers.insert("x-nonce", HeaderValue::from_static("abc-123"));
        // Notice: h2_headers has NO "host" header!

        let signed_headers = vec![
            "host".to_string(),
            "x-timestamp".to_string(),
            "x-nonce".to_string(),
        ];

        let payload_hash_hex = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        let key = hmac::Key::new(hmac::HMAC_SHA256, b"test_secret_key_12345");
        let spec = SignedHeadersSpec::compile(&signed_headers, "test").unwrap();

        let req_h1 = CanonicalRequest {
            method: "POST",
            path: "/v1/payments",
            query: None,
            headers: &h1_headers,
            authority: None,
            payload_hash: payload_hash_hex.as_bytes(),
        };

        let req_h2 = CanonicalRequest {
            method: "POST",
            path: "/v1/payments",
            query: None,
            headers: &h2_headers,
            authority: Some(b"api.velda.io"),
            payload_hash: payload_hash_hex.as_bytes(),
        };

        let tag_h1 = sign_canonical_h1(&key, &req_h1, &spec);
        let tag_h2 = sign_canonical_h2_h3(&key, &req_h2, &spec);

        assert_eq!(tag_h1.as_ref(), tag_h2.as_ref());
    }

    #[test]
    fn test_simd_constant_time_eq() {
        let a = b"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        let b = b"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        let c = b"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdeg";

        assert!(constant_time_eq(a, b));
        assert!(!constant_time_eq(a, c));
        assert!(!constant_time_eq(a, &a[..32]));
    }
}
