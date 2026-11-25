//! HTTP Cache Key Computation & Zero-Alloc Pre-Hashed Header Extraction.
//!
//! Both `http_cache_lookup` (`pre_upstream`) and `http_cache_store` (`post_response`) derive the
//! exact same 64-bit key from `(req_state.route_id, req_state.method, key_headers)`:
//!
//! ```text
//!   hash_route_request(route_id, method, headers, compiled_key_headers)  [0 heap alloc]
//!     ├─ write route_id (u16) + method (u8) + 0xFF delimiter directly into FxHasher
//!     ├─ iterate pre-sorted CompiledKeyHeader slice (no runtime sort!)
//!     │    └─ headers.get_prehashed(&hdr.key) ─► tag7 SIMD scan ─► tag value (1+bytes / 0)
//!     └─ output u64 FxHash (0 reserved by normalize_tag in shard)
//! ```

use std::hash::Hasher;

use http::header::HeaderName;
use rustc_hash::FxHasher;
use velda_core::HeaderWorkspace;
use velda_core::buffer::header::{HeaderKey, X_REQUEST_ID};
use velda_core::lut::HttpMethod;

use crate::http_cache::constants::X_CACHE;

/// Pre-compiled, canonically lowercased cache key header descriptor.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompiledKeyHeader {
    pub canonical_name: Box<str>,
    pub key: HeaderKey,
}

/// Compiles, lowercases, deduplicates, and sorts custom segmentation header names once at config load.
///
/// Sorting during compilation eliminates per-request header sorting on the frontline hot path while
/// guaranteeing identical hashes regardless of the declaration order in `plugins.json`.
pub fn compile_key_headers(header_names: &[String]) -> Result<Box<[CompiledKeyHeader]>, String> {
    let mut compiled = Vec::with_capacity(header_names.len());
    for raw in header_names {
        let trimmed = raw.trim();
        let lower = trimmed.to_ascii_lowercase();
        let hdr_name = HeaderName::from_bytes(lower.as_bytes())
            .map_err(|e| format!("invalid header name '{raw}': {e}"))?;
        let key = HeaderKey::from_name(hdr_name);
        compiled.push(CompiledKeyHeader {
            canonical_name: lower.into_boxed_str(),
            key,
        });
    }
    compiled.sort_unstable_by(|a, b| a.canonical_name.cmp(&b.canonical_name));
    compiled.dedup_by(|a, b| a.canonical_name == b.canonical_name);
    Ok(compiled.into_boxed_slice())
}

/// Computes the deterministic 64-bit cache slot key from `(route_id, method, key_headers)`
/// with zero heap allocations and O(1) pre-hashed SIMD header tag scans.
#[inline]
pub fn hash_route_request(
    route_id: u16,
    method: HttpMethod,
    headers: &HeaderWorkspace,
    key_headers: &[CompiledKeyHeader],
) -> u64 {
    let mut hasher = FxHasher::default();
    hasher.write_u16(route_id);
    hasher.write_u8(method as u8);
    hasher.write_u8(0xff);

    for hdr in key_headers {
        hasher.write(hdr.canonical_name.as_bytes());
        hasher.write_u8(b'=');
        if let Some(val) = headers
            .get_prehashed(&hdr.key)
            .and_then(|v| v.to_str().ok())
        {
            hasher.write_u8(1);
            hasher.write(val.as_bytes());
        } else {
            hasher.write_u8(0);
        }
        hasher.write_u8(b'|');
    }

    hasher.finish()
}

/// Strips dynamic, session-specific, and hop-by-hop headers from an upstream response.
pub fn filter_cacheable_headers(headers: &HeaderWorkspace) -> HeaderWorkspace {
    use http::header::{
        CONNECTION, PROXY_AUTHENTICATE, PROXY_AUTHORIZATION, SET_COOKIE, TE, TRAILER,
        TRANSFER_ENCODING, UPGRADE,
    };

    let mut filtered = headers.clone();
    filtered.remove(SET_COOKIE);
    filtered.remove(CONNECTION);
    filtered.remove(TRANSFER_ENCODING);
    filtered.remove(UPGRADE);
    filtered.remove(TRAILER);
    filtered.remove(TE);
    filtered.remove(PROXY_AUTHENTICATE);
    filtered.remove(PROXY_AUTHORIZATION);
    filtered.remove(&X_REQUEST_ID);
    filtered.remove(&X_CACHE);

    filtered
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderValue;

    #[test]
    fn test_route_id_method_and_key_headers_partitioning() {
        let h_ab = compile_key_headers(&["X-Tenant-ID".into(), "X-User-ID".into()]).unwrap();
        let h_ba = compile_key_headers(&["x-user-id".into(), "x-tenant-id".into()]).unwrap();

        let mut ws1 = HeaderWorkspace::new(4);
        ws1.insert("x-tenant-id", HeaderValue::from_static("acme"));
        ws1.insert("x-user-id", HeaderValue::from_static("u42"));

        // Different declaration order must produce identical hash.
        assert_eq!(
            hash_route_request(7, HttpMethod::Get, &ws1, &h_ab),
            hash_route_request(7, HttpMethod::Get, &ws1, &h_ba)
        );

        // Different route_id isolates cache slots.
        assert_ne!(
            hash_route_request(7, HttpMethod::Get, &ws1, &h_ab),
            hash_route_request(8, HttpMethod::Get, &ws1, &h_ab)
        );

        // Different HTTP method isolates cache slots.
        assert_ne!(
            hash_route_request(7, HttpMethod::Get, &ws1, &h_ab),
            hash_route_request(7, HttpMethod::Head, &ws1, &h_ab)
        );

        // Different header value (tenant/user segmentation) isolates cache slots.
        let mut ws2 = HeaderWorkspace::new(4);
        ws2.insert("x-tenant-id", HeaderValue::from_static("acme"));
        ws2.insert("x-user-id", HeaderValue::from_static("u99"));
        assert_ne!(
            hash_route_request(7, HttpMethod::Get, &ws1, &h_ab),
            hash_route_request(7, HttpMethod::Get, &ws2, &h_ab)
        );

        // Missing header vs empty header value must not collide.
        let ws_missing = HeaderWorkspace::new(2);
        let mut ws_empty = HeaderWorkspace::new(2);
        ws_empty.insert("x-tenant-id", HeaderValue::from_static(""));
        assert_ne!(
            hash_route_request(7, HttpMethod::Get, &ws_missing, &h_ab),
            hash_route_request(7, HttpMethod::Get, &ws_empty, &h_ab)
        );
    }
}
