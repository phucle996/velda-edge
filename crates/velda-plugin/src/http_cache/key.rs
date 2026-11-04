//! HTTP Cache Key Computation & URI Query Normalization.
//!
//! Provides deterministic cache key construction, alphabetical query string sorting,
//! multi-tenant header discrimination, and 64-bit FxHash computation.

use std::hash::Hasher;

use http::{HeaderMap, Method};
use rustc_hash::FxHasher;

use crate::header::{X_REQUEST_ID, X_ROUTE_ID, X_UPSTREAM_ADDR};
use crate::http_cache::constants::{X_CACHE, X_VELDA_CACHE_HASH, X_VELDA_CACHE_KEY};

/// Normalizes and sorts query string parameters alphabetically (e.g. `b=2&a=1` -> `a=1&b=2`).
pub fn normalize_query(query: &str) -> String {
    let mut params: Vec<&str> = query.split('&').filter(|s| !s.is_empty()).collect();
    params.sort_unstable();
    params.join("&")
}

/// Computes the 64-bit FxHash of a cache key string.
#[inline(always)]
pub fn hash_key(key: &str) -> u64 {
    let mut hasher = FxHasher::default();
    hasher.write(key.as_bytes());
    hasher.finish()
}

/// Computes the canonical cache key and its 64-bit FxHash.
pub fn compute_cache_key(
    method: &Method,
    host: Option<&str>,
    path: &str,
    query: Option<&str>,
    ignore_query_order: bool,
) -> (String, u64) {
    compute_cache_key_with_headers(method, host, path, query, ignore_query_order, None)
}

/// Computes the canonical cache key incorporating custom segmentation headers.
pub fn compute_cache_key_with_headers(
    method: &Method,
    host: Option<&str>,
    path: &str,
    query: Option<&str>,
    ignore_query_order: bool,
    extra_headers: Option<&[(&str, &str)]>,
) -> (String, u64) {
    let mut key = String::with_capacity(128);
    key.push_str(method.as_str());
    key.push(':');
    if let Some(h) = host {
        key.push_str(h);
    }
    key.push(':');
    key.push_str(path);

    if let Some(q) = query
        && !q.is_empty()
    {
        key.push('?');
        if ignore_query_order {
            key.push_str(&normalize_query(q));
        } else {
            key.push_str(q);
        }
    }

    if let Some(headers) = extra_headers
        && !headers.is_empty()
    {
        let mut sorted_headers: Vec<(&str, &str)> = headers.to_vec();
        sorted_headers.sort_unstable_by(|a, b| a.0.cmp(b.0));
        for (h_name, h_val) in sorted_headers {
            key.push('|');
            key.push_str(h_name);
            key.push('=');
            key.push_str(h_val);
        }
    }

    let hash = hash_key(&key);
    (key, hash)
}

/// Strips dynamic, session-specific, and hop-by-hop headers from an upstream response
/// before storing into cache, strictly complying with RFC 9111 §3.
pub fn filter_cacheable_headers(headers: &HeaderMap) -> HeaderMap {
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
    filtered.remove(&X_ROUTE_ID);
    filtered.remove(&X_UPSTREAM_ADDR);
    filtered.remove(&X_CACHE);
    filtered.remove(&X_VELDA_CACHE_KEY);
    filtered.remove(&X_VELDA_CACHE_HASH);

    filtered
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_query() {
        assert_eq!(normalize_query("b=2&a=1&c=3"), "a=1&b=2&c=3");
        assert_eq!(normalize_query("tag=rust&page=2"), "page=2&tag=rust");
        assert_eq!(normalize_query("a=1"), "a=1");
        assert_eq!(normalize_query(""), "");
    }

    #[test]
    fn test_compute_cache_key_with_headers() {
        let (key1, hash1) = compute_cache_key_with_headers(
            &Method::GET,
            Some("api.example.com"),
            "/profile",
            Some("sort=desc&page=1"),
            true,
            Some(&[("authorization", "Bearer alice"), ("x-tenant", "tenant-1")]),
        );

        let (key2, hash2) = compute_cache_key_with_headers(
            &Method::GET,
            Some("api.example.com"),
            "/profile",
            Some("page=1&sort=desc"),
            true,
            Some(&[("x-tenant", "tenant-1"), ("authorization", "Bearer alice")]),
        );

        assert_eq!(key1, key2);
        assert_eq!(hash1, hash2);
        assert_eq!(
            key1,
            "GET:api.example.com:/profile?page=1&sort=desc|authorization=Bearer alice|x-tenant=tenant-1"
        );

        let (key_bob, hash_bob) = compute_cache_key_with_headers(
            &Method::GET,
            Some("api.example.com"),
            "/profile",
            Some("page=1&sort=desc"),
            true,
            Some(&[("authorization", "Bearer bob")]),
        );

        assert_ne!(key1, key_bob);
        assert_ne!(hash1, hash_bob);
    }

    #[test]
    fn test_filter_cacheable_headers() {
        use http::header::{CONTENT_TYPE, HeaderValue, SET_COOKIE};

        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        headers.insert(SET_COOKIE, HeaderValue::from_static("session_id=secret"));
        headers.insert(
            &X_VELDA_CACHE_KEY,
            HeaderValue::from_static("GET:api.example.com:/path"),
        );

        let filtered = filter_cacheable_headers(&headers);
        assert!(filtered.contains_key(CONTENT_TYPE));
        assert!(!filtered.contains_key(SET_COOKIE));
        assert!(!filtered.contains_key(&X_VELDA_CACHE_KEY));
    }
}
