//! HTTP Caching Infrastructure Module (`velda-plugin::http_cache`).
//!
//! Provides the underlying shared memory cache storage engine ([`HttpCacheStorage`]),
//! entity representations, RFC 9111 parsing, hardware DRAM confinement,
//! key hashing, query normalization, and storage registration for [`http_cache_lookup`]
//! and [`http_cache_store`] plugins.

pub mod cache_control;
pub mod constants;
pub mod dram;
pub mod entity;
pub mod key;
pub mod storage;

pub use cache_control::{
    CC_FLAG_MAX_AGE, CC_FLAG_NO_CACHE, CC_FLAG_NO_STORE, CC_FLAG_PRIVATE, CC_FLAG_PUBLIC,
    CC_FLAG_S_MAXAGE, CacheControlFast, parse_cache_control_fast, parse_cache_control_ttl,
};
pub use constants::{X_CACHE, X_VELDA_CACHE_HASH, X_VELDA_CACHE_KEY};
pub use dram::{create_dram_confined_bytes, stream_payload_to_dram};
pub use entity::{CacheEntry, CompressedVariants, StoreVariantParams};
pub use key::{
    compute_cache_key, compute_cache_key_with_headers, filter_cacheable_headers, hash_key,
    normalize_query,
};
pub use storage::{CacheShard, HttpCacheRegistry, HttpCacheStorage, NUM_SHARDS};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::post_response::PostResponsePlugin;
    use crate::post_response::http_cache_store::{HttpCacheStoreConfig, HttpCacheStorePlugin};
    use crate::pre_upstream::PreUpstreamPlugin;
    use crate::pre_upstream::http_cache_lookup::{HttpCacheLookupConfig, HttpCacheLookupPlugin};
    use http::header::{CACHE_CONTROL, CONTENT_TYPE, SET_COOKIE};
    use http::{HeaderMap, Method, StatusCode, Uri, Version};
    use std::sync::Arc;
    use std::time::Duration;
    use velda_core::{Action, Body, L7Request, L7Response};

    #[test]
    fn test_end_to_end_http_cache_flow() {
        let storage = Arc::new(HttpCacheStorage::new(
            "e2e_storage",
            1024 * 1024,
            Duration::from_secs(120),
        ));

        let lookup_plugin = HttpCacheLookupPlugin::with_storage(
            "lookup",
            HttpCacheLookupConfig::default(),
            Arc::clone(&storage),
        );
        let store_plugin = HttpCacheStorePlugin::with_storage(
            "store",
            HttpCacheStoreConfig::default(),
            Arc::clone(&storage),
        );

        // --- Step 1: Request 1 (Query with un-sorted params) -> CACHE MISS ---
        let uri1: Uri = "http://api.example.com/v1/products?sort=desc&limit=10"
            .parse()
            .unwrap();
        let mut headers1 = HeaderMap::new();
        headers1.insert(http::header::HOST, "api.example.com".parse().unwrap());
        headers1.insert("x-request-id", "req-001".parse().unwrap());
        let mut req1 = L7Request::new(Method::GET, uri1, Version::HTTP_11, headers1, Body::Empty);
        let mut target1: Arc<str> = Arc::from("default_upstream");
        let action1 = lookup_plugin.on_upstream_request(&mut req1, &mut target1);
        assert!(action1.is_continue());
        assert!(req1.headers.contains_key("x-velda-cache-key"));
        let cache_key = req1
            .headers
            .get("x-velda-cache-key")
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        assert_eq!(
            cache_key,
            "GET:api.example.com:/v1/products?limit=10&sort=desc"
        );

        // --- Step 2: Upstream responds with 200 OK + payload + Set-Cookie ---
        let mut resp_headers = HeaderMap::new();
        resp_headers.insert("x-velda-cache-key", cache_key.parse().unwrap());
        resp_headers.insert("x-request-id", "req-001".parse().unwrap());
        resp_headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());
        resp_headers.insert(
            SET_COOKIE,
            "session_id=user1_secret; Path=/; HttpOnly".parse().unwrap(),
        );
        resp_headers.insert(CACHE_CONTROL, "public, max-age=300".parse().unwrap());

        let mut resp1 = L7Response::from_bytes(StatusCode::OK, b"{\"items\":[1,2,3]}".to_vec())
            .with_headers(resp_headers);

        store_plugin.on_response(&req1.headers, &mut resp1);

        // Store plugin sets diagnostic headers and removes internal transit keys
        assert_eq!(resp1.headers.get("x-cache").unwrap(), "MISS");
        assert_eq!(resp1.headers.get("age").unwrap(), "0");
        assert!(!resp1.headers.contains_key("x-velda-cache-key"));

        // --- Step 3: Request 2 (Identical path, inverted query params) -> CACHE HIT! ---
        let uri2: Uri = "http://api.example.com/v1/products?limit=10&sort=desc"
            .parse()
            .unwrap();
        let mut headers2 = HeaderMap::new();
        headers2.insert(http::header::HOST, "api.example.com".parse().unwrap());
        let mut req2 = L7Request::new(Method::GET, uri2, Version::HTTP_11, headers2, Body::Empty);

        let mut target2: Arc<str> = Arc::from("default_upstream");
        let action2 = lookup_plugin.on_upstream_request(&mut req2, &mut target2);
        match action2 {
            Action::ServeCached(cached) => {
                let cached_resp = cached.into_l7_response();
                assert_eq!(cached_resp.status, StatusCode::OK);
                assert_eq!(cached_resp.headers.get("x-cache").unwrap(), "HIT");
                assert_eq!(cached_resp.headers.get("age").unwrap(), "0");
                assert_eq!(
                    cached_resp.headers.get(CONTENT_TYPE).unwrap(),
                    "application/json"
                );
                assert_eq!(
                    cached_resp
                        .headers
                        .get(http::header::CONTENT_LENGTH)
                        .unwrap(),
                    "17"
                );
                // CRITICAL SECURITY INVARIANT: Set-Cookie MUST NOT be served from cache!
                assert!(!cached_resp.headers.contains_key(SET_COOKIE));
                match cached_resp.body {
                    Body::Bytes(b) => {
                        assert_eq!(b, bytes::Bytes::from_static(b"{\"items\":[1,2,3]}"))
                    }
                    _ => panic!("Expected cached Body::Bytes"),
                }
            }
            _ => panic!("Expected Action::ServeCached on cache hit"),
        }

        // --- Step 4: Request 3 (no-store from upstream) -> CACHE BYPASS ---
        let mut bypass_headers = HeaderMap::new();
        bypass_headers.insert(
            "x-velda-cache-key",
            "GET:api.example.com:/v1/checkout".parse().unwrap(),
        );
        bypass_headers.insert(CACHE_CONTROL, "no-store, private".parse().unwrap());

        let mut bypass_resp = L7Response::from_bytes(StatusCode::OK, b"{\"order\":42}".to_vec())
            .with_headers(bypass_headers);
        let empty_req_headers = velda_core::HeaderWorkspace::default();

        store_plugin.on_response(&empty_req_headers, &mut bypass_resp);
        assert_eq!(bypass_resp.headers.get("x-cache").unwrap(), "BYPASS");

        let hash_bypass = HttpCacheStorage::hash_key("GET:api.example.com:/v1/checkout");
        assert!(storage.get(hash_bypass).is_none());
    }
}
