//! HTTP Caching Infrastructure Module (`velda-plugin::http_cache`).
//!
//! Provides the underlying shared memory cache storage engine ([`HttpCacheZone`]),
//! key hashing, query normalization, and zone registration for [`http_cache_lookup`]
//! and [`http_cache_store`] plugins.

pub mod zone;

pub use zone::{
    CacheEntry, CacheShard, HttpCacheZone, HttpCacheZoneRegistry, NUM_SHARDS,
    filter_cacheable_headers, normalize_query, parse_cache_control_ttl,
};

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
        let zone = Arc::new(HttpCacheZone::new(
            "e2e_zone",
            1024 * 1024,
            Duration::from_secs(120),
        ));

        let lookup_plugin = HttpCacheLookupPlugin::with_zone(
            "lookup",
            HttpCacheLookupConfig::default(),
            Arc::clone(&zone),
        );
        let store_plugin = HttpCacheStorePlugin::with_zone(
            "store",
            HttpCacheStoreConfig::default(),
            Arc::clone(&zone),
        );

        // --- Step 1: Request 1 (Query with un-sorted params) -> CACHE MISS ---
        let uri1: Uri = "http://api.example.com/v1/products?sort=desc&limit=10"
            .parse()
            .unwrap();
        let mut headers1 = HeaderMap::new();
        headers1.insert(http::header::HOST, "api.example.com".parse().unwrap());
        headers1.insert("x-request-id", "req-001".parse().unwrap());
        let mut req1 = L7Request::new(Method::GET, uri1, Version::HTTP_11, headers1, Body::Empty);

        let action1 = lookup_plugin.on_upstream_request(&mut req1);
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

        let mut resp1 = L7Response::from_bytes(StatusCode::OK, b"{\"items\":[1,2,3]}".to_vec());
        resp1.headers = resp_headers;

        store_plugin.on_response(&mut resp1);

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

        let action2 = lookup_plugin.on_upstream_request(&mut req2);
        match action2 {
            Action::Respond(cached_resp) => {
                assert_eq!(cached_resp.status, StatusCode::OK);
                assert_eq!(cached_resp.headers.get("x-cache").unwrap(), "HIT");
                assert_eq!(cached_resp.headers.get("age").unwrap(), "0");
                assert_eq!(
                    cached_resp.headers.get(CONTENT_TYPE).unwrap(),
                    "application/json"
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
            _ => panic!("Expected Action::Respond on cache hit"),
        }

        // --- Step 4: Request 3 (no-store from upstream) -> CACHE BYPASS ---
        let mut bypass_headers = HeaderMap::new();
        bypass_headers.insert(
            "x-velda-cache-key",
            "GET:api.example.com:/v1/checkout".parse().unwrap(),
        );
        bypass_headers.insert(CACHE_CONTROL, "no-store, private".parse().unwrap());

        let mut bypass_resp = L7Response::from_bytes(StatusCode::OK, b"{\"order\":42}".to_vec());
        bypass_resp.headers = bypass_headers;

        store_plugin.on_response(&mut bypass_resp);
        assert_eq!(bypass_resp.headers.get("x-cache").unwrap(), "BYPASS");

        let hash_bypass = HttpCacheZone::hash_key("GET:api.example.com:/v1/checkout");
        assert!(zone.get(hash_bypass).is_none());
    }
}
