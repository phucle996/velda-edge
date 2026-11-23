//! Integration Test Suite for HTTP Cache Infrastructure (`http_cache`, `http_cache_lookup`, `http_cache_store`).

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http::header::{CACHE_CONTROL, CONTENT_TYPE, SET_COOKIE};
use http::{HeaderMap, HeaderValue, Method, StatusCode, Uri, Version};
use serde_json::json;
use velda_core::clock::coarse_sec;
use velda_core::{Action, Body, HeaderWorkspace, L7Request, L7Response};
use velda_plugin::PluginConfig;
use velda_plugin::http_cache::{
    CacheEntry, CompressedVariants, HttpCacheStorage, LookupOutcome, SLAB_SIZE, StoreVariantParams,
    X_VELDA_CACHE_HASH, X_VELDA_CACHE_KEY, compute_cache_key, compute_cache_key_with_headers,
    create_dram_confined_bytes, filter_cacheable_headers, hash_key, hash_request_key_zero_alloc,
    normalize_query, parse_cache_control_fast, parse_cache_control_ttl,
};
use velda_plugin::post_response::PostResponsePlugin;
use velda_plugin::post_response::http_cache_store::validation::validate_http_cache_store_config;
use velda_plugin::post_response::http_cache_store::{HttpCacheStoreConfig, HttpCacheStorePlugin};
use velda_plugin::pre_upstream::PreUpstreamPlugin;
use velda_plugin::pre_upstream::http_cache_lookup::validation::validate_http_cache_lookup_config;
use velda_plugin::pre_upstream::http_cache_lookup::{HttpCacheLookupConfig, HttpCacheLookupPlugin};

// -----------------------------------------------------------------------------
// 1. Key Computation, Cache-Control Parser & DRAM Streaming
// -----------------------------------------------------------------------------

#[test]
fn test_normalize_query() {
    assert_eq!(normalize_query("b=2&a=1&c=3"), "a=1&b=2&c=3");
    assert_eq!(normalize_query("tag=rust&page=2"), "page=2&tag=rust");
    assert_eq!(normalize_query("a=1"), "a=1");
    assert_eq!(normalize_query(""), "");
}

#[test]
fn test_compute_cache_key_with_headers_and_zero_alloc_equivalence() {
    let headers_a = &[("authorization", "Bearer alice"), ("x-tenant", "tenant-1")];
    let headers_b = &[("x-tenant", "tenant-1"), ("authorization", "Bearer alice")];

    let (key1, hash1) = compute_cache_key_with_headers(
        &Method::GET,
        Some("api.example.com"),
        "/profile",
        Some("sort=desc&page=1"),
        true,
        Some(headers_a),
    );
    let (key2, hash2) = compute_cache_key_with_headers(
        &Method::GET,
        Some("api.example.com"),
        "/profile",
        Some("page=1&sort=desc"),
        true,
        Some(headers_b),
    );

    assert_eq!(key1, key2);
    assert_eq!(hash1, hash2);
    assert_eq!(
        key1,
        "GET:api.example.com:/profile?page=1&sort=desc|authorization=Bearer alice|x-tenant=tenant-1"
    );

    let zero_alloc_hash1 = hash_request_key_zero_alloc(
        &Method::GET,
        Some("api.example.com"),
        "/profile",
        Some("sort=desc&page=1"),
        true,
        Some(headers_a),
    );
    let zero_alloc_hash2 = hash_request_key_zero_alloc(
        &Method::GET,
        Some("api.example.com"),
        "/profile",
        Some("page=1&sort=desc"),
        true,
        Some(headers_b),
    );
    assert_eq!(zero_alloc_hash1, hash1);
    assert_eq!(zero_alloc_hash2, hash2);

    // >32 query params fallback path must still match canonical string hash
    let long_q = (0..40)
        .rev()
        .map(|i| format!("k{i:02}=v{i}"))
        .collect::<Vec<_>>()
        .join("&");
    let (_, long_expected) = compute_cache_key(
        &Method::GET,
        Some("api.example.com"),
        "/many",
        Some(&long_q),
        true,
    );
    let long_zero = hash_request_key_zero_alloc(
        &Method::GET,
        Some("api.example.com"),
        "/many",
        Some(&long_q),
        true,
        None,
    );
    assert_eq!(long_zero, long_expected);
}

#[test]
fn test_filter_cacheable_headers() {
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

#[test]
fn test_parse_cache_control_ttl_and_directives() {
    assert_eq!(
        parse_cache_control_ttl("public, max-age=3600"),
        Some(Duration::from_secs(3600))
    );
    assert_eq!(
        parse_cache_control_ttl("max-age=60, s-maxage=300"),
        Some(Duration::from_secs(300))
    );
    assert_eq!(
        parse_cache_control_ttl("s-maxage=\"120\", max-age=60"),
        Some(Duration::from_secs(120))
    );
    assert_eq!(parse_cache_control_ttl("no-store, no-cache"), None);
    assert_eq!(parse_cache_control_ttl(""), None);

    let fast = parse_cache_control_fast(b"public, max-age=120");
    assert!(!fast.is_no_store_or_private());
    assert!(fast.is_public_or_s_maxage());
    assert_eq!(fast.effective_ttl_secs(), Some(120));

    let no_store = parse_cache_control_fast(b"no-store, private");
    assert!(no_store.is_no_store_or_private());
    assert_eq!(no_store.effective_ttl_secs(), None);

    let complex = parse_cache_control_fast(b"  public , s-maxage = 600 , max-age = 300 ");
    assert!(complex.is_public_or_s_maxage());
    assert_eq!(complex.effective_ttl_secs(), Some(600));
}

#[test]
fn test_dram_confinement_payload_lengths() {
    let small = b"small payload under 256 bytes";
    assert_eq!(create_dram_confined_bytes(small).as_ref(), small);

    for len in [255, 256, 257, 511, 512, 513, 1023, 1024, 1025, 4096] {
        let original: Vec<u8> = (0..len).map(|i| (i % 256) as u8).collect();
        let bytes = create_dram_confined_bytes(&original);
        assert_eq!(bytes.as_ref(), original.as_slice(), "Failed at len {len}");
    }
}

#[test]
fn test_cache_entry_and_smart_variant_matching() {
    let entry = CacheEntry::with_raw(
        "GET:example.com:/pub",
        StatusCode::OK,
        None,
        Bytes::from_static(b"public data"),
        Duration::from_secs(60),
    )
    .with_public(true);
    assert!(entry.is_public);
    assert!(entry.is_fresh());

    let mut lazy = CacheEntry::new(
        "GET:example.com:/lazy",
        StatusCode::OK,
        None,
        None,
        Duration::from_secs(60),
    );
    lazy.variants.brotli = Some(Bytes::from_static(b"brotli-only-data"));

    let ae = HeaderValue::from_static("gzip, br;q=0.8");
    let (payload, enc) = lazy.negotiate_smart_variant(Some(&ae)).unwrap();
    assert_eq!(payload.as_ref(), b"brotli-only-data");
    assert_eq!(enc.unwrap().to_str().unwrap(), "br");

    let ae_gzip_only = HeaderValue::from_static("gzip");
    assert!(lazy.negotiate_smart_variant(Some(&ae_gzip_only)).is_none());
}

// -----------------------------------------------------------------------------
// 2. Contiguous 2 MiB Slab Pool Storage Engine
// -----------------------------------------------------------------------------

#[test]
fn test_cache_storage_clock_and_expiration() {
    let storage = HttpCacheStorage::new("test_storage", 4 * 1024 * 1024, Duration::from_secs(120));
    let (_, hash) = compute_cache_key(&Method::GET, Some("example.com"), "/clock", None, true);

    let now = coarse_sec() as u32;
    let entry = CacheEntry::with_raw(
        "GET:example.com:/clock",
        StatusCode::OK,
        None,
        Bytes::from_static(b"clock payload"),
        Duration::from_secs(60),
    )
    .with_timestamps(now, now + 100);

    storage.store(hash, entry);

    let retrieved = storage.get(hash).expect("Fresh entry must hit");
    assert_eq!(retrieved.raw_body.unwrap().as_ref(), b"clock payload");

    let expired_entry = CacheEntry::with_raw(
        "GET:example.com:/clock",
        StatusCode::OK,
        None,
        Bytes::from_static(b"expired payload"),
        Duration::from_secs(60),
    )
    .with_timestamps(now.saturating_sub(200), now.saturating_sub(100));

    storage.store(hash, expired_entry);
    assert!(storage.get(hash).is_none());
}

#[test]
fn test_storage_store_or_merge_lazy_variants() {
    let storage = HttpCacheStorage::new("lazy_storage", 4 * 1024 * 1024, Duration::from_secs(60));
    let (_, hash) = compute_cache_key(&Method::GET, Some("example.com"), "/lazy-data", None, true);

    storage.store_variant(StoreVariantParams {
        hash,
        key: "GET:example.com:/lazy-data".to_string(),
        status: StatusCode::OK,
        content_type: None,
        ttl: Duration::from_secs(60),
        encoding: Some("br"),
        payload: Bytes::from_static(b"br-compressed"),
        variants: None,
        is_public: false,
    });

    let entry = storage.get(hash).expect("Must exist");
    assert_eq!(
        entry.variants.brotli.as_ref().unwrap().as_ref(),
        b"br-compressed"
    );
    assert!(entry.variants.gzip.is_none());

    storage.store_variant(StoreVariantParams {
        hash,
        key: "GET:example.com:/lazy-data".to_string(),
        status: StatusCode::OK,
        content_type: None,
        ttl: Duration::from_secs(60),
        encoding: Some("gzip"),
        payload: Bytes::from_static(b"gzip-compressed"),
        variants: None,
        is_public: false,
    });

    let entry2 = storage.get(hash).expect("Must exist");
    assert_eq!(
        entry2.variants.brotli.as_ref().unwrap().as_ref(),
        b"br-compressed"
    );
    assert_eq!(
        entry2.variants.gzip.as_ref().unwrap().as_ref(),
        b"gzip-compressed"
    );
}

#[test]
fn test_storage_merge_variants() {
    let storage = HttpCacheStorage::new("test_merge", 4 * 1024 * 1024, Duration::from_secs(60));
    let (key, hash) = compute_cache_key(&Method::GET, Some("example.com"), "/items", None, true);

    let entry = CacheEntry::with_raw(
        key,
        StatusCode::OK,
        None,
        Bytes::from_static(b"pristine raw data"),
        Duration::from_secs(120),
    );
    storage.store(hash, entry);

    assert!(storage.get(hash).unwrap().variants.gzip.is_none());

    let variants = CompressedVariants {
        gzip: Some(Bytes::from_static(b"async gzip")),
        brotli: Some(Bytes::from_static(b"async br")),
        zstd: Some(Bytes::from_static(b"async zstd")),
    };
    assert!(storage.merge_variants(hash, variants));

    let updated = storage.get(hash).unwrap();
    assert_eq!(updated.variants.gzip.unwrap().as_ref(), b"async gzip");
    assert_eq!(updated.variants.brotli.unwrap().as_ref(), b"async br");
    assert_eq!(updated.variants.zstd.unwrap().as_ref(), b"async zstd");
}

#[test]
fn test_slab_pool_bump_down_rotation_and_recycle() {
    // 2 slabs x 2 MiB = 4 MiB total
    let storage = HttpCacheStorage::new("slab_rotate", 4 * 1024 * 1024, Duration::from_secs(120));
    let initial = storage.slab_stats().unwrap();
    assert_eq!(initial.num_slabs, 2);
    assert_eq!(initial.free_slabs, 1);
    assert_eq!(initial.active_slab, 0);
    assert_eq!(initial.active_remaining_bytes, SLAB_SIZE as u32);

    // Fill slab 0 with 1.5 MiB + 1.0 MiB -> second write underflows active slab 0 and rotates to slab 1
    let chunk_a = vec![b'A'; 1_500_000];
    let chunk_b = vec![b'B'; 1_000_000];
    let hash_a = hash_key("slab:chunk_a");
    let hash_b = hash_key("slab:chunk_b");

    storage.store(
        hash_a,
        CacheEntry::with_raw(
            "a",
            StatusCode::OK,
            None,
            Bytes::from(chunk_a.clone()),
            Duration::from_secs(120),
        ),
    );
    let after_a = storage.slab_stats().unwrap();
    assert_eq!(after_a.active_slab, 0);
    assert_eq!(
        after_a.active_remaining_bytes,
        (SLAB_SIZE - 1_500_000) as u32
    );

    storage.store(
        hash_b,
        CacheEntry::with_raw(
            "b",
            StatusCode::OK,
            None,
            Bytes::from(chunk_b.clone()),
            Duration::from_secs(120),
        ),
    );
    let after_b = storage.slab_stats().unwrap();
    assert_eq!(after_b.active_slab, 1);
    assert_eq!(after_b.sealed_slabs, 1);
    assert_eq!(after_b.free_slabs, 0);
    assert_eq!(
        after_b.active_remaining_bytes,
        (SLAB_SIZE - 1_000_000) as u32
    );

    // Purging chunk_a drops slab 0 live_spans to 0 -> returns whole 2 MiB slab 0 to free_pool
    assert!(storage.purge(hash_a));
    assert!(storage.get(hash_a).is_none());
    assert_eq!(
        storage.get(hash_b).unwrap().raw_body.unwrap().as_ref(),
        chunk_b.as_slice()
    );

    let after_purge = storage.slab_stats().unwrap();
    assert_eq!(after_purge.sealed_slabs, 0);
    assert_eq!(after_purge.free_slabs, 1);
}

#[test]
fn test_slab_lease_reader_pin_prevents_use_after_free() {
    let storage = HttpCacheStorage::new("slab_pin", 4 * 1024 * 1024, Duration::from_secs(120));
    let payload_1 = vec![0x5Au8; 1_500_000];
    let hash_1 = hash_key("pin:1");
    storage.store(
        hash_1,
        CacheEntry::with_raw(
            "1",
            StatusCode::OK,
            None,
            Bytes::from(payload_1.clone()),
            Duration::from_secs(120),
        ),
    );

    // Pin zero-copy Bytes slice on active reader while slab rotates & entry is purged
    let pinned_slice = match storage.lookup_variant(hash_1, None, false) {
        LookupOutcome::Hit { payload, .. } => payload,
        other => panic!("Expected LookupOutcome::Hit, got {other:?}"),
    };

    // Force slab 0 -> sealed by storing 1.5 MiB into slab 1, then purge hash_1
    let payload_2 = vec![0x33u8; 1_500_000];
    let hash_2 = hash_key("pin:2");
    storage.store(
        hash_2,
        CacheEntry::with_raw(
            "2",
            StatusCode::OK,
            None,
            Bytes::from(payload_2),
            Duration::from_secs(120),
        ),
    );
    assert!(storage.purge(hash_1));

    // Slab 0 must remain draining until `pinned_slice` drops
    let stats_draining = storage.slab_stats().unwrap();
    assert_eq!(stats_draining.draining_slabs, 1);
    assert_eq!(pinned_slice.as_ref(), payload_1.as_slice());

    drop(pinned_slice);

    // Writing a third 1.5 MiB item triggers draining cleanup and safely recycles slab 0
    let payload_3 = vec![0x77u8; 1_500_000];
    let hash_3 = hash_key("pin:3");
    storage.store(
        hash_3,
        CacheEntry::with_raw(
            "3",
            StatusCode::OK,
            None,
            Bytes::from(payload_3.clone()),
            Duration::from_secs(120),
        ),
    );
    let stats_recycled = storage.slab_stats().unwrap();
    assert_eq!(stats_recycled.draining_slabs, 0);
    assert_eq!(
        storage.get(hash_3).unwrap().raw_body.unwrap().as_ref(),
        payload_3.as_slice()
    );
}

// -----------------------------------------------------------------------------
// 3. Pre-Upstream Lookup Plugin & Validation
// -----------------------------------------------------------------------------

#[test]
fn test_valid_and_invalid_http_cache_lookup_configs() {
    let valid = json!({
        "zone": "api_cache",
        "cacheable_methods": ["GET", "HEAD"],
        "ignore_query_order": true,
        "honor_client_no_cache": false,
        "bypass_authenticated_requests": true,
        "key_headers": ["x-user-id", "authorization"],
        "max_memory_mb": 256,
        "default_ttl_secs": 300
    });
    assert!(validate_http_cache_lookup_config("cache_look_1", &valid).is_ok());

    let empty_zone = json!({
        "zone": "",
        "cacheable_methods": ["GET"],
        "ignore_query_order": true,
        "honor_client_no_cache": false,
        "bypass_authenticated_requests": true,
        "key_headers": [],
        "max_memory_mb": 256,
        "default_ttl_secs": 300
    });
    let err = validate_http_cache_lookup_config("cache_bad_name", &empty_zone).unwrap_err();
    assert!(err.to_string().contains("zone cannot be empty"));

    let missing = json!({ "zone": "cache1" });
    let err2 = validate_http_cache_lookup_config("cache_missing", &missing).unwrap_err();
    assert!(err2.to_string().contains("missing field"));
}

#[test]
fn test_http_cache_lookup_miss_and_hit() {
    let storage = Arc::new(HttpCacheStorage::new(
        "test_lookup",
        4 * 1024 * 1024,
        Duration::from_secs(60),
    ));
    let plugin = HttpCacheLookupPlugin::with_storage(
        "lookup-1",
        HttpCacheLookupConfig::default(),
        Arc::clone(&storage),
    );

    let uri: Uri = "http://api.example.com/products?sort=asc&page=1"
        .parse()
        .unwrap();
    let mut headers = HeaderMap::new();
    headers.insert(http::header::HOST, "api.example.com".parse().unwrap());
    let mut req = L7Request::new(Method::GET, uri, Version::HTTP_11, headers, Body::Empty);

    let mut target1 = 1;
    let action = plugin.on_upstream_request(&mut req, &mut target1);
    assert!(action.is_continue());
    let expected_hash = hash_key("GET:api.example.com:/products?page=1&sort=asc");
    assert_eq!(
        req.headers.get_u64(&X_VELDA_CACHE_HASH),
        Some(expected_hash)
    );

    storage.store(
        expected_hash,
        CacheEntry::with_raw(
            "GET:api.example.com:/products?page=1&sort=asc",
            StatusCode::OK,
            Some(HeaderValue::from_static("application/json")),
            Bytes::from_static(b"[{\"id\":1}]"),
            Duration::from_secs(60),
        ),
    );

    let alt_uri: Uri = "http://api.example.com/products?page=1&sort=asc"
        .parse()
        .unwrap();
    let mut alt_headers = HeaderMap::new();
    alt_headers.insert(http::header::HOST, "api.example.com".parse().unwrap());
    let mut req2 = L7Request::new(
        Method::GET,
        alt_uri,
        Version::HTTP_11,
        alt_headers,
        Body::Empty,
    );

    let mut target2 = 1;
    match plugin.on_upstream_request(&mut req2, &mut target2) {
        Action::ServeCached(cached) => {
            let resp = cached.into_l7_response();
            assert_eq!(resp.status, StatusCode::OK);
            assert_eq!(resp.headers.get("x-cache").unwrap(), "HIT");
            assert_eq!(resp.headers.get("age").unwrap(), "0");
            match resp.body {
                Body::Bytes(b) => assert_eq!(b, Bytes::from_static(b"[{\"id\":1}]")),
                _ => panic!("Expected Body::Bytes"),
            }
        }
        _ => panic!("Expected Action::ServeCached"),
    }
}

#[test]
fn test_http_cache_lookup_negotiates_precompressed_and_lazy_variants() {
    let storage = Arc::new(HttpCacheStorage::new(
        "test_variants",
        4 * 1024 * 1024,
        Duration::from_secs(60),
    ));
    let plugin = HttpCacheLookupPlugin::with_storage(
        "lookup-var",
        HttpCacheLookupConfig::default(),
        Arc::clone(&storage),
    );

    let (_, hash) = compute_cache_key(
        &Method::GET,
        Some("api.example.com"),
        "/large-data",
        None,
        true,
    );
    let raw_payload = Bytes::from_static(b"lots of repetitive data for compression test");
    let gzip_payload = Bytes::from_static(b"gzip_compressed_bytes");
    let br_payload = Bytes::from_static(b"br_compressed_bytes");

    let entry = CacheEntry::with_raw(
        "GET:api.example.com:/large-data",
        StatusCode::OK,
        Some(HeaderValue::from_static("application/json")),
        raw_payload.clone(),
        Duration::from_secs(60),
    )
    .with_variants(CompressedVariants {
        gzip: Some(gzip_payload.clone()),
        brotli: Some(br_payload.clone()),
        zstd: None,
    });
    storage.store(hash, entry);

    let mut headers = HeaderMap::new();
    headers.insert(http::header::HOST, "api.example.com".parse().unwrap());
    headers.insert(
        http::header::ACCEPT_ENCODING,
        "gzip, deflate".parse().unwrap(),
    );
    let mut req = L7Request::new(
        Method::GET,
        "http://api.example.com/large-data".parse().unwrap(),
        Version::HTTP_11,
        headers,
        Body::Empty,
    );
    let mut target = 1;
    match plugin.on_upstream_request(&mut req, &mut target) {
        Action::ServeCached(cached) => {
            let resp = cached.into_l7_response();
            assert_eq!(resp.headers.get("content-encoding").unwrap(), "gzip");
            assert_eq!(resp.headers.get("vary").unwrap(), "Accept-Encoding");
            match resp.body {
                Body::Bytes(b) => assert_eq!(b, gzip_payload),
                _ => panic!("Expected Body::Bytes"),
            }
        }
        _ => panic!("Expected Action::ServeCached"),
    }

    let mut headers_br = HeaderMap::new();
    headers_br.insert(http::header::HOST, "api.example.com".parse().unwrap());
    headers_br.insert(
        http::header::ACCEPT_ENCODING,
        "br;q=1.0, gzip;q=0.8".parse().unwrap(),
    );
    let mut req_br = L7Request::new(
        Method::GET,
        "http://api.example.com/large-data".parse().unwrap(),
        Version::HTTP_11,
        headers_br,
        Body::Empty,
    );
    let mut target_br = 1;
    match plugin.on_upstream_request(&mut req_br, &mut target_br) {
        Action::ServeCached(cached) => {
            let resp = cached.into_l7_response();
            assert_eq!(resp.headers.get("content-encoding").unwrap(), "br");
            match resp.body {
                Body::Bytes(b) => assert_eq!(b, br_payload),
                _ => panic!("Expected Body::Bytes"),
            }
        }
        _ => panic!("Expected Action::ServeCached"),
    }

    // Lazy miss fallback when only brotli cached and client accepts only gzip
    let (_, lazy_hash) = compute_cache_key(
        &Method::GET,
        Some("api.example.com"),
        "/lazy-item",
        None,
        true,
    );
    let mut lazy_entry = CacheEntry::new(
        "GET:api.example.com:/lazy-item",
        StatusCode::OK,
        Some(HeaderValue::from_static("application/json")),
        None,
        Duration::from_secs(60),
    );
    lazy_entry.variants.brotli = Some(Bytes::from_static(b"brotli-cached-bytes-145B"));
    storage.store(lazy_hash, lazy_entry);

    let mut headers_gzip_only = HeaderMap::new();
    headers_gzip_only.insert(http::header::HOST, "api.example.com".parse().unwrap());
    headers_gzip_only.insert(http::header::ACCEPT_ENCODING, "gzip".parse().unwrap());
    let mut req_miss = L7Request::new(
        Method::GET,
        "http://api.example.com/lazy-item".parse().unwrap(),
        Version::HTTP_11,
        headers_gzip_only,
        Body::Empty,
    );
    let mut target_miss = 1;
    assert!(
        plugin
            .on_upstream_request(&mut req_miss, &mut target_miss)
            .is_continue()
    );
    assert_eq!(
        req_miss.headers.get_u64(&X_VELDA_CACHE_HASH),
        Some(lazy_hash)
    );
}

#[test]
fn test_http_cache_lookup_auth_post_and_per_user_isolation() {
    let storage = Arc::new(HttpCacheStorage::new(
        "test_lookup_auth",
        4 * 1024 * 1024,
        Duration::from_secs(60),
    ));
    let plugin = HttpCacheLookupPlugin::with_storage(
        "lookup-auth",
        HttpCacheLookupConfig::default(),
        Arc::clone(&storage),
    );

    // POST bypasses
    let mut req_post = L7Request::new(
        Method::POST,
        "http://api.example.com/products".parse().unwrap(),
        Version::HTTP_11,
        HeaderMap::new(),
        Body::Bytes(Bytes::from_static(b"{}")),
    );
    let mut t = 1;
    assert!(
        plugin
            .on_upstream_request(&mut req_post, &mut t)
            .is_continue()
    );
    assert!(req_post.headers.get_u64(&X_VELDA_CACHE_HASH).is_none());

    // Private Authorization entry bypasses without attaching hash
    let (_, priv_hash) = compute_cache_key(
        &Method::GET,
        Some("api.example.com"),
        "/profile",
        None,
        true,
    );
    storage.store(
        priv_hash,
        CacheEntry::with_raw(
            "priv",
            StatusCode::OK,
            Some(HeaderValue::from_static("application/json")),
            Bytes::from_static(b"{\"user\":\"alice\"}"),
            Duration::from_secs(60),
        ),
    );
    let mut auth_headers = HeaderMap::new();
    auth_headers.insert(http::header::HOST, "api.example.com".parse().unwrap());
    auth_headers.insert(
        http::header::AUTHORIZATION,
        "Bearer secret".parse().unwrap(),
    );
    let mut req_auth = L7Request::new(
        Method::GET,
        "http://api.example.com/profile".parse().unwrap(),
        Version::HTTP_11,
        auth_headers.clone(),
        Body::Empty,
    );
    assert!(
        plugin
            .on_upstream_request(&mut req_auth, &mut t)
            .is_continue()
    );
    assert!(req_auth.headers.get_u64(&X_VELDA_CACHE_HASH).is_none());

    // Public entry serves Authorization requests
    let (_, pub_hash) = compute_cache_key(
        &Method::GET,
        Some("api.example.com"),
        "/announcements",
        None,
        true,
    );
    storage.store(
        pub_hash,
        CacheEntry::with_raw(
            "pub",
            StatusCode::OK,
            Some(HeaderValue::from_static("application/json")),
            Bytes::from_static(b"{\"news\":\"public\"}"),
            Duration::from_secs(60),
        )
        .with_public(true),
    );
    let mut req_pub = L7Request::new(
        Method::GET,
        "http://api.example.com/announcements".parse().unwrap(),
        Version::HTTP_11,
        auth_headers,
        Body::Empty,
    );
    assert!(matches!(
        plugin.on_upstream_request(&mut req_pub, &mut t),
        Action::ServeCached(_)
    ));
}

#[test]
fn test_http_cache_lookup_from_config() {
    let config = PluginConfig {
        id: "http_cache_lookup".into(),
        config: json!({
            "zone": "api_cache",
            "cacheable_methods": ["GET", "HEAD"],
            "ignore_query_order": true,
            "honor_client_no_cache": false,
            "bypass_authenticated_requests": true,
            "key_headers": ["x-user-id", "authorization"],
            "max_memory_mb": 256,
            "default_ttl_secs": 300
        }),
    };
    let plugin = HttpCacheLookupPlugin::from_config(&config).expect("valid config");
    assert_eq!(plugin.config.zone, "api_cache");
    assert_eq!(plugin.config.cacheable_methods.len(), 2);

    let bad = PluginConfig {
        id: "http_cache_lookup".into(),
        config: json!({}),
    };
    assert!(HttpCacheLookupPlugin::from_config(&bad).is_err());
}

// -----------------------------------------------------------------------------
// 4. Post-Response Store Plugin, Validation & End-to-End Flow
// -----------------------------------------------------------------------------

#[test]
fn test_valid_and_invalid_http_cache_store_configs() {
    let valid = json!({
        "zone": "edge_cache_tier1",
        "cacheable_status": [200, 203, 204, 206, 300, 301, 404],
        "max_payload_bytes": SLAB_SIZE,
        "default_ttl_secs": 3600,
        "respect_cache_control": true,
        "bypass_authenticated_requests": true,
        "key_headers": ["accept-language"],
        "async_precompression": true,
        "max_memory_mb": 256
    });
    assert!(validate_http_cache_store_config("cache_store_1", &valid).is_ok());

    let bad_status = json!({
        "zone": "cache1",
        "cacheable_status": [199],
        "max_payload_bytes": 1024,
        "default_ttl_secs": 3600,
        "respect_cache_control": true,
        "bypass_authenticated_requests": true,
        "key_headers": [],
        "async_precompression": true,
        "max_memory_mb": 256
    });
    assert!(
        validate_http_cache_store_config("bad_status", &bad_status)
            .unwrap_err()
            .to_string()
            .contains("between 200 and 599")
    );
}

#[test]
fn test_http_cache_store_gates_and_precompression() {
    let storage = Arc::new(HttpCacheStorage::new(
        "test_store_gates",
        4 * 1024 * 1024,
        Duration::from_secs(60),
    ));
    let plugin = HttpCacheStorePlugin::with_storage(
        "store-1",
        HttpCacheStoreConfig {
            async_precompression: false,
            max_payload_bytes: 2048,
            ..Default::default()
        },
        Arc::clone(&storage),
    );

    // 1. Sync precompression via X_VELDA_CACHE_HASH numeric register
    let cache_key = "GET:api.example.com:/sync-data";
    let hash = hash_key(cache_key);
    let mut req_headers = HeaderWorkspace::default();
    req_headers.set_u64(X_VELDA_CACHE_HASH, hash);

    let mut resp_headers = HeaderMap::new();
    resp_headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());
    resp_headers.insert(CACHE_CONTROL, "public, s-maxage=3600".parse().unwrap());
    let payload = vec![b'A'; 512];
    let mut resp =
        L7Response::from_bytes(StatusCode::OK, payload.clone()).with_headers(resp_headers);
    plugin.on_response(&req_headers, &mut resp);

    assert!(!resp.headers.contains_key("x-velda-cache-key"));
    assert!(!resp.headers.contains_key("x-velda-cache-hash"));
    assert_eq!(resp.headers.get("x-cache").unwrap(), "MISS");
    assert_eq!(resp.headers.get("age").unwrap(), "0");

    let stored = storage.get(hash).expect("Must be stored");
    assert_eq!(stored.raw_body.unwrap().as_ref(), &payload[..]);
    assert!(stored.is_public);
    assert!(stored.variants.gzip.is_some());
    assert!(stored.variants.brotli.is_some());
    assert!(stored.variants.zstd.is_some());

    // 2. Oversized payload bypasses
    let big_hash = hash_key("GET:api.example.com:/too-big");
    let mut big_req = HeaderWorkspace::default();
    big_req.set_u64(X_VELDA_CACHE_HASH, big_hash);
    let mut big_resp = L7Response::from_bytes(StatusCode::OK, vec![b'X'; 4096]);
    plugin.on_response(&big_req, &mut big_resp);
    assert_eq!(big_resp.headers.get("x-cache").unwrap(), "BYPASS");
    assert!(storage.get(big_hash).is_none());

    // 3. Authenticated request without public -> BYPASS
    let auth_hash = hash_key("GET:api.example.com:/private");
    let mut auth_req = HeaderWorkspace::default();
    auth_req.set_u64(X_VELDA_CACHE_HASH, auth_hash);
    auth_req.insert(
        http::header::AUTHORIZATION,
        HeaderValue::from_static("Bearer token"),
    );
    let mut auth_resp = L7Response::from_bytes(StatusCode::OK, b"secret".to_vec());
    plugin.on_response(&auth_req, &mut auth_resp);
    assert_eq!(auth_resp.headers.get("x-cache").unwrap(), "BYPASS");
    assert!(storage.get(auth_hash).is_none());
}

#[tokio::test]
async fn test_http_cache_store_async_precompression() {
    velda_core::clock::init_clock();
    let storage = Arc::new(HttpCacheStorage::new(
        "test_async_precomp",
        4 * 1024 * 1024,
        Duration::from_secs(60),
    ));
    let plugin = HttpCacheStorePlugin::with_storage(
        "store-async",
        HttpCacheStoreConfig {
            async_precompression: true,
            ..Default::default()
        },
        Arc::clone(&storage),
    );

    let hash = hash_key("GET:api.example.com:/async-data");
    let mut req_headers = HeaderWorkspace::default();
    req_headers.set_u64(X_VELDA_CACHE_HASH, hash);

    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());
    headers.insert(CACHE_CONTROL, "public, max-age=300".parse().unwrap());
    let payload = b"{\"message\":\"testing asynchronous background precompression for high throughput\",\"items\":[1,2,3,4,5]}".repeat(5);
    let mut resp = L7Response::from_bytes(StatusCode::OK, payload.clone()).with_headers(headers);

    plugin.on_response(&req_headers, &mut resp);
    assert_eq!(resp.headers.get("x-cache").unwrap(), "MISS");
    assert_eq!(
        storage.get(hash).unwrap().raw_body.unwrap().as_ref(),
        payload.as_slice()
    );

    tokio::time::sleep(Duration::from_millis(100)).await;
    let merged = storage.get(hash).expect("Must still exist");
    assert!(merged.variants.gzip.is_some());
    assert!(merged.variants.brotli.is_some());
    assert!(merged.variants.zstd.is_some());
}

#[test]
fn test_end_to_end_http_cache_flow() {
    let storage = Arc::new(HttpCacheStorage::new(
        "e2e_storage",
        4 * 1024 * 1024,
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

    // Step 1: Request 1 (unsorted query) -> MISS, zero-alloc hash set in workspace
    let uri1: Uri = "http://api.example.com/v1/products?sort=desc&limit=10"
        .parse()
        .unwrap();
    let mut headers1 = HeaderMap::new();
    headers1.insert(http::header::HOST, "api.example.com".parse().unwrap());
    let mut req1 = L7Request::new(Method::GET, uri1, Version::HTTP_11, headers1, Body::Empty);
    let mut target = 1;
    assert!(
        lookup_plugin
            .on_upstream_request(&mut req1, &mut target)
            .is_continue()
    );
    let expected_hash = hash_key("GET:api.example.com:/v1/products?limit=10&sort=desc");
    assert_eq!(
        req1.headers.get_u64(&X_VELDA_CACHE_HASH),
        Some(expected_hash)
    );

    // Step 2: Upstream response with Set-Cookie -> stored without Set-Cookie
    let mut resp_headers = HeaderMap::new();
    resp_headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());
    resp_headers.insert(
        SET_COOKIE,
        "session_id=user1_secret; Path=/; HttpOnly".parse().unwrap(),
    );
    resp_headers.insert(CACHE_CONTROL, "public, max-age=300".parse().unwrap());
    let mut resp1 = L7Response::from_bytes(StatusCode::OK, b"{\"items\":[1,2,3]}".to_vec())
        .with_headers(resp_headers);
    store_plugin.on_response(&req1.headers, &mut resp1);
    assert_eq!(resp1.headers.get("x-cache").unwrap(), "MISS");
    assert_eq!(resp1.headers.get("age").unwrap(), "0");

    // Step 3: Request 2 (sorted query) -> HIT via SlabLease zero-copy slice
    let uri2: Uri = "http://api.example.com/v1/products?limit=10&sort=desc"
        .parse()
        .unwrap();
    let mut headers2 = HeaderMap::new();
    headers2.insert(http::header::HOST, "api.example.com".parse().unwrap());
    let mut req2 = L7Request::new(Method::GET, uri2, Version::HTTP_11, headers2, Body::Empty);
    match lookup_plugin.on_upstream_request(&mut req2, &mut target) {
        Action::ServeCached(cached) => {
            let cached_resp = cached.into_l7_response();
            assert_eq!(cached_resp.status, StatusCode::OK);
            assert_eq!(cached_resp.headers.get("x-cache").unwrap(), "HIT");
            assert_eq!(
                cached_resp.headers.get(CONTENT_TYPE).unwrap(),
                "application/json"
            );
            assert!(!cached_resp.headers.contains_key(SET_COOKIE));
            match cached_resp.body {
                Body::Bytes(b) => assert_eq!(b.as_ref(), b"{\"items\":[1,2,3]}"),
                _ => panic!("Expected cached Body::Bytes"),
            }
        }
        other => panic!("Expected ServeCached, got {other:?}"),
    }
}
