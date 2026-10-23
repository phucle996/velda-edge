//! HTTP Cache Lookup Pre-Upstream Plugin (`http_cache_lookup`).
//!
//! Evaluated during the `pre_upstream` phase immediately before forwarding requests to backends.
//! Computes canonical cache keys, queries the shared [`HttpCacheZone`], and returns an
//! immediate [`Action::Respond`] with zero-copy payload on CACHE HIT.
//! On CACHE MISS, enriches the request with cache key metadata and yields [`Action::Continue`].

use std::sync::Arc;
use std::time::Duration;

use http::header::HeaderValue;
use http::{HeaderMap, Method, Version};
use velda_core::{Action, Body, L7Request, L7Response};
use velda_sync::post_sync::plugin::PluginConfig;

use crate::header::X_REQUEST_ID;
use crate::http_cache::constants::{X_CACHE, X_VELDA_CACHE_HASH, X_VELDA_CACHE_KEY};

use crate::capability::{AccessLevel, PluginCapability, SupportedProtocols};
use crate::error::PluginError;
use crate::http_cache::{HttpCacheZone, HttpCacheZoneRegistry};
use crate::pre_upstream::PreUpstreamPlugin;

/// Configuration schema for [`HttpCacheLookupPlugin`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpCacheLookupConfig {
    /// Shared cache zone identifier.
    pub zone: String,
    /// HTTP methods eligible for cache lookup (default: GET, HEAD).
    pub cacheable_methods: Vec<Method>,
    /// Whether to sort query string keys alphabetically before hashing.
    pub ignore_query_order: bool,
    /// Honor client `Cache-Control: no-cache` / `Pragma: no-cache` directives (default: false).
    pub honor_client_no_cache: bool,
    /// Whether to bypass cache lookup on requests carrying an `Authorization` header (RFC 9111 §3.5).
    /// Default: true (preventing shared cache data leaks).
    pub bypass_authenticated_requests: bool,
    /// Custom request header names to incorporate into the cache key for per-user / segmented caching.
    /// (e.g. `["x-user-id", "authorization"]`). Names are normalized to lowercase.
    pub key_headers: Vec<String>,
    /// Allocated memory limit for this zone if created here (in megabytes).
    pub max_memory_mb: usize,
    /// Default TTL fallback in seconds if response does not specify Cache-Control.
    pub default_ttl_secs: u64,
}

impl Default for HttpCacheLookupConfig {
    fn default() -> Self {
        Self {
            zone: "default".to_string(),
            cacheable_methods: vec![Method::GET, Method::HEAD],
            ignore_query_order: true,
            honor_client_no_cache: false,
            bypass_authenticated_requests: true,
            key_headers: Vec::new(),
            max_memory_mb: 256,
            default_ttl_secs: 300,
        }
    }
}

/// HTTP Cache Lookup Plugin instance.
pub struct HttpCacheLookupPlugin {
    pub id: String,
    pub config: HttpCacheLookupConfig,
    pub zone: Arc<HttpCacheZone>,
}

impl std::fmt::Debug for HttpCacheLookupPlugin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpCacheLookupPlugin")
            .field("id", &self.id)
            .field("config", &self.config)
            .field("zone", &self.zone.name)
            .finish()
    }
}

impl HttpCacheLookupPlugin {
    /// Constructs a new `HttpCacheLookupPlugin` with explicit configuration.
    pub fn new(id: impl Into<String>, config: HttpCacheLookupConfig) -> Self {
        let max_bytes = config.max_memory_mb.max(1) * 1024 * 1024;
        let default_ttl = Duration::from_secs(config.default_ttl_secs.max(1));
        let zone =
            HttpCacheZoneRegistry::global().get_or_create(&config.zone, max_bytes, default_ttl);

        Self {
            id: id.into(),
            config,
            zone,
        }
    }

    /// Constructs an instance binding to an existing zone (convenient for testing).
    pub fn with_zone(
        id: impl Into<String>,
        config: HttpCacheLookupConfig,
        zone: Arc<HttpCacheZone>,
    ) -> Self {
        Self {
            id: id.into(),
            config,
            zone,
        }
    }

    /// Deserializes and constructs an instance directly from a [`PluginConfig`].
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let cfg = velda_sync::validate_http_cache_lookup_config(&config.id, &config.config)
            .map_err(|e| {
                PluginError::Config(format!(
                    "Invalid http_cache_lookup configuration for '{}': {e}",
                    config.id
                ))
            })?;

        let mut cacheable_methods = Vec::with_capacity(cfg.cacheable_methods.len());
        for m_str in &cfg.cacheable_methods {
            let method = Method::from_bytes(m_str.trim().as_bytes()).map_err(|e| {
                PluginError::Config(format!(
                    "Invalid cacheable method '{m_str}' in plugin '{}': {e}",
                    config.id
                ))
            })?;
            cacheable_methods.push(method);
        }

        let parsed_cfg = HttpCacheLookupConfig {
            zone: cfg.zone,
            cacheable_methods,
            ignore_query_order: cfg.ignore_query_order,
            honor_client_no_cache: cfg.honor_client_no_cache,
            bypass_authenticated_requests: cfg.bypass_authenticated_requests,
            key_headers: cfg.key_headers,
            max_memory_mb: cfg.max_memory_mb,
            default_ttl_secs: cfg.default_ttl_secs,
        };

        Ok(Self::new(&config.id, parsed_cfg))
    }
}

impl PreUpstreamPlugin for HttpCacheLookupPlugin {
    #[inline(always)]
    fn id(&self) -> &str {
        &self.id
    }

    fn on_upstream_request(&self, req: &mut L7Request, _target_upstream: &mut Arc<str>) -> Action {
        // 1. Method check: only cacheable methods (GET, HEAD) are evaluated
        if !self.config.cacheable_methods.contains(&req.method) {
            return Action::Continue;
        }

        // 2. Client Cache-Control bypass check
        if self.config.honor_client_no_cache {
            let has_no_cache = req
                .headers
                .get(http::header::CACHE_CONTROL)
                .and_then(|v| v.to_str().ok())
                .map(|s| s.contains("no-cache") || s.contains("no-store"))
                .unwrap_or(false)
                || req
                    .headers
                    .get(http::header::PRAGMA)
                    .and_then(|v| v.to_str().ok())
                    .map(|s| s.contains("no-cache"))
                    .unwrap_or(false);

            if has_no_cache {
                return Action::Continue;
            }
        }

        // 3. RFC 9111 §3.5 Authentication check (Safety Shield) setup
        let has_auth = req.headers.contains_key(http::header::AUTHORIZATION);
        let auth_in_key = self
            .config
            .key_headers
            .iter()
            .any(|h| h.eq_ignore_ascii_case("authorization"));

        // 4. Extract custom key headers if configured
        let extra_headers: Option<Vec<(&str, &str)>> = if !self.config.key_headers.is_empty() {
            let mut extracted = Vec::with_capacity(self.config.key_headers.len());
            for hdr_name in &self.config.key_headers {
                if let Some(val) = req.headers.get(hdr_name).and_then(|v| v.to_str().ok()) {
                    extracted.push((hdr_name.as_str(), val));
                }
            }
            Some(extracted)
        } else {
            None
        };

        // 5. Compute canonical cache key
        let host = req
            .headers
            .get(http::header::HOST)
            .and_then(|v| v.to_str().ok());
        let (key_str, key_hash) = HttpCacheZone::compute_cache_key_with_headers(
            &req.method,
            host,
            req.uri.path(),
            req.uri.query(),
            self.config.ignore_query_order,
            extra_headers.as_deref(),
        );

        // 6. Query the in-memory cache zone
        if let Some(entry) = self.zone.get(key_hash) {
            // RFC 9111 §3.5: If request has Authorization and is not partitioned via key_headers,
            // a shared cache MUST NOT serve the cached entry unless explicitly marked public.
            if has_auth
                && self.config.bypass_authenticated_requests
                && !auth_in_key
                && !entry.is_public
            {
                return Action::Continue;
            }

            let accept_encoding = req.headers.get(http::header::ACCEPT_ENCODING);
            if let Some((payload, content_encoding)) =
                entry.negotiate_smart_variant(accept_encoding)
            {
                // CACHE HIT: Construct response directly from in-memory zero-copy payload.
                // Pure Payload Model: Headers are NEVER restored from cache.
                // Only set Content-Type, Content-Length, Age, X-Cache, Content-Encoding, Vary.
                let mut resp_headers = HeaderMap::with_capacity(6);
                if let Some(ref ct) = entry.content_type {
                    resp_headers.insert(http::header::CONTENT_TYPE, ct.clone());
                }
                resp_headers.insert(
                    http::header::CONTENT_LENGTH,
                    HeaderValue::from(payload.len()),
                );
                resp_headers.insert("age", HeaderValue::from(entry.age_secs()));
                resp_headers.insert(X_CACHE, HeaderValue::from_static("HIT"));

                if let Some(enc) = content_encoding {
                    resp_headers.insert(http::header::CONTENT_ENCODING, enc);
                    resp_headers.insert(
                        http::header::VARY,
                        HeaderValue::from_static("Accept-Encoding"),
                    );
                } else if entry.variants.gzip.is_some()
                    || entry.variants.brotli.is_some()
                    || entry.variants.zstd.is_some()
                {
                    resp_headers.insert(
                        http::header::VARY,
                        HeaderValue::from_static("Accept-Encoding"),
                    );
                }

                let resp = L7Response::new(
                    entry.status,
                    Version::HTTP_11,
                    resp_headers,
                    Body::Bytes(payload),
                );

                return Action::Respond(resp);
            }
            // If entry lacks a compatible variant for this client (Lazy Miss),
            // cleanly fall through to Step 7 to fetch the missing variant from upstream!
        }

        // 7. CACHE MISS: Enrich request headers with transient key metadata for post-response store
        if let Ok(val) = HeaderValue::from_str(&key_str) {
            req.headers.insert(X_VELDA_CACHE_KEY, val);
        }
        req.headers
            .insert(X_VELDA_CACHE_HASH, HeaderValue::from(key_hash));

        // If request ID is present, associate it in the zone's pending table
        if let Some(req_id) = req.headers.get(&X_REQUEST_ID).and_then(|v| v.to_str().ok()) {
            self.zone.record_pending_key(req_id, key_str);
        }

        Action::Continue
    }

    fn capability(&self) -> PluginCapability {
        // Requires write permission on headers to inject transient x-velda-cache-key
        PluginCapability::header_only(AccessLevel::Write)
    }

    fn supported_protocols(&self) -> SupportedProtocols {
        SupportedProtocols::ALL_HTTP
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http_cache::CacheEntry;
    use http::{HeaderMap, StatusCode, Uri};

    #[test]
    fn test_http_cache_lookup_miss_and_hit() {
        let zone = Arc::new(HttpCacheZone::new(
            "test_lookup",
            1024 * 1024,
            Duration::from_secs(60),
        ));
        let plugin = HttpCacheLookupPlugin::with_zone(
            "lookup-1",
            HttpCacheLookupConfig::default(),
            Arc::clone(&zone),
        );

        let uri: Uri = "http://api.example.com/products?sort=asc&page=1"
            .parse()
            .unwrap();
        let mut headers = HeaderMap::new();
        headers.insert(http::header::HOST, "api.example.com".parse().unwrap());
        let mut req = L7Request::new(
            Method::GET,
            uri.clone(),
            Version::HTTP_11,
            headers,
            Body::Empty,
        );

        // 1. Initial request: MISS -> Action::Continue and injected x-velda-cache-key
        let mut target1 = Arc::from("default-upstream");
        let action = plugin.on_upstream_request(&mut req, &mut target1);
        assert!(action.is_continue());
        assert!(req.headers.contains_key("x-velda-cache-key"));
        assert_eq!(
            req.headers.get("x-velda-cache-key").unwrap(),
            "GET:api.example.com:/products?page=1&sort=asc"
        );

        // 2. Seed cache entry into the zone
        let (_, hash) = HttpCacheZone::compute_cache_key(
            &Method::GET,
            Some("api.example.com"),
            "/products",
            Some("sort=asc&page=1"),
            true,
        );
        zone.store(
            hash,
            CacheEntry::with_raw(
                "GET:api.example.com:/products?page=1&sort=asc",
                StatusCode::OK,
                Some(http::HeaderValue::from_static("application/json")),
                bytes::Bytes::from_static(b"[{\"id\":1}]"),
                Duration::from_secs(60),
            ),
        );

        // 3. Second request (even with reversed query order): HIT -> Action::Respond
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

        let mut target2 = Arc::from("default-upstream");
        let action2 = plugin.on_upstream_request(&mut req2, &mut target2);
        match action2 {
            Action::Respond(resp) => {
                assert_eq!(resp.status, StatusCode::OK);
                assert_eq!(resp.headers.get("x-cache").unwrap(), "HIT");
                assert_eq!(resp.headers.get("age").unwrap(), "0");
                match resp.body {
                    Body::Bytes(b) => assert_eq!(b, bytes::Bytes::from_static(b"[{\"id\":1}]")),
                    _ => panic!("Expected Body::Bytes"),
                }
            }
            _ => panic!("Expected Action::Respond"),
        }
    }

    #[test]
    fn test_http_cache_lookup_negotiates_precompressed_variant() {
        use crate::http_cache::CompressedVariants;

        let zone = Arc::new(HttpCacheZone::new(
            "test_variants",
            1024 * 1024,
            Duration::from_secs(60),
        ));
        let plugin = HttpCacheLookupPlugin::with_zone(
            "lookup-var",
            HttpCacheLookupConfig::default(),
            Arc::clone(&zone),
        );

        let (_, hash) = HttpCacheZone::compute_cache_key(
            &Method::GET,
            Some("api.example.com"),
            "/large-data",
            None,
            true,
        );
        let raw_payload =
            bytes::Bytes::from_static(b"lots of repetitive data for compression test");
        let gzip_payload = bytes::Bytes::from_static(b"gzip_compressed_bytes");
        let br_payload = bytes::Bytes::from_static(b"br_compressed_bytes");

        let entry = CacheEntry::with_raw(
            "GET:api.example.com:/large-data",
            StatusCode::OK,
            Some(http::HeaderValue::from_static("application/json")),
            raw_payload.clone(),
            Duration::from_secs(60),
        )
        .with_variants(CompressedVariants {
            gzip: Some(gzip_payload.clone()),
            brotli: Some(br_payload.clone()),
            zstd: None,
        });

        zone.store(hash, entry);

        // 1. Request with Accept-Encoding: gzip
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
        let mut target = Arc::from("default-upstream");
        let action = plugin.on_upstream_request(&mut req, &mut target);
        match action {
            Action::Respond(resp) => {
                assert_eq!(resp.status, StatusCode::OK);
                assert_eq!(resp.headers.get("x-cache").unwrap(), "HIT");
                assert_eq!(resp.headers.get("content-encoding").unwrap(), "gzip");
                assert_eq!(resp.headers.get("vary").unwrap(), "Accept-Encoding");
                assert_eq!(
                    resp.headers.get("content-length").unwrap(),
                    gzip_payload.len().to_string().as_str()
                );
                match resp.body {
                    Body::Bytes(b) => assert_eq!(b, gzip_payload),
                    _ => panic!("Expected Body::Bytes"),
                }
            }
            _ => panic!("Expected Action::Respond"),
        }

        // 2. Request with Accept-Encoding: br
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
        let mut target_br = Arc::from("default-upstream");
        let action_br = plugin.on_upstream_request(&mut req_br, &mut target_br);
        match action_br {
            Action::Respond(resp) => {
                assert_eq!(resp.headers.get("content-encoding").unwrap(), "br");
                match resp.body {
                    Body::Bytes(b) => assert_eq!(b, br_payload),
                    _ => panic!("Expected Body::Bytes"),
                }
            }
            _ => panic!("Expected Action::Respond"),
        }

        // 3. Request without Accept-Encoding: serves uncompressed raw body
        let mut headers_raw = HeaderMap::new();
        headers_raw.insert(http::header::HOST, "api.example.com".parse().unwrap());
        let mut req_raw = L7Request::new(
            Method::GET,
            "http://api.example.com/large-data".parse().unwrap(),
            Version::HTTP_11,
            headers_raw,
            Body::Empty,
        );
        let mut target_raw = Arc::from("default-upstream");
        let action_raw = plugin.on_upstream_request(&mut req_raw, &mut target_raw);
        match action_raw {
            Action::Respond(resp) => {
                assert!(resp.headers.get("content-encoding").is_none());
                assert_eq!(resp.headers.get("vary").unwrap(), "Accept-Encoding");
                match resp.body {
                    Body::Bytes(b) => assert_eq!(b, raw_payload),
                    _ => panic!("Expected Body::Bytes"),
                }
            }
            _ => panic!("Expected Action::Respond"),
        }
    }

    #[test]
    fn test_http_cache_lookup_lazy_smart_matching() {
        let zone = Arc::new(HttpCacheZone::new(
            "test_lazy_lookup",
            1024 * 1024,
            Duration::from_secs(60),
        ));
        let plugin = HttpCacheLookupPlugin::with_zone(
            "lookup-lazy",
            HttpCacheLookupConfig::default(),
            Arc::clone(&zone),
        );

        let (_, hash) = HttpCacheZone::compute_cache_key(
            &Method::GET,
            Some("api.example.com"),
            "/lazy-item",
            None,
            true,
        );

        let br_payload = bytes::Bytes::from_static(b"brotli-cached-bytes-145B");

        // Lazy entry: NO raw_body, only Brotli variant
        let mut entry = CacheEntry::new(
            "GET:api.example.com:/lazy-item",
            StatusCode::OK,
            Some(http::HeaderValue::from_static("application/json")),
            None,
            Duration::from_secs(60),
        );
        entry.variants.brotli = Some(br_payload.clone());
        zone.store(hash, entry);

        // 1. Client sends Accept-Encoding: gzip, deflate, br, zstd
        // Smart Variant Matching: client accepts Brotli and Brotli is in cache -> HIT!
        let mut headers1 = HeaderMap::new();
        headers1.insert(http::header::HOST, "api.example.com".parse().unwrap());
        headers1.insert(
            http::header::ACCEPT_ENCODING,
            "gzip, deflate, br, zstd".parse().unwrap(),
        );
        let mut req1 = L7Request::new(
            Method::GET,
            "http://api.example.com/lazy-item".parse().unwrap(),
            Version::HTTP_11,
            headers1,
            Body::Empty,
        );
        let mut target1 = Arc::from("default-upstream");
        let action1 = plugin.on_upstream_request(&mut req1, &mut target1);
        match action1 {
            Action::Respond(resp) => {
                assert_eq!(resp.status, StatusCode::OK);
                assert_eq!(resp.headers.get("x-cache").unwrap(), "HIT");
                assert_eq!(resp.headers.get("content-encoding").unwrap(), "br");
                assert_eq!(resp.headers.get("vary").unwrap(), "Accept-Encoding");
                match resp.body {
                    Body::Bytes(b) => assert_eq!(b, br_payload),
                    _ => panic!("Expected Body::Bytes"),
                }
            }
            _ => panic!("Expected Action::Respond"),
        }

        // 2. Client sends ONLY Accept-Encoding: gzip
        // Entry lacks Gzip and raw_body is None -> Lazy Miss -> Action::Continue!
        let mut headers2 = HeaderMap::new();
        headers2.insert(http::header::HOST, "api.example.com".parse().unwrap());
        headers2.insert(http::header::ACCEPT_ENCODING, "gzip".parse().unwrap());
        let mut req2 = L7Request::new(
            Method::GET,
            "http://api.example.com/lazy-item".parse().unwrap(),
            Version::HTTP_11,
            headers2,
            Body::Empty,
        );
        let mut target2 = Arc::from("default-upstream");
        let action2 = plugin.on_upstream_request(&mut req2, &mut target2);
        assert!(action2.is_continue());
        assert!(req2.headers.contains_key("x-velda-cache-key"));
    }

    #[test]
    fn test_http_cache_lookup_post_method_bypasses() {
        let zone = Arc::new(HttpCacheZone::new(
            "test_post",
            1024 * 1024,
            Duration::from_secs(60),
        ));
        let plugin =
            HttpCacheLookupPlugin::with_zone("lookup-post", HttpCacheLookupConfig::default(), zone);

        let uri: Uri = "http://api.example.com/products".parse().unwrap();
        let headers = HeaderMap::new();
        let mut req = L7Request::new(
            Method::POST,
            uri,
            Version::HTTP_11,
            headers,
            Body::Bytes(bytes::Bytes::from_static(b"{}")),
        );

        let mut target = Arc::from("default-upstream");
        let action = plugin.on_upstream_request(&mut req, &mut target);
        assert!(action.is_continue());
        assert!(!req.headers.contains_key("x-velda-cache-key"));
    }

    #[test]
    fn test_http_cache_lookup_authorization_bypasses_by_default() {
        let zone = Arc::new(HttpCacheZone::new(
            "test_auth_bypass",
            1024 * 1024,
            Duration::from_secs(60),
        ));
        let plugin = HttpCacheLookupPlugin::with_zone(
            "lookup-auth",
            HttpCacheLookupConfig::default(),
            Arc::clone(&zone),
        );

        let (_, hash) = HttpCacheZone::compute_cache_key(
            &Method::GET,
            Some("api.example.com"),
            "/profile",
            None,
            true,
        );
        let entry = CacheEntry::with_raw(
            "GET:api.example.com:/profile",
            StatusCode::OK,
            Some(HeaderValue::from_static("application/json")),
            bytes::Bytes::from_static(b"{\"user\":\"alice\"}"),
            Duration::from_secs(60),
        );
        zone.store(hash, entry);

        // Request has Authorization header: must bypass cache lookup to prevent data leak!
        let mut headers = HeaderMap::new();
        headers.insert(http::header::HOST, "api.example.com".parse().unwrap());
        headers.insert(
            http::header::AUTHORIZATION,
            "Bearer secret_token_123".parse().unwrap(),
        );
        let mut req = L7Request::new(
            Method::GET,
            "http://api.example.com/profile".parse().unwrap(),
            Version::HTTP_11,
            headers,
            Body::Empty,
        );

        let mut target = Arc::from("default-upstream");
        let action = plugin.on_upstream_request(&mut req, &mut target);
        assert!(action.is_continue());
        // Transient cache key is NOT injected because it bypassed
        assert!(!req.headers.contains_key("x-velda-cache-key"));
    }

    #[test]
    fn test_http_cache_lookup_authorization_serves_public_entry() {
        let zone = Arc::new(HttpCacheZone::new(
            "test_auth_public",
            1024 * 1024,
            Duration::from_secs(60),
        ));
        let plugin = HttpCacheLookupPlugin::with_zone(
            "lookup-auth-pub",
            HttpCacheLookupConfig::default(),
            Arc::clone(&zone),
        );

        let (_, hash) = HttpCacheZone::compute_cache_key(
            &Method::GET,
            Some("api.example.com"),
            "/announcements",
            None,
            true,
        );
        // Entry explicitly marked public according to RFC 9111 §3.5
        let entry = CacheEntry::with_raw(
            "GET:api.example.com:/announcements",
            StatusCode::OK,
            Some(HeaderValue::from_static("application/json")),
            bytes::Bytes::from_static(b"{\"news\":\"public_announcement\"}"),
            Duration::from_secs(60),
        )
        .with_public(true);
        zone.store(hash, entry);

        // Request has Authorization header, but entry is explicitly public -> Safe to HIT!
        let mut headers = HeaderMap::new();
        headers.insert(http::header::HOST, "api.example.com".parse().unwrap());
        headers.insert(
            http::header::AUTHORIZATION,
            "Bearer user_token".parse().unwrap(),
        );
        let mut req = L7Request::new(
            Method::GET,
            "http://api.example.com/announcements".parse().unwrap(),
            Version::HTTP_11,
            headers,
            Body::Empty,
        );

        let mut target = Arc::from("default-upstream");
        let action = plugin.on_upstream_request(&mut req, &mut target);
        match action {
            Action::Respond(resp) => {
                assert_eq!(resp.status, StatusCode::OK);
                assert_eq!(resp.headers.get("x-cache").unwrap(), "HIT");
            }
            _ => panic!("Expected Action::Respond for public entry"),
        }
    }

    #[test]
    fn test_http_cache_lookup_custom_key_headers_per_user() {
        let zone = Arc::new(HttpCacheZone::new(
            "test_per_user",
            1024 * 1024,
            Duration::from_secs(60),
        ));
        let config = HttpCacheLookupConfig {
            key_headers: vec!["authorization".to_string()],
            ..Default::default()
        };
        let plugin = HttpCacheLookupPlugin::with_zone("lookup-per-user", config, Arc::clone(&zone));

        // Alice stores her own cached entry
        let (alice_key, alice_hash) = HttpCacheZone::compute_cache_key_with_headers(
            &Method::GET,
            Some("api.example.com"),
            "/profile",
            None,
            true,
            Some(&[("authorization", "Bearer alice")]),
        );
        let alice_entry = CacheEntry::with_raw(
            alice_key,
            StatusCode::OK,
            Some(HeaderValue::from_static("application/json")),
            bytes::Bytes::from_static(b"{\"balance\":1000}"),
            Duration::from_secs(60),
        );
        zone.store(alice_hash, alice_entry);

        // 1. Bob sends request with Bearer bob -> Must MISS (different cache key)
        let mut bob_headers = HeaderMap::new();
        bob_headers.insert(http::header::HOST, "api.example.com".parse().unwrap());
        bob_headers.insert(http::header::AUTHORIZATION, "Bearer bob".parse().unwrap());
        let mut req_bob = L7Request::new(
            Method::GET,
            "http://api.example.com/profile".parse().unwrap(),
            Version::HTTP_11,
            bob_headers,
            Body::Empty,
        );
        let mut target_bob = Arc::from("default-upstream");
        let action_bob = plugin.on_upstream_request(&mut req_bob, &mut target_bob);
        assert!(action_bob.is_continue());

        // 2. Alice sends request with Bearer alice -> Must HIT!
        let mut alice_headers = HeaderMap::new();
        alice_headers.insert(http::header::HOST, "api.example.com".parse().unwrap());
        alice_headers.insert(http::header::AUTHORIZATION, "Bearer alice".parse().unwrap());
        let mut req_alice = L7Request::new(
            Method::GET,
            "http://api.example.com/profile".parse().unwrap(),
            Version::HTTP_11,
            alice_headers,
            Body::Empty,
        );
        let mut target_alice = Arc::from("default-upstream");
        let action_alice = plugin.on_upstream_request(&mut req_alice, &mut target_alice);
        match action_alice {
            Action::Respond(resp) => {
                assert_eq!(resp.status, StatusCode::OK);
                assert_eq!(resp.headers.get("x-cache").unwrap(), "HIT");
                match resp.body {
                    Body::Bytes(b) => {
                        assert_eq!(b, bytes::Bytes::from_static(b"{\"balance\":1000}"))
                    }
                    _ => panic!("Expected Body::Bytes"),
                }
            }
            _ => panic!("Expected Action::Respond for Alice"),
        }
    }

    #[test]
    fn test_from_config_complete() {
        let json_cfg = serde_json::json!({
            "zone": "api_cache",
            "cacheable_methods": ["GET", "HEAD"],
            "ignore_query_order": true,
            "honor_client_no_cache": false,
            "bypass_authenticated_requests": true,
            "key_headers": ["x-user-id", "authorization"],
            "max_memory_mb": 256,
            "default_ttl_secs": 300
        });
        let config = PluginConfig {
            id: "http_cache_lookup".into(),
            phase: String::new(),
            config: json_cfg,
        };
        let plugin = HttpCacheLookupPlugin::from_config(&config).expect("valid config");
        assert_eq!(plugin.id, "http_cache_lookup");
        assert_eq!(plugin.config.zone, "api_cache");
        assert_eq!(plugin.config.cacheable_methods.len(), 2);
        assert!(plugin.config.ignore_query_order);
        assert!(!plugin.config.honor_client_no_cache);
        assert!(plugin.config.bypass_authenticated_requests);
        assert_eq!(plugin.config.key_headers.len(), 2);
        assert_eq!(plugin.config.max_memory_mb, 256);
        assert_eq!(plugin.config.default_ttl_secs, 300);

        // Incomplete config must fail validation
        let bad_config = PluginConfig {
            id: "http_cache_lookup".into(),
            phase: String::new(),
            config: serde_json::json!({}),
        };
        assert!(HttpCacheLookupPlugin::from_config(&bad_config).is_err());
    }
}
