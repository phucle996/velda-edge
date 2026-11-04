//! HTTP Cache Store Post-Response Plugin (`http_cache_store`).
//!
//! Evaluated on outgoing responses returned from Upstream backends during the `post_response` phase.
//! Determines cacheability based on HTTP status, payload size, and `Cache-Control` directives.
//! Stores valid responses into the shared [`HttpCacheStorage`] and injects `X-Cache` diagnostic headers.

use std::sync::Arc;
use std::time::Duration;

use http::StatusCode;
use http::header::HeaderValue;
use velda_core::L7Response;
use velda_sync::post_sync::plugin::PluginConfig;

use crate::http_cache::constants::{X_CACHE, X_VELDA_CACHE_HASH, X_VELDA_CACHE_KEY};

use crate::capability::{AccessLevel, PluginCapability, SupportedProtocols};
use crate::error::PluginError;
use crate::http_cache::{
    CompressedVariants, HttpCacheRegistry, HttpCacheStorage, StoreVariantParams,
    create_dram_confined_bytes, parse_cache_control_fast,
};
use crate::post_response::PostResponsePlugin;

/// Configuration schema for [`HttpCacheStorePlugin`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpCacheStoreConfig {
    /// Shared cache storage identifier (must match `http_cache_lookup`).
    pub zone: String,
    /// HTTP status codes eligible for caching (default: 200, 203, 204, 206, 300, 301, 404).
    pub cacheable_status: Vec<StatusCode>,
    /// Maximum payload size in bytes eligible for caching (default: 2MB).
    pub max_payload_bytes: usize,
    /// Default TTL fallback in seconds when upstream does not emit Cache-Control max-age.
    pub default_ttl_secs: u64,
    /// Honor upstream `Cache-Control: no-store` / `Cache-Control: private` (default: true).
    pub respect_cache_control: bool,
    /// Whether to bypass storing responses for requests carrying an `Authorization` header (RFC 9111 §3.5),
    /// unless the response is explicitly marked `public` or `s-maxage`, or partitioned via `key_headers`.
    pub bypass_authenticated_requests: bool,
    /// Custom request header names incorporated into the cache key for per-user / segmented caching.
    pub key_headers: Vec<String>,
    /// Whether to offload variant pre-compression (gzip, brotli, zstd) to a background task (default: true).
    /// When true, responses are served immediately to clients with 0ms wait on Cache Miss,
    /// while variants are compressed asynchronously in the background and merged into cache.
    pub async_precompression: bool,
    /// Allocated memory limit for this storage if created here (in megabytes).
    pub max_memory_mb: usize,
}

impl Default for HttpCacheStoreConfig {
    fn default() -> Self {
        Self {
            zone: "default".to_string(),
            cacheable_status: vec![
                StatusCode::OK,
                StatusCode::NON_AUTHORITATIVE_INFORMATION,
                StatusCode::NO_CONTENT,
                StatusCode::PARTIAL_CONTENT,
                StatusCode::MULTIPLE_CHOICES,
                StatusCode::MOVED_PERMANENTLY,
                StatusCode::NOT_FOUND,
            ],
            max_payload_bytes: 2 * 1024 * 1024,
            default_ttl_secs: 300,
            respect_cache_control: true,
            bypass_authenticated_requests: true,
            key_headers: Vec::new(),
            async_precompression: true,
            max_memory_mb: 256,
        }
    }
}

/// HTTP Cache Store Plugin instance.
pub struct HttpCacheStorePlugin {
    pub id: String,
    pub config: HttpCacheStoreConfig,
    pub storage: Arc<HttpCacheStorage>,
}

impl std::fmt::Debug for HttpCacheStorePlugin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpCacheStorePlugin")
            .field("id", &self.id)
            .field("config", &self.config)
            .field("storage", &self.storage.name)
            .finish()
    }
}

impl HttpCacheStorePlugin {
    /// Constructs a new `HttpCacheStorePlugin` with explicit configuration.
    pub fn new(id: impl Into<String>, config: HttpCacheStoreConfig) -> Self {
        let max_bytes = config.max_memory_mb.max(1) * 1024 * 1024;
        let default_ttl = Duration::from_secs(config.default_ttl_secs.max(1));
        let storage =
            HttpCacheRegistry::global().get_or_create(&config.zone, max_bytes, default_ttl);

        Self {
            id: id.into(),
            config,
            storage,
        }
    }

    /// Constructs an instance binding to an existing storage (convenient for testing).
    pub fn with_storage(
        id: impl Into<String>,
        config: HttpCacheStoreConfig,
        storage: Arc<HttpCacheStorage>,
    ) -> Self {
        Self {
            id: id.into(),
            config,
            storage,
        }
    }

    /// Deserializes and constructs an instance directly from a [`PluginConfig`].
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let cfg = velda_sync::validate_http_cache_store_config(&config.id, &config.config)
            .map_err(|e| {
                PluginError::Config(format!(
                    "Invalid http_cache_store configuration for '{}': {e}",
                    config.id
                ))
            })?;

        let mut cacheable_status = Vec::with_capacity(cfg.cacheable_status.len());
        for code in cfg.cacheable_status {
            let status = StatusCode::from_u16(code).map_err(|e| {
                PluginError::Config(format!(
                    "Invalid status code {code} in plugin '{}': {e}",
                    config.id
                ))
            })?;
            cacheable_status.push(status);
        }

        Ok(Self::new(
            &config.id,
            HttpCacheStoreConfig {
                zone: cfg.zone,
                cacheable_status,
                max_payload_bytes: cfg.max_payload_bytes,
                default_ttl_secs: cfg.default_ttl_secs,
                respect_cache_control: cfg.respect_cache_control,
                bypass_authenticated_requests: cfg.bypass_authenticated_requests,
                key_headers: cfg.key_headers,
                async_precompression: cfg.async_precompression,
                max_memory_mb: cfg.max_memory_mb,
            },
        ))
    }
}

#[inline]
fn is_compressible_mime(content_type: Option<&HeaderValue>) -> bool {
    let Some(ct_val) = content_type else {
        return false;
    };
    let Ok(ct_str) = ct_val.to_str() else {
        return false;
    };
    let ct = ct_str.split(';').next().unwrap_or("").trim();
    ct.starts_with("text/")
        || ct.starts_with("application/json")
        || ct.starts_with("application/javascript")
        || ct.starts_with("application/xml")
        || ct.starts_with("image/svg+xml")
        || ct.starts_with("application/wasm")
}

#[inline]
fn precompress_variants(body: &bytes::Bytes) -> CompressedVariants {
    use crate::post_response::response_compression::algorithm::{
        encode_brotli, encode_gzip, encode_zstd,
    };

    // Pareto Level Tuning:
    // - Gzip: Level 1 (SIMD vectorized via zlib-rs, ~3x faster than L6 with ~95% ratio)
    // - Brotli: Level 2 (~4x faster than L4, sub-millisecond JSON compression)
    // - Zstd: Level 1 (ultra-fast line-rate throughput ~800MB/s)
    let gzip = encode_gzip(body, 1).ok().filter(|c| c.len() < body.len());

    let brotli = encode_brotli(body, 2).ok().filter(|c| c.len() < body.len());

    let zstd = encode_zstd(body, 1).ok().filter(|c| c.len() < body.len());

    CompressedVariants { gzip, brotli, zstd }
}

impl PostResponsePlugin for HttpCacheStorePlugin {
    #[inline(always)]
    fn id(&self) -> &str {
        &self.id
    }

    fn on_response(&self, req_headers: &velda_core::HeaderWorkspace, resp: &mut L7Response) {
        // 1. If already served from cache (HIT), skip storing
        if let Some(val) = resp.headers.get(&X_CACHE)
            && val == "HIT"
        {
            return;
        }

        // 2. Extract cache key: prioritize HeaderWorkspace headers map,
        // fallback to resp.headers (for unit test mock pipelines).
        let key_str = req_headers
            .get(&X_VELDA_CACHE_KEY)
            .and_then(|v| v.to_str().ok().map(|s| s.to_string()))
            .or_else(|| {
                resp.headers
                    .remove(&X_VELDA_CACHE_KEY)
                    .and_then(|v| v.to_str().ok().map(|s| s.to_string()))
            });

        // Strip internal cache metadata from downstream response to prevent header leakage
        resp.headers.remove(&X_VELDA_CACHE_KEY);
        resp.headers.remove(&X_VELDA_CACHE_HASH);

        let Some(cache_key) = key_str else {
            // Cannot associate response with request key, cleanly bypass
            return;
        };

        // Extract precomputed hash from req_headers headers map, then fallback to hashing
        let hash = req_headers
            .get(&X_VELDA_CACHE_HASH)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or_else(|| HttpCacheStorage::hash_key(&cache_key));

        // 3. Status eligibility check
        if !self.config.cacheable_status.contains(&resp.status) {
            resp.headers
                .insert(X_CACHE, HeaderValue::from_static("BYPASS"));
            return;
        }

        // 4. Cache-Control directives check (RFC 9111 Single-Pass Zero-Alloc Scanner)
        let mut computed_ttl = Duration::from_secs(self.config.default_ttl_secs.max(1));
        let mut is_public = false;

        if self.config.respect_cache_control
            && let Some(cc) = resp.headers.get(http::header::CACHE_CONTROL)
        {
            let parsed = parse_cache_control_fast(cc.as_bytes());
            if parsed.is_no_store_or_private() {
                resp.headers
                    .insert(X_CACHE, HeaderValue::from_static("BYPASS"));
                return;
            }

            if parsed.is_public_or_s_maxage() {
                is_public = true;
            }

            if let Some(ttl_secs) = parsed.effective_ttl_secs() {
                if ttl_secs == 0 {
                    resp.headers
                        .insert(X_CACHE, HeaderValue::from_static("BYPASS"));
                    return;
                }
                computed_ttl = Duration::from_secs(ttl_secs as u64);
            }
        }

        // 5. RFC 9111 §3.5 Authentication check (Safety Shield)
        // A shared cache MUST NOT store responses to requests with an Authorization header
        // unless explicitly marked public/s-maxage, or partitioned via key_headers.
        let has_auth = req_headers.contains_key(http::header::AUTHORIZATION);
        let auth_in_key = self
            .config
            .key_headers
            .iter()
            .any(|h| h.eq_ignore_ascii_case("authorization"));

        if has_auth && self.config.bypass_authenticated_requests && !auth_in_key && !is_public {
            resp.headers
                .insert(X_CACHE, HeaderValue::from_static("BYPASS"));
            return;
        }

        // 6. Payload bounds check & DRAM Confinement - STRICT PAYLOAD PLUGIN INVARIANT
        // Payload is written directly to DRAM using Non-Temporal Stores (_mm256_stream_si256 / VMOVNTDQ)
        // when >= 256 bytes, bypassing CPU L1/L2 caches to eliminate cache pollution!
        if resp.body.is_empty() {
            return;
        }
        let body_len = resp.body.len();
        if body_len > self.config.max_payload_bytes {
            resp.headers
                .insert(X_CACHE, HeaderValue::from_static("BYPASS"));
            return;
        }
        let body_bytes = create_dram_confined_bytes(resp.body.as_slice());

        // 7. Extract ONLY Content-Type for MIME fidelity; NEVER store headers map
        let content_type = resp.headers.get(http::header::CONTENT_TYPE).cloned();

        // 8. Dual-Mode Storage (Eager vs Lazy):
        // If Content-Encoding is present, response is already compressed (Lazy Mode).
        // Store or merge this specific single variant into the storage!
        // If Content-Encoding is absent, response is raw canonical data (Eager Mode).
        // Immediately store canonical raw uncompressed payload (0ms client latency),
        // then asynchronously offload pre-compression of variants in the background!
        if let Some(enc_val) = resp.headers.get(http::header::CONTENT_ENCODING) {
            let enc_str = enc_val.to_str().unwrap_or("").trim();
            self.storage.store_variant(StoreVariantParams {
                hash,
                key: cache_key,
                status: resp.status,
                content_type,
                ttl: computed_ttl,
                encoding: Some(enc_str),
                payload: body_bytes,
                variants: None,
                is_public,
            });
        } else {
            // Eager Mode: Store pristine raw canonical body into RAM cache immediately.
            self.storage.store_variant(StoreVariantParams {
                hash,
                key: cache_key,
                status: resp.status,
                content_type: content_type.clone(),
                ttl: computed_ttl,
                encoding: None,
                payload: body_bytes.clone(),
                variants: None,
                is_public,
            });

            // Asynchronous Background Pre-compression (0ms client delay on Cache Miss):
            if body_bytes.len() >= 256 && is_compressible_mime(content_type.as_ref()) {
                if self.config.async_precompression {
                    if let Ok(handle) = tokio::runtime::Handle::try_current() {
                        let storage = Arc::clone(&self.storage);
                        let body_clone = body_bytes;
                        handle.spawn_blocking(move || {
                            let variants = precompress_variants(&body_clone);
                            storage.merge_variants(hash, variants);
                        });
                    } else {
                        // Fallback outside Tokio runtime (e.g. sync unit tests)
                        let variants = precompress_variants(&body_bytes);
                        self.storage.merge_variants(hash, variants);
                    }
                } else {
                    // Synchronous pre-compression when explicitly configured
                    let variants = precompress_variants(&body_bytes);
                    self.storage.merge_variants(hash, variants);
                }
            }
        }

        // 9. Inject response diagnostic headers
        resp.headers
            .insert(X_CACHE, HeaderValue::from_static("MISS"));
        resp.headers.insert("age", HeaderValue::from_static("0"));
    }

    fn capability(&self) -> PluginCapability {
        PluginCapability::new(AccessLevel::Write, AccessLevel::Read)
    }

    fn supported_protocols(&self) -> SupportedProtocols {
        SupportedProtocols::ALL_HTTP
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderMap;
    use http::header::{CACHE_CONTROL, CONTENT_TYPE, SET_COOKIE};

    #[test]
    fn test_http_cache_store_valid_response() {
        let storage = Arc::new(HttpCacheStorage::new(
            "test_store",
            1024 * 1024,
            Duration::from_secs(60),
        ));
        let plugin = HttpCacheStorePlugin::with_storage(
            "store-1",
            HttpCacheStoreConfig::default(),
            Arc::clone(&storage),
        );

        let mut headers = HeaderMap::new();
        headers.insert(
            "x-velda-cache-key",
            "GET:api.example.com:/items".parse().unwrap(),
        );
        headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());
        headers.insert(SET_COOKIE, "session_id=secret".parse().unwrap());
        headers.insert(CACHE_CONTROL, "public, max-age=120".parse().unwrap());

        let mut resp = L7Response::from_bytes(StatusCode::OK, b"{\"data\":[1,2,3]}".to_vec())
            .with_headers(headers);
        let req_headers = velda_core::HeaderWorkspace::empty();

        plugin.on_response(&req_headers, &mut resp);

        // Header check: internal key removed, x-cache and age injected
        assert!(!resp.headers.contains_key("x-velda-cache-key"));
        assert_eq!(resp.headers.get("x-cache").unwrap(), "MISS");
        assert_eq!(resp.headers.get("age").unwrap(), "0");

        // Verify stored entry in storage
        let hash = HttpCacheStorage::hash_key("GET:api.example.com:/items");
        let stored = storage.get(hash).expect("Must be stored in storage");
        assert_eq!(stored.status, StatusCode::OK);
        assert_eq!(
            stored.raw_body.as_ref().unwrap(),
            &bytes::Bytes::from_static(b"{\"data\":[1,2,3]}")
        );
        assert_eq!(stored.content_type.as_ref().unwrap(), "application/json");
        // Pure Payload Invariant: Headers map is never stored in cache storage!
    }

    #[test]
    fn test_http_cache_store_lazy_compressed_response() {
        let storage = Arc::new(HttpCacheStorage::new(
            "test_lazy_store",
            1024 * 1024,
            Duration::from_secs(60),
        ));
        let plugin = HttpCacheStorePlugin::with_storage(
            "store-lazy",
            HttpCacheStoreConfig::default(),
            Arc::clone(&storage),
        );

        let mut headers = HeaderMap::new();
        headers.insert(
            "x-velda-cache-key",
            "GET:api.example.com:/lazy".parse().unwrap(),
        );
        headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());
        headers.insert(http::header::CONTENT_ENCODING, "br".parse().unwrap());

        let mut resp = L7Response::from_bytes(StatusCode::OK, b"brotli-compressed-binary".to_vec())
            .with_headers(headers);
        let req_headers = velda_core::HeaderWorkspace::empty();

        plugin.on_response(&req_headers, &mut resp);

        let hash = HttpCacheStorage::hash_key("GET:api.example.com:/lazy");
        let stored = storage.get(hash).expect("Must be stored in storage");
        assert!(stored.raw_body.is_none());
        assert_eq!(
            stored.variants.brotli.as_ref().unwrap(),
            &bytes::Bytes::from_static(b"brotli-compressed-binary")
        );
        assert!(stored.variants.gzip.is_none());
    }

    #[test]
    fn test_http_cache_store_no_store_bypasses() {
        let storage = Arc::new(HttpCacheStorage::new(
            "test_store_bypass",
            1024 * 1024,
            Duration::from_secs(60),
        ));
        let plugin = HttpCacheStorePlugin::with_storage(
            "store-bypass",
            HttpCacheStoreConfig::default(),
            Arc::clone(&storage),
        );

        let mut headers = HeaderMap::new();
        headers.insert(
            "x-velda-cache-key",
            "GET:api.example.com:/secret".parse().unwrap(),
        );
        headers.insert(CACHE_CONTROL, "no-store, private".parse().unwrap());

        let mut resp = L7Response::from_bytes(StatusCode::OK, b"{\"secret\":true}".to_vec())
            .with_headers(headers);
        let req_headers = velda_core::HeaderWorkspace::empty();

        plugin.on_response(&req_headers, &mut resp);

        assert_eq!(resp.headers.get("x-cache").unwrap(), "BYPASS");
        let hash = HttpCacheStorage::hash_key("GET:api.example.com:/secret");
        assert!(storage.get(hash).is_none());
    }

    #[test]
    fn test_http_cache_store_payload_too_large_bypasses() {
        let storage = Arc::new(HttpCacheStorage::new(
            "test_large",
            1024 * 1024,
            Duration::from_secs(60),
        ));
        let config = HttpCacheStoreConfig {
            max_payload_bytes: 100, // max 100 bytes
            ..HttpCacheStoreConfig::default()
        };

        let plugin =
            HttpCacheStorePlugin::with_storage("store-large", config, Arc::clone(&storage));

        let mut headers = HeaderMap::new();
        headers.insert(
            "x-velda-cache-key",
            "GET:api.example.com:/large".parse().unwrap(),
        );

        let large_payload = vec![b'x'; 200];
        let mut resp = L7Response::from_bytes(StatusCode::OK, large_payload).with_headers(headers);
        let req_headers = velda_core::HeaderWorkspace::empty();

        plugin.on_response(&req_headers, &mut resp);

        assert_eq!(resp.headers.get("x-cache").unwrap(), "BYPASS");
        let hash = HttpCacheStorage::hash_key("GET:api.example.com:/large");
        assert!(storage.get(hash).is_none());
    }

    #[test]
    fn test_http_cache_store_authorization_bypasses_by_default() {
        let storage = Arc::new(HttpCacheStorage::new(
            "test_auth_store_bypass",
            1024 * 1024,
            Duration::from_secs(60),
        ));
        let plugin = HttpCacheStorePlugin::with_storage(
            "store-auth-bypass",
            HttpCacheStoreConfig::default(),
            Arc::clone(&storage),
        );

        let mut headers = HeaderMap::new();
        headers.insert(
            "x-velda-cache-key",
            "GET:api.example.com:/profile".parse().unwrap(),
        );
        headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());
        // Backend did NOT specify public
        headers.insert(CACHE_CONTROL, "max-age=300".parse().unwrap());

        let mut resp = L7Response::from_bytes(StatusCode::OK, b"{\"user\":\"alice\"}".to_vec())
            .with_headers(headers);

        let mut req_headers = velda_core::HeaderWorkspace::empty();
        req_headers.insert(
            http::header::AUTHORIZATION,
            "Bearer secret_token".parse().unwrap(),
        );

        plugin.on_response(&req_headers, &mut resp);

        // Must be BYPASS to prevent shared cache data leak!
        assert_eq!(resp.headers.get("x-cache").unwrap(), "BYPASS");
        let hash = HttpCacheStorage::hash_key("GET:api.example.com:/profile");
        assert!(storage.get(hash).is_none());
    }

    #[test]
    fn test_http_cache_store_authorization_stores_if_public() {
        let storage = Arc::new(HttpCacheStorage::new(
            "test_auth_store_pub",
            1024 * 1024,
            Duration::from_secs(60),
        ));
        let plugin = HttpCacheStorePlugin::with_storage(
            "store-auth-pub",
            HttpCacheStoreConfig::default(),
            Arc::clone(&storage),
        );

        let mut headers = HeaderMap::new();
        headers.insert(
            "x-velda-cache-key",
            "GET:api.example.com:/announcements".parse().unwrap(),
        );
        headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());
        // Backend explicitly declared public per RFC 9111 §3.5
        headers.insert(CACHE_CONTROL, "public, max-age=300".parse().unwrap());

        let mut resp = L7Response::from_bytes(StatusCode::OK, b"{\"news\":\"all\"}".to_vec())
            .with_headers(headers);

        let mut req_headers = velda_core::HeaderWorkspace::empty();
        req_headers.insert(
            http::header::AUTHORIZATION,
            "Bearer any_token".parse().unwrap(),
        );

        plugin.on_response(&req_headers, &mut resp);

        assert_eq!(resp.headers.get("x-cache").unwrap(), "MISS");
        let hash = HttpCacheStorage::hash_key("GET:api.example.com:/announcements");
        let stored = storage.get(hash).expect("Must be stored in storage");
        assert!(stored.is_public);
    }

    #[test]
    fn test_http_cache_store_authorization_stores_if_key_headers_configured() {
        let storage = Arc::new(HttpCacheStorage::new(
            "test_auth_store_per_user",
            1024 * 1024,
            Duration::from_secs(60),
        ));
        let config = HttpCacheStoreConfig {
            key_headers: vec!["authorization".to_string()],
            ..HttpCacheStoreConfig::default()
        };
        let plugin =
            HttpCacheStorePlugin::with_storage("store-per-user", config, Arc::clone(&storage));

        let user_key = "GET:api.example.com:/profile|authorization=Bearer alice";
        let mut headers = HeaderMap::new();
        headers.insert("x-velda-cache-key", user_key.parse().unwrap());
        headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());
        headers.insert(CACHE_CONTROL, "max-age=300".parse().unwrap());

        let mut resp = L7Response::from_bytes(StatusCode::OK, b"{\"user\":\"alice\"}".to_vec())
            .with_headers(headers);

        let mut req_headers = velda_core::HeaderWorkspace::empty();
        req_headers.insert(http::header::AUTHORIZATION, "Bearer alice".parse().unwrap());

        plugin.on_response(&req_headers, &mut resp);

        assert_eq!(resp.headers.get("x-cache").unwrap(), "MISS");
        let hash = HttpCacheStorage::hash_key(user_key);
        let stored = storage
            .get(hash)
            .expect("Must be stored in storage under partitioned key");
        assert_eq!(stored.key.as_ref(), user_key);
    }

    #[tokio::test]
    async fn test_http_cache_store_async_precompression() {
        let storage = Arc::new(HttpCacheStorage::new(
            "test_async_precomp",
            1024 * 1024,
            Duration::from_secs(60),
        ));
        let config = HttpCacheStoreConfig {
            async_precompression: true,
            ..HttpCacheStoreConfig::default()
        };
        let plugin =
            HttpCacheStorePlugin::with_storage("store-async", config, Arc::clone(&storage));

        let cache_key = "GET:api.example.com:/async-data";
        let mut headers = HeaderMap::new();
        headers.insert("x-velda-cache-key", cache_key.parse().unwrap());
        headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());
        headers.insert(CACHE_CONTROL, "public, max-age=300".parse().unwrap());

        let payload = b"{\"message\":\"testing asynchronous background precompression for high throughput\",\"items\":[1,2,3,4,5]}".repeat(5);
        let mut resp =
            L7Response::from_bytes(StatusCode::OK, payload.clone()).with_headers(headers);

        let req_headers = velda_core::HeaderWorkspace::empty();
        plugin.on_response(&req_headers, &mut resp);

        assert_eq!(resp.headers.get("x-cache").unwrap(), "MISS");
        let hash = HttpCacheStorage::hash_key(cache_key);

        // Immediate check: raw canonical body is available instantly in cache
        let initial = storage
            .get(hash)
            .expect("Must be stored in cache immediately");
        assert_eq!(
            initial.raw_body.as_ref().unwrap(),
            &bytes::Bytes::from(payload.clone())
        );

        // Wait brief moment for background task to complete and merge variants
        tokio::time::sleep(Duration::from_millis(100)).await;

        let merged = storage.get(hash).expect("Must still exist in cache");
        assert!(
            merged.variants.gzip.is_some(),
            "Gzip variant must be pre-compressed in background"
        );
        assert!(
            merged.variants.brotli.is_some(),
            "Brotli variant must be pre-compressed in background"
        );
        assert!(
            merged.variants.zstd.is_some(),
            "Zstd variant must be pre-compressed in background"
        );
    }

    #[test]
    fn test_http_cache_store_sync_precompression() {
        let storage = Arc::new(HttpCacheStorage::new(
            "test_sync_precomp",
            1024 * 1024,
            Duration::from_secs(60),
        ));
        let config = HttpCacheStoreConfig {
            async_precompression: false,
            ..HttpCacheStoreConfig::default()
        };
        let plugin = HttpCacheStorePlugin::with_storage("store-sync", config, Arc::clone(&storage));

        let cache_key = "GET:api.example.com:/sync-data";
        let mut headers = HeaderMap::new();
        headers.insert("x-velda-cache-key", cache_key.parse().unwrap());
        headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());
        headers.insert(CACHE_CONTROL, "public, max-age=300".parse().unwrap());

        let payload = b"{\"message\":\"testing synchronous precompression when configured\",\"items\":[1,2,3,4,5]}".repeat(5);
        let mut resp =
            L7Response::from_bytes(StatusCode::OK, payload.clone()).with_headers(headers);

        let req_headers = velda_core::HeaderWorkspace::empty();
        plugin.on_response(&req_headers, &mut resp);

        let hash = HttpCacheStorage::hash_key(cache_key);
        let stored = storage.get(hash).expect("Must be stored in cache");
        assert_eq!(
            stored.raw_body.as_ref().unwrap(),
            &bytes::Bytes::from(payload)
        );
        assert!(
            stored.variants.gzip.is_some(),
            "Gzip variant must be present synchronously"
        );
        assert!(
            stored.variants.brotli.is_some(),
            "Brotli variant must be present synchronously"
        );
        assert!(
            stored.variants.zstd.is_some(),
            "Zstd variant must be present synchronously"
        );
    }

    #[test]
    fn test_from_config_strict_validation() {
        let valid_json = serde_json::json!({
            "zone": "zone_test_from_config",
            "cacheable_status": [200, 203, 204],
            "max_payload_bytes": 1048576,
            "default_ttl_secs": 600,
            "respect_cache_control": true,
            "bypass_authenticated_requests": false,
            "key_headers": ["accept-language"],
            "async_precompression": true,
            "max_memory_mb": 128
        });

        let p_cfg = PluginConfig {
            id: "cache_store_test".into(),
            phase: "post_response".into(),
            config: valid_json,
        };

        let plugin = HttpCacheStorePlugin::from_config(&p_cfg).unwrap();
        assert_eq!(plugin.config.zone, "zone_test_from_config");
        assert_eq!(plugin.config.cacheable_status.len(), 3);
        assert_eq!(plugin.config.max_payload_bytes, 1048576);

        let invalid_json = serde_json::json!({
            "zone": "zone_test"
            // Missing required fields
        });
        let bad_p_cfg = PluginConfig {
            id: "cache_store_bad".into(),
            phase: "post_response".into(),
            config: invalid_json,
        };
        assert!(HttpCacheStorePlugin::from_config(&bad_p_cfg).is_err());
    }

    #[test]
    fn test_http_cache_store_req_headers_key_and_precomputed_hash() {
        let storage = Arc::new(HttpCacheStorage::new(
            "test_req_headers_storage",
            1024 * 1024,
            Duration::from_secs(60),
        ));
        let plugin = HttpCacheStorePlugin::with_storage(
            "store-req-headers",
            HttpCacheStoreConfig::default(),
            Arc::clone(&storage),
        );

        let cache_key = "GET:api.example.com:/fast-path";
        let precomputed_hash = HttpCacheStorage::hash_key(cache_key);

        let mut req_headers = velda_core::HeaderWorkspace::empty();
        req_headers.insert(X_VELDA_CACHE_KEY, cache_key.parse().unwrap());
        req_headers.insert(X_VELDA_CACHE_HASH, HeaderValue::from(precomputed_hash));

        let mut resp_headers = HeaderMap::new();
        resp_headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());
        resp_headers.insert(CACHE_CONTROL, "public, s-maxage=3600".parse().unwrap());

        let payload = vec![b'A'; 512]; // >= 256 bytes invokes non-temporal DRAM stream
        let mut resp =
            L7Response::from_bytes(StatusCode::OK, payload.clone()).with_headers(resp_headers);

        plugin.on_response(&req_headers, &mut resp);

        // Header check: internal key and hash must not be present in response
        assert!(!resp.headers.contains_key("x-velda-cache-key"));
        assert!(!resp.headers.contains_key("x-velda-cache-hash"));
        assert_eq!(resp.headers.get("x-cache").unwrap(), "MISS");

        // Verify stored in storage directly by precomputed hash
        let stored = storage
            .get(precomputed_hash)
            .expect("Must be stored in storage");
        assert_eq!(stored.status, StatusCode::OK);
        assert_eq!(stored.raw_body.as_ref().unwrap().as_ref(), &payload[..]);
        assert!(stored.is_public);
    }
}
