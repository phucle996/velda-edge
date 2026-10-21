//! HTTP Cache Store Post-Response Plugin (`http_cache_store`).
//!
//! Evaluated on outgoing responses returned from Upstream backends during the `post_response` phase.
//! Determines cacheability based on HTTP status, payload size, and `Cache-Control` directives.
//! Stores valid responses into the shared [`HttpCacheZone`] and injects `X-Cache` diagnostic headers.

use std::sync::Arc;
use std::time::Duration;

use http::StatusCode;
use http::header::HeaderValue;
use velda_core::{Body, L7Response};
use velda_sync::post_sync::plugin::PluginConfig;

use crate::header::X_REQUEST_ID;
use crate::http_cache::constants::{X_CACHE, X_VELDA_CACHE_HASH, X_VELDA_CACHE_KEY};

use crate::capability::{AccessLevel, PluginCapability, SupportedProtocols};
use crate::error::PluginError;
use crate::http_cache::{
    CompressedVariants, HttpCacheZone, HttpCacheZoneRegistry, parse_cache_control_ttl,
};
use crate::post_response::PostResponsePlugin;

/// Configuration schema for [`HttpCacheStorePlugin`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpCacheStoreConfig {
    /// Shared cache zone identifier (must match `http_cache_lookup`).
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
    /// Allocated memory limit for this zone if created here (in megabytes).
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
    pub zone: Arc<HttpCacheZone>,
}

impl std::fmt::Debug for HttpCacheStorePlugin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpCacheStorePlugin")
            .field("id", &self.id)
            .field("config", &self.config)
            .field("zone", &self.zone.name)
            .finish()
    }
}

impl HttpCacheStorePlugin {
    /// Constructs a new `HttpCacheStorePlugin` with explicit configuration.
    pub fn new(id: impl Into<String>, config: HttpCacheStoreConfig) -> Self {
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
        config: HttpCacheStoreConfig,
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
        let defaults = HttpCacheStoreConfig::default();

        if config.config.is_null() {
            return Ok(Self::new(&config.id, defaults));
        }

        let raw = config.config.as_object().ok_or_else(|| {
            PluginError::Config(format!("Plugin '{}' config must be an object", config.id))
        })?;

        let zone = raw
            .get("zone")
            .and_then(|v| v.as_str())
            .unwrap_or(&defaults.zone)
            .to_string();

        let mut cacheable_status = defaults.cacheable_status;
        if let Some(arr) = raw.get("cacheable_status").and_then(|v| v.as_array()) {
            let mut statuses = Vec::new();
            for item in arr {
                if let Some(code) = item.as_u64()
                    && let Ok(status) = StatusCode::from_u16(code as u16)
                {
                    statuses.push(status);
                }
            }
            if !statuses.is_empty() {
                cacheable_status = statuses;
            }
        }

        let max_payload_bytes = raw
            .get("max_payload_bytes")
            .and_then(|v| v.as_u64())
            .map(|v| v as usize)
            .unwrap_or(defaults.max_payload_bytes);

        let default_ttl_secs = raw
            .get("default_ttl_secs")
            .and_then(|v| v.as_u64())
            .unwrap_or(defaults.default_ttl_secs);

        let respect_cache_control = raw
            .get("respect_cache_control")
            .and_then(|v| v.as_bool())
            .unwrap_or(defaults.respect_cache_control);

        let bypass_authenticated_requests = raw
            .get("bypass_authenticated_requests")
            .and_then(|v| v.as_bool())
            .unwrap_or(defaults.bypass_authenticated_requests);

        let mut key_headers = defaults.key_headers;
        if let Some(arr) = raw.get("key_headers").and_then(|v| v.as_array()) {
            let mut headers = Vec::new();
            for item in arr {
                if let Some(s) = item.as_str() {
                    let trimmed = s.trim().to_ascii_lowercase();
                    if !trimmed.is_empty() && !headers.contains(&trimmed) {
                        headers.push(trimmed);
                    }
                }
            }
            key_headers = headers;
        }

        let async_precompression = raw
            .get("async_precompression")
            .and_then(|v| v.as_bool())
            .unwrap_or(defaults.async_precompression);

        let max_memory_mb = raw
            .get("max_memory_mb")
            .and_then(|v| v.as_u64())
            .map(|v| v as usize)
            .unwrap_or(defaults.max_memory_mb);

        Ok(Self::new(
            &config.id,
            HttpCacheStoreConfig {
                zone,
                cacheable_status,
                max_payload_bytes,
                default_ttl_secs,
                respect_cache_control,
                bypass_authenticated_requests,
                key_headers,
                async_precompression,
                max_memory_mb,
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

    fn on_response(&self, req_headers: &http::HeaderMap, resp: &mut L7Response) {
        // 1. If already served from cache (HIT), skip storing
        if let Some(val) = resp.headers.get(&X_CACHE)
            && val == "HIT"
        {
            return;
        }

        // 2. Extract cache key: prioritize transient header injected by http_cache_lookup
        let key_str = resp
            .headers
            .remove(&X_VELDA_CACHE_KEY)
            .and_then(|v| v.to_str().ok().map(|s| s.to_string()))
            .or_else(|| {
                // Secondary fallback: lookup via authoritative request-id from request workspace
                req_headers
                    .get(&X_REQUEST_ID)
                    .and_then(|v| v.to_str().ok())
                    .and_then(|req_id| self.zone.take_pending_key(req_id))
            });

        // Strip internal cache metadata from downstream response
        resp.headers.remove(&X_VELDA_CACHE_HASH);

        let Some(cache_key) = key_str else {
            // Cannot associate response with request key, cleanly bypass
            return;
        };

        // 3. Status eligibility check
        if !self.config.cacheable_status.contains(&resp.status) {
            resp.headers
                .insert(X_CACHE, HeaderValue::from_static("BYPASS"));
            return;
        }

        // 4. Cache-Control directives check (RFC 9111)
        let mut computed_ttl = Duration::from_secs(self.config.default_ttl_secs.max(1));
        let mut is_public = false;

        if self.config.respect_cache_control
            && let Some(cc) = resp
                .headers
                .get(http::header::CACHE_CONTROL)
                .and_then(|v| v.to_str().ok())
        {
            // Must not cache if no-store or private is present
            if cc.contains("no-store") || cc.contains("private") {
                resp.headers
                    .insert(X_CACHE, HeaderValue::from_static("BYPASS"));
                return;
            }

            if cc.contains("public") || cc.contains("s-maxage") {
                is_public = true;
            }

            if let Some(parsed_ttl) = parse_cache_control_ttl(cc) {
                if parsed_ttl.is_zero() {
                    resp.headers
                        .insert(X_CACHE, HeaderValue::from_static("BYPASS"));
                    return;
                }
                computed_ttl = parsed_ttl;
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

        // 6. Payload bounds check - STRICT PAYLOAD PLUGIN INVARIANT
        // A Payload Plugin only caches if a genuine payload is supplied by the data plane.
        let body_bytes = match &resp.body {
            Body::Bytes(b) => {
                if b.is_empty() {
                    return;
                }
                if b.len() > self.config.max_payload_bytes {
                    resp.headers
                        .insert(X_CACHE, HeaderValue::from_static("BYPASS"));
                    return;
                }
                b.clone()
            }
            Body::Empty => {
                // Pure Payload Invariant: Never cache empty body
                return;
            }
        };

        // 7. Extract ONLY Content-Type for MIME fidelity; NEVER store headers map
        let content_type = resp.headers.get(http::header::CONTENT_TYPE).cloned();

        // 8. Dual-Mode Storage (Eager vs Lazy):
        // If Content-Encoding is present, response is already compressed (Lazy Mode).
        // Store or merge this specific single variant into the zone!
        // If Content-Encoding is absent, response is raw canonical data (Eager Mode).
        // Immediately store canonical raw uncompressed payload (0ms client latency),
        // then asynchronously offload pre-compression of variants in the background!
        let hash = HttpCacheZone::hash_key(&cache_key);

        if let Some(enc_val) = resp.headers.get(http::header::CONTENT_ENCODING) {
            let enc_str = enc_val.to_str().unwrap_or("").trim();
            self.zone.store_variant(
                hash,
                cache_key,
                resp.status,
                content_type,
                computed_ttl,
                Some(enc_str),
                body_bytes,
                None,
                is_public,
            );
        } else {
            // Eager Mode: Store pristine raw canonical body into RAM cache immediately.
            self.zone.store_variant(
                hash,
                cache_key,
                resp.status,
                content_type.clone(),
                computed_ttl,
                None,
                body_bytes.clone(),
                None,
                is_public,
            );

            // Asynchronous Background Pre-compression (0ms client delay on Cache Miss):
            if body_bytes.len() >= 256 && is_compressible_mime(content_type.as_ref()) {
                if self.config.async_precompression {
                    if let Ok(handle) = tokio::runtime::Handle::try_current() {
                        let zone = Arc::clone(&self.zone);
                        let body_clone = body_bytes;
                        handle.spawn_blocking(move || {
                            let variants = precompress_variants(&body_clone);
                            zone.merge_variants(hash, variants);
                        });
                    } else {
                        // Fallback outside Tokio runtime (e.g. sync unit tests)
                        let variants = precompress_variants(&body_bytes);
                        self.zone.merge_variants(hash, variants);
                    }
                } else {
                    // Synchronous pre-compression when explicitly configured
                    let variants = precompress_variants(&body_bytes);
                    self.zone.merge_variants(hash, variants);
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
        let zone = Arc::new(HttpCacheZone::new(
            "test_store",
            1024 * 1024,
            Duration::from_secs(60),
        ));
        let plugin = HttpCacheStorePlugin::with_zone(
            "store-1",
            HttpCacheStoreConfig::default(),
            Arc::clone(&zone),
        );

        let mut headers = HeaderMap::new();
        headers.insert(
            "x-velda-cache-key",
            "GET:api.example.com:/items".parse().unwrap(),
        );
        headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());
        headers.insert(SET_COOKIE, "session_id=secret".parse().unwrap());
        headers.insert(CACHE_CONTROL, "public, max-age=120".parse().unwrap());

        let mut resp = L7Response::from_bytes(StatusCode::OK, b"{\"data\":[1,2,3]}".to_vec());
        resp.headers = headers;
        let req_headers = HeaderMap::new();

        plugin.on_response(&req_headers, &mut resp);

        // Header check: internal key removed, x-cache and age injected
        assert!(!resp.headers.contains_key("x-velda-cache-key"));
        assert_eq!(resp.headers.get("x-cache").unwrap(), "MISS");
        assert_eq!(resp.headers.get("age").unwrap(), "0");

        // Verify stored entry in zone
        let hash = HttpCacheZone::hash_key("GET:api.example.com:/items");
        let stored = zone.get(hash).expect("Must be stored in zone");
        assert_eq!(stored.status, StatusCode::OK);
        assert_eq!(
            stored.raw_body.as_ref().unwrap(),
            &bytes::Bytes::from_static(b"{\"data\":[1,2,3]}")
        );
        assert_eq!(stored.content_type.as_ref().unwrap(), "application/json");
        // Pure Payload Invariant: Headers map is never stored in cache zone!
    }

    #[test]
    fn test_http_cache_store_lazy_compressed_response() {
        let zone = Arc::new(HttpCacheZone::new(
            "test_lazy_store",
            1024 * 1024,
            Duration::from_secs(60),
        ));
        let plugin = HttpCacheStorePlugin::with_zone(
            "store-lazy",
            HttpCacheStoreConfig::default(),
            Arc::clone(&zone),
        );

        let mut headers = HeaderMap::new();
        headers.insert(
            "x-velda-cache-key",
            "GET:api.example.com:/lazy".parse().unwrap(),
        );
        headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());
        headers.insert(http::header::CONTENT_ENCODING, "br".parse().unwrap());

        let mut resp = L7Response::from_bytes(StatusCode::OK, b"brotli-compressed-binary".to_vec());
        resp.headers = headers;
        let req_headers = HeaderMap::new();

        plugin.on_response(&req_headers, &mut resp);

        let hash = HttpCacheZone::hash_key("GET:api.example.com:/lazy");
        let stored = zone.get(hash).expect("Must be stored in zone");
        assert!(stored.raw_body.is_none());
        assert_eq!(
            stored.variants.brotli.as_ref().unwrap(),
            &bytes::Bytes::from_static(b"brotli-compressed-binary")
        );
        assert!(stored.variants.gzip.is_none());
    }

    #[test]
    fn test_http_cache_store_no_store_bypasses() {
        let zone = Arc::new(HttpCacheZone::new(
            "test_store_bypass",
            1024 * 1024,
            Duration::from_secs(60),
        ));
        let plugin = HttpCacheStorePlugin::with_zone(
            "store-bypass",
            HttpCacheStoreConfig::default(),
            Arc::clone(&zone),
        );

        let mut headers = HeaderMap::new();
        headers.insert(
            "x-velda-cache-key",
            "GET:api.example.com:/secret".parse().unwrap(),
        );
        headers.insert(CACHE_CONTROL, "no-store, private".parse().unwrap());

        let mut resp = L7Response::from_bytes(StatusCode::OK, b"{\"secret\":true}".to_vec());
        resp.headers = headers;
        let req_headers = HeaderMap::new();

        plugin.on_response(&req_headers, &mut resp);

        assert_eq!(resp.headers.get("x-cache").unwrap(), "BYPASS");
        let hash = HttpCacheZone::hash_key("GET:api.example.com:/secret");
        assert!(zone.get(hash).is_none());
    }

    #[test]
    fn test_http_cache_store_payload_too_large_bypasses() {
        let zone = Arc::new(HttpCacheZone::new(
            "test_large",
            1024 * 1024,
            Duration::from_secs(60),
        ));
        let config = HttpCacheStoreConfig {
            max_payload_bytes: 100, // max 100 bytes
            ..HttpCacheStoreConfig::default()
        };

        let plugin = HttpCacheStorePlugin::with_zone("store-large", config, Arc::clone(&zone));

        let mut headers = HeaderMap::new();
        headers.insert(
            "x-velda-cache-key",
            "GET:api.example.com:/large".parse().unwrap(),
        );

        let large_payload = vec![b'x'; 200];
        let mut resp = L7Response::from_bytes(StatusCode::OK, large_payload);
        resp.headers = headers;
        let req_headers = HeaderMap::new();

        plugin.on_response(&req_headers, &mut resp);

        assert_eq!(resp.headers.get("x-cache").unwrap(), "BYPASS");
        let hash = HttpCacheZone::hash_key("GET:api.example.com:/large");
        assert!(zone.get(hash).is_none());
    }

    #[test]
    fn test_http_cache_store_authorization_bypasses_by_default() {
        let zone = Arc::new(HttpCacheZone::new(
            "test_auth_store_bypass",
            1024 * 1024,
            Duration::from_secs(60),
        ));
        let plugin = HttpCacheStorePlugin::with_zone(
            "store-auth-bypass",
            HttpCacheStoreConfig::default(),
            Arc::clone(&zone),
        );

        let mut headers = HeaderMap::new();
        headers.insert(
            "x-velda-cache-key",
            "GET:api.example.com:/profile".parse().unwrap(),
        );
        headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());
        // Backend did NOT specify public
        headers.insert(CACHE_CONTROL, "max-age=300".parse().unwrap());

        let mut resp = L7Response::from_bytes(StatusCode::OK, b"{\"user\":\"alice\"}".to_vec());
        resp.headers = headers;

        let mut req_headers = HeaderMap::new();
        req_headers.insert(
            http::header::AUTHORIZATION,
            "Bearer secret_token".parse().unwrap(),
        );

        plugin.on_response(&req_headers, &mut resp);

        // Must be BYPASS to prevent shared cache data leak!
        assert_eq!(resp.headers.get("x-cache").unwrap(), "BYPASS");
        let hash = HttpCacheZone::hash_key("GET:api.example.com:/profile");
        assert!(zone.get(hash).is_none());
    }

    #[test]
    fn test_http_cache_store_authorization_stores_if_public() {
        let zone = Arc::new(HttpCacheZone::new(
            "test_auth_store_pub",
            1024 * 1024,
            Duration::from_secs(60),
        ));
        let plugin = HttpCacheStorePlugin::with_zone(
            "store-auth-pub",
            HttpCacheStoreConfig::default(),
            Arc::clone(&zone),
        );

        let mut headers = HeaderMap::new();
        headers.insert(
            "x-velda-cache-key",
            "GET:api.example.com:/announcements".parse().unwrap(),
        );
        headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());
        // Backend explicitly declared public per RFC 9111 §3.5
        headers.insert(CACHE_CONTROL, "public, max-age=300".parse().unwrap());

        let mut resp = L7Response::from_bytes(StatusCode::OK, b"{\"news\":\"all\"}".to_vec());
        resp.headers = headers;

        let mut req_headers = HeaderMap::new();
        req_headers.insert(
            http::header::AUTHORIZATION,
            "Bearer any_token".parse().unwrap(),
        );

        plugin.on_response(&req_headers, &mut resp);

        assert_eq!(resp.headers.get("x-cache").unwrap(), "MISS");
        let hash = HttpCacheZone::hash_key("GET:api.example.com:/announcements");
        let stored = zone.get(hash).expect("Must be stored in zone");
        assert!(stored.is_public);
    }

    #[test]
    fn test_http_cache_store_authorization_stores_if_key_headers_configured() {
        let zone = Arc::new(HttpCacheZone::new(
            "test_auth_store_per_user",
            1024 * 1024,
            Duration::from_secs(60),
        ));
        let config = HttpCacheStoreConfig {
            key_headers: vec!["authorization".to_string()],
            ..HttpCacheStoreConfig::default()
        };
        let plugin = HttpCacheStorePlugin::with_zone("store-per-user", config, Arc::clone(&zone));

        let user_key = "GET:api.example.com:/profile|authorization=Bearer alice";
        let mut headers = HeaderMap::new();
        headers.insert("x-velda-cache-key", user_key.parse().unwrap());
        headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());
        headers.insert(CACHE_CONTROL, "max-age=300".parse().unwrap());

        let mut resp = L7Response::from_bytes(StatusCode::OK, b"{\"user\":\"alice\"}".to_vec());
        resp.headers = headers;

        let mut req_headers = HeaderMap::new();
        req_headers.insert(http::header::AUTHORIZATION, "Bearer alice".parse().unwrap());

        plugin.on_response(&req_headers, &mut resp);

        assert_eq!(resp.headers.get("x-cache").unwrap(), "MISS");
        let hash = HttpCacheZone::hash_key(user_key);
        let stored = zone
            .get(hash)
            .expect("Must be stored in zone under partitioned key");
        assert_eq!(stored.key, user_key);
    }

    #[tokio::test]
    async fn test_http_cache_store_async_precompression() {
        let zone = Arc::new(HttpCacheZone::new(
            "test_async_precomp",
            1024 * 1024,
            Duration::from_secs(60),
        ));
        let config = HttpCacheStoreConfig {
            async_precompression: true,
            ..HttpCacheStoreConfig::default()
        };
        let plugin = HttpCacheStorePlugin::with_zone("store-async", config, Arc::clone(&zone));

        let cache_key = "GET:api.example.com:/async-data";
        let mut headers = HeaderMap::new();
        headers.insert("x-velda-cache-key", cache_key.parse().unwrap());
        headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());
        headers.insert(CACHE_CONTROL, "public, max-age=300".parse().unwrap());

        let payload = b"{\"message\":\"testing asynchronous background precompression for high throughput\",\"items\":[1,2,3,4,5]}".repeat(5);
        let mut resp = L7Response::from_bytes(StatusCode::OK, payload.clone());
        resp.headers = headers;

        let req_headers = HeaderMap::new();
        plugin.on_response(&req_headers, &mut resp);

        assert_eq!(resp.headers.get("x-cache").unwrap(), "MISS");
        let hash = HttpCacheZone::hash_key(cache_key);

        // Immediate check: raw canonical body is available instantly in cache
        let initial = zone.get(hash).expect("Must be stored in cache immediately");
        assert_eq!(
            initial.raw_body.as_ref().unwrap(),
            &bytes::Bytes::from(payload.clone())
        );

        // Wait brief moment for background task to complete and merge variants
        tokio::time::sleep(Duration::from_millis(100)).await;

        let merged = zone.get(hash).expect("Must still exist in cache");
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
        let zone = Arc::new(HttpCacheZone::new(
            "test_sync_precomp",
            1024 * 1024,
            Duration::from_secs(60),
        ));
        let config = HttpCacheStoreConfig {
            async_precompression: false,
            ..HttpCacheStoreConfig::default()
        };
        let plugin = HttpCacheStorePlugin::with_zone("store-sync", config, Arc::clone(&zone));

        let cache_key = "GET:api.example.com:/sync-data";
        let mut headers = HeaderMap::new();
        headers.insert("x-velda-cache-key", cache_key.parse().unwrap());
        headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());
        headers.insert(CACHE_CONTROL, "public, max-age=300".parse().unwrap());

        let payload = b"{\"message\":\"testing synchronous precompression when configured\",\"items\":[1,2,3,4,5]}".repeat(5);
        let mut resp = L7Response::from_bytes(StatusCode::OK, payload.clone());
        resp.headers = headers;

        let req_headers = HeaderMap::new();
        plugin.on_response(&req_headers, &mut resp);

        let hash = HttpCacheZone::hash_key(cache_key);
        let stored = zone.get(hash).expect("Must be stored in cache");
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
}
