//! HTTP Cache Store Post-Response Plugin (`http_cache_store`).
//!
//! Evaluated on upstream responses during the `post_response` phase. Derives the canonical cache
//! key hash directly from `(req_state.route_id, req_state.method, key_headers)` via [`HttpCacheStorage`],
//! enforces upstream `Cache-Control` exclusion (`no-cache`, `no-store`, `private`), streams
//! eligible payloads into contiguous 2 MiB DRAM slabs via non-temporal stores, and emits
//! `X-Cache` / `Age` headers.
//!
//! ```text
//!   on_response(req_state, req_headers, resp)
//!     ├─ X-Cache == "HIT" or non-cacheable method ─────────────► return
//!     ├─ status / upstream Cache-Control (no-cache/no-store) ──► X-Cache: BYPASS ─► return
//!     └─► hash = storage.route_key_hash(route_id, req_state.method, req_headers, compiled_keys)
//!           └─► storage.store_bytes()  [stream_payload_to_dram into 2MB slab]
//!                 ├─ X-Cache: MISS, Age: 0
//!                 └─ optional variant precompression (raw & len >= 256 & compressible MIME)
//! ```

use std::sync::Arc;
use std::time::Duration;

use http::header::HeaderValue;
use http::{Method, StatusCode};
use velda_core::L7Response;
use velda_core::buffer::header::HeaderKey;
use velda_core::lut::HttpMethod;

use crate::capability::{AccessLevel, PluginCapability, SupportedProtocols};
use crate::config::PluginConfig;
use crate::error::PluginError;
use crate::http_cache::{
    CC_FLAG_PRIVATE, CompiledKeyHeader, CompressedVariants, HttpCacheRegistry, HttpCacheStorage,
    SLAB_SIZE, StoreBytesParams, compile_key_headers, parse_cache_control_fast,
};
use crate::post_response::PostResponsePlugin;

const KEY_X_CACHE: HeaderKey = HeaderKey::from_static("x-cache");
const KEY_CACHE_CONTROL: HeaderKey = HeaderKey::from_static("cache-control");
const KEY_AUTHORIZATION: HeaderKey = HeaderKey::from_static("authorization");
const KEY_CONTENT_TYPE: HeaderKey = HeaderKey::from_static("content-type");
const KEY_CONTENT_ENCODING: HeaderKey = HeaderKey::from_static("content-encoding");

/// Configuration schema for [`HttpCacheStorePlugin`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpCacheStoreConfig {
    /// Shared cache storage identifier (must match `http_cache_lookup`).
    pub zone: String,
    /// HTTP methods eligible for caching (default: GET, HEAD).
    pub cacheable_methods: Vec<Method>,
    /// HTTP status codes eligible for caching (default: 200, 203, 204, 206, 300, 301, 404).
    pub cacheable_status: Vec<StatusCode>,
    /// Maximum payload size in bytes eligible for caching (default: 2 MiB).
    pub max_payload_bytes: usize,
    /// Default TTL fallback in seconds when upstream does not emit Cache-Control max-age.
    pub default_ttl_secs: u64,
    /// Honor upstream `Cache-Control: no-cache` / `no-store` / `private` (default: true).
    pub respect_cache_control: bool,
    /// Bypass storing responses carrying an `Authorization` header unless public/partitioned.
    pub bypass_authenticated_requests: bool,
    /// Custom request header names incorporated into the cache key (`x-user-id`, `x-tenant-id`).
    pub key_headers: Vec<String>,
    /// Whether to offload variant pre-compression (`gzip`, `brotli`, `zstd`) asynchronously.
    pub async_precompression: bool,
    /// Allocated memory limit for this storage if created here (in megabytes).
    pub max_memory_mb: usize,
}

impl Default for HttpCacheStoreConfig {
    fn default() -> Self {
        Self {
            zone: "default".to_string(),
            cacheable_methods: vec![Method::GET, Method::HEAD],
            cacheable_status: vec![
                StatusCode::OK,
                StatusCode::NON_AUTHORITATIVE_INFORMATION,
                StatusCode::NO_CONTENT,
                StatusCode::PARTIAL_CONTENT,
                StatusCode::MULTIPLE_CHOICES,
                StatusCode::MOVED_PERMANENTLY,
                StatusCode::NOT_FOUND,
            ],
            max_payload_bytes: SLAB_SIZE,
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
    method_mask: u16,
    compiled_key_headers: Box<[CompiledKeyHeader]>,
    auth_in_key: bool,
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
        Self::with_storage(id, config, storage)
    }

    /// Constructs an instance binding to an existing storage (convenient for testing).
    pub fn with_storage(
        id: impl Into<String>,
        config: HttpCacheStoreConfig,
        storage: Arc<HttpCacheStorage>,
    ) -> Self {
        let mut method_mask = 0u16;
        for m in &config.cacheable_methods {
            let hm = HttpMethod::from_http_method(m);
            if hm != HttpMethod::Unknown {
                method_mask |= 1u16 << (hm as u8);
            }
        }
        let compiled_key_headers = compile_key_headers(&config.key_headers)
            .unwrap_or_else(|_| Vec::new().into_boxed_slice());
        let auth_in_key = compiled_key_headers
            .iter()
            .any(|h| &*h.canonical_name == "authorization");

        Self {
            id: id.into(),
            config,
            storage,
            method_mask,
            compiled_key_headers,
            auth_in_key,
        }
    }

    /// Deserializes and constructs an instance directly from a [`PluginConfig`].
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let cfg = super::validation::validate_http_cache_store_config(&config.id, &config.config)
            .map_err(|e| {
            PluginError::Config(format!(
                "Invalid http_cache_store configuration for '{}': {e}",
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

        let _compiled = compile_key_headers(&cfg.key_headers).map_err(|e| {
            PluginError::Config(format!(
                "Invalid key_headers in plugin '{}': {e}",
                config.id
            ))
        })?;

        Ok(Self::new(
            &config.id,
            HttpCacheStoreConfig {
                zone: cfg.zone,
                cacheable_methods,
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
fn precompress_variants(body: &[u8]) -> CompressedVariants {
    use crate::post_response::response_compression::{encode_brotli, encode_gzip, encode_zstd};

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

    #[inline(always)]
    fn pbww(&self) -> u32 {
        0
    }

    fn on_response(
        &self,
        req_state: &velda_core::RequestState,
        req_headers: &velda_core::HeaderWorkspace,
        resp: &mut L7Response,
    ) {
        // 1. Skip responses already served from cache.
        if resp
            .headers
            .get_prehashed(&KEY_X_CACHE)
            .is_some_and(|v| v == "HIT")
        {
            return;
        }

        // 2. Method eligibility check via RequestState.
        if (self.method_mask & (1u16 << (req_state.method as u8))) == 0 {
            return;
        }

        // 3. Status eligibility check.
        if !self.config.cacheable_status.contains(&resp.status) {
            resp.headers
                .insert_prehashed(&KEY_X_CACHE, HeaderValue::from_static("BYPASS"));
            return;
        }

        // 4. Upstream Cache-Control directives check (`no-cache`, `no-store`, `private`, `max-age`).
        let mut computed_ttl = Duration::from_secs(self.config.default_ttl_secs.max(1));
        let mut is_public = false;

        if self.config.respect_cache_control
            && let Some(cc) = resp.headers.get_prehashed(&KEY_CACHE_CONTROL)
        {
            let parsed = parse_cache_control_fast(cc.as_bytes());
            if parsed.is_no_cache_or_no_store() || (parsed.flags & CC_FLAG_PRIVATE) != 0 {
                resp.headers
                    .insert_prehashed(&KEY_X_CACHE, HeaderValue::from_static("BYPASS"));
                return;
            }

            if parsed.is_public_or_s_maxage() {
                is_public = true;
            }

            if let Some(ttl_secs) = parsed.effective_ttl_secs() {
                if ttl_secs == 0 {
                    resp.headers
                        .insert_prehashed(&KEY_X_CACHE, HeaderValue::from_static("BYPASS"));
                    return;
                }
                computed_ttl = Duration::from_secs(ttl_secs as u64);
            }
        }

        // 5. RFC 9111 §3.5 Authentication check.
        let has_auth = req_headers.contains_prehashed(&KEY_AUTHORIZATION);
        if has_auth && self.config.bypass_authenticated_requests && !self.auth_in_key && !is_public
        {
            resp.headers
                .insert_prehashed(&KEY_X_CACHE, HeaderValue::from_static("BYPASS"));
            return;
        }

        // 6. Payload bounds check (fits inside a single 2 MiB contiguous slab).
        if resp.body.is_empty() {
            return;
        }
        let body_len = resp.body.len();
        if body_len > self.config.max_payload_bytes || body_len > SLAB_SIZE {
            resp.headers
                .insert_prehashed(&KEY_X_CACHE, HeaderValue::from_static("BYPASS"));
            return;
        }

        // 7. Compute canonical slot key shared with `http_cache_lookup`.
        let hash = self
            .storage
            .route_key_hash(req_state, req_headers, &self.compiled_key_headers);

        let content_type = resp.headers.get_prehashed(&KEY_CONTENT_TYPE).cloned();
        let encoding = resp
            .headers
            .get_prehashed(&KEY_CONTENT_ENCODING)
            .and_then(|v| v.to_str().ok())
            .map(str::trim);

        // 8. Stream payload directly into contiguous 2 MiB slab pool (zero intermediate Vec).
        let payload_slice = resp.body.as_slice();
        self.storage.store_bytes(StoreBytesParams {
            hash,
            status: resp.status,
            content_type: content_type.clone(),
            ttl: computed_ttl,
            encoding,
            payload: payload_slice,
            is_public,
        });

        // 9. Optional variant pre-compression when stored response is uncompressed canonical payload.
        if encoding.is_none() && body_len >= 256 && is_compressible_mime(content_type.as_ref()) {
            if self.config.async_precompression
                && let Ok(handle) = tokio::runtime::Handle::try_current()
            {
                let storage = Arc::clone(&self.storage);
                let body_owned = match &resp.body {
                    velda_core::Body::Bytes(b) => b.clone(),
                    _ => bytes::Bytes::copy_from_slice(payload_slice),
                };
                handle.spawn_blocking(move || {
                    let variants = precompress_variants(body_owned.as_ref());
                    storage.merge_variants(hash, variants);
                });
            } else {
                let variants = precompress_variants(payload_slice);
                self.storage.merge_variants(hash, variants);
            }
        }

        // 10. Inject response diagnostic headers.
        resp.headers
            .insert_prehashed(&KEY_X_CACHE, HeaderValue::from_static("MISS"));
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
    use crate::http_cache::X_CACHE;
    use velda_core::{HeaderWorkspace, RequestState};

    #[test]
    fn test_upstream_no_cache_and_non_cacheable_method_bypass() {
        velda_core::clock::init_clock();
        let storage = Arc::new(HttpCacheStorage::new(
            "test_store_rules",
            4 * 1024 * 1024,
            Duration::from_secs(60),
        ));
        let plugin = HttpCacheStorePlugin::with_storage(
            "http_cache_store",
            HttpCacheStoreConfig {
                zone: "test_store_rules".into(),
                async_precompression: false,
                ..Default::default()
            },
            Arc::clone(&storage),
        );

        let mut req_state = RequestState::new(1).with_method(HttpMethod::Get);
        req_state.set_route(42);
        let req_headers = HeaderWorkspace::new(4);

        // Upstream Cache-Control: no-cache -> must BYPASS and NOT store.
        let mut resp_no_cache =
            L7Response::from_bytes(StatusCode::OK, b"secret-dynamic".as_slice());
        resp_no_cache.headers.insert(
            http::header::CACHE_CONTROL,
            HeaderValue::from_static("no-cache"),
        );
        plugin.on_response(&req_state, &req_headers, &mut resp_no_cache);
        assert_eq!(resp_no_cache.headers.get(&X_CACHE).unwrap(), "BYPASS");

        let hash = storage.route_key_hash(&req_state, &req_headers, &[]);
        assert!(storage.get(hash).is_none());

        // Non-cacheable method (POST) -> skipped cleanly without storing.
        req_state.set_method(HttpMethod::Post);
        let mut resp_post = L7Response::from_bytes(StatusCode::OK, b"created".as_slice());
        plugin.on_response(&req_state, &req_headers, &mut resp_post);
        assert!(resp_post.headers.get(&X_CACHE).is_none());
        let post_hash = storage.route_key_hash(&req_state, &req_headers, &[]);
        assert!(storage.get(post_hash).is_none());
    }
}
