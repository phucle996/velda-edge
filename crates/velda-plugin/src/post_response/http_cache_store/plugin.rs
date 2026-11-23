//! HTTP Cache Store Post-Response Plugin (`http_cache_store`).
//!
//! Evaluated on upstream responses during the `post_response` phase. Validates cacheability,
//! streams eligible payloads directly into the contiguous 2 MiB DRAM slab pool via non-temporal
//! stores, and sets diagnostic `X-Cache` / `Age` headers.
//!
//! ```text
//!   on_response(req_headers, resp)
//!     ├─ X-Cache == "HIT" ────────────────────────────────► return
//!     ├─ strip X_VELDA_CACHE_* + resolve hash (0 alloc) ──► None ─► return
//!     ├─ status / Cache-Control / Auth / size gates fail ─► X-Cache: BYPASS ─► return
//!     └─► storage.store_bytes()  [stream_payload_to_dram into 2MB slab]
//!           ├─ X-Cache: MISS, Age: 0
//!           └─ optional variant precompression (raw & len >= 256 & compressible MIME)
//! ```

use std::sync::Arc;
use std::time::Duration;

use http::StatusCode;
use http::header::HeaderValue;
use velda_core::L7Response;

use crate::capability::{AccessLevel, PluginCapability, SupportedProtocols};
use crate::config::PluginConfig;
use crate::error::PluginError;
use crate::http_cache::{
    CompressedVariants, HttpCacheRegistry, HttpCacheStorage, SLAB_SIZE, StoreBytesParams, X_CACHE,
    X_VELDA_CACHE_HASH, X_VELDA_CACHE_KEY, hash_key, parse_cache_control_fast,
};
use crate::post_response::PostResponsePlugin;

/// Configuration schema for [`HttpCacheStorePlugin`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpCacheStoreConfig {
    /// Shared cache storage identifier (must match `http_cache_lookup`).
    pub zone: String,
    /// HTTP status codes eligible for caching (default: 200, 203, 204, 206, 300, 301, 404).
    pub cacheable_status: Vec<StatusCode>,
    /// Maximum payload size in bytes eligible for caching (default: 2 MiB).
    pub max_payload_bytes: usize,
    /// Default TTL fallback in seconds when upstream does not emit Cache-Control max-age.
    pub default_ttl_secs: u64,
    /// Honor upstream `Cache-Control: no-store` / `Cache-Control: private` (default: true).
    pub respect_cache_control: bool,
    /// Bypass storing responses carrying an `Authorization` header unless public/partitioned.
    pub bypass_authenticated_requests: bool,
    /// Custom request header names incorporated into the cache key for segmented caching.
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
        let auth_in_key = config
            .key_headers
            .iter()
            .any(|h| h.eq_ignore_ascii_case("authorization"));

        Self {
            id: id.into(),
            config,
            storage,
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

    fn on_response(&self, req_headers: &velda_core::HeaderWorkspace, resp: &mut L7Response) {
        // 1. Skip responses already served from cache
        if resp.headers.get(&X_CACHE).is_some_and(|v| v == "HIT") {
            return;
        }

        // 2. Resolve hash from fast numeric workspace register or fallback header, then strip internals
        let precomputed_hash = req_headers.get_u64(&X_VELDA_CACHE_HASH);
        let resp_fallback_key = resp.headers.remove(&X_VELDA_CACHE_KEY);
        resp.headers.remove(&X_VELDA_CACHE_HASH);

        let Some(hash) = precomputed_hash.or_else(|| {
            req_headers
                .get(&X_VELDA_CACHE_KEY)
                .and_then(|v| v.to_str().ok())
                .or_else(|| resp_fallback_key.as_ref().and_then(|v| v.to_str().ok()))
                .map(hash_key)
        }) else {
            return;
        };

        // 3. Status eligibility check
        if !self.config.cacheable_status.contains(&resp.status) {
            resp.headers
                .insert(X_CACHE, HeaderValue::from_static("BYPASS"));
            return;
        }

        // 4. Cache-Control directives check (RFC 9111 single-pass zero-alloc tokenizer)
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

        // 5. RFC 9111 §3.5 Authentication check
        let has_auth = req_headers.contains_key(http::header::AUTHORIZATION);
        if has_auth && self.config.bypass_authenticated_requests && !self.auth_in_key && !is_public
        {
            resp.headers
                .insert(X_CACHE, HeaderValue::from_static("BYPASS"));
            return;
        }

        // 6. Payload bounds check (fits inside a single 2 MiB contiguous slab)
        if resp.body.is_empty() {
            return;
        }
        let body_len = resp.body.len();
        if body_len > self.config.max_payload_bytes || body_len > SLAB_SIZE {
            resp.headers
                .insert(X_CACHE, HeaderValue::from_static("BYPASS"));
            return;
        }

        let content_type = resp.headers.get(http::header::CONTENT_TYPE).cloned();
        let encoding = resp
            .headers
            .get(http::header::CONTENT_ENCODING)
            .and_then(|v| v.to_str().ok())
            .map(str::trim);

        // 7. Stream payload directly into contiguous 2 MiB slab pool (zero intermediate Vec)
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

        // 8. Optional variant pre-compression when stored response is uncompressed canonical payload
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
