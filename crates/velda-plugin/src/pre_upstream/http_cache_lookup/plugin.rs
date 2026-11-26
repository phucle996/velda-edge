//! HTTP Cache Lookup Pre-Upstream Plugin (`http_cache_lookup`).
//!
//! Evaluated during the `pre_upstream` phase immediately before forwarding requests to backends.
//! Derives a zero-allocation 64-bit cache key from `(req.state.route_id, req.method, key_headers)`,
//! negotiates payload variants against `Accept-Encoding` and configured priority-ordered
//! `preferred_encodings`, and short-circuits with `Action::Respond(L7Response)` directly on HIT.
//!
//! ```text
//!   on_upstream_request(req)
//!     ├─ method not in cacheable_methods ──────────────────────► Action::Continue
//!     └─ hash = storage.route_key_hash(route_id, method, hdrs)
//!          └─► storage.lookup_variant(hash, accept_encoding, preferred_encodings, bypass_auth)
//!                ├─ Hit        ──► build L7Response (X-Cache: HIT, HeaderWorkspace + Bytes)
//!                │                 └─► Action::Respond(resp)
//!                ├─ AuthBypass ──► Action::Continue (upstream)
//!                └─ Miss       ──► Action::Continue (upstream ─► http_cache_store populates slot)
//! ```

use std::sync::Arc;
use std::time::Duration;

use http::{HeaderValue, Method};
use velda_core::buffer::header::HeaderKey;
use velda_core::lut::HttpMethod;
use velda_core::{Action, Body, HeaderWorkspace, L7Request, L7Response};

use crate::capability::{AccessLevel, PluginCapability, SupportedProtocols};
use crate::config::PluginConfig;
use crate::error::PluginError;
use crate::http_cache::{
    CompiledKeyHeader, HttpCacheRegistry, HttpCacheStorage, LookupOutcome, compile_key_headers,
};
use crate::post_response::response_compression::CompressionAlgorithm;
use crate::pre_upstream::PreUpstreamPlugin;

const KEY_AUTHORIZATION: HeaderKey = HeaderKey::from_static("authorization");
const KEY_ACCEPT_ENCODING: HeaderKey = HeaderKey::from_static("accept-encoding");
const KEY_AGE: HeaderKey = HeaderKey::from_static("age");
const KEY_CONTENT_TYPE: HeaderKey = HeaderKey::from_static("content-type");
const KEY_CONTENT_LENGTH: HeaderKey = HeaderKey::from_static("content-length");
const KEY_CONTENT_ENCODING: HeaderKey = HeaderKey::from_static("content-encoding");
const KEY_VARY: HeaderKey = HeaderKey::from_static("vary");
const KEY_X_CACHE: HeaderKey = HeaderKey::from_static("x-cache");

/// Configuration schema for [`HttpCacheLookupPlugin`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpCacheLookupConfig {
    /// Shared cache storage identifier (must match `http_cache_store`).
    pub zone: String,
    /// HTTP methods eligible for cache lookup (default: GET, HEAD).
    pub cacheable_methods: Vec<Method>,
    /// Whether to bypass cache lookup on requests carrying an `Authorization` header (RFC 9111 §3.5).
    pub bypass_authenticated_requests: bool,
    /// Custom request header names to incorporate into the cache key (`x-user-id`, `x-tenant-id`, ...).
    pub key_headers: Vec<String>,
    /// Ordered compression encodings negotiated on `Accept-Encoding` (`[]` = plain-text only).
    pub preferred_encodings: Vec<CompressionAlgorithm>,
    /// Allocated memory limit for this storage if created here (in megabytes).
    pub max_memory_mb: usize,
    /// Default TTL fallback in seconds if response does not specify Cache-Control.
    pub default_ttl_secs: u64,
}

impl Default for HttpCacheLookupConfig {
    fn default() -> Self {
        Self {
            zone: "default".to_string(),
            cacheable_methods: vec![Method::GET, Method::HEAD],
            bypass_authenticated_requests: true,
            key_headers: Vec::new(),
            preferred_encodings: vec![
                CompressionAlgorithm::Zstd,
                CompressionAlgorithm::Brotli,
                CompressionAlgorithm::Gzip,
            ],
            max_memory_mb: 256,
            default_ttl_secs: 300,
        }
    }
}

/// HTTP Cache Lookup Plugin instance.
pub struct HttpCacheLookupPlugin {
    pub id: String,
    pub config: HttpCacheLookupConfig,
    pub storage: Arc<HttpCacheStorage>,
    method_mask: u16,
    compiled_key_headers: Box<[CompiledKeyHeader]>,
    preferred_encodings: Box<[CompressionAlgorithm]>,
    auth_in_key: bool,
}

impl std::fmt::Debug for HttpCacheLookupPlugin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpCacheLookupPlugin")
            .field("id", &self.id)
            .field("config", &self.config)
            .field("storage", &self.storage.name)
            .finish()
    }
}

impl HttpCacheLookupPlugin {
    /// Constructs a new `HttpCacheLookupPlugin` with explicit configuration.
    pub fn new(id: impl Into<String>, config: HttpCacheLookupConfig) -> Self {
        let max_bytes = config.max_memory_mb.max(1) * 1024 * 1024;
        let default_ttl = Duration::from_secs(config.default_ttl_secs.max(1));
        let storage =
            HttpCacheRegistry::global().get_or_create(&config.zone, max_bytes, default_ttl);
        Self::with_storage(id, config, storage)
    }

    /// Constructs an instance binding to an existing storage (convenient for testing).
    pub fn with_storage(
        id: impl Into<String>,
        config: HttpCacheLookupConfig,
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
        let preferred_encodings = config.preferred_encodings.clone().into_boxed_slice();

        Self {
            id: id.into(),
            config,
            storage,
            method_mask,
            compiled_key_headers,
            preferred_encodings,
            auth_in_key,
        }
    }

    /// Deserializes and constructs an instance directly from a [`PluginConfig`].
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let cfg = super::validation::validate_http_cache_lookup_config(&config.id, &config.config)
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

        let mut preferred_encodings = Vec::with_capacity(cfg.preferred_encodings.len());
        for enc in &cfg.preferred_encodings {
            let algo = super::validation::parse_cache_encoding_token(enc).ok_or_else(|| {
                PluginError::Config(format!(
                    "Invalid encoding '{enc}' in plugin '{}'",
                    config.id
                ))
            })?;
            preferred_encodings.push(algo);
        }

        let _compiled = compile_key_headers(&cfg.key_headers).map_err(|e| {
            PluginError::Config(format!(
                "Invalid key_headers in plugin '{}': {e}",
                config.id
            ))
        })?;

        let parsed_cfg = HttpCacheLookupConfig {
            zone: cfg.zone,
            cacheable_methods,
            bypass_authenticated_requests: cfg.bypass_authenticated_requests,
            key_headers: cfg.key_headers,
            preferred_encodings,
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

    #[inline(always)]
    fn pbww(&self) -> u32 {
        0
    }

    fn on_upstream_request(&self, req: &mut L7Request, _targets: &mut u32) -> Action {
        if (self.method_mask & (1u16 << (req.state.method as u8))) == 0 {
            return Action::Continue;
        }

        let has_auth = req.headers.contains_prehashed(&KEY_AUTHORIZATION);
        let bypass_auth =
            has_auth && self.config.bypass_authenticated_requests && !self.auth_in_key;

        let key_hash =
            self.storage
                .route_key_hash(&req.state, &req.headers, &self.compiled_key_headers);
        let accept_encoding = req.headers.get_prehashed(&KEY_ACCEPT_ENCODING);

        match self.storage.lookup_variant(
            key_hash,
            accept_encoding,
            &self.preferred_encodings,
            bypass_auth,
        ) {
            LookupOutcome::Hit {
                status,
                payload,
                content_type,
                encoding,
                age_secs,
                has_variants,
            } => {
                let mut resp_headers = HeaderWorkspace::new(8);
                if let Some(ct) = content_type {
                    resp_headers.insert_prehashed(&KEY_CONTENT_TYPE, ct);
                }
                resp_headers
                    .insert_prehashed(&KEY_CONTENT_LENGTH, HeaderValue::from(payload.len()));
                resp_headers.insert_prehashed(&KEY_AGE, HeaderValue::from(age_secs));
                resp_headers.insert_prehashed(&KEY_X_CACHE, HeaderValue::from_static("HIT"));
                if let Some(enc) = encoding {
                    resp_headers.insert_prehashed(&KEY_CONTENT_ENCODING, enc);
                    resp_headers
                        .insert_prehashed(&KEY_VARY, HeaderValue::from_static("Accept-Encoding"));
                } else if has_variants || !self.preferred_encodings.is_empty() {
                    resp_headers
                        .insert_prehashed(&KEY_VARY, HeaderValue::from_static("Accept-Encoding"));
                }

                Action::Respond(L7Response::new(
                    status,
                    req.version,
                    resp_headers,
                    Body::Bytes(payload),
                ))
            }
            LookupOutcome::AuthBypass | LookupOutcome::Miss => Action::Continue,
        }
    }

    fn capability(&self) -> PluginCapability {
        PluginCapability::header_only(AccessLevel::Read)
    }

    fn supported_protocols(&self) -> SupportedProtocols {
        SupportedProtocols::ALL_HTTP
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http_cache::{CompressedVariants, StoreBytesParams, X_CACHE};
    use crate::post_response::PostResponsePlugin;
    use crate::post_response::http_cache_store::{HttpCacheStoreConfig, HttpCacheStorePlugin};
    use bytes::Bytes;
    use http::{StatusCode, Uri, Version};

    fn make_req(route_id: u16, method: Method, headers: Vec<(&str, &str)>) -> L7Request {
        let mut ws = HeaderWorkspace::new(8);
        for (k, v) in headers {
            ws.insert(k, HeaderValue::from_str(v).unwrap());
        }
        let mut req = L7Request::new(
            method,
            Uri::from_static("/ignored"),
            Version::HTTP_11,
            ws,
            Body::Empty,
        );
        req.state.set_route(route_id);
        req
    }

    #[test]
    fn test_config_validation_preferred_encodings_required_and_ordered() {
        // Missing preferred_encodings must fail sync validation.
        let missing_cfg = PluginConfig {
            id: "http_cache_lookup".into(),
            config: serde_json::json!({
                "zone": "api_cache",
                "cacheable_methods": ["GET", "HEAD"],
                "bypass_authenticated_requests": true,
                "key_headers": ["x-tenant-id"],
                "max_memory_mb": 64,
                "default_ttl_secs": 300
            }),
        };
        assert!(HttpCacheLookupPlugin::from_config(&missing_cfg).is_err());

        // Empty preferred_encodings must also fail sync validation.
        let empty_cfg = PluginConfig {
            id: "http_cache_lookup".into(),
            config: serde_json::json!({
                "zone": "api_cache",
                "cacheable_methods": ["GET", "HEAD"],
                "bypass_authenticated_requests": true,
                "key_headers": [],
                "preferred_encodings": [],
                "max_memory_mb": 64,
                "default_ttl_secs": 300
            }),
        };
        assert!(HttpCacheLookupPlugin::from_config(&empty_cfg).is_err());

        let custom_cfg = PluginConfig {
            id: "http_cache_lookup".into(),
            config: serde_json::json!({
                "zone": "api_cache",
                "cacheable_methods": ["GET"],
                "bypass_authenticated_requests": false,
                "key_headers": [],
                "preferred_encodings": ["gzip", "br"],
                "max_memory_mb": 64,
                "default_ttl_secs": 300
            }),
        };
        let custom_plugin = HttpCacheLookupPlugin::from_config(&custom_cfg).unwrap();
        assert_eq!(
            custom_plugin.config.preferred_encodings,
            vec![CompressionAlgorithm::Gzip, CompressionAlgorithm::Brotli]
        );

        let dup_cfg = PluginConfig {
            id: "http_cache_lookup".into(),
            config: serde_json::json!({
                "zone": "api_cache",
                "cacheable_methods": ["GET"],
                "bypass_authenticated_requests": true,
                "key_headers": [],
                "preferred_encodings": ["br", "brotli"],
                "max_memory_mb": 64,
                "default_ttl_secs": 300
            }),
        };
        assert!(HttpCacheLookupPlugin::from_config(&dup_cfg).is_err());

        let bad_codec_cfg = PluginConfig {
            id: "http_cache_lookup".into(),
            config: serde_json::json!({
                "zone": "api_cache",
                "cacheable_methods": ["GET"],
                "bypass_authenticated_requests": true,
                "key_headers": [],
                "preferred_encodings": ["deflate"],
                "max_memory_mb": 64,
                "default_ttl_secs": 300
            }),
        };
        assert!(HttpCacheLookupPlugin::from_config(&bad_codec_cfg).is_err());

        // Exceeding the hardware RAM ceiling must fail validation.
        let oom_cfg = PluginConfig {
            id: "http_cache_lookup".into(),
            config: serde_json::json!({
                "zone": "api_cache",
                "cacheable_methods": ["GET"],
                "bypass_authenticated_requests": true,
                "key_headers": [],
                "preferred_encodings": ["zstd"],
                "max_memory_mb": crate::http_cache::max_cache_memory_mb() + 1,
                "default_ttl_secs": 300
            }),
        };
        assert!(HttpCacheLookupPlugin::from_config(&oom_cfg).is_err());

        let unknown_legacy = PluginConfig {
            id: "http_cache_lookup".into(),
            config: serde_json::json!({
                "zone": "api_cache",
                "cacheable_methods": ["GET"],
                "ignore_query_order": true,
                "bypass_authenticated_requests": true,
                "key_headers": [],
                "preferred_encodings": ["zstd"],
                "max_memory_mb": 64,
                "default_ttl_secs": 300
            }),
        };
        let err = HttpCacheLookupPlugin::from_config(&unknown_legacy).unwrap_err();
        assert!(err.to_string().contains("unknown field"));

        // Aggregate across distinct zones exceeding the ceiling must fail cross-plugin validation.
        let half_ceil = (crate::http_cache::max_cache_memory_mb() / 2) + 1;
        let zone_a = PluginConfig {
            id: "http_cache_lookup".into(),
            config: serde_json::json!({
                "zone": "zone_a",
                "cacheable_methods": ["GET"],
                "bypass_authenticated_requests": true,
                "key_headers": [],
                "preferred_encodings": ["zstd"],
                "max_memory_mb": half_ceil,
                "default_ttl_secs": 300
            }),
        };
        let zone_b = PluginConfig {
            id: "http_cache_store".into(),
            config: serde_json::json!({
                "zone": "zone_b",
                "cacheable_status": [200],
                "max_payload_bytes": 1048576,
                "default_ttl_secs": 300,
                "respect_cache_control": true,
                "bypass_authenticated_requests": true,
                "key_headers": [],
                "async_precompression": true,
                "max_memory_mb": half_ceil
            }),
        };
        assert!(crate::validate_plugin_dependencies(&[zone_a, zone_b]).is_err());
    }

    #[test]
    fn test_two_phase_shared_storage_hit_and_variant_priority() {
        velda_core::clock::init_clock();
        let storage = Arc::new(HttpCacheStorage::new(
            "test_two_phase",
            4 * 1024 * 1024,
            Duration::from_secs(60),
        ));

        let lookup = HttpCacheLookupPlugin::with_storage(
            "lookup",
            HttpCacheLookupConfig {
                zone: "test_two_phase".into(),
                cacheable_methods: vec![Method::GET],
                bypass_authenticated_requests: true,
                key_headers: vec!["x-tenant-id".into()],
                preferred_encodings: vec![CompressionAlgorithm::Gzip, CompressionAlgorithm::Brotli],
                max_memory_mb: 4,
                default_ttl_secs: 60,
            },
            Arc::clone(&storage),
        );

        let store = HttpCacheStorePlugin::with_storage(
            "store",
            HttpCacheStoreConfig {
                zone: "test_two_phase".into(),
                key_headers: vec!["x-tenant-id".into()],
                async_precompression: false,
                ..Default::default()
            },
            Arc::clone(&storage),
        );

        let mut target_upstream = 0u32;

        // 1. Initial lookup misses -> forwards to upstream.
        let mut req_a = make_req(10, Method::GET, vec![("x-tenant-id", "acme")]);
        assert!(
            lookup
                .on_upstream_request(&mut req_a, &mut target_upstream)
                .is_continue()
        );

        // 2. Upstream returns plain-text compressible JSON -> store populates raw + variants.
        let json_body = "{\"items\":[1,2,3,4,5],\"padding\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"}";
        let mut upstream_resp = L7Response::from_bytes(StatusCode::OK, json_body.as_bytes());
        upstream_resp.headers.insert(
            http::header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
        store.on_response(&req_a.state, &req_a.headers, &mut upstream_resp);
        assert_eq!(upstream_resp.headers.get(&X_CACHE).unwrap(), "MISS");

        // 3. Client without Accept-Encoding hits raw plain text via Action::Respond.
        let mut req_plain = make_req(10, Method::GET, vec![("x-tenant-id", "acme")]);
        match lookup.on_upstream_request(&mut req_plain, &mut target_upstream) {
            Action::Respond(resp) => {
                assert_eq!(resp.status, StatusCode::OK);
                assert_eq!(resp.headers.get(&X_CACHE).unwrap(), "HIT");
                assert!(resp.headers.get(http::header::CONTENT_ENCODING).is_none());
                assert_eq!(resp.body.as_slice(), json_body.as_bytes());
            }
            other => panic!("expected Action::Respond on HIT, got {other:?}"),
        }

        // 4. Client accepting br + gzip + zstd selects `gzip` per configured priority order [Gzip, Brotli].
        let mut req_enc = make_req(
            10,
            Method::GET,
            vec![
                ("x-tenant-id", "acme"),
                ("accept-encoding", "zstd, br, gzip"),
            ],
        );
        match lookup.on_upstream_request(&mut req_enc, &mut target_upstream) {
            Action::Respond(resp) => {
                assert_eq!(resp.headers.get(&X_CACHE).unwrap(), "HIT");
                assert_eq!(
                    resp.headers.get(http::header::CONTENT_ENCODING).unwrap(),
                    "gzip"
                );
                assert_eq!(
                    resp.headers.get(http::header::VARY).unwrap(),
                    "Accept-Encoding"
                );
                assert_eq!(
                    resp.headers.get(http::header::CONTENT_LENGTH).unwrap(),
                    resp.body.len().to_string().as_str()
                );
            }
            other => panic!("expected Action::Respond on HIT, got {other:?}"),
        }

        // 5. Different tenant (`globex`) or different route (`11`) must miss (isolated cache keys).
        let mut req_other_tenant = make_req(10, Method::GET, vec![("x-tenant-id", "globex")]);
        assert!(
            lookup
                .on_upstream_request(&mut req_other_tenant, &mut target_upstream)
                .is_continue()
        );
        let mut req_other_route = make_req(11, Method::GET, vec![("x-tenant-id", "acme")]);
        assert!(
            lookup
                .on_upstream_request(&mut req_other_route, &mut target_upstream)
                .is_continue()
        );
    }

    #[test]
    fn test_missing_accept_encoding_misses_when_only_compressed_then_fills_raw() {
        velda_core::clock::init_clock();
        let storage = Arc::new(HttpCacheStorage::new(
            "test_raw_fallback",
            4 * 1024 * 1024,
            Duration::from_secs(60),
        ));

        let lookup = HttpCacheLookupPlugin::with_storage(
            "lookup",
            HttpCacheLookupConfig {
                zone: "test_raw_fallback".into(),
                ..Default::default()
            },
            Arc::clone(&storage),
        );
        let store = HttpCacheStorePlugin::with_storage(
            "store",
            HttpCacheStoreConfig {
                zone: "test_raw_fallback".into(),
                async_precompression: false,
                ..Default::default()
            },
            Arc::clone(&storage),
        );

        let req_no_ae = make_req(5, Method::GET, vec![]);
        let hash = storage.route_key_hash(&req_no_ae.state, &req_no_ae.headers, &[]);
        // Seed slot with zstd only (no raw plain-text variant).
        storage.store_bytes(StoreBytesParams {
            hash,
            status: StatusCode::OK,
            content_type: Some(HeaderValue::from_static("text/plain")),
            ttl: Duration::from_secs(60),
            encoding: Some("zstd"),
            payload: b"zstd-bytes",
            is_public: true,
        });
        let _ = storage.merge_variants(
            hash,
            CompressedVariants {
                zstd: Some(Bytes::from_static(b"zstd-bytes")),
                gzip: None,
                brotli: None,
            },
        );

        // Client without Accept-Encoding -> cannot use compressed variant and !has_raw -> MISS!
        let mut req_miss = make_req(5, Method::GET, vec![]);
        let mut upstream = 0u32;
        assert!(
            lookup
                .on_upstream_request(&mut req_miss, &mut upstream)
                .is_continue()
        );

        // Upstream responds with uncompressed plain text -> post_response fills raw into the slot.
        let mut resp = L7Response::from_bytes(StatusCode::OK, b"plain-text-payload".as_slice());
        resp.headers.insert(
            http::header::CONTENT_TYPE,
            HeaderValue::from_static("text/plain"),
        );
        store.on_response(&req_miss.state, &req_miss.headers, &mut resp);

        // Next client without Accept-Encoding gets HIT with plain text, while zstd stays intact!
        let mut req_hit_raw = make_req(5, Method::GET, vec![]);
        match lookup.on_upstream_request(&mut req_hit_raw, &mut upstream) {
            Action::Respond(hit) => {
                assert!(hit.headers.get(http::header::CONTENT_ENCODING).is_none());
                assert_eq!(hit.body.as_slice(), b"plain-text-payload");
            }
            other => panic!("expected raw HIT, got {other:?}"),
        }

        let mut req_hit_zstd = make_req(5, Method::GET, vec![("accept-encoding", "zstd")]);
        match lookup.on_upstream_request(&mut req_hit_zstd, &mut upstream) {
            Action::Respond(hit) => {
                assert_eq!(
                    hit.headers.get(http::header::CONTENT_ENCODING).unwrap(),
                    "zstd"
                );
                assert_eq!(hit.body.as_slice(), b"zstd-bytes");
            }
            other => panic!("expected zstd HIT, got {other:?}"),
        }
    }
}
