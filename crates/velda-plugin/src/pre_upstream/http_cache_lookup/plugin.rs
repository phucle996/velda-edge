//! HTTP Cache Lookup Pre-Upstream Plugin (`http_cache_lookup`).
//!
//! Evaluated during the `pre_upstream` phase immediately before forwarding requests to backends.
//! Computes zero-allocation canonical hashes and serves cached payloads directly from the
//! contiguous 2 MiB slab arena on HIT.
//!
//! ```text
//!   on_upstream_request(req)
//!     ├─ method not cacheable / client no-cache ──► Action::Continue
//!     └─ hash = hash_request_key_zero_alloc()  [0 heap alloc]
//!          └─► storage.lookup_variant(hash, accept_encoding, bypass_auth)
//!                ├─ Hit        ──► Action::ServeCached(directive)  [0-copy SlabLease]
//!                ├─ AuthBypass ──► Action::Continue
//!                └─ Miss       ──► req.headers.set_u64(X_VELDA_CACHE_HASH, hash) ─► Continue
//! ```

use std::sync::Arc;
use std::time::Duration;

use http::Method;
use velda_core::{Action, CachedResponseDirective, L7Request};

use crate::capability::{AccessLevel, PluginCapability, SupportedProtocols};
use crate::config::PluginConfig;
use crate::error::PluginError;
use crate::http_cache::{
    HttpCacheRegistry, HttpCacheStorage, LookupOutcome, hash_request_key_zero_alloc,
    parse_cache_control_fast,
};
use crate::pre_upstream::PreUpstreamPlugin;

const STACK_KEY_HEADERS_CAP: usize = 16;

/// Configuration schema for [`HttpCacheLookupPlugin`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpCacheLookupConfig {
    /// Shared cache storage identifier.
    pub zone: String,
    /// HTTP methods eligible for cache lookup (default: GET, HEAD).
    pub cacheable_methods: Vec<Method>,
    /// Whether to sort query string keys alphabetically before hashing.
    pub ignore_query_order: bool,
    /// Honor client `Cache-Control: no-cache` / `Pragma: no-cache` directives.
    pub honor_client_no_cache: bool,
    /// Whether to bypass cache lookup on requests carrying an `Authorization` header (RFC 9111 §3.5).
    pub bypass_authenticated_requests: bool,
    /// Custom request header names to incorporate into the cache key for segmented caching.
    pub key_headers: Vec<String>,
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
    pub storage: Arc<HttpCacheStorage>,
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

    #[inline]
    fn compute_hash(&self, req: &L7Request) -> u64 {
        let host = req
            .headers
            .get(http::header::HOST)
            .and_then(|v| v.to_str().ok());

        if self.config.key_headers.is_empty() {
            return hash_request_key_zero_alloc(
                &req.method,
                host,
                req.uri.path(),
                req.uri.query(),
                self.config.ignore_query_order,
                None,
            );
        }

        if self.config.key_headers.len() <= STACK_KEY_HEADERS_CAP {
            let mut stack_buf = [("", ""); STACK_KEY_HEADERS_CAP];
            let mut count = 0usize;
            for hdr_name in &self.config.key_headers {
                if let Some(val) = req.headers.get(hdr_name).and_then(|v| v.to_str().ok())
                    && let Some(slot) = stack_buf.get_mut(count)
                {
                    *slot = (hdr_name.as_str(), val);
                    count += 1;
                }
            }
            return hash_request_key_zero_alloc(
                &req.method,
                host,
                req.uri.path(),
                req.uri.query(),
                self.config.ignore_query_order,
                stack_buf.get(..count),
            );
        }

        self.compute_hash_heap(req, host)
    }

    #[cold]
    #[inline(never)]
    fn compute_hash_heap(&self, req: &L7Request, host: Option<&str>) -> u64 {
        let mut extracted = Vec::with_capacity(self.config.key_headers.len());
        for hdr_name in &self.config.key_headers {
            if let Some(val) = req.headers.get(hdr_name).and_then(|v| v.to_str().ok()) {
                extracted.push((hdr_name.as_str(), val));
            }
        }
        hash_request_key_zero_alloc(
            &req.method,
            host,
            req.uri.path(),
            req.uri.query(),
            self.config.ignore_query_order,
            Some(&extracted),
        )
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

    fn on_upstream_request(&self, req: &mut L7Request, _target_upstream: &mut u16) -> Action {
        if !self.config.cacheable_methods.contains(&req.method) {
            return Action::Continue;
        }

        if self.config.honor_client_no_cache {
            let cc_bypass = req
                .headers
                .get(http::header::CACHE_CONTROL)
                .is_some_and(|v| parse_cache_control_fast(v.as_bytes()).is_no_cache_or_no_store());
            let pragma_bypass = !cc_bypass
                && req.headers.get(http::header::PRAGMA).is_some_and(|v| {
                    v.as_bytes()
                        .windows(8)
                        .any(|w| w.eq_ignore_ascii_case(b"no-cache"))
                });
            if cc_bypass || pragma_bypass {
                return Action::Continue;
            }
        }

        let has_auth = req.headers.contains_key(http::header::AUTHORIZATION);
        let bypass_auth =
            has_auth && self.config.bypass_authenticated_requests && !self.auth_in_key;

        let key_hash = self.compute_hash(req);
        let accept_encoding = req.headers.get(http::header::ACCEPT_ENCODING);

        match self
            .storage
            .lookup_variant(key_hash, accept_encoding, bypass_auth)
        {
            LookupOutcome::Hit {
                status,
                payload,
                content_type,
                encoding,
                age_secs,
                has_variants,
            } => Action::ServeCached(CachedResponseDirective::new(
                status,
                payload,
                content_type,
                encoding,
                age_secs,
                has_variants,
            )),
            LookupOutcome::AuthBypass => Action::Continue,
            LookupOutcome::Miss => {
                req.headers.cache_hash = if key_hash == 0 { 1 } else { key_hash };
                Action::Continue
            }
        }
    }

    fn capability(&self) -> PluginCapability {
        PluginCapability::header_only(AccessLevel::Read)
    }

    fn supported_protocols(&self) -> SupportedProtocols {
        SupportedProtocols::ALL_HTTP
    }
}
