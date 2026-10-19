//! HTTP Cache Lookup Pre-Upstream Plugin (`http_cache_lookup`).
//!
//! Evaluated during the `pre_upstream` phase immediately before forwarding requests to backends.
//! Computes canonical cache keys, queries the shared [`HttpCacheZone`], and returns an
//! immediate [`Action::Respond`] with zero-copy payload on CACHE HIT.
//! On CACHE MISS, enriches the request with cache key metadata and yields [`Action::Continue`].

use std::sync::Arc;
use std::time::Duration;

use http::header::HeaderValue;
use http::{Method, Version};
use velda_core::{Action, Body, L7Request, L7Response};
use velda_sync::post_sync::plugin::PluginConfig;

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
        let defaults = HttpCacheLookupConfig::default();

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

        let mut cacheable_methods = defaults.cacheable_methods;
        if let Some(arr) = raw.get("cacheable_methods").and_then(|v| v.as_array()) {
            let mut methods = Vec::new();
            for item in arr {
                if let Some(m_str) = item.as_str()
                    && let Ok(method) = Method::from_bytes(m_str.trim().as_bytes())
                {
                    methods.push(method);
                }
            }
            if !methods.is_empty() {
                cacheable_methods = methods;
            }
        }

        let ignore_query_order = raw
            .get("ignore_query_order")
            .and_then(|v| v.as_bool())
            .unwrap_or(defaults.ignore_query_order);

        let honor_client_no_cache = raw
            .get("honor_client_no_cache")
            .and_then(|v| v.as_bool())
            .unwrap_or(defaults.honor_client_no_cache);

        let max_memory_mb = raw
            .get("max_memory_mb")
            .and_then(|v| v.as_u64())
            .map(|v| v as usize)
            .unwrap_or(defaults.max_memory_mb);

        let default_ttl_secs = raw
            .get("default_ttl_secs")
            .and_then(|v| v.as_u64())
            .unwrap_or(defaults.default_ttl_secs);

        Ok(Self::new(
            &config.id,
            HttpCacheLookupConfig {
                zone,
                cacheable_methods,
                ignore_query_order,
                honor_client_no_cache,
                max_memory_mb,
                default_ttl_secs,
            },
        ))
    }
}

impl PreUpstreamPlugin for HttpCacheLookupPlugin {
    #[inline(always)]
    fn id(&self) -> &str {
        &self.id
    }

    fn on_upstream_request(&self, req: &mut L7Request) -> Action {
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

        // 3. Compute canonical cache key
        let host = req
            .headers
            .get(http::header::HOST)
            .and_then(|v| v.to_str().ok());
        let (key_str, key_hash) = HttpCacheZone::compute_cache_key(
            &req.method,
            host,
            req.uri.path(),
            req.uri.query(),
            self.config.ignore_query_order,
        );

        // 4. Query the in-memory cache zone
        if let Some(entry) = self.zone.get(key_hash) {
            // CACHE HIT: Construct response directly from in-memory zero-copy bytes
            let mut resp_headers = entry.headers.clone();
            resp_headers.insert("age", HeaderValue::from(entry.age_secs()));
            resp_headers.insert("x-cache", HeaderValue::from_static("HIT"));

            let resp = L7Response {
                status: entry.status,
                version: Version::HTTP_11,
                headers: resp_headers,
                body: Body::Bytes(entry.body),
            };

            return Action::Respond(resp);
        }

        // 5. CACHE MISS: Enrich request headers with transient key metadata for post-response store
        if let Ok(val) = HeaderValue::from_str(&key_str) {
            req.headers.insert("x-velda-cache-key", val);
        }
        req.headers
            .insert("x-velda-cache-hash", HeaderValue::from(key_hash));

        // If request ID is present, associate it in the zone's pending table
        if let Some(req_id) = req
            .headers
            .get("x-request-id")
            .and_then(|v| v.to_str().ok())
        {
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
        let action = plugin.on_upstream_request(&mut req);
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
            CacheEntry {
                key: "GET:api.example.com:/products?page=1&sort=asc".into(),
                status: StatusCode::OK,
                headers: HeaderMap::new(),
                body: bytes::Bytes::from_static(b"[{\"id\":1}]"),
                created_at: std::time::Instant::now(),
                ttl: Duration::from_secs(60),
            },
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

        let action2 = plugin.on_upstream_request(&mut req2);
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

        let action = plugin.on_upstream_request(&mut req);
        assert!(action.is_continue());
        assert!(!req.headers.contains_key("x-velda-cache-key"));
    }
}
