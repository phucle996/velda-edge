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

use crate::capability::{AccessLevel, PluginCapability, SupportedProtocols};
use crate::error::PluginError;
use crate::http_cache::{
    CacheEntry, HttpCacheZone, HttpCacheZoneRegistry, filter_cacheable_headers,
    parse_cache_control_ttl,
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
                max_memory_mb,
            },
        ))
    }
}

impl PostResponsePlugin for HttpCacheStorePlugin {
    #[inline(always)]
    fn id(&self) -> &str {
        &self.id
    }

    fn on_response(&self, resp: &mut L7Response) {
        // 1. If already served from cache (HIT), skip storing
        if let Some(val) = resp.headers.get("x-cache")
            && val == "HIT"
        {
            return;
        }

        // 2. Extract cache key: prioritize transient headers injected by http_cache_lookup
        let key_str = resp
            .headers
            .remove("x-velda-cache-key")
            .or_else(|| resp.headers.remove("x-cache-key"))
            .and_then(|v| v.to_str().ok().map(|s| s.to_string()))
            .or_else(|| {
                // Secondary fallback: lookup via request-id in pending table
                resp.headers
                    .get("x-request-id")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|req_id| self.zone.take_pending_key(req_id))
            })
            .or_else(|| {
                // Tertiary fallback: check x-request-uri or x-original-uri
                resp.headers
                    .get("x-request-uri")
                    .or_else(|| resp.headers.get("x-original-uri"))
                    .and_then(|v| v.to_str().ok().map(|s| format!("GET::{s}")))
            });

        // Strip internal cache metadata from downstream response
        resp.headers.remove("x-velda-cache-hash");

        let Some(cache_key) = key_str else {
            // Cannot associate response with request key, cleanly bypass
            return;
        };

        // 3. Status eligibility check
        if !self.config.cacheable_status.contains(&resp.status) {
            resp.headers
                .insert("x-cache", HeaderValue::from_static("BYPASS"));
            return;
        }

        // 4. Cache-Control directives check (RFC 9111)
        let mut computed_ttl = Duration::from_secs(self.config.default_ttl_secs.max(1));

        if self.config.respect_cache_control
            && let Some(cc) = resp
                .headers
                .get(http::header::CACHE_CONTROL)
                .and_then(|v| v.to_str().ok())
        {
            // Must not cache if no-store or private is present
            if cc.contains("no-store") || cc.contains("private") {
                resp.headers
                    .insert("x-cache", HeaderValue::from_static("BYPASS"));
                return;
            }

            if let Some(parsed_ttl) = parse_cache_control_ttl(cc) {
                if parsed_ttl.is_zero() {
                    resp.headers
                        .insert("x-cache", HeaderValue::from_static("BYPASS"));
                    return;
                }
                computed_ttl = parsed_ttl;
            }
        }

        // 5. Payload size bounds check
        let body_bytes = match &resp.body {
            Body::Bytes(b) => {
                if b.len() > self.config.max_payload_bytes {
                    resp.headers
                        .insert("x-cache", HeaderValue::from_static("BYPASS"));
                    return;
                }
                b.clone()
            }
            Body::Empty => bytes::Bytes::new(),
        };

        // 6. Filter safe response headers (stripping Set-Cookie, tracing, hop-by-hop)
        let clean_headers = filter_cacheable_headers(&resp.headers);

        // 7. Store entry into designated cache zone
        let hash = HttpCacheZone::hash_key(&cache_key);
        self.zone.store(
            hash,
            CacheEntry {
                key: cache_key,
                status: resp.status,
                headers: clean_headers,
                body: body_bytes,
                created_at: std::time::Instant::now(),
                ttl: computed_ttl,
            },
        );

        // 8. Inject response diagnostic headers
        resp.headers
            .insert("x-cache", HeaderValue::from_static("MISS"));
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

        plugin.on_response(&mut resp);

        // Header check: internal key removed, x-cache and age injected
        assert!(!resp.headers.contains_key("x-velda-cache-key"));
        assert_eq!(resp.headers.get("x-cache").unwrap(), "MISS");
        assert_eq!(resp.headers.get("age").unwrap(), "0");

        // Verify stored entry in zone
        let hash = HttpCacheZone::hash_key("GET:api.example.com:/items");
        let stored = zone.get(hash).expect("Must be stored in zone");
        assert_eq!(stored.status, StatusCode::OK);
        assert_eq!(
            stored.body,
            bytes::Bytes::from_static(b"{\"data\":[1,2,3]}")
        );
        assert_eq!(stored.ttl, Duration::from_secs(120));
        assert!(stored.headers.contains_key(CONTENT_TYPE));
        assert!(!stored.headers.contains_key(SET_COOKIE)); // Set-Cookie strictly stripped!
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

        plugin.on_response(&mut resp);

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

        plugin.on_response(&mut resp);

        assert_eq!(resp.headers.get("x-cache").unwrap(), "BYPASS");
        let hash = HttpCacheZone::hash_key("GET:api.example.com:/large");
        assert!(zone.get(hash).is_none());
    }
}
