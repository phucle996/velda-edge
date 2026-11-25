//! HTTP Cache Store Configuration Schema & Semantic Validation.
//!
//! ```text
//!   PluginConfig (JSON) ─► serde_json::from_value ─► zone / methods / status / bounds / key_headers
//!                                                        └─► Ok(HttpCacheStoreConfig) | Err
//! ```

use serde::{Deserialize, Serialize};

use crate::error::PluginError;
use crate::http_cache::{
    CACHE_MEMORY_CEILING_PERCENT, SLAB_SIZE, is_valid_header_name, max_cache_memory_mb,
};

fn default_cacheable_methods() -> Vec<String> {
    vec!["GET".to_string(), "HEAD".to_string()]
}

fn is_valid_http_method(m: &str) -> bool {
    velda_core::lut::HttpMethod::from_bytes(m.trim().as_bytes())
        != velda_core::lut::HttpMethod::Unknown
}

/// Declarative configuration for the HTTP Cache Store plugin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HttpCacheStoreConfig {
    /// Target designated cache storage zone name.
    pub zone: String,
    /// HTTP methods eligible for caching (default: `["GET", "HEAD"]`).
    #[serde(default = "default_cacheable_methods")]
    pub cacheable_methods: Vec<String>,
    /// HTTP status codes eligible for caching (e.g. `[200, 203, 204, 206, 300, 301, 404]`).
    pub cacheable_status: Vec<u16>,
    /// Maximum payload size in bytes eligible for caching (`1..=2_097_152` per 2 MiB slab).
    pub max_payload_bytes: usize,
    /// Default TTL fallback in seconds when upstream headers omit explicit directives.
    pub default_ttl_secs: u64,
    /// Honor upstream `Cache-Control: no-cache` / `no-store` / `private` (default: true).
    pub respect_cache_control: bool,
    /// Whether to bypass storing responses for requests carrying an `Authorization` header.
    pub bypass_authenticated_requests: bool,
    /// Custom request header names incorporated into the cache key (must match `http_cache_lookup`).
    pub key_headers: Vec<String>,
    /// Whether to offload variant pre-compression (`gzip`, `brotli`, `zstd`) asynchronously.
    pub async_precompression: bool,
    /// Allocated memory limit for this zone if created here (in megabytes).
    pub max_memory_mb: usize,
}

/// Validates HTTP Cache Store configuration strictly according to production standards.
pub fn validate_http_cache_store_config(
    plugin_id: &str,
    config_val: &serde_json::Value,
) -> Result<HttpCacheStoreConfig, PluginError> {
    let cfg: HttpCacheStoreConfig = serde_json::from_value(config_val.clone()).map_err(|e| {
        PluginError::Config(format!(
            "Plugin '{plugin_id}' has malformed or incomplete http_cache_store config: {e}"
        ))
    })?;

    if cfg.zone.trim().is_empty() {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': zone cannot be empty"
        )));
    }

    if cfg.cacheable_methods.is_empty() {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': cacheable_methods cannot be empty"
        )));
    }
    for m in &cfg.cacheable_methods {
        if !is_valid_http_method(m) {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': invalid HTTP method '{m}' in cacheable_methods"
            )));
        }
    }

    if cfg.cacheable_status.is_empty() {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': cacheable_status cannot be empty"
        )));
    }
    for s in &cfg.cacheable_status {
        if !(200..=599).contains(s) {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': status code {s} in cacheable_status must be between 200 and 599"
            )));
        }
    }

    if !(1..=SLAB_SIZE).contains(&cfg.max_payload_bytes) {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': max_payload_bytes ({}) must be between 1 and {SLAB_SIZE} bytes",
            cfg.max_payload_bytes
        )));
    }

    if !(1..=31_536_000).contains(&cfg.default_ttl_secs) {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': default_ttl_secs ({}) must be between 1 and 31,536,000 seconds",
            cfg.default_ttl_secs
        )));
    }

    let max_cache_mb = max_cache_memory_mb();
    if !(1..=max_cache_mb).contains(&cfg.max_memory_mb) {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': max_memory_mb ({}) must be between 1 and {max_cache_mb} MB ({CACHE_MEMORY_CEILING_PERCENT}% of system memory ceiling)",
            cfg.max_memory_mb
        )));
    }

    for h in &cfg.key_headers {
        if !is_valid_header_name(h) {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': invalid custom header token '{h}' in key_headers"
            )));
        }
    }

    Ok(cfg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_store_config_validation_rules_and_deny_unknown_fields() {
        let valid = json!({
            "zone": "api_cache",
            "cacheable_methods": ["GET", "HEAD"],
            "cacheable_status": [200, 301],
            "max_payload_bytes": 1048576,
            "default_ttl_secs": 300,
            "respect_cache_control": true,
            "bypass_authenticated_requests": true,
            "key_headers": ["x-tenant-id"],
            "async_precompression": true,
            "max_memory_mb": 64
        });
        assert!(validate_http_cache_store_config("http_cache_store", &valid).is_ok());

        let unknown = json!({
            "zone": "api_cache",
            "cacheable_methods": ["GET"],
            "cacheable_status": [200],
            "max_payload_bytes": 1024,
            "default_ttl_secs": 60,
            "respect_cache_control": true,
            "bypass_authenticated_requests": true,
            "key_headers": [],
            "async_precompression": false,
            "max_memory_mb": 64,
            "legacy_field": true
        });
        let err = validate_http_cache_store_config("http_cache_store", &unknown).unwrap_err();
        assert!(err.to_string().contains("unknown field"));

        let bad_method = json!({
            "zone": "api_cache",
            "cacheable_methods": ["BAD METHOD!"],
            "cacheable_status": [200],
            "max_payload_bytes": 1024,
            "default_ttl_secs": 60,
            "respect_cache_control": true,
            "bypass_authenticated_requests": true,
            "key_headers": [],
            "async_precompression": false,
            "max_memory_mb": 64
        });
        assert!(validate_http_cache_store_config("http_cache_store", &bad_method).is_err());

        let oom = json!({
            "zone": "api_cache",
            "cacheable_methods": ["GET"],
            "cacheable_status": [200],
            "max_payload_bytes": 1024,
            "default_ttl_secs": 60,
            "respect_cache_control": true,
            "bypass_authenticated_requests": true,
            "key_headers": [],
            "async_precompression": false,
            "max_memory_mb": max_cache_memory_mb() + 1
        });
        assert!(validate_http_cache_store_config("http_cache_store", &oom).is_err());
    }
}
