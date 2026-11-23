//! HTTP Cache Store Configuration Schema & Semantic Validation.
//!
//! ```text
//!   PluginConfig (JSON) ─► serde_json::from_value ─► zone / status / bounds / key_headers
//!                                                        └─► Ok(HttpCacheStoreConfig) | Err
//! ```

use serde::{Deserialize, Serialize};

use crate::error::PluginError;
use crate::http_cache::{MAX_CACHE_MEMORY_MB, SLAB_SIZE, is_valid_header_name};

/// Declarative configuration for the HTTP Cache Store plugin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HttpCacheStoreConfig {
    /// Target designated cache storage zone name.
    pub zone: String,
    /// HTTP status codes eligible for caching (e.g. `[200, 203, 204, 206, 300, 301, 404]`).
    pub cacheable_status: Vec<u16>,
    /// Maximum payload size in bytes eligible for caching (`1..=2_097_152` per 2 MiB slab).
    pub max_payload_bytes: usize,
    /// Default TTL fallback in seconds when upstream headers omit explicit directives.
    pub default_ttl_secs: u64,
    /// Honor upstream `Cache-Control: no-store` / `Cache-Control: private`.
    pub respect_cache_control: bool,
    /// Whether to bypass storing responses for requests carrying an `Authorization` header.
    pub bypass_authenticated_requests: bool,
    /// Custom request header names incorporated into the cache key for per-user caching.
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

    if !(1..=MAX_CACHE_MEMORY_MB).contains(&cfg.max_memory_mb) {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': max_memory_mb ({}) must be between 1 and {MAX_CACHE_MEMORY_MB} MB",
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
