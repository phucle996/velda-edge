//! HTTP Cache Lookup Configuration Schema & Semantic Validation.
//!
//! ```text
//!   PluginConfig (JSON) ─► serde_json::from_value ─► zone / methods / key_headers / bounds
//!                                                        └─► Ok(HttpCacheLookupConfig) | Err
//! ```

use serde::{Deserialize, Serialize};

use crate::error::PluginError;
use crate::http_cache::{MAX_CACHE_MEMORY_MB, is_valid_header_name};

/// Declarative configuration for the HTTP Cache Lookup plugin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HttpCacheLookupConfig {
    /// Shared cache zone identifier.
    pub zone: String,
    /// HTTP methods eligible for cache lookup (e.g. `["GET", "HEAD"]`).
    pub cacheable_methods: Vec<String>,
    /// Whether to sort query string keys alphabetically before hashing.
    pub ignore_query_order: bool,
    /// Honor client `Cache-Control: no-cache` / `Pragma: no-cache` directives.
    pub honor_client_no_cache: bool,
    /// Whether to bypass cache lookup on requests carrying an `Authorization` header.
    pub bypass_authenticated_requests: bool,
    /// Custom request header names to incorporate into the cache key for per-user caching.
    pub key_headers: Vec<String>,
    /// Allocated memory limit for this zone if created here (in megabytes).
    pub max_memory_mb: usize,
    /// Default TTL fallback in seconds if response does not specify Cache-Control.
    pub default_ttl_secs: u64,
}

fn is_valid_http_method(m: &str) -> bool {
    let trimmed = m.trim();
    if trimmed.is_empty() {
        return false;
    }
    trimmed
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-')
}

/// Validates HTTP Cache Lookup configuration strictly according to production standards.
pub fn validate_http_cache_lookup_config(
    plugin_id: &str,
    config_val: &serde_json::Value,
) -> Result<HttpCacheLookupConfig, PluginError> {
    let cfg: HttpCacheLookupConfig = serde_json::from_value(config_val.clone()).map_err(|e| {
        PluginError::Config(format!(
            "Plugin '{plugin_id}' has malformed or incomplete http_cache_lookup config: {e}"
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

    for h in &cfg.key_headers {
        if !is_valid_header_name(h) {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': invalid header name '{h}' in key_headers"
            )));
        }
    }

    if !(1..=MAX_CACHE_MEMORY_MB).contains(&cfg.max_memory_mb) {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': max_memory_mb ({}) must be between 1 and {MAX_CACHE_MEMORY_MB} MB",
            cfg.max_memory_mb
        )));
    }

    if !(1..=31_536_000).contains(&cfg.default_ttl_secs) {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': default_ttl_secs must be between 1 and 31,536,000s"
        )));
    }

    Ok(cfg)
}
