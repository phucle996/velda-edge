//! HTTP Cache Lookup Configuration Schema & Semantic Validation.
//!
//! ```text
//!   PluginConfig (JSON) ─► serde_json::from_value
//!     ├─ validate zone, cacheable_methods, key_headers, max_memory_mb, default_ttl_secs
//!     ├─ validate preferred_encodings (ordered zstd | br | gzip allowlist, no duplicates)
//!     └─► Ok(HttpCacheLookupConfig) | Err
//! ```

use serde::{Deserialize, Serialize};

use crate::error::PluginError;
use crate::http_cache::{CACHE_MEMORY_CEILING_PERCENT, is_valid_header_name, max_cache_memory_mb};
use crate::post_response::response_compression::CompressionAlgorithm;

/// Declarative configuration for the HTTP Cache Lookup plugin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HttpCacheLookupConfig {
    /// Shared cache zone identifier.
    pub zone: String,
    /// HTTP methods eligible for cache lookup (e.g. `["GET", "HEAD"]`).
    pub cacheable_methods: Vec<String>,
    /// Whether to bypass cache lookup on requests carrying an `Authorization` header.
    pub bypass_authenticated_requests: bool,
    /// Custom request header names to incorporate into the cache key (e.g. `x-user-id`, `x-tenant-id`).
    pub key_headers: Vec<String>,
    /// Mandatory priority-ordered compressed content encodings eligible during `Accept-Encoding` negotiation.
    ///
    /// Must be non-empty. Supported tokens: `"zstd"`, `"br"`, `"gzip"`.
    pub preferred_encodings: Vec<String>,
    /// Allocated memory limit for this zone if created here (in megabytes).
    pub max_memory_mb: usize,
    /// Default TTL fallback in seconds if response does not specify Cache-Control.
    pub default_ttl_secs: u64,
}

fn is_valid_http_method(m: &str) -> bool {
    velda_core::lut::HttpMethod::from_bytes(m.trim().as_bytes())
        != velda_core::lut::HttpMethod::Unknown
}

/// Parses and validates a single `preferred_encodings` token for cached variants (`zstd`, `br`, `gzip`).
pub(crate) fn parse_cache_encoding_token(token: &str) -> Option<CompressionAlgorithm> {
    match CompressionAlgorithm::from_token(token)? {
        algo @ (CompressionAlgorithm::Zstd
        | CompressionAlgorithm::Brotli
        | CompressionAlgorithm::Gzip) => Some(algo),
        CompressionAlgorithm::Deflate => None,
    }
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

    if cfg.preferred_encodings.is_empty() {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': preferred_encodings cannot be empty"
        )));
    }

    let mut seen_encodings = Vec::with_capacity(cfg.preferred_encodings.len());
    for enc in &cfg.preferred_encodings {
        let parsed = parse_cache_encoding_token(enc).ok_or_else(|| {
            PluginError::Config(format!(
                "Plugin '{plugin_id}': unsupported encoding '{enc}' in preferred_encodings (supported: zstd, br, gzip)"
            ))
        })?;
        if seen_encodings.contains(&parsed) {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': duplicate encoding '{enc}' in preferred_encodings"
            )));
        }
        seen_encodings.push(parsed);
    }

    let max_cache_mb = max_cache_memory_mb();
    if !(1..=max_cache_mb).contains(&cfg.max_memory_mb) {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': max_memory_mb ({}) must be between 1 and {max_cache_mb} MB ({CACHE_MEMORY_CEILING_PERCENT}% of system memory ceiling)",
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
