//! Shared HTTP Cache internal headers and constants.
//!
//! These constants define transient communication between `http_cache_lookup`
//! and `http_cache_store` plugins within the HTTP Cache subsystem.

use http::header::HeaderName;

/// Diagnostic cache status header emitted to downstream clients (`HIT`, `MISS`, `BYPASS`).
pub const X_CACHE: HeaderName = HeaderName::from_static("x-cache");

/// Transient cache key passed from lookup to store.
pub const X_VELDA_CACHE_KEY: HeaderName = HeaderName::from_static("x-velda-cache-key");

/// Transient cache key hash passed from lookup to store.
pub const X_VELDA_CACHE_HASH: HeaderName = HeaderName::from_static("x-velda-cache-hash");
