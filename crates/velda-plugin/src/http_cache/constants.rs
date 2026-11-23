//! Shared HTTP Cache internal headers, constants, and validator helpers.
//!
//! ```text
//!   http_cache_lookup (pre_upstream)
//!     │  1. Hit  ──► Action::ServeCached (emits X-Cache: HIT via CachedResponseDirective)
//!     │  2. Miss ──► req.headers.set_u64(X_VELDA_CACHE_HASH, hash) ──► Upstream
//!     ▼
//!   http_cache_store (post_response)
//!     ├─ Reads X_VELDA_CACHE_HASH from numeric workspace (zero alloc)
//!     ├─ Strips internal X-Velda-Cache-* headers from downstream response
//!     └─ Emits X-Cache: MISS | BYPASS
//! ```

use http::header::HeaderName;

/// Diagnostic cache status header emitted to downstream clients (`HIT`, `MISS`, `BYPASS`).
pub const X_CACHE: HeaderName = HeaderName::from_static("x-cache");

/// Transient cache key header (used by diagnostic / test harnesses and header stripping).
pub const X_VELDA_CACHE_KEY: HeaderName = HeaderName::from_static("x-velda-cache-key");

/// Transient 64-bit cache key hash passed in [`velda_core::HeaderWorkspace`] numeric registers.
pub const X_VELDA_CACHE_HASH: HeaderName = HeaderName::from_static("x-velda-cache-hash");

/// Shared memory ceiling across lookup and store configuration validators (64 GiB).
pub const MAX_CACHE_MEMORY_MB: usize = 65_536;

/// Validates an HTTP header field name against RFC 9110 `token` grammar.
pub fn is_valid_header_name(h: &str) -> bool {
    let trimmed = h.trim();
    if trimmed.is_empty() {
        return false;
    }
    trimmed.bytes().all(|b| {
        b.is_ascii_alphanumeric()
            || matches!(
                b,
                b'!' | b'#'
                    | b'$'
                    | b'%'
                    | b'&'
                    | b'\''
                    | b'*'
                    | b'+'
                    | b'-'
                    | b'.'
                    | b'^'
                    | b'_'
                    | b'`'
                    | b'|'
                    | b'~'
            )
    })
}
