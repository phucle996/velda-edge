//! Shared HTTP Cache constants, hardware RAM ceiling helpers, and validation utilities.
//!
//! ```text
//!   velda_core::hardware::global_hardware_topology().memory_bytes()  (cgroup/RAM OnceLock)
//!     └─► CACHE_MEMORY_CEILING_PERCENT (50%) ─► max_cache_memory_mb() / max_cache_memory_bytes()
//!           ├─ validate_http_cache_lookup_config / validate_http_cache_store_config (per zone)
//!           ├─ validate_plugin_dependencies (sum across distinct zones <= ceiling)
//!           └─ HttpCacheStorage::new (hard runtime clamp)
//!
//!   http_cache_lookup (pre_upstream)
//!     ├─► hash = storage.route_key_hash(route_id, method, req.headers, key_headers)
//!     └─► storage.lookup_variant(...)
//!           ├─ Hit  ──► build L7Response (X-Cache: HIT) ─► Action::Respond(resp)
//!           └─ Miss ──► Action::Continue ─► Upstream ─► http_cache_store (post_response)
//!                                                         └─► same route_key_hash(...)
//!                                                               └─► storage.store_bytes()
//! ```

use http::header::HeaderName;

use crate::http_cache::slab::SLAB_SIZE;

/// Diagnostic cache status header emitted to downstream clients (`HIT`, `MISS`, `BYPASS`).
pub const X_CACHE: HeaderName = HeaderName::from_static("x-cache");

/// Maximum percentage of system/cgroup memory (`velda-core`) permitted for HTTP cache slabs.
pub const CACHE_MEMORY_CEILING_PERCENT: usize = 50;

/// Minimum cache arena size in bytes (2 contiguous 2 MiB slabs).
const MIN_CACHE_BYTES: usize = SLAB_SIZE * 2;

/// Computes the HTTP cache memory ceiling in bytes for a given total physical/cgroup memory budget.
#[inline]
pub const fn max_cache_memory_bytes_for(total_memory_bytes: usize) -> usize {
    let pct_cap = (total_memory_bytes.saturating_mul(CACHE_MEMORY_CEILING_PERCENT)) / 100;
    if pct_cap < MIN_CACHE_BYTES {
        MIN_CACHE_BYTES
    } else {
        pct_cap
    }
}

/// Computes the HTTP cache memory ceiling in megabytes for a given total physical/cgroup memory budget.
#[inline]
pub const fn max_cache_memory_mb_for(total_memory_bytes: usize) -> usize {
    max_cache_memory_bytes_for(total_memory_bytes) / (1024 * 1024)
}

/// Returns the HTTP cache memory ceiling in bytes derived from `velda-core`'s probed RAM/cgroup limit.
#[inline]
pub fn max_cache_memory_bytes() -> usize {
    max_cache_memory_bytes_for(velda_core::hardware::global_hardware_topology().memory_bytes())
}

/// Returns the HTTP cache memory ceiling in megabytes derived from `velda-core`'s probed RAM/cgroup limit.
#[inline]
pub fn max_cache_memory_mb() -> usize {
    max_cache_memory_mb_for(velda_core::hardware::global_hardware_topology().memory_bytes())
}

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_memory_ceiling_scales_with_core_memory() {
        assert_eq!(max_cache_memory_mb_for(1024 * 1024 * 1024), 512);
        assert_eq!(max_cache_memory_mb_for(4 * 1024 * 1024 * 1024), 2048);
        assert_eq!(max_cache_memory_mb_for(2 * 1024 * 1024), 4);
        assert_eq!(
            max_cache_memory_mb(),
            max_cache_memory_mb_for(
                velda_core::hardware::global_hardware_topology().memory_bytes()
            )
        );
    }
}
