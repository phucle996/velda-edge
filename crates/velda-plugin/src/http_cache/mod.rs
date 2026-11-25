//! HTTP Caching Infrastructure Module (`velda-plugin::http_cache`).
//!
//! Shared contiguous 2 MiB slab cache engine, zero-allocation `(route_id, method, key_headers)`
//! request hashing, RFC 9111 `Cache-Control` tokenizer, priority-ordered variant negotiation,
//! and hardware DRAM streaming store.
//!
//! ```text
//!   http_cache_lookup ──► storage.route_key_hash(route_id, method, headers, key_headers)
//!     (pre_upstream)       └─► storage.lookup_variant(..., preferred_encodings)
//!                                └─ Hit ─► Action::Respond(L7Response + X-Cache: HIT)
//!
//!   http_cache_store  ──► storage.route_key_hash(route_id, method, headers, key_headers)
//!     (post_response)      └─► storage.store_bytes() ─► SlabPool (2MB slab, DRAM non-temporal)
//! ```

pub mod cache_control;
pub mod constants;
pub mod dram;
pub mod entity;
pub mod key;
pub mod shard;
pub mod slab;
pub mod storage;

pub use cache_control::{
    CC_FLAG_MAX_AGE, CC_FLAG_NO_CACHE, CC_FLAG_NO_STORE, CC_FLAG_PRIVATE, CC_FLAG_PUBLIC,
    CC_FLAG_S_MAXAGE, CacheControlFast, parse_cache_control_fast, parse_cache_control_ttl,
};
pub use constants::{
    CACHE_MEMORY_CEILING_PERCENT, X_CACHE, is_valid_header_name, max_cache_memory_bytes,
    max_cache_memory_bytes_for, max_cache_memory_mb, max_cache_memory_mb_for,
};
pub use dram::{create_dram_confined_bytes, stream_payload_to_dram};
pub use entity::{
    CacheEntry, CompressedVariants, StoreBytesParams, StoreVariantParams, VariantKind,
    negotiate_variant_kind,
};
pub use key::{
    CompiledKeyHeader, compile_key_headers, filter_cacheable_headers, hash_route_request,
};
pub use shard::{CacheShard, LookupOutcome, NUM_SHARDS};
pub use slab::{SLAB_OFFSET_MASK, SLAB_SHIFT, SLAB_SIZE, SlabPoolStats};
pub use storage::{HttpCacheRegistry, HttpCacheStorage};
