//! HTTP Caching Infrastructure Module (`velda-plugin::http_cache`).
//!
//! Shared contiguous 2 MiB slab cache engine, zero-allocation request hashing, RFC 9111
//! `Cache-Control` tokenizer, variant negotiation, and hardware DRAM streaming store.
//!
//! ```text
//!   http_cache_lookup ──► hash_request_key_zero_alloc() ─► HttpCacheStorage::lookup_variant()
//!                                                               ├─ Hit  ─► SlabArena::pin_span()
//!                                                               └─ Miss ─► X_VELDA_CACHE_HASH
//!                                                                              │
//!   http_cache_store  ◄──────────── upstream response ─────────────────────────┘
//!     └─► HttpCacheStorage::store_bytes() ─► SlabPool (2MB bump-down) + Non-Temporal DRAM write
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
    MAX_CACHE_MEMORY_MB, X_CACHE, X_VELDA_CACHE_HASH, X_VELDA_CACHE_KEY, is_valid_header_name,
};
pub use dram::{create_dram_confined_bytes, stream_payload_to_dram};
pub use entity::{
    CacheEntry, CompressedVariants, StoreBytesParams, StoreVariantParams, VariantKind,
    negotiate_variant_kind,
};
pub use key::{
    compute_cache_key, compute_cache_key_with_headers, filter_cacheable_headers, hash_key,
    hash_request_key_zero_alloc, normalize_query,
};
pub use shard::{CacheShard, LookupOutcome, NUM_SHARDS};
pub use slab::{SLAB_OFFSET_MASK, SLAB_SHIFT, SLAB_SIZE, SlabPoolStats};
pub use storage::{HttpCacheRegistry, HttpCacheStorage};
