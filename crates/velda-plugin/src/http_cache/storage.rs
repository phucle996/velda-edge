//! Sharded In-Memory HTTP Cache Storage Engine & Zone Registry.
//!
//! Coordinates concurrent lock-free read pinning across [`CacheShard`] index tables and
//! contiguous 2 MiB slab writes in [`SlabPool`]:
//!
//! ```text
//!   HttpCacheStorage
//!     ├─ shards[0..64] : CacheShard   (read lock on lookup; shard-first lock order on write)
//!     ├─ pool          : SlabPool     (bump-down 2 MiB active slab + free/sealed/draining lists)
//!     └─ arena         : SlabArena    (contiguous DRAM bytes + atomic reader pins / generations)
//! ```

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::time::Duration;

use rustc_hash::FxHashMap;

use crate::http_cache::entity::{
    CacheEntry, CompressedVariants, StoreBytesParams, StoreVariantParams,
};
use crate::http_cache::key::{CompiledKeyHeader, hash_route_request};
use crate::http_cache::shard::{CacheShard, LookupOutcome, NUM_SHARDS, SHARD_MASK};
use crate::http_cache::slab::{SLAB_SHIFT, SLAB_SIZE, SlabArena, SlabPool, SlabPoolStats};
use crate::post_response::response_compression::CompressionAlgorithm;

/// Production In-Memory HTTP Cache Storage Engine backed by a contiguous 2 MiB slab pool.
pub struct HttpCacheStorage {
    pub name: String,
    pub max_memory_bytes: usize,
    pub default_ttl: Duration,
    arena: Arc<SlabArena>,
    pool: Mutex<SlabPool>,
    shards: Box<[CacheShard]>,
    hit_count: AtomicU64,
    miss_count: AtomicU64,
}

impl std::fmt::Debug for HttpCacheStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpCacheStorage")
            .field("name", &self.name)
            .field("max_memory_bytes", &self.max_memory_bytes)
            .field("default_ttl", &self.default_ttl)
            .field("num_slabs", &self.arena.num_slabs)
            .field("hits", &self.hit_count.load(Ordering::Relaxed))
            .field("misses", &self.miss_count.load(Ordering::Relaxed))
            .finish()
    }
}

impl HttpCacheStorage {
    /// Creates a new named cache storage backed by at least 2 contiguous 2 MiB slabs,
    /// clamped strictly to the hardware RAM percentage ceiling from `velda-core`.
    pub fn new(name: impl Into<String>, max_memory_bytes: usize, default_ttl: Duration) -> Self {
        let clamped_bytes = max_memory_bytes
            .min(crate::http_cache::max_cache_memory_bytes())
            .max(SLAB_SIZE * 2);
        let num_slabs = (clamped_bytes >> SLAB_SHIFT).max(2);
        let arena = Arc::new(SlabArena::new(num_slabs));
        let pool = Mutex::new(SlabPool::new(Arc::clone(&arena)));
        let shards = (0..NUM_SHARDS)
            .map(|_| CacheShard::new())
            .collect::<Vec<_>>()
            .into_boxed_slice();

        Self {
            name: name.into(),
            max_memory_bytes: clamped_bytes,
            default_ttl,
            arena,
            pool,
            shards,
            hit_count: AtomicU64::new(0),
            miss_count: AtomicU64::new(0),
        }
    }

    #[inline(always)]
    fn shard(&self, hash: u64) -> Option<&CacheShard> {
        self.shards.get((hash as usize) & SHARD_MASK)
    }

    /// Computes the canonical 64-bit cache key hash shared by both `pre_upstream` lookup and
    /// `post_response` store phases (`req_state.route_id + req_state.method + key_headers`).
    #[inline]
    pub fn route_key_hash(
        &self,
        req_state: &velda_core::RequestState,
        headers: &velda_core::HeaderWorkspace,
        key_headers: &[CompiledKeyHeader],
    ) -> u64 {
        hash_route_request(req_state.route_id, req_state.method, headers, key_headers)
    }

    /// Hot-path variant lookup returning only the single negotiated zero-copy payload slice
    /// within the configured priority-ordered `preferred_encodings` allowlist.
    #[inline]
    pub fn lookup_variant(
        &self,
        hash: u64,
        accept_encoding: Option<&http::HeaderValue>,
        preferred_encodings: &[CompressionAlgorithm],
        bypass_auth: bool,
    ) -> LookupOutcome {
        let Some(shard) = self.shard(hash) else {
            self.miss_count.fetch_add(1, Ordering::Relaxed);
            return LookupOutcome::Miss;
        };
        let outcome = shard.lookup_variant(
            hash,
            &self.arena,
            accept_encoding,
            preferred_encodings,
            bypass_auth,
        );
        match outcome {
            LookupOutcome::Hit { .. } => {
                self.hit_count.fetch_add(1, Ordering::Relaxed);
            }
            LookupOutcome::Miss | LookupOutcome::AuthBypass => {
                self.miss_count.fetch_add(1, Ordering::Relaxed);
            }
        }
        outcome
    }

    /// Materializes a cached entry view by key hash using a concurrent Read Lock.
    #[inline]
    pub fn get(&self, hash: u64) -> Option<CacheEntry> {
        let res = self.shard(hash)?.get_entry(hash, &self.arena);
        if res.is_some() {
            self.hit_count.fetch_add(1, Ordering::Relaxed);
        } else {
            self.miss_count.fetch_add(1, Ordering::Relaxed);
        }
        res
    }

    /// Stores a full [`CacheEntry`] into the 2 MiB slab pool and flat shard index.
    #[inline]
    pub fn store(&self, hash: u64, entry: CacheEntry) {
        if let Some(shard) = self.shard(hash)
            && let Ok(mut pool) = self.pool.lock()
        {
            shard.store_entry(hash, entry, &mut pool);
        }
    }

    /// Streams raw response bytes directly into the active 2 MiB slab via non-temporal stores.
    #[inline]
    pub fn store_bytes(&self, params: StoreBytesParams<'_>) {
        if let Some(shard) = self.shard(params.hash)
            && let Ok(mut pool) = self.pool.lock()
        {
            shard.store_bytes(params, &mut pool);
        }
    }

    /// Stores or merges a response variant (`StoreVariantParams`) into the storage.
    #[inline]
    pub fn store_variant(&self, params: StoreVariantParams<'_>) {
        if let Some(variants) = params.variants {
            let entry = CacheEntry::new(
                params.key,
                params.status,
                params.content_type,
                Some(params.payload),
                params.ttl,
            )
            .with_variants(variants)
            .with_public(params.is_public);
            self.store(params.hash, entry);
            return;
        }

        self.store_bytes(StoreBytesParams {
            hash: params.hash,
            status: params.status,
            content_type: params.content_type,
            ttl: params.ttl,
            encoding: params.encoding,
            payload: params.payload.as_ref(),
            is_public: params.is_public,
        });
    }

    /// Merges pre-compressed payload variants into an existing fresh cached entry.
    #[inline]
    pub fn merge_variants(&self, hash: u64, variants: CompressedVariants) -> bool {
        let Some(shard) = self.shard(hash) else {
            return false;
        };
        if let Ok(mut pool) = self.pool.lock() {
            shard.merge_variants(hash, &variants, &mut pool)
        } else {
            false
        }
    }

    /// Purges an entry by hash and decrements its slab `live_spans` (recycling at 0).
    pub fn purge(&self, hash: u64) -> bool {
        let Some(shard) = self.shard(hash) else {
            return false;
        };
        if let Ok(mut pool) = self.pool.lock() {
            shard.remove(hash, &mut pool)
        } else {
            false
        }
    }

    /// Clears all entries across all shards and returns idle slabs to the pool.
    pub fn clear(&self) {
        for shard in self.shards.iter() {
            shard.clear_slots();
        }
        if let Ok(mut pool) = self.pool.lock() {
            pool.clear_all();
        }
    }

    /// Returns a snapshot of the 2 MiB slab pool state.
    pub fn slab_stats(&self) -> Option<SlabPoolStats> {
        self.pool.lock().ok().map(|p| p.stats())
    }

    /// Returns the cumulative number of cache hits.
    #[inline(always)]
    pub fn hit_count(&self) -> u64 {
        self.hit_count.load(Ordering::Relaxed)
    }

    /// Returns the cumulative number of cache misses.
    #[inline(always)]
    pub fn miss_count(&self) -> u64 {
        self.miss_count.load(Ordering::Relaxed)
    }
}

/// Global registry managing named [`HttpCacheStorage`] instances.
pub struct HttpCacheRegistry {
    storages: RwLock<FxHashMap<String, Arc<HttpCacheStorage>>>,
}

impl HttpCacheRegistry {
    /// Returns the global singleton instance.
    pub fn global() -> &'static Self {
        static REGISTRY: OnceLock<HttpCacheRegistry> = OnceLock::new();
        REGISTRY.get_or_init(|| HttpCacheRegistry {
            storages: RwLock::new(FxHashMap::default()),
        })
    }

    /// Gets an existing storage or creates and registers a new one.
    pub fn get_or_create(
        &self,
        name: &str,
        max_memory_bytes: usize,
        default_ttl: Duration,
    ) -> Arc<HttpCacheStorage> {
        {
            let guard = self.storages.read().unwrap_or_else(|p| p.into_inner());
            if let Some(storage) = guard.get(name) {
                return Arc::clone(storage);
            }
        }

        let mut guard = self.storages.write().unwrap_or_else(|p| p.into_inner());
        if let Some(storage) = guard.get(name) {
            Arc::clone(storage)
        } else {
            let storage = Arc::new(HttpCacheStorage::new(name, max_memory_bytes, default_ttl));
            guard.insert(name.to_string(), Arc::clone(&storage));
            storage
        }
    }

    /// Registers a custom pre-allocated storage instance.
    pub fn register(&self, storage: Arc<HttpCacheStorage>) {
        let mut guard = self.storages.write().unwrap_or_else(|p| p.into_inner());
        guard.insert(storage.name.clone(), storage);
    }
}
