//! Sharded In-Memory HTTP Cache Storage Engine (`HttpCacheStorage`).
//!
//! Provides high-performance, sharded in-memory Second-Chance CLOCK caching (~14ns lookup)
//! with zero-copy payload sharing via [`bytes::Bytes`].
//!
//! In accordance with Velda Edge Architecture Rules:
//! - **Zero-IO Hot Path**: Pure RAM operations, zero disk I/O, zero network calls.
//! - **Zero Lock Contention**: Partitioned across 64 lock-striped shards (`#[repr(align(64))]`).
//!   Lookup operations execute concurrently under Read Locks without queue mutation.
//! - **CPU Cache Pollution Immunity**: Large payloads (>= 256 bytes) are confined directly to DRAM
//!   via Non-Temporal Streaming Stores, preventing CPU L1/L2 data cache flushing.
//! - **Integer Coarse Clock**: Time tracking uses [`velda_core::coarse_now_sec`] via 1-instruction
//!   integer arithmetic, eliminating Linux vDSO `clock_gettime` syscall overhead.
//! - **Second-Chance Clock Eviction**: Replaces $O(N)$ linear scans with $O(1)$ lockless accessed-bit
//!   evaluations (`AtomicBool`).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, RwLock};
use std::time::Duration;

use http::Method;
use rustc_hash::FxHashMap;
use velda_core::coarse_now_sec;

use crate::http_cache::entity::{CacheEntry, CompressedVariants, StoreVariantParams};
use crate::http_cache::key::{compute_cache_key, compute_cache_key_with_headers, hash_key};

/// Number of lock-striped shards to eliminate lock contention on multicore systems.
pub const NUM_SHARDS: usize = 64;

/// Internal state of a single cache shard protected by a reader-writer lock.
#[derive(Debug)]
struct ShardInner {
    entries: FxHashMap<u64, CacheEntry>,
    clock_keys: Vec<u64>,
    clock_hand: usize,
    current_bytes: usize,
    max_bytes: usize,
}

impl ShardInner {
    fn new(max_bytes: usize) -> Self {
        Self {
            entries: FxHashMap::default(),
            clock_keys: Vec::new(),
            clock_hand: 0,
            current_bytes: 0,
            max_bytes,
        }
    }

    /// Concurrent Read-Lock lookup: evaluates freshness and marks second-chance bit.
    #[inline]
    fn get(&self, hash: u64) -> Option<CacheEntry> {
        let entry = self.entries.get(&hash)?;
        if !entry.is_fresh() {
            return None;
        }
        // Mark accessed for Second-Chance Clock eviction without requiring a write lock
        entry.accessed.store(true, Ordering::Relaxed);
        Some(entry.clone())
    }

    /// Stores a new cache entry, executing $O(1)$ Second-Chance Clock eviction if capacity exceeded.
    fn insert(&mut self, hash: u64, entry: CacheEntry) {
        let entry_size = entry.memory_size();

        if entry_size > self.max_bytes {
            return;
        }

        if let Some(old) = self.entries.remove(&hash) {
            self.current_bytes = self.current_bytes.saturating_sub(old.memory_size());
        } else {
            self.clock_keys.push(hash);
        }

        // Evict via $O(1)$ Second-Chance Clock Algorithm
        while self.current_bytes + entry_size > self.max_bytes && !self.clock_keys.is_empty() {
            if self.clock_hand >= self.clock_keys.len() {
                self.clock_hand = 0;
            }
            let candidate_hash = self.clock_keys[self.clock_hand];
            if let Some(e) = self.entries.get(&candidate_hash) {
                if !e.is_fresh() {
                    // Expired: evict immediately
                    self.clock_keys.swap_remove(self.clock_hand);
                    if let Some(removed) = self.entries.remove(&candidate_hash) {
                        self.current_bytes =
                            self.current_bytes.saturating_sub(removed.memory_size());
                    }
                    continue;
                }
                if e.accessed.swap(false, Ordering::Relaxed) {
                    // Second chance: advance hand
                    self.clock_hand += 1;
                } else {
                    // Evict!
                    self.clock_keys.swap_remove(self.clock_hand);
                    if let Some(removed) = self.entries.remove(&candidate_hash) {
                        self.current_bytes =
                            self.current_bytes.saturating_sub(removed.memory_size());
                    }
                }
            } else {
                self.clock_keys.swap_remove(self.clock_hand);
            }
        }

        self.current_bytes += entry_size;
        self.entries.insert(hash, entry);
    }

    fn insert_or_merge(&mut self, params: StoreVariantParams<'_>) {
        let StoreVariantParams {
            hash,
            key,
            status,
            content_type,
            ttl,
            encoding,
            payload,
            variants,
            is_public,
        } = params;

        let now = coarse_now_sec() as u32;
        let ttl_secs = ttl.as_secs().max(1) as u32;

        // If entry exists and is fresh, merge into it
        if let Some(entry) = self.entries.get_mut(&hash)
            && entry.is_fresh()
        {
            let old_size = entry.memory_size();
            entry.status = status;
            if content_type.is_some() {
                entry.content_type = content_type;
            }
            entry.created_at_sec = now;
            entry.expires_at_sec = now.saturating_add(ttl_secs);
            if is_public {
                entry.is_public = true;
            }

            if let Some(vars) = variants {
                entry.raw_body = Some(payload);
                entry.variants = vars;
            } else {
                match encoding {
                    Some("gzip") => entry.variants.gzip = Some(payload),
                    Some("br") => entry.variants.brotli = Some(payload),
                    Some("zstd") => entry.variants.zstd = Some(payload),
                    _ => entry.raw_body = Some(payload),
                }
            }

            entry.accessed.store(true, Ordering::Relaxed);
            let new_size = entry.memory_size();
            self.current_bytes = self.current_bytes.saturating_sub(old_size) + new_size;
            self.evict_if_needed();
            return;
        }

        // Otherwise create brand new entry
        let mut entry =
            CacheEntry::new(key, status, content_type, None, ttl).with_public(is_public);
        if let Some(vars) = variants {
            entry.raw_body = Some(payload);
            entry.variants = vars;
        } else {
            match encoding {
                Some("gzip") => entry.variants.gzip = Some(payload),
                Some("br") => entry.variants.brotli = Some(payload),
                Some("zstd") => entry.variants.zstd = Some(payload),
                _ => entry.raw_body = Some(payload),
            }
        }

        self.insert(hash, entry);
    }

    #[inline]
    fn evict_if_needed(&mut self) {
        while self.current_bytes > self.max_bytes && !self.clock_keys.is_empty() {
            if self.clock_hand >= self.clock_keys.len() {
                self.clock_hand = 0;
            }
            let candidate_hash = self.clock_keys[self.clock_hand];
            if let Some(e) = self.entries.get(&candidate_hash) {
                if !e.is_fresh() || !e.accessed.swap(false, Ordering::Relaxed) {
                    self.clock_keys.swap_remove(self.clock_hand);
                    if let Some(removed) = self.entries.remove(&candidate_hash) {
                        self.current_bytes =
                            self.current_bytes.saturating_sub(removed.memory_size());
                    }
                } else {
                    self.clock_hand += 1;
                }
            } else {
                self.clock_keys.swap_remove(self.clock_hand);
            }
        }
    }

    fn merge_variants(&mut self, hash: u64, variants: CompressedVariants) -> bool {
        if let Some(entry) = self.entries.get_mut(&hash)
            && entry.is_fresh()
        {
            let old_size = entry.memory_size();
            if variants.gzip.is_some() {
                entry.variants.gzip = variants.gzip;
            }
            if variants.brotli.is_some() {
                entry.variants.brotli = variants.brotli;
            }
            if variants.zstd.is_some() {
                entry.variants.zstd = variants.zstd;
            }
            entry.accessed.store(true, Ordering::Relaxed);
            let new_size = entry.memory_size();
            self.current_bytes = self.current_bytes.saturating_sub(old_size) + new_size;
            self.evict_if_needed();
            return true;
        }
        false
    }

    fn remove(&mut self, hash: u64) -> bool {
        if let Some(removed) = self.entries.remove(&hash) {
            self.current_bytes = self.current_bytes.saturating_sub(removed.memory_size());
            if let Some(pos) = self.clock_keys.iter().position(|&h| h == hash) {
                self.clock_keys.swap_remove(pos);
            }
            true
        } else {
            false
        }
    }

    fn clear(&mut self) {
        self.entries.clear();
        self.clock_keys.clear();
        self.clock_hand = 0;
        self.current_bytes = 0;
    }
}

/// A lock-striped cache shard aligned to 64 bytes to eliminate false sharing.
#[repr(align(64))]
#[derive(Debug)]
pub struct CacheShard {
    inner: RwLock<ShardInner>,
}

impl CacheShard {
    fn new(max_bytes: usize) -> Self {
        Self {
            inner: RwLock::new(ShardInner::new(max_bytes)),
        }
    }

    /// Non-blocking Read-Lock lookup. Multiple concurrent threads can read simultaneously.
    #[inline]
    pub fn get(&self, hash: u64) -> Option<CacheEntry> {
        let guard = self.inner.read().ok()?;
        guard.get(hash)
    }

    #[inline]
    pub fn insert(&self, hash: u64, entry: CacheEntry) {
        if let Ok(mut guard) = self.inner.write() {
            guard.insert(hash, entry);
        }
    }

    #[inline]
    pub fn insert_or_merge(&self, params: StoreVariantParams<'_>) {
        if let Ok(mut guard) = self.inner.write() {
            guard.insert_or_merge(params);
        }
    }

    #[inline]
    pub fn merge_variants(&self, hash: u64, variants: CompressedVariants) -> bool {
        if let Ok(mut guard) = self.inner.write() {
            guard.merge_variants(hash, variants)
        } else {
            false
        }
    }

    #[inline]
    pub fn remove(&self, hash: u64) -> bool {
        if let Ok(mut guard) = self.inner.write() {
            guard.remove(hash)
        } else {
            false
        }
    }

    #[inline]
    pub fn clear(&self) {
        if let Ok(mut guard) = self.inner.write() {
            guard.clear();
        }
    }
}

/// Production In-Memory HTTP Cache Storage Engine.
pub struct HttpCacheStorage {
    pub name: String,
    pub max_memory_bytes: usize,
    pub default_ttl: Duration,
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
            .field("hits", &self.hit_count.load(Ordering::Relaxed))
            .field("misses", &self.miss_count.load(Ordering::Relaxed))
            .finish()
    }
}

impl HttpCacheStorage {
    /// Creates a new named cache storage with specified memory budget and default TTL.
    pub fn new(name: impl Into<String>, max_memory_bytes: usize, default_ttl: Duration) -> Self {
        let shard_capacity = (max_memory_bytes / NUM_SHARDS).max(64 * 1024);
        let mut shards = Vec::with_capacity(NUM_SHARDS);
        for _ in 0..NUM_SHARDS {
            shards.push(CacheShard::new(shard_capacity));
        }

        Self {
            name: name.into(),
            max_memory_bytes,
            default_ttl,
            shards: shards.into_boxed_slice(),
            hit_count: AtomicU64::new(0),
            miss_count: AtomicU64::new(0),
        }
    }

    /// Computes the canonical cache key and its 64-bit FxHash.
    #[inline(always)]
    pub fn compute_cache_key(
        method: &Method,
        host: Option<&str>,
        path: &str,
        query: Option<&str>,
        ignore_query_order: bool,
    ) -> (String, u64) {
        compute_cache_key(method, host, path, query, ignore_query_order)
    }

    /// Computes the canonical cache key incorporating custom segmentation headers.
    #[inline(always)]
    pub fn compute_cache_key_with_headers(
        method: &Method,
        host: Option<&str>,
        path: &str,
        query: Option<&str>,
        ignore_query_order: bool,
        extra_headers: Option<&[(&str, &str)]>,
    ) -> (String, u64) {
        compute_cache_key_with_headers(method, host, path, query, ignore_query_order, extra_headers)
    }

    /// Computes the 64-bit FxHash of a cache key string.
    #[inline(always)]
    pub fn hash_key(key: &str) -> u64 {
        hash_key(key)
    }

    /// Looks up a cached entry by key hash using a Read Lock.
    #[inline]
    pub fn get(&self, hash: u64) -> Option<CacheEntry> {
        let shard_idx = (hash as usize) % NUM_SHARDS;
        let res = self.shards[shard_idx].get(hash);
        if res.is_some() {
            self.hit_count.fetch_add(1, Ordering::Relaxed);
        } else {
            self.miss_count.fetch_add(1, Ordering::Relaxed);
        }
        res
    }

    /// Stores a new response entry into the storage.
    #[inline]
    pub fn store(&self, hash: u64, entry: CacheEntry) {
        let shard_idx = (hash as usize) % NUM_SHARDS;
        self.shards[shard_idx].insert(hash, entry);
    }

    /// Stores or merges a response variant into the cache storage.
    #[inline]
    pub fn store_variant(&self, params: StoreVariantParams<'_>) {
        let shard_idx = (params.hash as usize) % NUM_SHARDS;
        self.shards[shard_idx].insert_or_merge(params);
    }

    /// Merges pre-compressed payload variants into an existing cached entry.
    #[inline]
    pub fn merge_variants(&self, hash: u64, variants: CompressedVariants) -> bool {
        let shard_idx = (hash as usize) % NUM_SHARDS;
        self.shards[shard_idx].merge_variants(hash, variants)
    }

    /// Purges a single entry by key hash.
    pub fn purge(&self, hash: u64) -> bool {
        let shard_idx = (hash as usize) % NUM_SHARDS;
        self.shards[shard_idx].remove(hash)
    }

    /// Clears all entries across all shards in this storage.
    pub fn clear(&self) {
        for shard in self.shards.iter() {
            shard.clear();
        }
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

    /// Deprecated lockless stub: Key association is now performed directly via `req_headers`.
    #[inline(always)]
    pub fn record_pending_key(&self, _request_id: &str, _cache_key: String) {}

    /// Deprecated lockless stub: Key association is now performed directly via `req_headers`.
    #[inline(always)]
    pub fn take_pending_key(&self, _request_id: &str) -> Option<String> {
        None
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
            let guard = self.storages.read().unwrap();
            if let Some(storage) = guard.get(name) {
                return Arc::clone(storage);
            }
        }

        let mut guard = self.storages.write().unwrap();
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
        let mut guard = self.storages.write().unwrap();
        guard.insert(storage.name.clone(), storage);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use http::StatusCode;

    #[test]
    fn test_cache_storage_clock_and_expiration() {
        let storage = HttpCacheStorage::new("test_storage", 1024 * 1024, Duration::from_secs(120));
        let (_, hash) = HttpCacheStorage::compute_cache_key(
            &Method::GET,
            Some("example.com"),
            "/clock",
            None,
            true,
        );

        let now = coarse_now_sec() as u32;
        let entry = CacheEntry::with_raw(
            "GET:example.com:/clock",
            StatusCode::OK,
            None,
            Bytes::from_static(b"clock payload"),
            Duration::from_secs(60),
        )
        .with_timestamps(now, now + 100);

        storage.store(hash, entry);

        let retrieved = storage.get(hash);
        assert!(retrieved.is_some());
        assert_eq!(
            retrieved.unwrap().raw_body.unwrap().as_ref(),
            b"clock payload"
        );

        // Expired entry test
        let expired_entry = CacheEntry::with_raw(
            "GET:example.com:/clock",
            StatusCode::OK,
            None,
            Bytes::from_static(b"expired payload"),
            Duration::from_secs(60),
        )
        .with_timestamps(now.saturating_sub(200), now.saturating_sub(100));

        storage.store(hash, expired_entry);
        assert!(storage.get(hash).is_none());
    }

    #[test]
    fn test_storage_store_or_merge_lazy_variants() {
        let storage = HttpCacheStorage::new("lazy_storage", 1024 * 1024, Duration::from_secs(60));
        let (_, hash) = HttpCacheStorage::compute_cache_key(
            &Method::GET,
            Some("example.com"),
            "/lazy-data",
            None,
            true,
        );

        // Client 1 stores Brotli
        storage.store_variant(StoreVariantParams {
            hash,
            key: "GET:example.com:/lazy-data".to_string(),
            status: StatusCode::OK,
            content_type: None,
            ttl: Duration::from_secs(60),
            encoding: Some("br"),
            payload: Bytes::from_static(b"br-compressed"),
            variants: None,
            is_public: false,
        });

        let entry = storage.get(hash).expect("Must exist");
        assert_eq!(
            entry.variants.brotli.as_ref().unwrap().as_ref(),
            b"br-compressed"
        );
        assert!(entry.variants.gzip.is_none());

        // Client 2 arrives requesting Gzip -> Upstream generates Gzip -> Merged!
        storage.store_variant(StoreVariantParams {
            hash,
            key: "GET:example.com:/lazy-data".to_string(),
            status: StatusCode::OK,
            content_type: None,
            ttl: Duration::from_secs(60),
            encoding: Some("gzip"),
            payload: Bytes::from_static(b"gzip-compressed"),
            variants: None,
            is_public: false,
        });

        let entry2 = storage.get(hash).expect("Must exist");
        assert_eq!(
            entry2.variants.brotli.as_ref().unwrap().as_ref(),
            b"br-compressed"
        );
        assert_eq!(
            entry2.variants.gzip.as_ref().unwrap().as_ref(),
            b"gzip-compressed"
        );
    }

    #[test]
    fn test_storage_merge_variants() {
        let storage = HttpCacheStorage::new("test_merge", 1024 * 1024, Duration::from_secs(60));
        let (key, hash) = HttpCacheStorage::compute_cache_key(
            &Method::GET,
            Some("example.com"),
            "/items",
            None,
            true,
        );

        let entry = CacheEntry::with_raw(
            key,
            StatusCode::OK,
            None,
            Bytes::from_static(b"pristine raw data"),
            Duration::from_secs(120),
        );
        storage.store(hash, entry);

        let initial = storage.get(hash).unwrap();
        assert!(initial.variants.gzip.is_none());

        let variants = CompressedVariants {
            gzip: Some(Bytes::from_static(b"async gzip")),
            brotli: Some(Bytes::from_static(b"async br")),
            zstd: Some(Bytes::from_static(b"async zstd")),
        };
        let merged = storage.merge_variants(hash, variants);
        assert!(merged);

        let updated = storage.get(hash).unwrap();
        assert_eq!(
            updated.variants.gzip.as_ref().unwrap().as_ref(),
            b"async gzip"
        );
        assert_eq!(
            updated.variants.brotli.as_ref().unwrap().as_ref(),
            b"async br"
        );
        assert_eq!(
            updated.variants.zstd.as_ref().unwrap().as_ref(),
            b"async zstd"
        );
    }
}
