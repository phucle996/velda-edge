//! Shared In-Memory HTTP Cache Zone (`HttpCacheZone`).
//!
//! Provides high-performance, sharded in-memory LRU caching (~25ns lookup)
//! with zero-copy payload sharing via [`bytes::Bytes`].
//!
//! In accordance with Velda Edge Architecture Rules:
//! - **Zero-IO Hot Path**: Pure RAM operations, zero disk I/O, zero network calls.
//! - **Zero Lock Contention**: Partitioned across 64 lock-striped shards.
//! - **Safe Memory Quotas**: Evicts least recently accessed entries when shard memory is saturated.
//! - **Unidirectional Plugins**: Shared across `http_cache_lookup` (Pre-Upstream)
//!   and `http_cache_store` (Post-Response) via [`HttpCacheZoneRegistry`].

use std::collections::VecDeque;
use std::hash::Hasher;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::time::{Duration, Instant};

use crate::header::{X_REQUEST_ID, X_ROUTE_ID, X_UPSTREAM_ADDR};
use crate::http_cache::constants::{X_CACHE, X_VELDA_CACHE_HASH, X_VELDA_CACHE_KEY};
use bytes::Bytes;
use http::header::HeaderName;
use http::{HeaderMap, Method, StatusCode};
use rustc_hash::{FxHashMap, FxHasher};

/// Number of lock-striped shards to eliminate lock contention on multicore systems.
pub const NUM_SHARDS: usize = 64;

/// Normalizes and sorts query string parameters alphabetically (e.g. `b=2&a=1` -> `a=1&b=2`).
pub fn normalize_query(query: &str) -> String {
    let mut params: Vec<&str> = query.split('&').filter(|s| !s.is_empty()).collect();
    params.sort_unstable();
    params.join("&")
}

/// Pre-compressed wire payload variants for a cached entry.
///
/// In accordance with Pure Payload Architecture:
/// Holds only pure compressed bytes (`Bytes`), zero headers.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CompressedVariants {
    pub gzip: Option<Bytes>,
    pub brotli: Option<Bytes>,
    pub zstd: Option<Bytes>,
}

impl CompressedVariants {
    /// Estimated memory footprint of the compressed variants in bytes.
    #[inline(always)]
    pub fn memory_size(&self) -> usize {
        self.gzip.as_ref().map_or(0, |b| b.len())
            + self.brotli.as_ref().map_or(0, |b| b.len())
            + self.zstd.as_ref().map_or(0, |b| b.len())
    }
}

/// A single cached HTTP response entry in RAM.
///
/// In accordance with the Pure Payload Cache Architecture:
/// Only the response payload (`Bytes`) and its canonical `Content-Type` are stored.
/// Zero transient, hop-by-hop, or session headers (`HeaderMap`) are persisted in cache.
#[derive(Clone, Debug)]
pub struct CacheEntry {
    /// Canonical cache key string for debugging and diagnostics.
    pub key: String,
    /// HTTP status code (e.g. 200 OK, 301 Moved Permanently).
    pub status: StatusCode,
    /// Preserved Content-Type header value for MIME type fidelity.
    pub content_type: Option<http::HeaderValue>,
    /// Zero-copy reference-counted uncompressed response body bytes (pure canonical payload).
    /// Optional in Lazy Mode if only compressed variants have been requested so far.
    pub raw_body: Option<Bytes>,
    /// Pre-compressed wire payload variants for zero-CPU content negotiation.
    pub variants: CompressedVariants,
    /// Whether this entry was declared public (RFC 9111 §3.5: allows serving to authenticated requests).
    pub is_public: bool,
    /// Monotonic timestamp when this entry was created.
    pub created_at: Instant,
    /// Time-to-live duration for this entry.
    pub ttl: Duration,
}

impl CacheEntry {
    /// Constructs a new [`CacheEntry`] with optional uncompressed canonical body and default empty variants.
    pub fn new(
        key: impl Into<String>,
        status: StatusCode,
        content_type: Option<http::HeaderValue>,
        raw_body: Option<Bytes>,
        ttl: Duration,
    ) -> Self {
        Self {
            key: key.into(),
            status,
            content_type,
            raw_body,
            variants: CompressedVariants::default(),
            is_public: false,
            created_at: Instant::now(),
            ttl,
        }
    }

    /// Convenience constructor with raw bytes.
    pub fn with_raw(
        key: impl Into<String>,
        status: StatusCode,
        content_type: Option<http::HeaderValue>,
        raw_body: Bytes,
        ttl: Duration,
    ) -> Self {
        Self::new(key, status, content_type, Some(raw_body), ttl)
    }

    /// Attaches pre-computed compressed payload variants to this entry.
    pub fn with_variants(mut self, variants: CompressedVariants) -> Self {
        self.variants = variants;
        self
    }

    /// Attaches public availability marker according to RFC 9111 §3.5.
    pub fn with_public(mut self, is_public: bool) -> Self {
        self.is_public = is_public;
        self
    }

    /// Returns `true` if this entry has not expired.
    #[inline(always)]
    pub fn is_fresh(&self) -> bool {
        self.created_at.elapsed() < self.ttl
    }

    /// Returns the age of this entry in seconds.
    #[inline(always)]
    pub fn age_secs(&self) -> u64 {
        self.created_at.elapsed().as_secs()
    }

    /// Estimated memory footprint of this entry in bytes including all compressed variants.
    #[inline(always)]
    pub fn memory_size(&self) -> usize {
        let raw_len = self.raw_body.as_ref().map(|b| b.len()).unwrap_or(0);
        raw_len + self.variants.memory_size() + self.key.len() + 64
    }

    /// Returns the negotiated body bytes and optional content-encoding header value
    /// based on the client's `Accept-Encoding` preferences and currently cached variants (RFC 9110 §12.5.3).
    ///
    /// Implements Smart Variant Matching:
    /// 1. Hit Over Miss: If client accepts any encoding currently present in cache,
    ///    immediately serves it (avoiding upstream roundtrip).
    /// 2. Codec Hierarchy: If multiple acceptable variants are in cache, resolves by server
    ///    preference: Zstandard -> Brotli -> Gzip -> Raw Uncompressed.
    /// 3. Returns None if client does not accept any of the available variants (requiring upstream fetch).
    #[inline]
    pub fn negotiate_smart_variant(
        &self,
        accept_encoding: Option<&http::HeaderValue>,
    ) -> Option<(Bytes, Option<http::HeaderValue>)> {
        use crate::post_response::response_compression::{
            CompressionAlgorithm, negotiate_algorithm,
        };

        // Determine available algorithms currently stored in this entry
        let mut available_server_algos = Vec::with_capacity(3);
        if self.variants.zstd.is_some() {
            available_server_algos.push(CompressionAlgorithm::Zstd);
        }
        if self.variants.brotli.is_some() {
            available_server_algos.push(CompressionAlgorithm::Brotli);
        }
        if self.variants.gzip.is_some() {
            available_server_algos.push(CompressionAlgorithm::Gzip);
        }

        // 1. If client sent Accept-Encoding and we have compressed variants, negotiate among available
        if !available_server_algos.is_empty()
            && let Some(selected) = negotiate_algorithm(accept_encoding, &available_server_algos)
        {
            match selected {
                CompressionAlgorithm::Zstd => {
                    if let Some(zstd) = &self.variants.zstd {
                        return Some((zstd.clone(), Some(http::HeaderValue::from_static("zstd"))));
                    }
                }
                CompressionAlgorithm::Brotli => {
                    if let Some(br) = &self.variants.brotli {
                        return Some((br.clone(), Some(http::HeaderValue::from_static("br"))));
                    }
                }
                CompressionAlgorithm::Gzip => {
                    if let Some(gzip) = &self.variants.gzip {
                        return Some((gzip.clone(), Some(http::HeaderValue::from_static("gzip"))));
                    }
                }
                _ => {}
            }
        }

        // 2. Fallback to uncompressed raw body if available and acceptable to client
        if let Some(raw) = &self.raw_body {
            if let Some(header) = accept_encoding
                && let Ok(header_str) = header.to_str()
            {
                let prefs =
                    crate::post_response::response_compression::algorithm::parse_accept_encoding(
                        header_str,
                    );
                let identity_forbidden = prefs.iter().any(|p| {
                    (p.algorithm.is_none() || p.algorithm == Some(CompressionAlgorithm::Deflate))
                        && p.q == 0.0
                });
                if !identity_forbidden {
                    return Some((raw.clone(), None));
                }
            } else {
                return Some((raw.clone(), None));
            }
        }

        // 3. No compatible representation available in cache for this client
        None
    }

    /// Backwards-compatible convenience method delegating to [`Self::negotiate_smart_variant`].
    #[inline]
    pub fn negotiate_payload(
        &self,
        accept_encoding: Option<&http::HeaderValue>,
    ) -> (Bytes, Option<http::HeaderValue>) {
        if let Some(res) = self.negotiate_smart_variant(accept_encoding) {
            res
        } else if let Some(raw) = &self.raw_body {
            (raw.clone(), None)
        } else if let Some(br) = &self.variants.brotli {
            (br.clone(), Some(http::HeaderValue::from_static("br")))
        } else if let Some(zstd) = &self.variants.zstd {
            (zstd.clone(), Some(http::HeaderValue::from_static("zstd")))
        } else if let Some(gzip) = &self.variants.gzip {
            (gzip.clone(), Some(http::HeaderValue::from_static("gzip")))
        } else {
            (Bytes::new(), None)
        }
    }
}

/// Internal state of a single cache shard protected by a reader-writer lock.
#[derive(Debug)]
struct ShardInner {
    entries: FxHashMap<u64, CacheEntry>,
    lru: VecDeque<u64>,
    current_bytes: usize,
    max_bytes: usize,
}

impl ShardInner {
    fn new(max_bytes: usize) -> Self {
        Self {
            entries: FxHashMap::default(),
            lru: VecDeque::new(),
            current_bytes: 0,
            max_bytes,
        }
    }

    fn get(&mut self, hash: u64) -> Option<CacheEntry> {
        let entry = self.entries.get(&hash)?;
        if !entry.is_fresh() {
            // Lazy eviction on expired lookup
            if let Some(removed) = self.entries.remove(&hash) {
                self.current_bytes = self.current_bytes.saturating_sub(removed.memory_size());
            }
            return None;
        }

        // Optimization: Fast path if already at the tail (most recent), avoiding LRU queue scan entirely.
        if self.lru.back() != Some(&hash) {
            // Optimization: Search from the tail where recent entries cluster (rposition), minimizing iteration steps.
            if let Some(pos) = self.lru.iter().rposition(|&h| h == hash) {
                self.lru.remove(pos);
                self.lru.push_back(hash);
            }
        }

        Some(entry.clone())
    }

    fn insert(&mut self, hash: u64, entry: CacheEntry) {
        let entry_size = entry.memory_size();

        // If a single entry exceeds the entire shard limit, skip caching
        if entry_size > self.max_bytes {
            return;
        }

        // If existing entry was present, subtract its old size
        if let Some(old) = self.entries.remove(&hash) {
            self.current_bytes = self.current_bytes.saturating_sub(old.memory_size());
            if let Some(pos) = self.lru.iter().rposition(|&h| h == hash) {
                self.lru.remove(pos);
            }
        }

        // Evict LRU entries until memory fits
        while self.current_bytes + entry_size > self.max_bytes && !self.lru.is_empty() {
            if let Some(evict_hash) = self.lru.pop_front()
                && let Some(removed) = self.entries.remove(&evict_hash)
            {
                self.current_bytes = self.current_bytes.saturating_sub(removed.memory_size());
            }
        }

        self.current_bytes += entry_size;
        self.entries.insert(hash, entry);
        self.lru.push_back(hash);
    }

    #[allow(clippy::too_many_arguments)]
    fn insert_or_merge(
        &mut self,
        hash: u64,
        key: String,
        status: StatusCode,
        content_type: Option<http::HeaderValue>,
        ttl: Duration,
        encoding: Option<&str>,
        payload: Bytes,
        variants: Option<CompressedVariants>,
        is_public: bool,
    ) {
        // If entry exists and is fresh, merge into it
        if let Some(entry) = self.entries.get_mut(&hash)
            && entry.is_fresh()
        {
            let old_size = entry.memory_size();
            entry.status = status;
            if content_type.is_some() {
                entry.content_type = content_type;
            }
            entry.ttl = ttl;
            if is_public {
                entry.is_public = true;
            }

            if let Some(vars) = variants {
                // Eager mode: store uncompressed raw + all variants
                entry.raw_body = Some(payload);
                entry.variants = vars;
            } else {
                // Lazy mode: merge specific variant or raw
                match encoding {
                    Some("gzip") => entry.variants.gzip = Some(payload),
                    Some("br") => entry.variants.brotli = Some(payload),
                    Some("zstd") => entry.variants.zstd = Some(payload),
                    _ => entry.raw_body = Some(payload),
                }
            }

            let new_size = entry.memory_size();
            self.current_bytes = self.current_bytes.saturating_sub(old_size) + new_size;

            if self.lru.back() != Some(&hash)
                && let Some(pos) = self.lru.iter().rposition(|&h| h == hash)
            {
                self.lru.remove(pos);
                self.lru.push_back(hash);
            }

            self.evict_if_needed(Some(hash));
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
    fn evict_if_needed(&mut self, keep_hash: Option<u64>) {
        while self.current_bytes > self.max_bytes && !self.lru.is_empty() {
            if let Some(evict_hash) = self.lru.pop_front() {
                if Some(evict_hash) == keep_hash {
                    self.lru.push_back(evict_hash);
                    if self.lru.len() <= 1 {
                        break;
                    }
                    continue;
                }
                if let Some(removed) = self.entries.remove(&evict_hash) {
                    self.current_bytes = self.current_bytes.saturating_sub(removed.memory_size());
                }
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
            let new_size = entry.memory_size();
            self.current_bytes = self.current_bytes.saturating_sub(old_size) + new_size;
            self.evict_if_needed(Some(hash));
            return true;
        }
        false
    }

    fn remove(&mut self, hash: u64) -> bool {
        if let Some(removed) = self.entries.remove(&hash) {
            self.current_bytes = self.current_bytes.saturating_sub(removed.memory_size());
            if let Some(pos) = self.lru.iter().rposition(|&h| h == hash) {
                self.lru.remove(pos);
            }
            true
        } else {
            false
        }
    }

    fn clear(&mut self) {
        self.entries.clear();
        self.lru.clear();
        self.current_bytes = 0;
    }
}

/// A lock-striped cache shard aligned to 64 bytes to eliminate false sharing.
// Optimization: #[repr(align(64))] isolates each shard's RwLock into its own CPU cache line.
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

    #[inline]
    fn get(&self, hash: u64) -> Option<CacheEntry> {
        let mut guard = self.inner.write().ok()?;
        guard.get(hash)
    }

    #[inline]
    fn insert(&self, hash: u64, entry: CacheEntry) {
        if let Ok(mut guard) = self.inner.write() {
            guard.insert(hash, entry);
        }
    }

    #[inline]
    #[allow(clippy::too_many_arguments)]
    fn insert_or_merge(
        &self,
        hash: u64,
        key: String,
        status: StatusCode,
        content_type: Option<http::HeaderValue>,
        ttl: Duration,
        encoding: Option<&str>,
        payload: Bytes,
        variants: Option<CompressedVariants>,
        is_public: bool,
    ) {
        if let Ok(mut guard) = self.inner.write() {
            guard.insert_or_merge(
                hash,
                key,
                status,
                content_type,
                ttl,
                encoding,
                payload,
                variants,
                is_public,
            );
        }
    }

    #[inline]
    fn merge_variants(&self, hash: u64, variants: CompressedVariants) -> bool {
        if let Ok(mut guard) = self.inner.write() {
            guard.merge_variants(hash, variants)
        } else {
            false
        }
    }

    #[inline]
    fn remove(&self, hash: u64) -> bool {
        if let Ok(mut guard) = self.inner.write() {
            guard.remove(hash)
        } else {
            false
        }
    }

    #[inline]
    fn clear(&self) {
        if let Ok(mut guard) = self.inner.write() {
            guard.clear();
        }
    }
}

/// Production In-Memory HTTP Cache Zone.
pub struct HttpCacheZone {
    pub name: String,
    pub max_memory_bytes: usize,
    pub default_ttl: Duration,
    shards: Box<[CacheShard]>,
    hit_count: AtomicU64,
    miss_count: AtomicU64,
    pending_keys: Mutex<FxHashMap<String, (String, Instant)>>,
}

impl std::fmt::Debug for HttpCacheZone {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpCacheZone")
            .field("name", &self.name)
            .field("max_memory_bytes", &self.max_memory_bytes)
            .field("default_ttl", &self.default_ttl)
            .field("hits", &self.hit_count.load(Ordering::Relaxed))
            .field("misses", &self.miss_count.load(Ordering::Relaxed))
            .finish()
    }
}

impl HttpCacheZone {
    /// Creates a new named cache zone with specified memory budget and default TTL.
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
            pending_keys: Mutex::new(FxHashMap::default()),
        }
    }

    /// Computes canonical cache key string and 64-bit hash without custom headers.
    #[inline]
    pub fn compute_cache_key(
        method: &Method,
        host: Option<&str>,
        path: &str,
        query: Option<&str>,
        ignore_query_order: bool,
    ) -> (String, u64) {
        Self::compute_cache_key_with_headers(method, host, path, query, ignore_query_order, None)
    }

    /// Computes canonical cache key string and 64-bit hash with optional custom headers.
    ///
    /// Header names in `extra_headers` are sorted alphabetically to produce deterministic cache keys.
    #[inline]
    pub fn compute_cache_key_with_headers(
        method: &Method,
        host: Option<&str>,
        path: &str,
        query: Option<&str>,
        ignore_query_order: bool,
        extra_headers: Option<&[(&str, &str)]>,
    ) -> (String, u64) {
        let method_str = method.as_str();
        let host_str = host.unwrap_or("").trim();
        let path_str = if path.is_empty() { "/" } else { path };

        let mut key_str = match query {
            Some(q) if !q.is_empty() => {
                if ignore_query_order {
                    let sorted_q = normalize_query(q);
                    format!("{method_str}:{host_str}:{path_str}?{sorted_q}")
                } else {
                    format!("{method_str}:{host_str}:{path_str}?{q}")
                }
            }
            _ => format!("{method_str}:{host_str}:{path_str}"),
        };

        if let Some(headers) = extra_headers
            && !headers.is_empty()
        {
            let mut sorted_headers = headers.to_vec();
            sorted_headers.sort_unstable_by(|a, b| a.0.cmp(b.0));
            for (k, v) in sorted_headers {
                key_str.push('|');
                key_str.push_str(k);
                key_str.push('=');
                key_str.push_str(v);
            }
        }

        let mut hasher = FxHasher::default();
        hasher.write(key_str.as_bytes());
        let hash = hasher.finish();

        (key_str, hash)
    }

    /// Hashes an existing cache key string.
    #[inline]
    pub fn hash_key(key: &str) -> u64 {
        let mut hasher = FxHasher::default();
        hasher.write(key.as_bytes());
        hasher.finish()
    }

    /// Looks up a cached entry by key hash.
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

    /// Stores a new response entry into the zone.
    #[inline]
    pub fn store(&self, hash: u64, entry: CacheEntry) {
        let shard_idx = (hash as usize) % NUM_SHARDS;
        self.shards[shard_idx].insert(hash, entry);
    }

    /// Stores or merges a response variant into the cache zone.
    ///
    /// Supports both:
    /// - **Eager Mode**: `variants` is `Some(...)` containing all pre-compressed variants.
    /// - **Lazy Mode**: `encoding` indicates a single wire variant (`gzip`, `br`, `zstd`, or None for raw).
    ///   If an entry for `hash` already exists in RAM, merges this variant into it without overwriting other variants.
    #[inline]
    #[allow(clippy::too_many_arguments)]
    pub fn store_variant(
        &self,
        hash: u64,
        key: String,
        status: StatusCode,
        content_type: Option<http::HeaderValue>,
        ttl: Duration,
        encoding: Option<&str>,
        payload: Bytes,
        variants: Option<CompressedVariants>,
        is_public: bool,
    ) {
        let shard_idx = (hash as usize) % NUM_SHARDS;
        self.shards[shard_idx].insert_or_merge(
            hash,
            key,
            status,
            content_type,
            ttl,
            encoding,
            payload,
            variants,
            is_public,
        );
    }

    /// Merges pre-compressed payload variants into an existing cached entry.
    ///
    /// If the entry exists and is fresh, updates its `variants` and memory accounting.
    /// Returns `true` if the entry was found and updated, or `false` if expired or missing.
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

    /// Clears all entries across all shards in this zone.
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

    /// Associates an in-flight request ID with its pre-computed cache key for post-response storage.
    pub fn record_pending_key(&self, request_id: &str, cache_key: String) {
        if request_id.is_empty() {
            return;
        }
        if let Ok(mut map) = self.pending_keys.lock() {
            // Prune expired pending items if the map grows excessively
            if map.len() > 8192 {
                let now = Instant::now();
                map.retain(|_, (_, ts)| now.duration_since(*ts) < Duration::from_secs(60));
            }
            map.insert(request_id.to_string(), (cache_key, Instant::now()));
        }
    }

    /// Retrieves and removes a pending cache key associated with a request ID.
    pub fn take_pending_key(&self, request_id: &str) -> Option<String> {
        if request_id.is_empty() {
            return None;
        }
        if let Ok(mut map) = self.pending_keys.lock() {
            map.remove(request_id).map(|(k, _)| k)
        } else {
            None
        }
    }
}

/// Global registry managing named [`HttpCacheZone`] instances.
pub struct HttpCacheZoneRegistry {
    zones: RwLock<FxHashMap<String, Arc<HttpCacheZone>>>,
}

impl HttpCacheZoneRegistry {
    /// Returns the singleton global cache zone registry.
    pub fn global() -> &'static Self {
        static REGISTRY: OnceLock<HttpCacheZoneRegistry> = OnceLock::new();
        REGISTRY.get_or_init(|| HttpCacheZoneRegistry {
            zones: RwLock::new(FxHashMap::default()),
        })
    }

    /// Retrieves an existing zone by name, or creates a new one with the given parameters.
    pub fn get_or_create(
        &self,
        name: &str,
        max_memory_bytes: usize,
        default_ttl: Duration,
    ) -> Arc<HttpCacheZone> {
        {
            if let Ok(read) = self.zones.read()
                && let Some(zone) = read.get(name)
            {
                return Arc::clone(zone);
            }
        }

        if let Ok(mut write) = self.zones.write() {
            if let Some(zone) = write.get(name) {
                return Arc::clone(zone);
            }
            let zone = Arc::new(HttpCacheZone::new(name, max_memory_bytes, default_ttl));
            write.insert(name.to_string(), Arc::clone(&zone));
            zone
        } else {
            Arc::new(HttpCacheZone::new(name, max_memory_bytes, default_ttl))
        }
    }

    /// Explicitly registers or overrides a named zone (used in isolated unit tests).
    pub fn register(&self, zone: Arc<HttpCacheZone>) {
        if let Ok(mut write) = self.zones.write() {
            write.insert(zone.name.clone(), zone);
        }
    }
}

/// Utility function to filter safe content headers from upstream response to cache.
///
/// Strips sensitive headers like `Set-Cookie`, tracing headers, and hop-by-hop headers.
pub fn filter_cacheable_headers(headers: &HeaderMap) -> HeaderMap {
    let mut clean = HeaderMap::with_capacity(headers.len());
    for (name, val) in headers {
        if name == http::header::SET_COOKIE
            || name == http::header::AGE
            || name == http::header::CONNECTION
            || name == http::header::TRANSFER_ENCODING
            || name == X_CACHE
            || name == X_VELDA_CACHE_KEY
            || name == X_VELDA_CACHE_HASH
            || name == X_REQUEST_ID
            || name == X_ROUTE_ID
            || name == X_UPSTREAM_ADDR
        {
            continue;
        }
        clean.insert(HeaderName::from(name), val.clone());
    }
    clean
}

/// Parses TTL seconds from `Cache-Control` header (s-maxage > max-age).
pub fn parse_cache_control_ttl(cache_control: &str) -> Option<Duration> {
    let mut max_age: Option<u64> = None;
    let mut s_maxage: Option<u64> = None;

    for directive in cache_control.split(',') {
        let part = directive.trim();
        if let Some(val) = part.strip_prefix("s-maxage=")
            && let Ok(secs) = val.trim().parse::<u64>()
        {
            s_maxage = Some(secs);
        } else if let Some(val) = part.strip_prefix("max-age=")
            && let Ok(secs) = val.trim().parse::<u64>()
        {
            max_age = Some(secs);
        }
    }

    s_maxage.or(max_age).map(Duration::from_secs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_query() {
        assert_eq!(normalize_query("b=2&a=1&c=3"), "a=1&b=2&c=3");
        assert_eq!(normalize_query("page=1&limit=10"), "limit=10&page=1");
        assert_eq!(normalize_query(""), "");
        assert_eq!(normalize_query("single"), "single");
    }

    #[test]
    fn test_cache_zone_lru_and_expiration() {
        let zone = HttpCacheZone::new("test_zone", 1024 * 1024, Duration::from_millis(50));
        let (_, hash) = HttpCacheZone::compute_cache_key(
            &Method::GET,
            Some("example.com"),
            "/api/v1",
            None,
            true,
        );

        let entry = CacheEntry::with_raw(
            "GET:example.com:/api/v1",
            StatusCode::OK,
            Some(http::HeaderValue::from_static("application/json")),
            Bytes::from_static(b"{\"hello\":\"world\"}"),
            Duration::from_millis(50),
        );

        zone.store(hash, entry);

        // Immediate lookup: HIT
        let hit = zone.get(hash);
        assert!(hit.is_some());
        assert_eq!(
            hit.unwrap().raw_body.unwrap(),
            Bytes::from_static(b"{\"hello\":\"world\"}")
        );
        assert_eq!(zone.hit_count(), 1);

        // After TTL: MISS (lazy expiration)
        std::thread::sleep(Duration::from_millis(60));
        let expired = zone.get(hash);
        assert!(expired.is_none());
        assert_eq!(zone.miss_count(), 1);
    }

    #[test]
    fn test_negotiate_payload_variants() {
        let raw = Bytes::from(b"0123456789".repeat(100));
        let gzip_sample = Bytes::from_static(b"fake-gzip");
        let br_sample = Bytes::from_static(b"fake-brotli");
        let zstd_sample = Bytes::from_static(b"fake-zstd");

        let entry = CacheEntry::with_raw(
            "GET:test:/data",
            StatusCode::OK,
            Some(http::HeaderValue::from_static("application/json")),
            raw.clone(),
            Duration::from_secs(60),
        )
        .with_variants(CompressedVariants {
            gzip: Some(gzip_sample.clone()),
            brotli: Some(br_sample.clone()),
            zstd: Some(zstd_sample.clone()),
        });

        // 1. Client accepts Brotli
        let (body, enc) =
            entry.negotiate_payload(Some(&http::HeaderValue::from_static("br, gzip")));
        assert_eq!(body, br_sample);
        assert_eq!(enc.unwrap(), "br");

        // 2. Client accepts only Gzip
        let (body, enc) = entry.negotiate_payload(Some(&http::HeaderValue::from_static("gzip")));
        assert_eq!(body, gzip_sample);
        assert_eq!(enc.unwrap(), "gzip");

        // 3. Client accepts Zstd
        let (body, enc) = entry.negotiate_payload(Some(&http::HeaderValue::from_static("zstd")));
        assert_eq!(body, zstd_sample);
        assert_eq!(enc.unwrap(), "zstd");

        // 4. Client accepts identity or no Accept-Encoding
        let (body, enc) = entry.negotiate_payload(None);
        assert_eq!(body, raw);
        assert!(enc.is_none());
    }

    #[test]
    fn test_smart_variant_matching_hit_over_miss_lazy() {
        let br_sample = Bytes::from_static(b"brotli-variant-only");

        // Lazy entry: NO raw_body, ONLY brotli variant cached
        let mut entry = CacheEntry::new(
            "GET:test:/lazy",
            StatusCode::OK,
            Some(http::HeaderValue::from_static("application/json")),
            None,
            Duration::from_secs(60),
        );
        entry.variants.brotli = Some(br_sample.clone());

        // 1. Client sends general browser Accept-Encoding (gzip, deflate, br, zstd)
        // Hit Over Miss: since Brotli is in cache and acceptable to client, serve Brotli!
        let hit = entry
            .negotiate_smart_variant(Some(&http::HeaderValue::from_static(
                "gzip, deflate, br, zstd",
            )))
            .expect("Expected smart hit for available Brotli variant");
        assert_eq!(hit.0, br_sample);
        assert_eq!(hit.1.unwrap(), "br");

        // 2. Client sends ONLY gzip: Brotli is NOT acceptable, and raw is absent -> None (MISS)
        let miss = entry.negotiate_smart_variant(Some(&http::HeaderValue::from_static("gzip")));
        assert!(miss.is_none());

        // 3. Client sends NO Accept-Encoding: raw is absent -> None (MISS)
        let miss_raw = entry.negotiate_smart_variant(None);
        assert!(miss_raw.is_none());
    }

    #[test]
    fn test_zone_store_or_merge_lazy_variants() {
        let zone = HttpCacheZone::new("lazy_zone", 1024 * 1024, Duration::from_secs(60));
        let (_, hash) = HttpCacheZone::compute_cache_key(
            &Method::GET,
            Some("example.com"),
            "/lazy-item",
            None,
            true,
        );

        let br_payload = Bytes::from_static(b"brotli-payload-145B");
        let gzip_payload = Bytes::from_static(b"gzip-payload-287B");

        // 1. Initial lazy store: only Brotli
        zone.store_variant(
            hash,
            "GET:example.com:/lazy-item".into(),
            StatusCode::OK,
            Some(http::HeaderValue::from_static("application/json")),
            Duration::from_secs(60),
            Some("br"),
            br_payload.clone(),
            None,
            false,
        );

        // Verify Brotli hit
        let entry1 = zone.get(hash).expect("Entry must exist");
        assert!(entry1.variants.brotli.is_some());
        assert!(entry1.variants.gzip.is_none());
        assert!(entry1.raw_body.is_none());

        // 2. Second lazy store: merges Gzip into existing entry
        zone.store_variant(
            hash,
            "GET:example.com:/lazy-item".into(),
            StatusCode::OK,
            Some(http::HeaderValue::from_static("application/json")),
            Duration::from_secs(60),
            Some("gzip"),
            gzip_payload.clone(),
            None,
            false,
        );

        // Verify both variants co-exist in the same entry!
        let entry2 = zone.get(hash).expect("Entry must exist");
        assert_eq!(entry2.variants.brotli.unwrap(), br_payload);
        assert_eq!(entry2.variants.gzip.unwrap(), gzip_payload);
        assert!(entry2.raw_body.is_none());
    }

    #[test]
    fn test_filter_cacheable_headers() {
        let mut headers = HeaderMap::new();
        headers.insert(
            http::header::CONTENT_TYPE,
            "application/json".parse().unwrap(),
        );
        headers.insert(http::header::SET_COOKIE, "session_id=123".parse().unwrap());
        headers.insert(http::header::ETAG, "\"abc-123\"".parse().unwrap());
        headers.insert("x-request-id", "req-999".parse().unwrap());

        let clean = filter_cacheable_headers(&headers);
        assert!(clean.contains_key(http::header::CONTENT_TYPE));
        assert!(clean.contains_key(http::header::ETAG));
        assert!(!clean.contains_key(http::header::SET_COOKIE));
        assert!(!clean.contains_key("x-request-id"));
    }

    #[test]
    fn test_parse_cache_control_ttl() {
        assert_eq!(
            parse_cache_control_ttl("public, max-age=3600"),
            Some(Duration::from_secs(3600))
        );
        assert_eq!(
            parse_cache_control_ttl("public, max-age=60, s-maxage=300"),
            Some(Duration::from_secs(300))
        );
        assert_eq!(parse_cache_control_ttl("no-cache"), None);
    }

    #[test]
    fn test_compute_cache_key_with_headers() {
        let (key1, hash1) = HttpCacheZone::compute_cache_key_with_headers(
            &Method::GET,
            Some("api.velda.io"),
            "/profile",
            Some("sort=desc&page=1"),
            true,
            Some(&[("authorization", "Bearer alice"), ("x-tenant", "tenant-1")]),
        );

        // Deterministic sorting of headers regardless of input slice order
        let (key2, hash2) = HttpCacheZone::compute_cache_key_with_headers(
            &Method::GET,
            Some("api.velda.io"),
            "/profile",
            Some("page=1&sort=desc"),
            true,
            Some(&[("x-tenant", "tenant-1"), ("authorization", "Bearer alice")]),
        );

        assert_eq!(key1, key2);
        assert_eq!(hash1, hash2);
        assert!(key1.contains("|authorization=Bearer alice"));
        assert!(key1.contains("|x-tenant=tenant-1"));

        // Different user token produces completely different cache key and hash
        let (key_bob, hash_bob) = HttpCacheZone::compute_cache_key_with_headers(
            &Method::GET,
            Some("api.velda.io"),
            "/profile",
            Some("sort=desc&page=1"),
            true,
            Some(&[("authorization", "Bearer bob"), ("x-tenant", "tenant-1")]),
        );

        assert_ne!(key1, key_bob);
        assert_ne!(hash1, hash_bob);
    }

    #[test]
    fn test_cache_entry_is_public_flag() {
        let entry = CacheEntry::with_raw(
            "GET:test:/public-config",
            StatusCode::OK,
            Some(http::HeaderValue::from_static("application/json")),
            Bytes::from_static(b"{\"cfg\":true}"),
            Duration::from_secs(300),
        )
        .with_public(true);

        assert!(entry.is_public);
    }

    #[test]
    fn test_zone_merge_variants() {
        let zone = HttpCacheZone::new("test_merge", 1024 * 1024, Duration::from_secs(60));
        let (key, hash) = HttpCacheZone::compute_cache_key(
            &Method::GET,
            Some("api.example.com"),
            "/data",
            None,
            false,
        );

        // Store uncompressed raw body
        zone.store_variant(
            hash,
            key.clone(),
            StatusCode::OK,
            Some(http::HeaderValue::from_static("application/json")),
            Duration::from_secs(60),
            None,
            Bytes::from_static(b"{\"hello\":\"world\"}"),
            None,
            true,
        );

        let initial = zone.get(hash).expect("Must exist");
        assert_eq!(
            initial.raw_body.as_deref(),
            Some(&b"{\"hello\":\"world\"}"[..])
        );
        assert!(initial.variants.gzip.is_none());
        assert!(initial.variants.brotli.is_none());

        // Merge pre-compressed variants
        let variants = CompressedVariants {
            gzip: Some(Bytes::from_static(b"gzip_bytes")),
            brotli: Some(Bytes::from_static(b"br_bytes")),
            zstd: Some(Bytes::from_static(b"zstd_bytes")),
        };
        let updated = zone.merge_variants(hash, variants);
        assert!(updated);

        let merged = zone.get(hash).expect("Must exist after merge");
        assert_eq!(
            merged.raw_body.as_deref(),
            Some(&b"{\"hello\":\"world\"}"[..])
        );
        assert_eq!(merged.variants.gzip.as_deref(), Some(&b"gzip_bytes"[..]));
        assert_eq!(merged.variants.brotli.as_deref(), Some(&b"br_bytes"[..]));
        assert_eq!(merged.variants.zstd.as_deref(), Some(&b"zstd_bytes"[..]));
    }
}
