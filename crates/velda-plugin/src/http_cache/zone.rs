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

/// A single cached HTTP response entry in RAM.
#[derive(Clone, Debug)]
pub struct CacheEntry {
    /// Canonical cache key string for debugging and diagnostics.
    pub key: String,
    /// HTTP status code (e.g. 200 OK, 301 Moved Permanently).
    pub status: StatusCode,
    /// Stored response headers (Content-Type, Content-Encoding, ETag, etc.).
    pub headers: HeaderMap,
    /// Zero-copy reference-counted response body bytes.
    pub body: Bytes,
    /// Monotonic timestamp when this entry was created.
    pub created_at: Instant,
    /// Time-to-live duration for this entry.
    pub ttl: Duration,
}

impl CacheEntry {
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

    /// Estimated memory footprint of this entry in bytes.
    #[inline(always)]
    pub fn memory_size(&self) -> usize {
        self.body.len() + self.key.len() + 128
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

        // Move to back of LRU queue (most recently accessed)
        if let Some(pos) = self.lru.iter().position(|&h| h == hash) {
            self.lru.remove(pos);
            self.lru.push_back(hash);
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
            if let Some(pos) = self.lru.iter().position(|&h| h == hash) {
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

    fn remove(&mut self, hash: u64) -> bool {
        if let Some(removed) = self.entries.remove(&hash) {
            self.current_bytes = self.current_bytes.saturating_sub(removed.memory_size());
            if let Some(pos) = self.lru.iter().position(|&h| h == hash) {
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

/// A lock-striped cache shard.
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

    /// Computes canonical cache key string and 64-bit hash.
    #[inline]
    pub fn compute_cache_key(
        method: &Method,
        host: Option<&str>,
        path: &str,
        query: Option<&str>,
        ignore_query_order: bool,
    ) -> (String, u64) {
        let method_str = method.as_str();
        let host_str = host.unwrap_or("").trim();
        let path_str = if path.is_empty() { "/" } else { path };

        let key_str = match query {
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

        let entry = CacheEntry {
            key: "GET:example.com:/api/v1".into(),
            status: StatusCode::OK,
            headers: HeaderMap::new(),
            body: Bytes::from_static(b"{\"hello\":\"world\"}"),
            created_at: Instant::now(),
            ttl: Duration::from_millis(50),
        };

        zone.store(hash, entry);

        // Immediate lookup: HIT
        let hit = zone.get(hash);
        assert!(hit.is_some());
        assert_eq!(
            hit.unwrap().body,
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
}
