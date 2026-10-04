//! Phase 4: Cache Phase.
//!
//! Responsible for providing the Single Source of Truth for DNS resolution in memory:
//! - Positive TTL caching for valid resolved IP addresses.
//! - Negative TTL caching (NXDOMAIN) to prevent external query storms.
//! - Lock-optimized concurrent reads guaranteeing sub-microsecond Zero-IO lookups on hot paths.
//! - Bounded capacity & auto-purging to prevent memory leaks / OOM attacks.

use std::borrow::Cow;
use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

/// Default maximum number of cached entries per table before triggering eviction.
pub const DEFAULT_CACHE_CAPACITY: usize = 50_000;

/// Returns a borrowed slice if `host` is already lowercase,
/// avoiding heap allocation of a new `String` on the request serving hot path.
#[inline]
fn normalize_host<'a>(host: &'a str) -> Cow<'a, str> {
    if host.bytes().any(|b| b.is_ascii_uppercase()) {
        Cow::Owned(host.to_lowercase())
    } else {
        Cow::Borrowed(host)
    }
}

/// Status of a DNS cache lookup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CacheLookup {
    /// Valid positive cache entry with resolved IP addresses.
    ///
    /// `Arc<[IpAddr]>` rather than `Vec<IpAddr>` so a hit is a refcount bump,
    /// not an allocation + copy, on the hot path.
    Hit(Arc<[IpAddr]>),
    /// Valid negative cache entry (NXDOMAIN).
    NegativeHit,
    /// Cache miss or entry expired.
    Miss,
}

#[derive(Debug, Clone)]
struct PositiveEntry {
    ips: Arc<[IpAddr]>,
    expires_at: Instant,
}

#[derive(Debug, Clone)]
struct NegativeEntry {
    expires_at: Instant,
}

/// Thread-safe in-memory cache for DNS responses with bounded capacity.
#[derive(Debug)]
pub struct DnsCache {
    positive: RwLock<HashMap<String, PositiveEntry>>,
    negative: RwLock<HashMap<String, NegativeEntry>>,
    // Hard cap per table: hostnames can be attacker-influenced (Host-derived upstreams, NXDOMAIN floods), so growth must be bounded.
    max_capacity: usize,
}

impl Default for DnsCache {
    fn default() -> Self {
        Self::new()
    }
}

impl DnsCache {
    /// Creates a new DNS cache with default capacity (50,000 entries).
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_CACHE_CAPACITY)
    }

    /// Creates a new DNS cache with a specified maximum entry capacity per table.
    pub fn with_capacity(max_capacity: usize) -> Self {
        Self {
            positive: RwLock::new(HashMap::new()),
            negative: RwLock::new(HashMap::new()),
            max_capacity,
        }
    }

    /// Looks up a hostname in the cache with zero heap allocations on the hot path.
    pub fn get(&self, host: &str) -> CacheLookup {
        let key = normalize_host(host);

        // 1. Check positive cache
        {
            let pos = self.positive.read().unwrap();
            // Borrowed lookup; no key allocation on the read path.
            if let Some(entry) = pos.get(key.as_ref())
                && entry.expires_at > Instant::now()
            {
                return CacheLookup::Hit(Arc::clone(&entry.ips));
            }
        }

        // 2. Check negative cache
        {
            let neg = self.negative.read().unwrap();
            if let Some(entry) = neg.get(key.as_ref())
                && entry.expires_at > Instant::now()
            {
                return CacheLookup::NegativeHit;
            }
        }

        CacheLookup::Miss
    }

    /// Inserts a positive resolution result with TTL.
    ///
    /// Automatically enforces bounded capacity and evicts expired/excess entries.
    pub fn insert_positive(&self, host: &str, ips: Vec<IpAddr>, ttl: Duration) {
        let key = normalize_host(host).into_owned();
        let expires_at = Instant::now() + ttl;

        // Clear negative cache entry if any
        self.negative.write().unwrap().remove(&key);

        let mut pos = self.positive.write().unwrap();

        // Bound eviction under write lock: sample up to 32 entries to evict expired ones,
        // avoiding an O(N) full-table sweep that blocks concurrent readers.
        if !pos.contains_key(&key) && pos.len() >= self.max_capacity {
            let now = Instant::now();
            let expired: Vec<String> = pos
                .iter()
                .take(32)
                .filter(|(_, v)| v.expires_at <= now)
                .map(|(k, _)| k.clone())
                .collect();

            for k in expired {
                pos.remove(&k);
            }

            // If still full after bounded purge, evict arbitrary entry to strictly enforce invariant
            if pos.len() >= self.max_capacity
                && let Some(first_key) = pos.keys().next().cloned()
            {
                pos.remove(&first_key);
            }
        }

        // Convert Vec to Arc<[IpAddr]> once upon insertion to enable zero-alloc reads
        pos.insert(
            key,
            PositiveEntry {
                ips: Arc::from(ips),
                expires_at,
            },
        );
    }

    /// Inserts a negative resolution result (NXDOMAIN) with TTL to prevent query storms.
    ///
    /// Automatically enforces bounded capacity and evicts expired/excess entries.
    pub fn insert_negative(&self, host: &str, ttl: Duration) {
        let key = normalize_host(host).into_owned();
        let expires_at = Instant::now() + ttl;

        // Clear positive cache entry if any
        self.positive.write().unwrap().remove(&key);

        let mut neg = self.negative.write().unwrap();

        // Bound eviction under write lock: sample up to 32 entries to evict expired ones,
        // avoiding an O(N) full-table sweep that blocks concurrent readers.
        if !neg.contains_key(&key) && neg.len() >= self.max_capacity {
            let now = Instant::now();
            let expired: Vec<String> = neg
                .iter()
                .take(32)
                .filter(|(_, v)| v.expires_at <= now)
                .map(|(k, _)| k.clone())
                .collect();

            for k in expired {
                neg.remove(&k);
            }

            // If still full after bounded purge, evict arbitrary entry to strictly enforce invariant
            if neg.len() >= self.max_capacity
                && let Some(first_key) = neg.keys().next().cloned()
            {
                neg.remove(&first_key);
            }
        }

        neg.insert(key, NegativeEntry { expires_at });
    }

    /// Time left before the live entry for `host` (positive, else negative) expires.
    ///
    /// Lets the background refresher wake exactly when the cache would go stale
    /// instead of polling on a fixed period.
    pub fn ttl_remaining(&self, host: &str) -> Option<Duration> {
        let key = normalize_host(host);
        let now = Instant::now();
        if let Some(e) = self.positive.read().unwrap().get(key.as_ref()) {
            return e.expires_at.checked_duration_since(now);
        }
        self.negative
            .read()
            .unwrap()
            .get(key.as_ref())
            .and_then(|e| e.expires_at.checked_duration_since(now))
    }

    /// Clears expired entries from both caches.
    pub fn sweep_expired(&self) {
        let now = Instant::now();
        self.positive
            .write()
            .unwrap()
            .retain(|_, v| v.expires_at > now);
        self.negative
            .write()
            .unwrap()
            .retain(|_, v| v.expires_at > now);
    }

    /// Returns the count of active entries in the positive cache.
    pub fn positive_len(&self) -> usize {
        self.positive.read().unwrap().len()
    }

    /// Returns the count of active entries in the negative cache.
    pub fn negative_len(&self) -> usize {
        self.negative.read().unwrap().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_positive_and_negative_cache() {
        let cache = DnsCache::new();
        let ip: IpAddr = "10.0.0.1".parse().unwrap();

        cache.insert_positive("api.local", vec![ip], Duration::from_secs(10));
        assert_eq!(cache.get("api.local"), CacheLookup::Hit(Arc::from([ip])));

        cache.insert_negative("bad.local", Duration::from_secs(10));
        assert_eq!(cache.get("bad.local"), CacheLookup::NegativeHit);
        assert_eq!(cache.get("unknown.local"), CacheLookup::Miss);
    }

    #[test]
    fn test_bounded_capacity_enforcement() {
        // The cache must never exceed max_capacity.
        let cache = DnsCache::with_capacity(3);

        cache.insert_positive(
            "host1.local",
            vec!["10.0.0.1".parse().unwrap()],
            Duration::from_secs(60),
        );
        cache.insert_positive(
            "host2.local",
            vec!["10.0.0.2".parse().unwrap()],
            Duration::from_secs(60),
        );
        cache.insert_positive(
            "host3.local",
            vec!["10.0.0.3".parse().unwrap()],
            Duration::from_secs(60),
        );
        assert_eq!(cache.positive_len(), 3);

        // 4th insert must trigger eviction, keeping capacity at 3
        cache.insert_positive(
            "host4.local",
            vec!["10.0.0.4".parse().unwrap()],
            Duration::from_secs(60),
        );
        assert_eq!(cache.positive_len(), 3);
    }

    #[test]
    fn test_case_insensitive_cache_lookup() {
        let cache = DnsCache::new();
        let ip: IpAddr = "10.0.0.1".parse().unwrap();

        // Insert lowercase, lookup uppercase and mixed case
        cache.insert_positive("api.velda.io", vec![ip], Duration::from_secs(10));
        assert_eq!(cache.get("API.VELDA.IO"), CacheLookup::Hit(Arc::from([ip])));
        assert_eq!(cache.get("Api.Velda.Io"), CacheLookup::Hit(Arc::from([ip])));
        assert_eq!(cache.get("api.velda.io"), CacheLookup::Hit(Arc::from([ip])));

        // Insert uppercase, lookup lowercase
        let ip2: IpAddr = "10.0.0.2".parse().unwrap();
        cache.insert_positive("UPPER.SERVICE.LOCAL", vec![ip2], Duration::from_secs(10));
        assert_eq!(
            cache.get("upper.service.local"),
            CacheLookup::Hit(Arc::from([ip2]))
        );
        assert_eq!(
            cache.get("Upper.Service.Local"),
            CacheLookup::Hit(Arc::from([ip2]))
        );
    }
}
