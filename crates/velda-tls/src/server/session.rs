//! High-performance sharded in-memory TLS session cache for downstream termination.
//!
//! Eliminates multi-core lock contention across concurrent worker threads by partitioning
//! session records across cache-line aligned (`#[repr(align(64))]`) independent mutex shards.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};

use rustls::server::StoresServerSessions;

/// Default number of partition shards to eliminate worker contention.
pub const DEFAULT_SESSION_SHARDS: usize = 32;

#[repr(align(64))]
struct SessionShard {
    lock: Mutex<HashMap<Vec<u8>, Vec<u8>>>,
    max_capacity: usize,
}

impl SessionShard {
    fn new(max_capacity: usize) -> Self {
        Self {
            lock: Mutex::new(HashMap::with_capacity(max_capacity.min(256))),
            max_capacity: max_capacity.max(1),
        }
    }
}

/// Sharded in-memory session cache implementing [`StoresServerSessions`].
pub struct ShardedServerSessionCache {
    shards: Box<[SessionShard]>,
    shard_mask: usize,
    total_capacity: usize,
}

impl fmt::Debug for ShardedServerSessionCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ShardedServerSessionCache")
            .field("shards", &self.shards.len())
            .field("total_capacity", &self.total_capacity)
            .finish()
    }
}

#[inline]
fn hash_session_key(bytes: &[u8]) -> usize {
    let mut h: u64 = 0xcbf29ce484222325;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h as usize
}

impl ShardedServerSessionCache {
    /// Creates a new sharded session cache with the given total capacity distributed across shards.
    pub fn new(total_capacity: usize) -> Arc<Self> {
        Self::with_shards(total_capacity, DEFAULT_SESSION_SHARDS)
    }

    /// Creates a new sharded session cache with explicit shard count (must be power of two).
    pub fn with_shards(total_capacity: usize, shard_count: usize) -> Arc<Self> {
        let shard_count = shard_count.next_power_of_two().clamp(4, 256);
        let per_shard_capacity = (total_capacity / shard_count).max(16);

        let mut shards = Vec::with_capacity(shard_count);
        for _ in 0..shard_count {
            shards.push(SessionShard::new(per_shard_capacity));
        }

        Arc::new(Self {
            shards: shards.into_boxed_slice(),
            shard_mask: shard_count - 1,
            total_capacity,
        })
    }

    #[inline]
    fn shard_for(&self, key: &[u8]) -> &SessionShard {
        let idx = hash_session_key(key) & self.shard_mask;
        &self.shards[idx]
    }
}

impl StoresServerSessions for ShardedServerSessionCache {
    fn put(&self, key: Vec<u8>, val: Vec<u8>) -> bool {
        let shard = self.shard_for(&key);
        let mut table = shard.lock.lock().unwrap();
        if table.len() >= shard.max_capacity && !table.contains_key(&key) {
            // Evict an arbitrary entry to keep bounded RAM invariant
            if let Some(first_key) = table.keys().next().cloned() {
                table.remove(&first_key);
            }
        }
        table.insert(key, val);
        true
    }

    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let shard = self.shard_for(key);
        let table = shard.lock.lock().unwrap();
        table.get(key).cloned()
    }

    fn take(&self, key: &[u8]) -> Option<Vec<u8>> {
        let shard = self.shard_for(key);
        let mut table = shard.lock.lock().unwrap();
        table.remove(key)
    }

    fn can_cache(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sharded_session_cache_lifecycle() {
        let cache = ShardedServerSessionCache::with_shards(128, 8);
        assert!(cache.can_cache());

        let key1 = b"session-key-1".to_vec();
        let val1 = b"session-val-1".to_vec();
        assert!(cache.put(key1.clone(), val1.clone()));

        assert_eq!(cache.get(&key1), Some(val1.clone()));
        assert_eq!(cache.take(&key1), Some(val1));
        assert_eq!(cache.get(&key1), None);
    }
}
