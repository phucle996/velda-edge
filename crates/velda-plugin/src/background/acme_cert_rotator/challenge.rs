//! Ephemeral HTTP-01 challenge store with RAII teardown.
//!
//! The Shadow Worker pre-bakes the complete HTTP response (status line, headers, key
//! authorization) into one immutable [`Bytes`] blob and publishes it under the challenge path.
//! Readers (frontline) perform a lock-free [`ArcSwap`] load: no allocation, no channel, no lock.
//! Writers are rare (a few per renewal) and use copy-on-write.
//!
//! Every published entry is owned by a [`ChallengeGuard`]; dropping the guard removes the entry
//! whether the ACME order succeeded, failed, timed out, or the task was cancelled.

use std::sync::Arc;

use arc_swap::ArcSwap;
use bytes::Bytes;

/// URL path prefix mandated by RFC 8555 §8.3.
pub const CHALLENGE_PATH_PREFIX: &str = "/.well-known/acme-challenge/";

/// Explicitly invalidates and flushes a memory slice out of L1 and L2 CPU caches.
///
/// Confines ephemeral ACME HTTP-01 response bytes strictly to L3/DRAM, preventing
/// them from occupying precious L1D/L2 cache lines while waiting for remote
/// ACME CA verification (which takes hundreds of milliseconds or seconds).
#[inline]
pub fn evict_from_l1_l2(slice: &[u8]) {
    #[cfg(target_arch = "x86_64")]
    {
        let ptr = slice.as_ptr();
        let len = slice.len();
        let mut offset = 0;
        while offset < len {
            unsafe {
                core::arch::x86_64::_mm_clflush(ptr.add(offset));
            }
            offset += 64;
        }
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        let _ = slice;
    }
}

struct Entry {
    path: Box<str>,
    response: Bytes,
}

/// Copy-on-write table of live challenges.
#[derive(Default)]
pub struct ChallengeStore {
    entries: ArcSwap<Vec<Arc<Entry>>>,
}

impl ChallengeStore {
    /// Creates an empty store.
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Looks up the pre-baked response for an exact request path. Lock-free.
    ///
    /// The response bytes are read from L3/DRAM and immediately flushed from L1D/L2
    /// so transient challenge payloads never linger inside frontline CPU caches.
    #[inline]
    pub fn lookup(&self, path: &str) -> Option<Bytes> {
        self.entries
            .load()
            .iter()
            .find(|e| &*e.path == path)
            .map(|e| {
                let resp = e.response.clone();
                evict_from_l1_l2(&resp);
                resp
            })
    }

    /// Returns the number of live challenges.
    pub fn len(&self) -> usize {
        self.entries.load().len()
    }

    /// Returns `true` when no challenge is live.
    pub fn is_empty(&self) -> bool {
        self.entries.load().is_empty()
    }

    /// Publishes a challenge and returns the guard that retracts it on drop.
    pub fn publish(self: &Arc<Self>, token: &str, key_authorization: &str) -> ChallengeGuard {
        let path: Box<str> = format!("{CHALLENGE_PATH_PREFIX}{token}").into();
        let body = key_authorization.as_bytes();
        let mut wire = Vec::with_capacity(96 + body.len());
        wire.extend_from_slice(
            b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nCache-Control: no-store\r\nConnection: close\r\nContent-Length: ",
        );
        wire.extend_from_slice(body.len().to_string().as_bytes());
        wire.extend_from_slice(b"\r\n\r\n");
        wire.extend_from_slice(body);

        let entry = Arc::new(Entry {
            path: path.clone(),
            response: Bytes::from(wire),
        });

        // Enforce L3/DRAM confinement: explicitly evict response bytes and path from L1D/L2
        // so they do not occupy high-priority frontline cache lines while waiting for ACME verification.
        evict_from_l1_l2(&entry.response);
        evict_from_l1_l2(entry.path.as_bytes());

        self.entries.rcu(|cur| {
            let mut next: Vec<Arc<Entry>> =
                cur.iter().filter(|e| e.path != path).cloned().collect();
            next.push(entry.clone());
            next
        });
        ChallengeGuard {
            store: Arc::clone(self),
            path,
        }
    }
}

/// RAII handle: removes its challenge from the store when dropped.
pub struct ChallengeGuard {
    store: Arc<ChallengeStore>,
    path: Box<str>,
}

impl Drop for ChallengeGuard {
    fn drop(&mut self) {
        let path = &self.path;
        self.store.entries.rcu(|cur| {
            cur.iter()
                .filter(|e| &e.path != path)
                .cloned()
                .collect::<Vec<_>>()
        });
        evict_from_l1_l2(self.path.as_bytes());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publish_lookup_and_drop() {
        let store = ChallengeStore::new();
        let path = "/.well-known/acme-challenge/tok";
        assert!(store.lookup(path).is_none());
        {
            let _g = store.publish("tok", "tok.thumb");
            let resp = store.lookup(path).unwrap();
            let text = std::str::from_utf8(&resp).unwrap();
            assert!(text.starts_with("HTTP/1.1 200 OK\r\n"));
            assert!(text.contains("Content-Length: 9\r\n"));
            assert!(text.ends_with("\r\n\r\ntok.thumb"));
            assert_eq!(store.len(), 1);
        }
        assert!(store.is_empty());
        assert!(store.lookup(path).is_none());
    }

    #[test]
    fn guards_are_independent() {
        let store = ChallengeStore::new();
        let a = store.publish("a", "a.k");
        let b = store.publish("b", "b.k");
        drop(a);
        assert!(store.lookup("/.well-known/acme-challenge/a").is_none());
        assert!(store.lookup("/.well-known/acme-challenge/b").is_some());
        drop(b);
        assert!(store.is_empty());
    }
}
