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
    #[inline]
    pub fn lookup(&self, path: &str) -> Option<Bytes> {
        self.entries
            .load()
            .iter()
            .find(|e| &*e.path == path)
            .map(|e| e.response.clone())
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
