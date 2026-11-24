//! HTTP Cache Key Computation, Zero-Alloc Hashing & Query Normalization.
//!
//! ```text
//!   Hot path: hash_request_key_zero_alloc(method, host, path, query, order, headers)
//!     ├─► write METHOD ":" host ":" path directly into FxHasher
//!     ├─► query present -> sort <= 32 params on stack ([&str; 32]) ─► feed "?k=v&..."
//!     ├─► extra headers -> sort <= 16 pairs  on stack ([(&str,&str); 16]) ─► feed "|k=v"
//!     └─► output u64 FxHash (0 heap allocations for standard requests)
//! ```

use std::hash::Hasher;

use http::Method;
use rustc_hash::FxHasher;

use crate::http_cache::constants::{X_CACHE, X_VELDA_CACHE_HASH, X_VELDA_CACHE_KEY};
use velda_core::HeaderWorkspace;
use velda_core::buffer::header::X_REQUEST_ID;

const STACK_KEY_BYTES_CAP: usize = 512;
const STACK_QUERY_CAP: usize = 32;
const STACK_HEADER_CAP: usize = 16;

struct StackKeyBuf {
    stack: [u8; STACK_KEY_BYTES_CAP],
    len: usize,
    heap: Option<Vec<u8>>,
}

impl StackKeyBuf {
    #[inline(always)]
    fn new() -> Self {
        Self {
            stack: [0u8; STACK_KEY_BYTES_CAP],
            len: 0,
            heap: None,
        }
    }

    #[inline(always)]
    fn push_bytes(&mut self, bytes: &[u8]) {
        if self.heap.is_none() && bytes.len() <= STACK_KEY_BYTES_CAP.saturating_sub(self.len) {
            // Safety: `self.len + bytes.len() <= STACK_KEY_BYTES_CAP`, non-overlapping stack slice.
            unsafe {
                std::ptr::copy_nonoverlapping(
                    bytes.as_ptr(),
                    self.stack.as_mut_ptr().add(self.len),
                    bytes.len(),
                );
            }
            self.len += bytes.len();
            return;
        }
        self.push_heap(bytes);
    }

    #[cold]
    #[inline(never)]
    fn push_heap(&mut self, bytes: &[u8]) {
        let vec = self.heap.get_or_insert_with(|| {
            let mut v = Vec::with_capacity(self.len + bytes.len() + 256);
            if let Some(existing) = self.stack.get(..self.len) {
                v.extend_from_slice(existing);
            }
            v
        });
        vec.extend_from_slice(bytes);
    }

    #[inline(always)]
    fn finish_hash(&self) -> u64 {
        let mut hasher = FxHasher::default();
        let slice = match &self.heap {
            Some(vec) => vec.as_slice(),
            None => self.stack.get(..self.len).unwrap_or(&[]),
        };
        hasher.write(slice);
        hasher.finish()
    }
}

/// Normalizes and sorts query string parameters alphabetically (e.g. `b=2&a=1` -> `a=1&b=2`).
pub fn normalize_query(query: &str) -> String {
    let mut params: Vec<&str> = query.split('&').filter(|s| !s.is_empty()).collect();
    params.sort_unstable();
    params.join("&")
}

/// Computes the 64-bit FxHash of a canonical cache key string.
#[inline(always)]
pub fn hash_key(key: &str) -> u64 {
    let mut hasher = FxHasher::default();
    hasher.write(key.as_bytes());
    hasher.finish()
}

#[inline]
fn feed_sorted_query(buf: &mut StackKeyBuf, query: &str) {
    let mut stack_buf = [""; STACK_QUERY_CAP];
    let mut count = 0usize;
    let mut overflow: Option<Vec<&str>> = None;

    for param in query.split('&').filter(|s| !s.is_empty()) {
        if let Some(ref mut vec) = overflow {
            vec.push(param);
        } else if let Some(slot) = stack_buf.get_mut(count) {
            *slot = param;
            count += 1;
        } else {
            let mut vec = Vec::with_capacity(STACK_QUERY_CAP * 2);
            vec.extend_from_slice(&stack_buf);
            vec.push(param);
            overflow = Some(vec);
        }
    }

    let slice = match overflow {
        Some(ref mut vec) => {
            vec.sort_unstable();
            vec.as_slice()
        }
        None => {
            let used = stack_buf.get_mut(..count).unwrap_or(&mut []);
            used.sort_unstable();
            used
        }
    };

    for (idx, param) in slice.iter().enumerate() {
        if idx > 0 {
            buf.push_bytes(b"&");
        }
        buf.push_bytes(param.as_bytes());
    }
}

#[inline]
fn feed_sorted_headers(buf: &mut StackKeyBuf, headers: &[(&str, &str)]) {
    let mut stack_buf = [("", ""); STACK_HEADER_CAP];
    let overflow: Option<Vec<(&str, &str)>>;

    let slice = if headers.len() <= STACK_HEADER_CAP {
        if let Some(dst) = stack_buf.get_mut(..headers.len()) {
            dst.copy_from_slice(headers);
            dst.sort_unstable_by(|a, b| a.0.cmp(b.0));
            &*dst
        } else {
            &[]
        }
    } else {
        let mut vec = headers.to_vec();
        vec.sort_unstable_by(|a, b| a.0.cmp(b.0));
        overflow = Some(vec);
        overflow.as_deref().unwrap_or(&[])
    };

    for &(h_name, h_val) in slice {
        buf.push_bytes(b"|");
        buf.push_bytes(h_name.as_bytes());
        buf.push_bytes(b"=");
        buf.push_bytes(h_val.as_bytes());
    }
}

/// Computes the canonical 64-bit FxHash directly from request parts with zero heap allocation
/// for canonical keys up to 512 bytes (32 query params, 16 segmentation headers).
///
/// Produces the exact same hash value as [`compute_cache_key_with_headers`].
#[inline]
pub fn hash_request_key_zero_alloc(
    method: &Method,
    host: Option<&str>,
    path: &str,
    query: Option<&str>,
    ignore_query_order: bool,
    extra_headers: Option<&[(&str, &str)]>,
) -> u64 {
    let mut buf = StackKeyBuf::new();
    buf.push_bytes(method.as_str().as_bytes());
    buf.push_bytes(b":");
    if let Some(h) = host {
        buf.push_bytes(h.as_bytes());
    }
    buf.push_bytes(b":");
    buf.push_bytes(path.as_bytes());

    if let Some(q) = query
        && !q.is_empty()
    {
        buf.push_bytes(b"?");
        if ignore_query_order {
            feed_sorted_query(&mut buf, q);
        } else {
            buf.push_bytes(q.as_bytes());
        }
    }

    if let Some(headers) = extra_headers
        && !headers.is_empty()
    {
        feed_sorted_headers(&mut buf, headers);
    }

    buf.finish_hash()
}

/// Computes the canonical cache key and its 64-bit FxHash.
pub fn compute_cache_key(
    method: &Method,
    host: Option<&str>,
    path: &str,
    query: Option<&str>,
    ignore_query_order: bool,
) -> (String, u64) {
    compute_cache_key_with_headers(method, host, path, query, ignore_query_order, None)
}

/// Computes the canonical cache key string and its 64-bit FxHash.
pub fn compute_cache_key_with_headers(
    method: &Method,
    host: Option<&str>,
    path: &str,
    query: Option<&str>,
    ignore_query_order: bool,
    extra_headers: Option<&[(&str, &str)]>,
) -> (String, u64) {
    let mut key = String::with_capacity(128);
    key.push_str(method.as_str());
    key.push(':');
    if let Some(h) = host {
        key.push_str(h);
    }
    key.push(':');
    key.push_str(path);

    if let Some(q) = query
        && !q.is_empty()
    {
        key.push('?');
        if ignore_query_order {
            key.push_str(&normalize_query(q));
        } else {
            key.push_str(q);
        }
    }

    if let Some(headers) = extra_headers
        && !headers.is_empty()
    {
        let mut sorted_headers: Vec<(&str, &str)> = headers.to_vec();
        sorted_headers.sort_unstable_by(|a, b| a.0.cmp(b.0));
        for (h_name, h_val) in sorted_headers {
            key.push('|');
            key.push_str(h_name);
            key.push('=');
            key.push_str(h_val);
        }
    }

    let hash = hash_key(&key);
    (key, hash)
}

/// Strips dynamic, session-specific, and hop-by-hop headers from an upstream response.
pub fn filter_cacheable_headers(headers: &HeaderWorkspace) -> HeaderWorkspace {
    use http::header::{
        CONNECTION, PROXY_AUTHENTICATE, PROXY_AUTHORIZATION, SET_COOKIE, TE, TRAILER,
        TRANSFER_ENCODING, UPGRADE,
    };

    let mut filtered = headers.clone();
    filtered.remove(SET_COOKIE);
    filtered.remove(CONNECTION);
    filtered.remove(TRANSFER_ENCODING);
    filtered.remove(UPGRADE);
    filtered.remove(TRAILER);
    filtered.remove(TE);
    filtered.remove(PROXY_AUTHENTICATE);
    filtered.remove(PROXY_AUTHORIZATION);
    filtered.remove(&X_REQUEST_ID);
    filtered.remove(&X_CACHE);
    filtered.remove(&X_VELDA_CACHE_KEY);
    filtered.remove(&X_VELDA_CACHE_HASH);

    filtered
}
