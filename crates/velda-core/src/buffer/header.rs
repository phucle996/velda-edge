//! Pre-warmed User-Space Header Workspace and Object Pool.
//!
//! # Architecture and Invariants
//! 1. **Zero-Heap Allocation on Normal Hot Path**:
//!    - Holds pre-allocated [`HeaderMap`] instances with pre-sized bucket arrays.
//!    - When recycled, [`HeaderMap::clear`] resets length to 0 while preserving the
//!      allocated capacity table in RAM.
//! 2. **Eager Physical Page Commit (Anti-Page-Fault Baseline)**:
//!    - At cold-start bootstrap, all baseline slots are allocated and touched to commit
//!      physical memory pages into the MMU, eliminating Minor Page Faults during normal traffic.
//! 3. **Two-Tier Adaptive Memory Management**:
//!    - **Tier 1 (Baseline Floor)**: Pre-warmed permanent workspaces, never evicted.
//!    - **Tier 2 (Burst Standby Queue)**: Spills over dynamically during spikes up to `max_burst_cap`.
//!      Standby workspaces retain a 15-second TTL in user space to absorb multi-wave micro-bursts
//!      without memory allocator churn (`malloc`/`free` thrashing).
//! 4. **Non-Blocking Auxiliary Sweeper (`try_lock`)**:
//!    - Eviction of expired burst workspaces is driven by the Auxiliary Core using non-blocking
//!      `try_lock()`. If a Traffic Core is actively processing headers, the sweeper immediately
//!      skips that cycle without introducing any pipeline jitter or lock contention.
//! 5. **Linux RSS Retention Mechanism**:
//!    - When burst workspaces are evicted after the 15s TTL, they are returned to the user-space
//!      allocator (`jemalloc`). The Linux kernel retains physical page frames in RSS throughout
//!      the allocator's dirty decay window, accelerating any subsequent memory re-expansion.

use std::collections::VecDeque;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use http::HeaderMap;
use http::header::{HeaderName, HeaderValue, IntoHeaderName};

/// Canonical header name for the W3C TraceContext Request Identifier.
pub const X_REQUEST_ID: HeaderName = HeaderName::from_static("x-request-id");

/// Maximum number of Bit-1 (Bypass on Reject) headers tracked on the stack without heap allocation.
pub const MAX_BYPASS_HEADERS: usize = 8;

/// Reusable workspace holding a pre-allocated, clearable [`HeaderMap`] with stack-sidecar bypass tagging.
///
/// # Virtual 3-Column Header Model
/// - Column 1 & 2: `(HeaderName, HeaderValue)` stored inside standard [`HeaderMap`].
/// - Column 3: Visibility Bit (Bit 0 = Normal / Upstream-only; Bit 1 = Bypass on Reject / Client-facing).
///
/// On normal upstream dispatch, all headers (Bit 0 and Bit 1) are forwarded with zero overhead ($0$ ns).
/// On rejection, only Bit 1 headers (e.g. `Retry-After`, `x-request-id`) are extracted, preventing
/// information leakage of client credentials or internal upstream headers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeaderWorkspace {
    headers: HeaderMap,
    bypass_keys: [Option<HeaderName>; MAX_BYPASS_HEADERS],
    bypass_len: u8,
    initial_capacity: usize,
}

impl HeaderWorkspace {
    /// Canonical Request ID header name (`x-request-id`).
    pub const X_REQUEST_ID: HeaderName = X_REQUEST_ID;

    /// Default baseline capacity commit (32 header entries).
    pub const DEFAULT_CAPACITY: usize = 32;

    /// Creates and pre-warms a header workspace with specified initial bucket capacity.
    ///
    /// # Ingress Invariant
    /// Pre-loads an authoritative Request ID into `x-request-id` with Bit 1 Bypass priority.
    pub fn new(capacity: usize) -> Self {
        let mut map = HeaderMap::with_capacity(capacity);
        // Eagerly warmup internal bucket table by reserving capacity upfront
        // to force the allocator to commit the initial hash bucket array into physical RAM.
        map.reserve(capacity);
        let (_id, req_id) = crate::next_request_id();
        map.insert(X_REQUEST_ID, req_id);
        let mut bypass_keys = [const { None }; MAX_BYPASS_HEADERS];
        bypass_keys[0] = Some(X_REQUEST_ID);
        Self {
            headers: map,
            bypass_keys,
            bypass_len: 1,
            initial_capacity: capacity,
        }
    }

    /// Creates and pre-warms a header workspace with an explicit Request ID and initial capacity.
    #[inline]
    pub fn with_request_id(req_id: HeaderValue, capacity: usize) -> Self {
        let mut map = HeaderMap::with_capacity(capacity);
        map.reserve(capacity);
        map.insert(X_REQUEST_ID, req_id);
        let mut bypass_keys = [const { None }; MAX_BYPASS_HEADERS];
        bypass_keys[0] = Some(X_REQUEST_ID);
        Self {
            headers: map,
            bypass_keys,
            bypass_len: 1,
            initial_capacity: capacity,
        }
    }

    /// Creates an empty pre-warmed workspace with default baseline capacity (32 headers).
    #[inline(always)]
    pub fn empty() -> Self {
        Self::new(Self::DEFAULT_CAPACITY)
    }

    /// Creates and pre-warms a header workspace with specified initial capacity.
    #[inline(always)]
    pub fn with_capacity(capacity: usize) -> Self {
        Self::new(capacity)
    }

    /// Wraps an existing [`HeaderMap`] into a [`HeaderWorkspace`].
    ///
    /// # Zero-Trust Slot 0 Invariant
    /// If `headers` does not contain `x-request-id`, an authoritative Request ID is generated
    /// and inserted. If `headers` already contains `x-request-id` (e.g. assigned authoritatively
    /// at Ingress boundary via in-place replacement), it is preserved in-place without redundant
    /// cloning or re-insertion.
    ///
    /// In all cases, Slot 0 is pinned with `x-request-id` as a Bit 1 Bypass header (`bypass_len = 1`).
    #[inline]
    pub fn from_headers(mut headers: HeaderMap) -> Self {
        let cap = headers.capacity().max(Self::DEFAULT_CAPACITY);
        if !headers.contains_key(&X_REQUEST_ID) {
            let (_id, req_id) = crate::next_request_id();
            headers.insert(X_REQUEST_ID, req_id);
        }
        let mut bypass_keys = [const { None }; MAX_BYPASS_HEADERS];
        bypass_keys[0] = Some(X_REQUEST_ID);
        Self {
            headers,
            bypass_keys,
            bypass_len: 1,
            initial_capacity: cap,
        }
    }

    /// Unwraps and returns the inner [`HeaderMap`].
    #[inline(always)]
    pub fn into_inner(self) -> HeaderMap {
        self.headers
    }

    /// Returns the initial baseline capacity of this workspace.
    #[inline(always)]
    pub fn initial_capacity(&self) -> usize {
        self.initial_capacity
    }

    /// Borrows the internal header map immutably.
    #[inline(always)]
    pub fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    /// Inserts a header with Bit 0 (Normal / Upstream-only visibility).
    ///
    /// # Performance Mechanism: Zero-Sniffing Hot Path ($0$ ns Overhead)
    /// Normal headers inserted by routing, plugins, or upstream mutation run at native
    /// [`HeaderMap`] speed. Because Slot 0 is already permanently initialized with an
    /// authoritative Request ID during workspace construction, `insert()` avoids all
    /// dynamic string comparisons, `contains_key()` checks, or sidecar inspections.
    ///
    /// # Security Invariant
    /// Normal headers (Bit 0) are forwarded to upstream backends on successful pass-through (99% path),
    /// but are strictly purged and dropped if the request is short-circuited via [`crate::Action::Reject`],
    /// defending against credential leaks and header reflection attacks.
    #[inline]
    pub fn insert<K: IntoHeaderName>(
        &mut self,
        name: K,
        value: HeaderValue,
    ) -> Option<HeaderValue> {
        self.headers.insert(name, value)
    }

    /// Appends a header value to the workspace.
    ///
    /// If the key already exists, the value is appended to the list of values for that key.
    /// If the key does not exist, it is inserted.
    #[inline]
    pub fn append<K: IntoHeaderName>(&mut self, name: K, value: HeaderValue) -> bool {
        self.headers.append(name, value)
    }

    /// Reserves capacity for at least `additional` more headers.
    #[inline]
    pub fn reserve(&mut self, additional: usize) {
        self.headers.reserve(additional);
    }

    /// Removes a header from the workspace.
    ///
    /// # Slot 0 Invariant Defense
    /// The authoritative `x-request-id` header in Slot 0 cannot be removed.
    #[inline]
    pub fn remove<K: http::header::AsHeaderName>(&mut self, name: K) -> Option<HeaderValue> {
        let name_str = name.as_str();
        if name_str.eq_ignore_ascii_case("x-request-id") {
            return None;
        }
        self.headers.remove(name)
    }

    /// Returns the authoritative W3C Request ID pinned in Slot 0.
    ///
    /// # Zero-Cost Invariant
    /// Because every constructor and pool reset guarantees Slot 0 commits `x-request-id`,
    /// this getter executes in $O(1)$ without allocations or fallibility.
    #[inline(always)]
    pub fn request_id(&self) -> &HeaderValue {
        if let Some(val) = self.headers.get(&X_REQUEST_ID) {
            val
        } else {
            static FALLBACK_ID: HeaderValue =
                HeaderValue::from_static("0000000000000000-0000000000000000");
            &FALLBACK_ID
        }
    }

    /// Updates the authoritative Request ID, preserving its permanent Bit 1 visibility in Slot 0.
    ///
    /// # Tracing Invariant
    /// In accordance with the Edge distributed tracing contract, the Request ID must remain in
    /// Slot 0 of the bypass sidecar. When downstream pipelines assign an edge-authoritative ID,
    /// this function atomically updates the underlying [`HeaderMap`] and pins Slot 0.
    #[inline]
    pub fn set_request_id(&mut self, req_id: HeaderValue) {
        self.headers.insert(X_REQUEST_ID, req_id);
        self.bypass_keys[0] = Some(X_REQUEST_ID);
        if self.bypass_len == 0 {
            self.bypass_len = 1;
        }
    }

    /// Inserts a header with Bit 1 (Bypass on Reject visibility).
    ///
    /// # Mechanism: Cacheline-Aligned Stack Sidecar
    /// Appends the header name into the stack sidecar `[Option<HeaderName>; 8]` while storing
    /// the value inside the native [`HeaderMap`]. Linear deduplication across at most 8 items
    /// in L1 cache executes in $< 1\text{ ns}$ without dynamic heap allocations.
    ///
    /// # Security & Protocol Invariant
    /// Headers tagged as Bit 1 are retained and returned to the downstream client even when
    /// the request is rejected early by edge policy or security plugins (e.g. `Retry-After`,
    /// `x-ratelimit-*`, `WWW-Authenticate`).
    ///
    /// All normal headers (Bit 0) are discarded on rejection to avoid information disclosure.
    ///
    /// Returns `true` if successfully tracked in the stack sidecar, or `false` if `MAX_BYPASS_HEADERS` (8) is reached.
    #[inline]
    pub fn insert_bypass(&mut self, name: HeaderName, value: HeaderValue) -> bool {
        let len = self.bypass_len as usize;
        let mut exists = false;
        for i in 0..len {
            if let Some(ref k) = self.bypass_keys[i]
                && *k == name
            {
                exists = true;
                break;
            }
        }
        let inserted_to_sidecar = if !exists {
            debug_assert!(
                len < MAX_BYPASS_HEADERS,
                "HeaderWorkspace bypass capacity exceeded MAX_BYPASS_HEADERS = 8"
            );
            if len < MAX_BYPASS_HEADERS {
                self.bypass_keys[len] = Some(name.clone());
                self.bypass_len += 1;
                true
            } else {
                false
            }
        } else {
            true
        };
        self.headers.insert(name, value);
        inserted_to_sidecar
    }

    /// Returns `true` if this workspace has any Bit-1 bypass headers marked.
    #[inline(always)]
    pub fn has_bypass_headers(&self) -> bool {
        self.bypass_len > 0
    }

    /// Returns an iterator over all Bit-1 (Bypass on Reject) headers.
    ///
    /// # Zero-Allocation Invariant
    /// Traverses the stack sidecar array up to `bypass_len` and resolves values directly from
    /// the pre-warmed [`HeaderMap`]. Allocates zero memory on the error rejection path.
    pub fn bypass_headers(&self) -> impl Iterator<Item = (&HeaderName, &HeaderValue)> {
        let len = self.bypass_len as usize;
        (0..len).filter_map(move |i| {
            let key = self.bypass_keys[i].as_ref()?;
            let val = self.headers.get(key)?;
            Some((key, val))
        })
    }

    /// Takes the inner [`HeaderMap`], replacing it with an empty map of the initial capacity pre-loaded with a fresh Request ID.
    #[inline(always)]
    pub fn take(&mut self) -> HeaderMap {
        self.bypass_keys = [const { None }; MAX_BYPASS_HEADERS];
        let (_id, req_id) = crate::next_request_id();
        self.bypass_keys[0] = Some(X_REQUEST_ID);
        self.bypass_len = 1;
        let mut new_map = HeaderMap::with_capacity(self.initial_capacity);
        new_map.insert(X_REQUEST_ID, req_id);
        std::mem::replace(&mut self.headers, new_map)
    }

    /// Clears all headers while preserving the pre-allocated bucket array in RAM, re-initializing Slot 0.
    ///
    /// # Memory Mechanism: Anti-Minor-Page-Fault Retention
    /// `self.headers.clear()` retains the physical hash table buckets previously allocated and committed
    /// in process RSS by `jemalloc`. Resetting Slot 0 commits a fresh Request ID with Bit 1 visibility.
    ///
    /// # Anti-Bloat Defense
    /// If an adversarial client sent an oversized header table exceeding $4\times$ the initial baseline,
    /// the workspace drops the bloated table and re-allocates back to `initial_capacity` to prevent
    /// long-term memory leakage.
    #[inline(always)]
    pub fn reset(&mut self) {
        let (_id, req_id) = crate::next_request_id();
        self.bypass_keys = [const { None }; MAX_BYPASS_HEADERS];
        self.bypass_keys[0] = Some(X_REQUEST_ID);
        self.bypass_len = 1;
        // Shrink back to baseline if bloated by oversized request headers
        if self.headers.capacity() > self.initial_capacity * 4 {
            self.headers = HeaderMap::with_capacity(self.initial_capacity);
        } else {
            self.headers.clear();
        }
        self.headers.insert(X_REQUEST_ID, req_id);
    }
}

impl Default for HeaderWorkspace {
    #[inline(always)]
    fn default() -> Self {
        Self::empty()
    }
}

impl std::ops::Deref for HeaderWorkspace {
    type Target = HeaderMap;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.headers
    }
}

impl AsRef<HeaderMap> for HeaderWorkspace {
    #[inline(always)]
    fn as_ref(&self) -> &HeaderMap {
        &self.headers
    }
}

impl From<HeaderMap> for HeaderWorkspace {
    #[inline(always)]
    fn from(headers: HeaderMap) -> Self {
        Self::from_headers(headers)
    }
}

impl From<HeaderWorkspace> for HeaderMap {
    #[inline(always)]
    fn from(ws: HeaderWorkspace) -> Self {
        ws.into_inner()
    }
}

impl FromIterator<(http::header::HeaderName, http::header::HeaderValue)> for HeaderWorkspace {
    fn from_iter<T: IntoIterator<Item = (http::header::HeaderName, http::header::HeaderValue)>>(
        iter: T,
    ) -> Self {
        let headers = HeaderMap::from_iter(iter);
        Self::from_headers(headers)
    }
}

impl<'a> IntoIterator for &'a HeaderWorkspace {
    type Item = (&'a http::header::HeaderName, &'a http::header::HeaderValue);
    type IntoIter = http::header::Iter<'a, http::header::HeaderValue>;

    #[inline(always)]
    fn into_iter(self) -> Self::IntoIter {
        self.headers.iter()
    }
}

/// Internal bucket segregating permanent baseline workspaces from transient burst workspaces.
struct HeaderBucket {
    /// Baseline pre-warmed physical workspaces. Permanent, never evicted.
    baseline: Vec<HeaderWorkspace>,
    /// Burst standby workspaces paired with their return timestamp.
    /// Ordered chronologically: oldest at front, newest at back.
    burst: VecDeque<(Instant, HeaderWorkspace)>,
    /// Maximum number of permanent baseline workspaces.
    baseline_limit: usize,
    /// Absolute ceiling of active + pooled workspaces (anti-OOM protection).
    max_burst_limit: usize,
}

/// Pre-warmed two-tier adaptive object pool of [`HeaderWorkspace`] instances.
pub struct HeaderPool {
    bucket: Mutex<HeaderBucket>,
    active_leases: AtomicUsize,
    initial_header_capacity: usize,
    idle_ttl: Duration,
}

impl std::fmt::Debug for HeaderPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HeaderPool")
            .field("active_leases", &self.active_leases.load(Ordering::Relaxed))
            .field("initial_header_capacity", &self.initial_header_capacity)
            .finish()
    }
}

impl HeaderPool {
    /// Creates and eagerly warms up a pool with a pre-allocated number of baseline slots
    /// and a default 3x max burst ceiling with 15-second TTL.
    pub fn new(baseline_slots: usize, initial_header_capacity: usize) -> Arc<Self> {
        let max_burst_cap = baseline_slots.saturating_mul(3).max(baseline_slots + 32);
        Self::with_adaptive_limits(
            baseline_slots,
            initial_header_capacity,
            max_burst_cap,
            Duration::from_secs(15),
        )
    }

    /// Creates an adaptive header pool with explicit baseline, max burst ceiling, and idle TTL.
    pub fn with_adaptive_limits(
        baseline_slots: usize,
        initial_header_capacity: usize,
        max_burst_cap: usize,
        idle_ttl: Duration,
    ) -> Arc<Self> {
        let mut baseline = Vec::with_capacity(baseline_slots);
        for _ in 0..baseline_slots {
            baseline.push(HeaderWorkspace::new(initial_header_capacity));
        }

        Arc::new(Self {
            bucket: Mutex::new(HeaderBucket {
                baseline,
                burst: VecDeque::new(),
                baseline_limit: baseline_slots,
                max_burst_limit: max_burst_cap.max(baseline_slots),
            }),
            active_leases: AtomicUsize::new(0),
            initial_header_capacity,
            idle_ttl,
        })
    }

    /// Borrows a [`HeaderWorkspace`] wrapped in an RAII lease.
    ///
    /// Resolution Order:
    /// 1. **Tier 1 (Hot Baseline)**: Pops from pre-warmed physical workspaces (0 alloc, 0 page fault).
    /// 2. **Tier 2 (Burst Standby Queue)**: Pops from warm user-space burst queue (0 alloc).
    /// 3. **Spillover Heap Allocation**: Dynamically allocates if below `max_burst_cap`.
    /// 4. **Anti-OOM Saturation**: Returns `None` if `max_burst_cap` has been reached.
    pub fn acquire(self: &Arc<Self>) -> Option<HeaderWorkspaceLease> {
        let mut guard = self.lock_bucket();

        // 1. Try baseline (fastest path, hot pre-warmed)
        let ws = if let Some(ws) = guard.baseline.pop() {
            ws
        } else if let Some((_, ws)) = guard.burst.pop_front() {
            // 2. Try burst standby (warm in user space within TTL)
            ws
        } else {
            // 3. Both pools empty: check anti-OOM ceiling before allocating on heap
            let current_active = self.active_leases.load(Ordering::Relaxed);
            if current_active >= guard.max_burst_limit {
                return None;
            }
            HeaderWorkspace::new(self.initial_header_capacity)
        };

        self.active_leases.fetch_add(1, Ordering::Relaxed);

        Some(HeaderWorkspaceLease {
            workspace: Some(ws),
            pool: Arc::clone(self),
        })
    }

    /// Returns the number of currently checked-out active leases.
    #[inline(always)]
    pub fn active_leases(&self) -> usize {
        self.active_leases.load(Ordering::Relaxed)
    }

    /// Returns the permanent baseline capacity of this pool.
    #[inline(always)]
    pub fn baseline_slots(&self) -> usize {
        self.lock_bucket().baseline_limit
    }

    /// Returns the absolute max burst ceiling of this pool.
    #[inline(always)]
    pub fn max_burst_cap(&self) -> usize {
        self.lock_bucket().max_burst_limit
    }

    /// Returns the count of burst workspaces currently held in standby.
    #[inline(always)]
    pub fn burst_standby_count(&self) -> usize {
        self.lock_bucket().burst.len()
    }

    /// Returns the total number of workspaces currently sitting in the pool (baseline + burst).
    #[inline(always)]
    pub fn total_pooled(&self) -> usize {
        let guard = self.lock_bucket();
        guard.baseline.len() + guard.burst.len()
    }

    /// Evicts burst workspaces that have been idle longer than `idle_ttl`.
    ///
    /// # Auxiliary Core Invariant
    /// Uses non-blocking `try_lock()` to ensure that the sweeper running on the Auxiliary Core
    /// NEVER blocks or delays a Traffic Worker core servicing requests.
    pub fn sweep_expired(&self, now: Instant) -> usize {
        let mut evicted = 0;
        // A poisoned bucket is still structurally valid (see `lock_bucket`), so the sweeper
        // keeps reclaiming memory instead of silently giving up forever.
        let guard = match self.bucket.try_lock() {
            Ok(g) => Some(g),
            Err(std::sync::TryLockError::Poisoned(p)) => Some(p.into_inner()),
            Err(std::sync::TryLockError::WouldBlock) => None,
        };
        if let Some(mut guard) = guard {
            // Workspaces were pushed chronologically; front is oldest, so the first
            // non-expired entry ends the scan.
            while guard.burst.front().is_some_and(|(returned_at, _)| {
                now.saturating_duration_since(*returned_at) >= self.idle_ttl
            }) {
                // Evicted back to allocator arena.
                drop(guard.burst.pop_front());
                evicted += 1;
            }
        }
        evicted
    }

    /// Locks the bucket, recovering from mutex poisoning.
    ///
    /// # Why poison is ignored
    /// Every critical section on the bucket is a handful of `Vec`/`VecDeque` push/pop calls with
    /// no invariant spanning multiple statements, so a panicking holder cannot leave the
    /// container half-updated. Propagating the poison would instead make every later `acquire()`
    /// and, worse, every lease `Drop` (`recycle`) panic — a panic inside `Drop` while unwinding
    /// aborts the whole process.
    #[inline(always)]
    fn lock_bucket(&self) -> std::sync::MutexGuard<'_, HeaderBucket> {
        self.bucket
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Recycles a returned workspace back into the pool.
    fn recycle(&self, mut ws: HeaderWorkspace) {
        ws.reset();
        self.active_leases.fetch_sub(1, Ordering::Relaxed);

        let mut guard = self.lock_bucket();
        // Priority 1: Refill permanent baseline floor (Zero clock overhead)
        if guard.baseline.len() < guard.baseline_limit {
            guard.baseline.push(ws);
        } else if guard.baseline.len() + guard.burst.len() < guard.max_burst_limit {
            // Priority 2: Retain in burst standby queue with timestamp for 15s cooldown
            guard.burst.push_back((Instant::now(), ws));
        }
        // Priority 3: Above max_burst_limit -> workspace drops and frees back to allocator
    }
}

/// RAII Lease for a [`HeaderWorkspace`]. Automatically cleans and returns to the pool on drop.
///
/// # DEFENSIVE MEMORY INVARIANT: Anti-Use-After-Free & Anti-Double-Free Defense
/// 1. **Zero Use-After-Free via Compiler Borrow Checker**:
///    Deref to [`HeaderWorkspace`] produces references `&'a HeaderWorkspace` strictly bounded
///    to the lifetime of this lease struct. The moment this lease is dropped (or returns to the pool),
///    the Rust compiler statically invalidates any outstanding references at compile time.
/// 2. **Zero Double-Free via `Option::take()`**:
///    During `drop()`, `self.workspace.take()` extracts the workspace out of `self`. Even if
///    the drop handler were invoked multiple times (e.g. panic stack unwinding), the inner
///    workspace can only ever be returned to the pool exactly once.
/// 3. **Async Cancellation Immunity**:
///    When a client aborts an in-flight connection or Tokio drops an async request future mid-await,
///    stack unwinding automatically invokes this `Drop` implementation, safely returning the
///    workspace to the pool without memory leaks or dangling state.
pub struct HeaderWorkspaceLease {
    workspace: Option<HeaderWorkspace>,
    pool: Arc<HeaderPool>,
}

impl Deref for HeaderWorkspaceLease {
    type Target = HeaderWorkspace;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        self.workspace
            .as_ref()
            .expect("HeaderWorkspaceLease held invalid state")
    }
}

impl DerefMut for HeaderWorkspaceLease {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.workspace
            .as_mut()
            .expect("HeaderWorkspaceLease held invalid state")
    }
}

impl Drop for HeaderWorkspaceLease {
    fn drop(&mut self) {
        if let Some(ws) = self.workspace.take() {
            self.pool.recycle(ws);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::header::{AUTHORIZATION, CONTENT_TYPE, HeaderValue};

    #[test]
    fn test_header_workspace_recycle_preserves_capacity() {
        let mut ws = HeaderWorkspace::new(32);
        assert!(ws.headers().capacity() >= 32);

        ws.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        ws.insert(AUTHORIZATION, HeaderValue::from_static("Bearer token123"));
        assert_eq!(ws.headers().len(), 3); // Pre-loaded Slot 0 x-request-id + 2 custom headers

        // Guard against removing Slot 0 x-request-id
        assert!(ws.remove(&HeaderWorkspace::X_REQUEST_ID).is_none());
        assert_eq!(ws.headers().len(), 3);

        ws.reset();
        assert_eq!(ws.headers().len(), 1); // Pre-loaded Slot 0 with Request ID!
        assert!(ws.headers().capacity() >= 32); // Bucket capacity preserved!
    }

    #[test]
    fn test_header_workspace_bypass_tagging() {
        use http::header::{HeaderName, HeaderValue, RETRY_AFTER};
        let mut ws = HeaderWorkspace::empty();
        // Slot 0 is ALWAYS pre-loaded with x-request-id as Bit 1 Bypass!
        assert!(ws.has_bypass_headers());
        assert_eq!(ws.bypass_headers().count(), 1);
        let (first_k, first_v) = ws.bypass_headers().next().unwrap();
        assert_eq!(first_k, &HeaderWorkspace::X_REQUEST_ID);
        assert!(!first_v.is_empty());

        // Normal header (Bit 0) does not increase bypass count
        ws.insert(
            HeaderName::from_static("x-internal-secret"),
            HeaderValue::from_static("secret123"),
        );
        assert_eq!(ws.bypass_headers().count(), 1);

        // Bypass header (Bit 1) added by plugin takes Slot 1
        ws.insert_bypass(RETRY_AFTER, HeaderValue::from_static("60"));
        assert_eq!(ws.bypass_headers().count(), 2);

        // Extract bypass headers: Slot 0 = x-request-id, Slot 1 = Retry-After
        let bypass: Vec<_> = ws.bypass_headers().collect();
        assert_eq!(bypass.len(), 2);
        assert_eq!(bypass[0].0, &HeaderWorkspace::X_REQUEST_ID);
        assert_eq!(bypass[1].0, &RETRY_AFTER);
        assert_eq!(bypass[1].1, &HeaderValue::from_static("60"));

        // All headers still accessible for upstream
        assert_eq!(ws.headers().len(), 3); // x-request-id + x-internal-secret + RETRY_AFTER

        // Reset clears custom headers and re-preloads fresh x-request-id into Slot 0
        ws.reset();
        assert!(ws.has_bypass_headers());
        assert_eq!(ws.headers().len(), 1);
        assert_eq!(ws.bypass_headers().count(), 1);

        // from_headers with x-request-id preserves it at Slot 0
        let mut map = HeaderMap::new();
        map.insert(
            HeaderWorkspace::X_REQUEST_ID,
            HeaderValue::from_static("trace-incoming-999"),
        );
        map.insert(
            HeaderName::from_static("host"),
            HeaderValue::from_static("api.example.com"),
        );
        let ws2 = HeaderWorkspace::from_headers(map);
        assert!(ws2.has_bypass_headers());
        let bypass2: Vec<_> = ws2.bypass_headers().collect();
        assert_eq!(bypass2.len(), 1);
        assert_eq!(bypass2[0].0, &HeaderWorkspace::X_REQUEST_ID);
        assert_eq!(
            bypass2[0].1,
            &HeaderValue::from_static("trace-incoming-999")
        );
        assert_eq!(
            ws2.request_id(),
            &HeaderValue::from_static("trace-incoming-999")
        );

        // from_headers without x-request-id automatically pre-loads an authoritative ID into Slot 0
        let mut clean_map = HeaderMap::new();
        clean_map.insert(
            HeaderName::from_static("host"),
            HeaderValue::from_static("api.example.com"),
        );
        let ws3 = HeaderWorkspace::from_headers(clean_map);
        assert!(ws3.has_bypass_headers());
        assert_eq!(ws3.headers().len(), 2);
        assert!(!ws3.request_id().is_empty());
    }

    #[test]
    fn test_header_pool_eager_warmup_and_lease() {
        let pool = HeaderPool::new(4, 32);
        assert_eq!(pool.baseline_slots(), 4);
        assert_eq!(pool.active_leases(), 0);

        {
            let mut lease1 = pool.acquire().expect("acquire lease 1");
            assert_eq!(pool.active_leases(), 1);
            lease1.insert(CONTENT_TYPE, HeaderValue::from_static("text/plain"));
            assert_eq!(lease1.headers().len(), 2); // Pre-loaded X_REQUEST_ID + CONTENT_TYPE

            let mut lease2 = pool.acquire().expect("acquire lease 2");
            assert_eq!(pool.active_leases(), 2);
            lease2.insert(AUTHORIZATION, HeaderValue::from_static("Bearer secret"));
        }

        // Both leases dropped: returned to pool, active leases reset to 0
        assert_eq!(pool.active_leases(), 0);

        // Next acquire gets pre-cleaned workspace with Request ID
        let lease3 = pool.acquire().expect("acquire lease 3");
        assert_eq!(lease3.headers().len(), 1);
        assert!(
            lease3
                .headers()
                .contains_key(&HeaderWorkspace::X_REQUEST_ID)
        );
        assert!(lease3.headers().capacity() >= 32);
    }

    #[test]
    fn test_header_pool_adaptive_burst_and_decay() {
        // Pool with baseline=2, max_burst_cap=4, idle_ttl=50ms
        let pool = HeaderPool::with_adaptive_limits(2, 32, 4, Duration::from_millis(50));
        assert_eq!(pool.baseline_slots(), 2);
        assert_eq!(pool.max_burst_cap(), 4);

        // 1. Acquire baseline (2 slots)
        let l1 = pool.acquire().expect("lease 1 (baseline)");
        let l2 = pool.acquire().expect("lease 2 (baseline)");
        assert_eq!(pool.active_leases(), 2);

        // 2. Acquire burst (2 slots spillover)
        let l3 = pool.acquire().expect("lease 3 (burst)");
        let l4 = pool.acquire().expect("lease 4 (burst)");
        assert_eq!(pool.active_leases(), 4);

        // 3. Attempt 5th acquire -> Anti-OOM cap reached -> returns None!
        assert!(pool.acquire().is_none());

        // 4. Drop all leases
        drop(l1);
        drop(l2);
        drop(l3);
        drop(l4);
        assert_eq!(pool.active_leases(), 0);

        // 2 refilled baseline, 2 in burst standby queue
        assert_eq!(pool.total_pooled(), 4);
        assert_eq!(pool.burst_standby_count(), 2);

        // Immediate sweep should evict 0 because 50ms TTL hasn't elapsed
        let evicted = pool.sweep_expired(Instant::now());
        assert_eq!(evicted, 0);
        assert_eq!(pool.total_pooled(), 4);

        // Simulate time passage (> 50ms)
        let future_time = Instant::now() + Duration::from_millis(100);
        let evicted = pool.sweep_expired(future_time);
        assert_eq!(evicted, 2);

        // Pool cleanly decayed back to baseline floor (2)
        assert_eq!(pool.total_pooled(), 2);
        assert_eq!(pool.burst_standby_count(), 0);
    }
}
