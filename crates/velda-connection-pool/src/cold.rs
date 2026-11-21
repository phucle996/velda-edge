//! Shared lock-free Cold Pool backed by a 4KB page-aligned contiguous region (`FixedPageSlab`).
//!
//! ```text
//! ColdSlot<K, R> (align=64B) State Machine:
//!
//!   push() CAS         write(k,r)       pop() CAS (match)       read(r)
//!  [EMPTY (0)] ───► [WRITING (1)] ───► [READY (2)] ───► [READING (3)] ───► [EMPTY (0)]
//!                                           ▲                 │ (key mismatch)
//!                                           └─────────────────┘
//!
//! Epoll Invariant:
//! - Entering ColdPool: `on_cold_store()` detaches socket (`into_std() -> PlainCold`)
//! - Leaving  ColdPool: `on_hot_restore()` re-attaches socket (`from_std() -> Plain`)
//! ```

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use crate::resource::PoolableResource;
use crate::slab::FixedPageSlab;

const SLOT_EMPTY: u8 = 0;
const SLOT_WRITING: u8 = 1;
const SLOT_READY: u8 = 2;
const SLOT_READING: u8 = 3;

/// Cache-line aligned lock-free slot in the shared [`ColdPool`].
#[repr(align(64))]
struct ColdSlot<K, R> {
    state: AtomicU8,
    key: UnsafeCell<MaybeUninit<K>>,
    resource: UnsafeCell<MaybeUninit<R>>,
}

impl<K, R> Drop for ColdSlot<K, R> {
    fn drop(&mut self) {
        if *self.state.get_mut() == SLOT_READY {
            unsafe {
                self.key.get_mut().assume_init_drop();
                self.resource.get_mut().assume_init_drop();
            }
            *self.state.get_mut() = SLOT_EMPTY;
        }
    }
}

/// Shared lock-free connection bank backed by a single contiguous `k * 4096` byte allocation.
pub struct ColdPool<K, R> {
    slots: FixedPageSlab<ColdSlot<K, R>>,
    ready_count: AtomicUsize,
    write_cursor: AtomicUsize,
    read_cursor: AtomicUsize,
}

unsafe impl<K: Send, R: Send> Send for ColdPool<K, R> {}
unsafe impl<K: Send, R: Send> Sync for ColdPool<K, R> {}

impl<K: Eq, R: PoolableResource> ColdPool<K, R> {
    /// Creates a new shared `ColdPool` with at least `min_slots` capacity.
    pub fn with_capacity(min_slots: usize) -> Self {
        Self {
            slots: FixedPageSlab::with_capacity(min_slots),
            ready_count: AtomicUsize::new(0),
            write_cursor: AtomicUsize::new(0),
            read_cursor: AtomicUsize::new(0),
        }
    }

    /// Total slot capacity in the contiguous page-aligned slab.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.slots.capacity()
    }

    /// Approximate number of ready connections currently resident in the Cold Pool.
    #[inline]
    pub fn len(&self) -> usize {
        self.ready_count.load(Ordering::Relaxed)
    }

    /// Returns `true` if no connections are currently in the Cold Pool.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Number of contiguous 4KB pages backing the Cold Pool.
    #[inline]
    pub fn page_count(&self) -> usize {
        self.slots.page_count()
    }

    /// Lock-free push of an idle healthy resource into the shared Cold Pool.
    ///
    /// Returns `Err(resource)` if all slots are occupied.
    pub fn push(&self, key: K, resource: R) -> Result<(), R> {
        let slots = self.slots.as_slice();
        let cap = slots.len();
        if cap == 0 {
            return Err(resource);
        }

        let start = self.write_cursor.fetch_add(1, Ordering::Relaxed) % cap;
        for offset in 0..cap {
            let idx = (start + offset) % cap;
            let slot = &slots[idx];
            if slot.state.load(Ordering::Relaxed) == SLOT_EMPTY
                && slot
                    .state
                    .compare_exchange_weak(
                        SLOT_EMPTY,
                        SLOT_WRITING,
                        Ordering::Acquire,
                        Ordering::Relaxed,
                    )
                    .is_ok()
            {
                unsafe {
                    (*slot.key.get()).write(key);
                    (*slot.resource.get()).write(resource);
                }
                self.ready_count.fetch_add(1, Ordering::Release);
                slot.state.store(SLOT_READY, Ordering::Release);
                return Ok(());
            }
        }

        Err(resource)
    }

    /// Lock-free pull of an idle healthy resource matching `key`.
    pub fn pop(
        &self,
        key: &K,
        idle_timeout: Duration,
        max_lifetime: Option<Duration>,
        now: Instant,
    ) -> Option<R> {
        if self.ready_count.load(Ordering::Acquire) == 0 {
            return None;
        }

        let slots = self.slots.as_slice();
        let cap = slots.len();
        let start = self.read_cursor.fetch_add(1, Ordering::Relaxed) % cap;

        for offset in 0..cap {
            if self.ready_count.load(Ordering::Relaxed) == 0 {
                return None;
            }
            let idx = (start + offset) % cap;
            let slot = &slots[idx];
            if slot.state.load(Ordering::Acquire) != SLOT_READY {
                continue;
            }
            if slot
                .state
                .compare_exchange_weak(
                    SLOT_READY,
                    SLOT_READING,
                    Ordering::Acquire,
                    Ordering::Relaxed,
                )
                .is_err()
            {
                continue;
            }

            let matches = unsafe { (*slot.key.get()).assume_init_ref() == key };
            if !matches {
                slot.state.store(SLOT_READY, Ordering::Release);
                continue;
            }

            let (_k, mut res) = unsafe {
                let k = (*slot.key.get()).assume_init_read();
                let r = (*slot.resource.get()).assume_init_read();
                (k, r)
            };
            self.ready_count.fetch_sub(1, Ordering::Release);
            slot.state.store(SLOT_EMPTY, Ordering::Release);

            let is_idle_valid = now.saturating_duration_since(res.last_used_at()) <= idle_timeout;
            let is_lifetime_valid = match max_lifetime {
                Some(ttl) => now.saturating_duration_since(res.created_at()) <= ttl,
                None => true,
            };

            if is_idle_valid && is_lifetime_valid && res.is_healthy() {
                res.touch_at(now);
                return Some(res);
            }
            res.close();
        }

        None
    }

    /// Counts ready slots currently matching `key`.
    pub fn count_for_key(&self, key: &K) -> usize {
        if self.ready_count.load(Ordering::Acquire) == 0 {
            return 0;
        }
        let mut count = 0;
        for slot in self.slots.as_slice() {
            if slot.state.load(Ordering::Acquire) == SLOT_READY
                && slot
                    .state
                    .compare_exchange_weak(
                        SLOT_READY,
                        SLOT_READING,
                        Ordering::Acquire,
                        Ordering::Relaxed,
                    )
                    .is_ok()
            {
                if unsafe { (*slot.key.get()).assume_init_ref() == key } {
                    count += 1;
                }
                slot.state.store(SLOT_READY, Ordering::Release);
            }
        }
        count
    }

    /// Evicts expired or unhealthy resources from the Cold Pool.
    pub fn evict_expired(&self, idle_timeout: Duration, max_lifetime: Option<Duration>) -> usize {
        if self.ready_count.load(Ordering::Acquire) == 0 {
            return 0;
        }
        let now = Instant::now();
        let mut evicted = 0;

        for slot in self.slots.as_slice() {
            if slot.state.load(Ordering::Acquire) != SLOT_READY {
                continue;
            }
            if slot
                .state
                .compare_exchange_weak(
                    SLOT_READY,
                    SLOT_READING,
                    Ordering::Acquire,
                    Ordering::Relaxed,
                )
                .is_err()
            {
                continue;
            }

            let keep = unsafe {
                let res = (*slot.resource.get()).assume_init_ref();
                let is_idle_valid =
                    now.saturating_duration_since(res.last_used_at()) <= idle_timeout;
                let is_lifetime_valid = match max_lifetime {
                    Some(ttl) => now.saturating_duration_since(res.created_at()) <= ttl,
                    None => true,
                };
                is_idle_valid && is_lifetime_valid && res.is_healthy()
            };

            if keep {
                slot.state.store(SLOT_READY, Ordering::Release);
            } else {
                let mut res = unsafe {
                    (*slot.key.get()).assume_init_drop();
                    (*slot.resource.get()).assume_init_read()
                };
                self.ready_count.fetch_sub(1, Ordering::Release);
                slot.state.store(SLOT_EMPTY, Ordering::Release);
                res.close();
                evicted += 1;
            }
        }
        evicted
    }

    /// Drains and closes all resources matching `key` from the Cold Pool.
    pub fn drain_key(&self, key: &K) -> usize {
        if self.ready_count.load(Ordering::Acquire) == 0 {
            return 0;
        }
        let mut drained = 0;
        for slot in self.slots.as_slice() {
            if slot.state.load(Ordering::Acquire) != SLOT_READY {
                continue;
            }
            if slot
                .state
                .compare_exchange_weak(
                    SLOT_READY,
                    SLOT_READING,
                    Ordering::Acquire,
                    Ordering::Relaxed,
                )
                .is_err()
            {
                continue;
            }

            let matches = unsafe { (*slot.key.get()).assume_init_ref() == key };
            if matches {
                let mut res = unsafe {
                    (*slot.key.get()).assume_init_drop();
                    (*slot.resource.get()).assume_init_read()
                };
                self.ready_count.fetch_sub(1, Ordering::Release);
                slot.state.store(SLOT_EMPTY, Ordering::Release);
                res.close();
                drained += 1;
            } else {
                slot.state.store(SLOT_READY, Ordering::Release);
            }
        }
        drained
    }

    /// Closes and drains all resources in the Cold Pool.
    pub fn clear(&self) -> usize {
        let mut drained = 0;
        for slot in self.slots.as_slice() {
            if slot.state.load(Ordering::Acquire) == SLOT_READY
                && slot
                    .state
                    .compare_exchange_weak(
                        SLOT_READY,
                        SLOT_READING,
                        Ordering::Acquire,
                        Ordering::Relaxed,
                    )
                    .is_ok()
            {
                let mut res = unsafe {
                    (*slot.key.get()).assume_init_drop();
                    (*slot.resource.get()).assume_init_read()
                };
                self.ready_count.fetch_sub(1, Ordering::Release);
                slot.state.store(SLOT_EMPTY, Ordering::Release);
                res.close();
                drained += 1;
            }
        }
        drained
    }
}
