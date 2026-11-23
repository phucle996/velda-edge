//! Contiguous 2 MiB (`1 << 21`) DRAM Slab Arena & Down-Counting Pool Allocator.
//!
//! Eliminates external memory fragmentation by allocating a single contiguous DRAM arena partitioned
//! into `SLAB_SIZE = 2 MiB` slabs rotated sequentially from a free pool:
//!
//! ```text
//!   Contiguous DRAM Arena: [ Slab 0 (2MB) | Slab 1 (2MB) | ... | Slab N-1 (2MB) ]
//!
//!   Alloc (`alloc_and_write`):
//!     remaining (2_097_152 -> 0) -= len  [offset = new_remaining]
//!       ├─ Some(offset) ──► packed_pos = (buf_id << 21) | offset ─► Non-Temporal DRAM write
//!       └─ None         ──► rotate_active_slab() ─► seal current / pop free_pool / reclaim sealed
//!
//!   Free / Evict (`release_span`):
//!     live_spans -= 1
//!       └─ reaches 0 (ZF=1) ─► bump generation ─► recycle whole 2 MiB slab back to free_pool
//!
//!   Reader Pin (`pin_span`):
//!     buf_id = packed_pos >> 21; readers[buf_id].fetch_add(1) + SeqCst gen check
//!       └─ Bytes::from_owner(SlabLease) ─► 0-copy payload slice until socket finishes sending
//! ```

use std::collections::VecDeque;
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use bytes::Bytes;
use velda_core::clock::coarse_sec;

use crate::http_cache::dram::stream_payload_to_dram;

/// Power-of-two bit shift for 2 MiB slabs (`1 << 21 = 2,097,152` bytes).
pub const SLAB_SHIFT: usize = 21;

/// Fixed slab buffer size (2 MiB).
pub const SLAB_SIZE: usize = 1 << SLAB_SHIFT;

/// Bitmask for extracting the intra-slab byte offset (`packed_pos & SLAB_OFFSET_MASK`).
pub const SLAB_OFFSET_MASK: usize = SLAB_SIZE - 1;

/// Snapshot counters for monitoring the 2 MiB slab pool lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SlabPoolStats {
    pub num_slabs: usize,
    pub free_slabs: usize,
    pub sealed_slabs: usize,
    pub draining_slabs: usize,
    pub active_slab: usize,
    pub active_remaining_bytes: u32,
}

/// 16-byte handle encoding an AGU byte offset into the contiguous slab arena.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct SpanHandle {
    /// Packed `(buf_id << SLAB_SHIFT) | offset`.
    pub packed_pos: u64,
    pub len: u32,
    pub generation: u32,
}

impl SpanHandle {
    #[inline(always)]
    pub(crate) fn buf_id(self) -> usize {
        (self.packed_pos >> SLAB_SHIFT) as usize
    }

    #[inline(always)]
    pub(crate) fn is_present(self) -> bool {
        self.len > 0
    }
}

/// Contiguous DRAM backing shared across all 2 MiB slabs.
///
/// Readers pin `slab_readers[buf_id]` with a `SeqCst` generation check before accessing
/// `[packed_pos .. packed_pos + len]`; writers only reuse a slab after `slab_readers[buf_id] == 0`.
#[derive(Debug)]
pub(crate) struct SlabArena {
    base: NonNull<u8>,
    total_bytes: usize,
    pub(crate) num_slabs: usize,
    slab_readers: Box<[AtomicU32]>,
    slab_generations: Box<[AtomicU32]>,
}

unsafe impl Send for SlabArena {}
unsafe impl Sync for SlabArena {}

impl Drop for SlabArena {
    fn drop(&mut self) {
        // Safety: `base` was created via `Box::into_raw` from a boxed slice of length `total_bytes`.
        unsafe {
            let slice_ptr =
                std::ptr::slice_from_raw_parts_mut(self.base.as_ptr(), self.total_bytes);
            drop(Box::from_raw(slice_ptr));
        }
    }
}

impl SlabArena {
    pub(crate) fn new(num_slabs: usize) -> Self {
        let total_bytes = num_slabs.saturating_mul(SLAB_SIZE);
        let boxed = vec![0u8; total_bytes].into_boxed_slice();
        let raw_ptr = Box::into_raw(boxed) as *mut u8;
        let base = NonNull::new(raw_ptr).expect("slab arena allocation returned null");

        let slab_readers = (0..num_slabs)
            .map(|_| AtomicU32::new(0))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let slab_generations = (0..num_slabs)
            .map(|_| AtomicU32::new(1))
            .collect::<Vec<_>>()
            .into_boxed_slice();

        Self {
            base,
            total_bytes,
            num_slabs,
            slab_readers,
            slab_generations,
        }
    }

    #[inline(always)]
    pub(crate) fn generation(&self, buf_id: usize) -> u32 {
        self.slab_generations
            .get(buf_id)
            .map_or(0, |g| g.load(Ordering::Acquire))
    }

    /// Pins a cached payload span with zero copy and returns a guard-backed [`Bytes`].
    #[inline]
    pub(crate) fn pin_span(self: &Arc<Self>, span: SpanHandle) -> Option<Bytes> {
        if span.len == 0 {
            return None;
        }
        let buf_id = span.buf_id();
        let readers = self.slab_readers.get(buf_id)?;
        let gen_atomic = self.slab_generations.get(buf_id)?;

        let start = span.packed_pos as usize;
        let end = start.checked_add(span.len as usize)?;
        if end > self.total_bytes {
            return None;
        }

        // Dekker-symmetric pin: increment readers first, then verify generation after SeqCst fence.
        readers.fetch_add(1, Ordering::AcqRel);
        std::sync::atomic::fence(Ordering::SeqCst);
        if gen_atomic.load(Ordering::Acquire) != span.generation {
            readers.fetch_sub(1, Ordering::Release);
            return None;
        }

        // Safety: `end <= self.total_bytes`, generation matches, and `readers > 0` blocks recycling.
        let ptr = unsafe { NonNull::new_unchecked(self.base.as_ptr().add(start)) };
        Some(Bytes::from_owner(SlabLease {
            arena: Arc::clone(self),
            buf_id,
            ptr,
            len: span.len as usize,
        }))
    }
}

/// Zero-copy [`Bytes`] owner keeping a slab pinned while a response payload is in flight.
struct SlabLease {
    arena: Arc<SlabArena>,
    buf_id: usize,
    ptr: NonNull<u8>,
    len: usize,
}

unsafe impl Send for SlabLease {}
unsafe impl Sync for SlabLease {}

impl AsRef<[u8]> for SlabLease {
    #[inline(always)]
    fn as_ref(&self) -> &[u8] {
        // Safety: `ptr..ptr+len` is immutable and valid for the lifetime of this pinned lease.
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }
}

impl Drop for SlabLease {
    #[inline(always)]
    fn drop(&mut self) {
        if let Some(readers) = self.arena.slab_readers.get(self.buf_id) {
            readers.fetch_sub(1, Ordering::Release);
        }
    }
}

#[derive(Debug)]
struct SlabMeta {
    /// Down-counting free byte cursor initialized to `SLAB_SIZE` (`2_097_152`).
    remaining: u32,
    /// Number of active live payload spans inside this slab (recycles at `0`).
    live_spans: u32,
    generation: u32,
    max_expires_at_sec: u32,
}

/// Pool managing rotation and down-counting recycling of contiguous 2 MiB slabs.
#[derive(Debug)]
pub(crate) struct SlabPool {
    pub(crate) arena: Arc<SlabArena>,
    slabs: Box<[SlabMeta]>,
    active_slab: usize,
    free_pool: VecDeque<usize>,
    sealed_fifo: VecDeque<usize>,
    draining: Vec<usize>,
}

impl SlabPool {
    pub(crate) fn new(arena: Arc<SlabArena>) -> Self {
        let num_slabs = arena.num_slabs;
        let slabs = (0..num_slabs)
            .map(|_| SlabMeta {
                remaining: SLAB_SIZE as u32,
                live_spans: 0,
                generation: 1,
                max_expires_at_sec: 0,
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();

        let mut free_pool = VecDeque::with_capacity(num_slabs);
        for id in 1..num_slabs {
            free_pool.push_back(id);
        }

        Self {
            arena,
            slabs,
            active_slab: 0,
            free_pool,
            sealed_fifo: VecDeque::with_capacity(num_slabs),
            draining: Vec::new(),
        }
    }

    #[inline]
    fn bump_slab_generation(&mut self, buf_id: usize) -> bool {
        let Some(gen_atomic) = self.arena.slab_generations.get(buf_id) else {
            return false;
        };
        let Some(readers) = self.arena.slab_readers.get(buf_id) else {
            return false;
        };
        let Some(meta) = self.slabs.get_mut(buf_id) else {
            return false;
        };

        let next_gen = meta.generation.wrapping_add(1).max(1);
        meta.generation = next_gen;
        gen_atomic.store(next_gen, Ordering::Release);
        std::sync::atomic::fence(Ordering::SeqCst);

        if readers.load(Ordering::Acquire) != 0 {
            return false;
        }

        meta.remaining = SLAB_SIZE as u32;
        meta.live_spans = 0;
        meta.max_expires_at_sec = 0;
        true
    }

    #[cold]
    fn drain_completed(&mut self) {
        let mut i = 0;
        while i < self.draining.len() {
            let Some(&buf_id) = self.draining.get(i) else {
                break;
            };
            let ready = self
                .arena
                .slab_readers
                .get(buf_id)
                .is_some_and(|r| r.load(Ordering::Acquire) == 0);
            if ready && self.bump_slab_generation(buf_id) {
                self.draining.swap_remove(i);
                self.free_pool.push_back(buf_id);
            } else {
                i += 1;
            }
        }
    }

    fn recycle_or_drain(&mut self, buf_id: usize) {
        if self.bump_slab_generation(buf_id) {
            self.free_pool.push_back(buf_id);
        } else if !self.draining.contains(&buf_id) {
            self.draining.push(buf_id);
        }
    }

    /// Decrements a slab's live span counter; when it reaches 0, returns the slab to `free_pool`.
    pub(crate) fn release_span(&mut self, span: SpanHandle) {
        if span.len == 0 {
            return;
        }
        let buf_id = span.buf_id();
        let should_recycle = match self.slabs.get_mut(buf_id) {
            Some(meta) if meta.generation == span.generation => {
                meta.live_spans = meta.live_spans.saturating_sub(1);
                meta.live_spans == 0 && buf_id != self.active_slab
            }
            _ => false,
        };

        if should_recycle {
            self.sealed_fifo.retain(|&id| id != buf_id);
            self.recycle_or_drain(buf_id);
        }
    }

    #[cold]
    fn reclaim_from_sealed(&mut self) -> Option<usize> {
        let now = coarse_sec() as u32;
        let len = self.sealed_fifo.len();

        for pass_expired_only in [true, false] {
            for idx in 0..len {
                let Some(&candidate) = self.sealed_fifo.get(idx) else {
                    continue;
                };
                if pass_expired_only
                    && !self
                        .slabs
                        .get(candidate)
                        .is_some_and(|m| m.live_spans == 0 || now >= m.max_expires_at_sec)
                {
                    continue;
                }
                let readers_idle = self
                    .arena
                    .slab_readers
                    .get(candidate)
                    .is_some_and(|r| r.load(Ordering::Acquire) == 0);
                if readers_idle {
                    self.sealed_fifo.remove(idx);
                    if self.bump_slab_generation(candidate) {
                        return Some(candidate);
                    }
                }
            }
        }

        None
    }

    #[cold]
    fn rotate_active_slab(&mut self) -> bool {
        self.drain_completed();

        let prev = self.active_slab;
        let prev_empty = self.slabs.get(prev).is_some_and(|m| m.live_spans == 0);

        let next = if let Some(free_id) = self.free_pool.pop_front() {
            Some(free_id)
        } else {
            self.reclaim_from_sealed()
        };

        if let Some(next_id) = next {
            if prev_empty {
                self.recycle_or_drain(prev);
            } else {
                self.sealed_fifo.push_back(prev);
            }
            self.active_slab = next_id;
            return true;
        }

        let prev_readers_idle = self
            .arena
            .slab_readers
            .get(prev)
            .is_some_and(|r| r.load(Ordering::Acquire) == 0);
        prev_readers_idle && self.bump_slab_generation(prev)
    }

    /// Writes `src` into the active 2 MiB slab by decrementing `remaining -= len`.
    pub(crate) fn alloc_and_write(
        &mut self,
        src: &[u8],
        expires_at_sec: u32,
    ) -> Option<SpanHandle> {
        if src.is_empty() || src.len() > SLAB_SIZE {
            return None;
        }
        let len = src.len() as u32;

        let offset = match self
            .slabs
            .get_mut(self.active_slab)
            .and_then(|s| s.remaining.checked_sub(len))
        {
            Some(new_rem) => {
                if let Some(meta) = self.slabs.get_mut(self.active_slab) {
                    meta.remaining = new_rem;
                }
                new_rem
            }
            None => {
                if !self.rotate_active_slab() {
                    return None;
                }
                let meta = self.slabs.get_mut(self.active_slab)?;
                let new_rem = meta.remaining.checked_sub(len)?;
                meta.remaining = new_rem;
                new_rem
            }
        };

        let buf_id = self.active_slab;
        let packed_pos = ((buf_id as u64) << SLAB_SHIFT) | (offset as u64);
        let generation = {
            let meta = self.slabs.get_mut(buf_id)?;
            meta.live_spans = meta.live_spans.saturating_add(1);
            if expires_at_sec > meta.max_expires_at_sec {
                meta.max_expires_at_sec = expires_at_sec;
            }
            meta.generation
        };

        // Safety: `[packed_pos .. packed_pos + len]` lies inside `buf_id`'s exclusive bump-down
        // range and is not exposed to any reader until slot publication.
        unsafe {
            let dst = self.arena.base.as_ptr().add(packed_pos as usize);
            stream_payload_to_dram(dst, src.as_ptr(), src.len());
        }

        Some(SpanHandle {
            packed_pos,
            len,
            generation,
        })
    }

    pub(crate) fn clear_all(&mut self) {
        self.free_pool.clear();
        self.sealed_fifo.clear();
        self.draining.clear();

        let num_slabs = self.slabs.len();
        self.active_slab = 0;

        for buf_id in 0..num_slabs {
            if self.bump_slab_generation(buf_id) {
                if buf_id != 0 {
                    self.free_pool.push_back(buf_id);
                }
            } else {
                self.draining.push(buf_id);
            }
        }
    }

    pub(crate) fn stats(&self) -> SlabPoolStats {
        SlabPoolStats {
            num_slabs: self.slabs.len(),
            free_slabs: self.free_pool.len(),
            sealed_slabs: self.sealed_fifo.len(),
            draining_slabs: self.draining.len(),
            active_slab: self.active_slab,
            active_remaining_bytes: self.slabs.get(self.active_slab).map_or(0, |m| m.remaining),
        }
    }
}
