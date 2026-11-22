//! Contiguous `k * 4096`-Byte Shared Usage Arena (`openai_token_quota`).
//!
//! Hardware & Hot-Path Mechanism:
//!
//! ```text
//!   1. OS PAGE & TWO-CACHE-LINE SLOT LAYOUT (k * 4096 B, align = 4096)
//!      =================================================================
//!      Virtual Region (ptr, align=4096) ──► 32 slots per 4096B OS page
//!      +----------------------------+----------------------------+--- ... ---+
//!      | Page 0: 32 x 128B slots    | Page 1: 32 x 128B slots    | Page k-1  |
//!      +----------------------------+----------------------------+--- ... ---+
//!        |
//!        v  Each UsageSlot (#[repr(C, align(64))]) == 2 Cache Lines (128 B):
//!        Cache Line 0 (HOT - touched by Frontline Worker on every request):
//!        +------------+-----------------+---------------+--------+------------+--------+--------+----------+
//!        | hash (8B)  | local_rem (8B)  | pending (8B)  | prompt | completion | total  | cached | last_sec |
//!        | AtomicU64  | AtomicI64       | AtomicU64     |  (8B)  |    (8B)    |  (8B)  |  (8B)  |   (8B)   |
//!        +------------+-----------------+---------------+--------+------------+--------+--------+----------+
//!        Cache Line 1 (COLD - written once on CAS insert, read by Shadow Worker for Redis key):
//!        +------------+------------+-----------------------------------------------------------------------+
//!        | key_len(1B)| flags (1B) | key_bytes: [AtomicU8; 62] (e.g. "abc.123", zero heap alloc)           |
//!        +------------+------------+-----------------------------------------------------------------------+
//!
//!   2. TWO NON-BLOCKING THREADS (Frontline Deduct vs Shadow Micro-Quota Refill)
//!      =========================================================================
//!      Frontline Worker (on_response):
//!        addr = base_ptr + (((hash & mask) + step) << 7)   [1-cycle AGU: idx * 128]
//!        ├─► fetch_sub(total_tokens) on local_remaining    [never waits for Redis!]
//!        ├─► fetch_add on prompt/completion/total/cached + pending_delta
//!        └─► if new_rem <= refill_watermark ──► urgent wake Shadow Worker (atomic flag + Notify)
//!
//!      Shadow Worker (background):
//!        ├─► sweeps active slots (or wakes immediately on urgent notify)
//!        ├─► builds Redis key: "velda:plugin:<plugin_id>:<abc.123>"
//!        ├─► EVAL Lua on Redis: leases micro_quota_batch (5,000 tokens)
//!        └─► local_remaining.fetch_add(leased)             [never locks Frontline!]
//! ```

use std::alloc::{Layout, alloc_zeroed, dealloc, handle_alloc_error};
use std::marker::PhantomData;
use std::mem::{align_of, size_of};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicI64, AtomicU8, AtomicU64, Ordering};

use crate::post_response::openai_token_quota::scanner::TokenUsage;

pub const PAGE_SIZE: usize = 4096;
pub const MAX_PROBE: usize = 8;
pub const MAX_CONSUMER_KEY_LEN: usize = 62;

pub const FLAG_EXHAUSTED: u8 = 1 << 0;
pub const FLAG_NEEDS_REFILL: u8 = 1 << 1;

/// Streaming FNV-1a 64-bit hasher for hierarchical `seg1.seg2...` consumer keys.
#[derive(Clone, Copy)]
pub struct Fnv1aHasher {
    state: u64,
}

impl Fnv1aHasher {
    #[inline(always)]
    pub const fn new() -> Self {
        Self {
            state: 0xcbf2_9ce4_8422_2325,
        }
    }

    #[inline(always)]
    pub fn write_byte(&mut self, b: u8) {
        self.state ^= b as u64;
        self.state = self.state.wrapping_mul(0x0100_0000_01b3);
    }

    #[inline(always)]
    pub fn write_bytes(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.write_byte(b);
        }
    }

    /// Returns a guaranteed non-zero 64-bit hash (`0` is reserved for empty slots).
    #[inline(always)]
    pub const fn finish_non_zero(self) -> u64 {
        if self.state == 0 { 1 } else { self.state }
    }
}

impl Default for Fnv1aHasher {
    #[inline(always)]
    fn default() -> Self {
        Self::new()
    }
}

/// Computes a non-zero FNV-1a 64-bit hash for `bytes`.
#[inline(always)]
pub fn fnv1a_64(bytes: &[u8]) -> u64 {
    let mut h = Fnv1aHasher::new();
    h.write_bytes(bytes);
    h.finish_non_zero()
}

/// Contiguous `k * 4096`-byte allocation aligned to `4096` bytes (`jemalloc` extent-friendly).
pub struct ContiguousPageRegion<T> {
    ptr: NonNull<u8>,
    byte_size: usize,
    cap: usize,
    _marker: PhantomData<T>,
}

unsafe impl<T: Send> Send for ContiguousPageRegion<T> {}
unsafe impl<T: Sync> Sync for ContiguousPageRegion<T> {}

impl<T> ContiguousPageRegion<T> {
    pub fn with_min_elements(min_elems: usize) -> Self {
        assert!(size_of::<T>() > 0);
        assert!(align_of::<T>() <= PAGE_SIZE);

        let raw_bytes = min_elems
            .max(1)
            .checked_mul(size_of::<T>())
            .expect("ContiguousPageRegion byte size overflow");
        let byte_size = raw_bytes.div_ceil(PAGE_SIZE) * PAGE_SIZE;
        let cap = byte_size / size_of::<T>();

        let layout = Layout::from_size_align(byte_size, PAGE_SIZE)
            .expect("Invalid ContiguousPageRegion layout");
        // Safety: `byte_size > 0` and `PAGE_SIZE` is a valid power-of-two alignment.
        let raw_ptr = unsafe { alloc_zeroed(layout) };
        let ptr = match NonNull::new(raw_ptr) {
            Some(p) => p,
            None => handle_alloc_error(layout),
        };

        Self {
            ptr,
            byte_size,
            cap,
            _marker: PhantomData,
        }
    }

    #[inline(always)]
    pub fn as_slice(&self) -> &[T] {
        // Safety: `ptr` is `4096`-byte aligned, zero-initialized, and valid for `cap` elements.
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr().cast::<T>(), self.cap) }
    }

    #[inline(always)]
    pub fn byte_size(&self) -> usize {
        self.byte_size
    }
}

impl<T> Drop for ContiguousPageRegion<T> {
    fn drop(&mut self) {
        let layout = Layout::from_size_align(self.byte_size, PAGE_SIZE)
            .expect("Invalid ContiguousPageRegion layout on drop");
        // Safety: allocated with identical `layout` in `with_min_elements`.
        unsafe { dealloc(self.ptr.as_ptr(), layout) };
    }
}

/// 128-byte (2 cache-line) shared token quota & usage slot.
#[repr(C, align(64))]
pub struct UsageSlot {
    // Cache Line 0 (Hot path)
    pub consumer_hash: AtomicU64,
    pub local_remaining: AtomicI64,
    pub pending_delta: AtomicU64,
    pub prompt_tokens: AtomicU64,
    pub completion_tokens: AtomicU64,
    pub total_tokens: AtomicU64,
    pub cached_tokens: AtomicU64,
    pub last_access_sec: AtomicU64,

    // Cache Line 1 (Cold path: inline key for Redis `velda:plugin:<id>:<key>`)
    pub key_len: AtomicU8,
    pub flags: AtomicU8,
    pub key_bytes: [AtomicU8; MAX_CONSUMER_KEY_LEN],
}

const _: () = assert!(size_of::<UsageSlot>() == 128);
const _: () = assert!(align_of::<UsageSlot>() == 64);

/// Outcome returned from hot-path `SharedUsageArena::record`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordOutcome {
    pub local_remaining: i64,
    pub needs_refill: bool,
    pub exhausted: bool,
}

/// Shared contiguous `k * 4096`-byte token quota & usage ledger with bounded linear probing.
pub struct SharedUsageArena {
    region: ContiguousPageRegion<UsageSlot>,
    slot_mask: usize,
    pub initial_local_quota: i64,
    pub refill_watermark: i64,
}

impl SharedUsageArena {
    pub fn new(ledger_slots: usize, initial_local_quota: i64, refill_watermark: i64) -> Self {
        let slots = ledger_slots.next_power_of_two().max(64);
        let region = ContiguousPageRegion::with_min_elements(slots);
        Self {
            region,
            slot_mask: slots - 1,
            initial_local_quota,
            refill_watermark,
        }
    }

    #[inline(always)]
    pub fn slots(&self) -> &[UsageSlot] {
        self.region.as_slice()
    }

    #[inline]
    fn write_cold_key(slot: &UsageSlot, key: &[u8]) {
        let len = key.len().min(MAX_CONSUMER_KEY_LEN);
        for (i, &b) in key[..len].iter().enumerate() {
            slot.key_bytes[i].store(b, Ordering::Relaxed);
        }
        slot.key_len.store(len as u8, Ordering::Release);
    }

    /// Accumulates token counts and deducts `usage.total_tokens` from `local_remaining` in O(1).
    #[inline]
    pub fn record(
        &self,
        consumer_hash: u64,
        consumer_key: &[u8],
        usage: TokenUsage,
        now_sec: u64,
    ) -> RecordOutcome {
        let slice = self.region.as_slice();
        let base = (consumer_hash as usize) & self.slot_mask;

        let mut victim_idx = base;
        let mut oldest_sec = u64::MAX;

        for step in 0..MAX_PROBE {
            let idx = (base + step) & self.slot_mask;
            // Safety: `idx = (base + step) & self.slot_mask < slots <= slice.len()`.
            let slot = unsafe { slice.get_unchecked(idx) };
            let cur = slot.consumer_hash.load(Ordering::Relaxed);

            if cur == consumer_hash {
                return self.update_existing_slot(slot, usage, now_sec);
            }

            if cur == 0 {
                match slot.consumer_hash.compare_exchange(
                    0,
                    consumer_hash,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        Self::write_cold_key(slot, consumer_key);
                        slot.local_remaining
                            .store(self.initial_local_quota, Ordering::Relaxed);
                        slot.flags.store(FLAG_NEEDS_REFILL, Ordering::Relaxed);
                        return self.update_existing_slot(slot, usage, now_sec);
                    }
                    Err(actual) if actual == consumer_hash => {
                        return self.update_existing_slot(slot, usage, now_sec);
                    }
                    Err(_) => {}
                }
            }

            let seen_sec = slot.last_access_sec.load(Ordering::Relaxed);
            if seen_sec < oldest_sec {
                oldest_sec = seen_sec;
                victim_idx = idx;
            }
        }

        // All `MAX_PROBE` slots occupied by other hashes: evict the oldest slot.
        // Safety: `victim_idx` was bounded by `& self.slot_mask`.
        let slot = unsafe { slice.get_unchecked(victim_idx) };
        slot.consumer_hash.store(consumer_hash, Ordering::Relaxed);
        Self::write_cold_key(slot, consumer_key);
        let rem = self
            .initial_local_quota
            .saturating_sub(usage.total_tokens as i64);
        slot.local_remaining.store(rem, Ordering::Relaxed);
        slot.pending_delta
            .store(usage.total_tokens, Ordering::Relaxed);
        slot.prompt_tokens
            .store(usage.prompt_tokens, Ordering::Relaxed);
        slot.completion_tokens
            .store(usage.completion_tokens, Ordering::Relaxed);
        slot.total_tokens
            .store(usage.total_tokens, Ordering::Relaxed);
        slot.cached_tokens
            .store(usage.cached_prompt_tokens, Ordering::Relaxed);
        slot.last_access_sec.store(now_sec, Ordering::Relaxed);
        slot.flags.store(FLAG_NEEDS_REFILL, Ordering::Relaxed);

        RecordOutcome {
            local_remaining: rem,
            needs_refill: rem <= self.refill_watermark,
            exhausted: rem <= 0,
        }
    }

    #[inline(always)]
    fn update_existing_slot(
        &self,
        slot: &UsageSlot,
        usage: TokenUsage,
        now_sec: u64,
    ) -> RecordOutcome {
        let prev_rem = slot
            .local_remaining
            .fetch_sub(usage.total_tokens as i64, Ordering::Relaxed);
        let new_rem = prev_rem.saturating_sub(usage.total_tokens as i64);
        slot.pending_delta
            .fetch_add(usage.total_tokens, Ordering::Relaxed);
        slot.prompt_tokens
            .fetch_add(usage.prompt_tokens, Ordering::Relaxed);
        slot.completion_tokens
            .fetch_add(usage.completion_tokens, Ordering::Relaxed);
        slot.total_tokens
            .fetch_add(usage.total_tokens, Ordering::Relaxed);
        slot.cached_tokens
            .fetch_add(usage.cached_prompt_tokens, Ordering::Relaxed);
        slot.last_access_sec.store(now_sec, Ordering::Relaxed);

        let needs_refill = new_rem <= self.refill_watermark;
        if needs_refill {
            slot.flags.fetch_or(FLAG_NEEDS_REFILL, Ordering::Relaxed);
        }
        let exhausted = new_rem <= 0 && (slot.flags.load(Ordering::Relaxed) & FLAG_EXHAUSTED) != 0;

        RecordOutcome {
            local_remaining: new_rem,
            needs_refill,
            exhausted,
        }
    }

    /// Reads `(prompt, completion, total, cached, local_remaining)` for a hierarchical `consumer_id`.
    pub fn read_consumer(&self, consumer_id: &str) -> Option<(u64, u64, u64, u64, i64)> {
        let hash = fnv1a_64(consumer_id.as_bytes());
        let slice = self.region.as_slice();
        let base = (hash as usize) & self.slot_mask;

        for step in 0..MAX_PROBE {
            let idx = (base + step) & self.slot_mask;
            let slot = &slice[idx];
            let cur = slot.consumer_hash.load(Ordering::Relaxed);
            if cur == hash {
                return Some((
                    slot.prompt_tokens.load(Ordering::Relaxed),
                    slot.completion_tokens.load(Ordering::Relaxed),
                    slot.total_tokens.load(Ordering::Relaxed),
                    slot.cached_tokens.load(Ordering::Relaxed),
                    slot.local_remaining.load(Ordering::Relaxed),
                ));
            }
            if cur == 0 {
                return None;
            }
        }
        None
    }

    /// Copies the inline cold key out of `slot` into `out` and returns the populated slice.
    pub fn read_slot_key<'a>(slot: &UsageSlot, out: &'a mut [u8; MAX_CONSUMER_KEY_LEN]) -> &'a str {
        let len = (slot.key_len.load(Ordering::Acquire) as usize).min(MAX_CONSUMER_KEY_LEN);
        for (i, dst) in out[..len].iter_mut().enumerate() {
            *dst = slot.key_bytes[i].load(Ordering::Relaxed);
        }
        std::str::from_utf8(&out[..len]).unwrap_or("anonymous")
    }

    #[inline(always)]
    pub fn byte_size(&self) -> usize {
        self.region.byte_size()
    }
}
