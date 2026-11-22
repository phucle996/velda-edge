//! Contiguous `k * 4096`-Byte Page-Aligned Arenas & Integer Key Engine (`api_key_auth`).
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────────────────────┐
//! │                     CONTIGUOUS USER-SPACE ARENAS (k * 4096 B, align=4096)               │
//! ├─────────────────────────────────────────────────────────────────────────────────────────┤
//! │  1. Per-Core Cache Arena (jemalloc user-space extent = total_cores * m * 4096 B)        │
//! │     ├── Core 0 Region [0 .. slots_per_core)   (aligned to 4096B boundary, 0 MESI RFO)   │
//! │     ├── Core 1 Region [N .. 2*N)              (aligned to 4096B boundary, 0 Mutex)      │
//! │     └── Slot (16B): tag: AtomicU64 | meta: AtomicU64 [expire:32 | neg:1 | idx:31]       │
//! │                                                                                         │
//! │  2. Contiguous Key Store (read-only shared across cores, k * 4096 B)                    │
//! │     ├── Index Slots (16B):  tag: u64 | record_idx: u32 | key_len: u8 | _pad[3]          │
//! │     ├── Key Records (80B):  words: [u64; 8] (64B) | expires_at: u64 | hdr_start/count   │
//! │     └── Header Arena:       Box<[(HeaderName, HeaderValue)]>                            │
//! └─────────────────────────────────────────────────────────────────────────────────────────┘
//! ```

use std::alloc::{Layout, alloc_zeroed, dealloc, handle_alloc_error};
use std::marker::PhantomData;
use std::mem::{align_of, size_of};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use http::header::{HeaderName, HeaderValue};

/// Standard 4 KiB page-alignment quantum for contiguous user-space jemalloc extents.
pub const PAGE_SIZE: usize = 4096;

static NEXT_CORE_LANE: AtomicUsize = AtomicUsize::new(0);
thread_local! {
    static LOCAL_CORE_LANE: usize = NEXT_CORE_LANE.fetch_add(1, Ordering::Relaxed);
}

/// Contiguous `k * 4096`-byte allocation aligned to `4096` bytes (`jemalloc` extent-friendly).
///
/// Allocates exact needed multiples of `4096` bytes in user-space without requesting 2MB HugePages.
pub struct ContiguousPageRegion<T> {
    ptr: NonNull<u8>,
    byte_size: usize,
    cap: usize,
    _marker: PhantomData<T>,
}

unsafe impl<T: Send> Send for ContiguousPageRegion<T> {}
unsafe impl<T: Sync> Sync for ContiguousPageRegion<T> {}

impl<T> ContiguousPageRegion<T> {
    /// Allocates a zero-initialized contiguous buffer of `k * 4096` bytes holding at least `min_elems`.
    pub fn with_min_elements(min_elems: usize) -> Self {
        assert!(size_of::<T>() > 0);
        assert!(align_of::<T>() <= PAGE_SIZE);

        let min_bytes = min_elems
            .max(1)
            .checked_mul(size_of::<T>())
            .expect("ContiguousPageRegion byte size overflow");
        let pages = min_bytes.div_ceil(PAGE_SIZE).max(1);
        let byte_size = pages
            .checked_mul(PAGE_SIZE)
            .expect("ContiguousPageRegion page overflow");
        let layout = Layout::from_size_align(byte_size, PAGE_SIZE)
            .expect("Invalid ContiguousPageRegion layout");

        let raw = unsafe { alloc_zeroed(layout) };
        let ptr = NonNull::new(raw).unwrap_or_else(|| handle_alloc_error(layout));

        Self {
            ptr,
            byte_size,
            cap: byte_size / size_of::<T>(),
            _marker: PhantomData,
        }
    }

    #[inline(always)]
    pub fn as_slice(&self) -> &[T] {
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr() as *const T, self.cap) }
    }

    #[inline(always)]
    pub fn as_mut_slice(&mut self) -> &mut [T] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr() as *mut T, self.cap) }
    }

    #[inline(always)]
    pub fn byte_size(&self) -> usize {
        self.byte_size
    }
}

impl<T> Drop for ContiguousPageRegion<T> {
    fn drop(&mut self) {
        let layout = Layout::from_size_align(self.byte_size, PAGE_SIZE)
            .expect("Invalid ContiguousPageRegion drop layout");
        unsafe {
            dealloc(self.ptr.as_ptr(), layout);
        }
    }
}

/// Packs `1..=64` ASCII/UTF-8 bytes into `[u64; 8]` (64 bytes, zero-padded) and computes
/// a non-zero 64-bit integer tag without calling `libc::memcpy`.
#[inline(always)]
pub fn encode_key_words(bytes: &[u8]) -> ([u64; 8], u64) {
    let mut words = [0u64; 8];
    let full_chunks = bytes.len() >> 3;
    let mut i = 0usize;
    while i < full_chunks && i < 8 {
        let base = i << 3;
        words[i] = u64::from_le_bytes([
            bytes[base],
            bytes[base + 1],
            bytes[base + 2],
            bytes[base + 3],
            bytes[base + 4],
            bytes[base + 5],
            bytes[base + 6],
            bytes[base + 7],
        ]);
        i += 1;
    }

    let rem = bytes.len() & 7;
    if rem > 0 && full_chunks < 8 {
        let base = full_chunks << 3;
        let mut tail = 0u64;
        let mut j = 0usize;
        while j < rem {
            tail |= (bytes[base + j] as u64) << (j << 3);
            j += 1;
        }
        words[full_chunks] = tail;
    }

    let mut mix = (bytes.len() as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15);
    let mut k = 0usize;
    while k < 8 {
        mix ^= words[k].wrapping_mul(0xbf58_476d_1ce4_e5b9);
        mix = mix.rotate_left(27).wrapping_mul(0x94d0_49bb_1331_11eb);
        k += 1;
    }
    mix ^= mix >> 31;
    let tag = if mix == 0 { 1 } else { mix };
    (words, tag)
}

/// Branchless constant-time integer comparison over 8 `u64` words (`64` bytes).
#[inline(always)]
pub fn ct_eq_words(a: &[u64; 8], a_len: u8, b: &[u64; 8], b_len: u8) -> bool {
    let mut diff = (a_len ^ b_len) as u64;
    diff |= a[0] ^ b[0];
    diff |= a[1] ^ b[1];
    diff |= a[2] ^ b[2];
    diff |= a[3] ^ b[3];
    diff |= a[4] ^ b[4];
    diff |= a[5] ^ b[5];
    diff |= a[6] ^ b[6];
    diff |= a[7] ^ b[7];
    diff == 0
}

/// 16-byte atomic slot in [`CoreCacheArena`].
#[repr(C)]
pub struct CacheSlot {
    pub tag: AtomicU64,
    pub meta: AtomicU64,
}

const META_IDX_MASK: u64 = 0xffff_ffff;

#[inline(always)]
const fn pack_meta(expire_sec: u64, record_idx: u32) -> u64 {
    let exp32 = if expire_sec > u32::MAX as u64 {
        u32::MAX as u64
    } else {
        expire_sec
    };
    (exp32 << 32) | (record_idx as u64)
}

/// Lock-free, per-core partitioned contiguous cache arena (`num_cores * m * 4096` bytes).
///
/// Each core reads and writes strictly within its own `4096`-byte-aligned slice, avoiding
/// cross-core MESI cache invalidations and mutex overhead.
pub struct CoreCacheArena {
    region: ContiguousPageRegion<CacheSlot>,
    lane_mask: usize,
    slots_per_core: usize,
    slot_mask: usize,
}

impl CoreCacheArena {
    /// Allocates a contiguous `k * 4096`-byte cache arena partitioned across power-of-2 lanes.
    pub fn new(slots_per_core: usize) -> Self {
        let num_lanes = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
            .max(1)
            .next_power_of_two();
        // Minimum 256 slots (256 * 16B = 4096B) ensures every core region is an exact multiple of 4096B.
        let slots = slots_per_core.max(256).next_power_of_two();
        let total_slots = num_lanes * slots;
        let region = ContiguousPageRegion::with_min_elements(total_slots);

        Self {
            region,
            lane_mask: num_lanes - 1,
            slots_per_core: slots,
            slot_mask: slots - 1,
        }
    }

    /// Returns the calling worker thread's masked lane index (`0..=lane_mask`).
    #[inline(always)]
    pub fn current_lane(&self) -> usize {
        LOCAL_CORE_LANE.with(|&id| id & self.lane_mask)
    }

    #[inline(always)]
    fn slot_index(&self, lane: usize, tag: u64) -> usize {
        (lane & self.lane_mask) * self.slots_per_core + ((tag as usize) & self.slot_mask)
    }

    /// Probes the calling core's positive-only cache slot using `coarse_sec()`.
    #[inline(always)]
    pub fn probe(&self, lane: usize, tag: u64, now_sec: u64) -> Option<u32> {
        let idx = self.slot_index(lane, tag);
        debug_assert!(idx < self.region.as_slice().len());
        // Safety: `lane & lane_mask < num_lanes` and `tag & slot_mask < slots_per_core`.
        let slot = unsafe { self.region.as_slice().get_unchecked(idx) };
        let stored_tag = slot.tag.load(Ordering::Relaxed);
        if stored_tag != tag {
            return None;
        }
        let meta = slot.meta.load(Ordering::Relaxed);
        let exp = meta >> 32;
        if now_sec >= exp {
            return None;
        }
        Some((meta & META_IDX_MASK) as u32)
    }

    /// Records a validated positive hit in the calling core's local cache slot.
    #[inline(always)]
    pub fn insert_positive(&self, lane: usize, tag: u64, record_idx: u32, expire_sec: u64) {
        let idx = self.slot_index(lane, tag);
        debug_assert!(idx < self.region.as_slice().len());
        let slot = unsafe { self.region.as_slice().get_unchecked(idx) };
        slot.meta
            .store(pack_meta(expire_sec, record_idx), Ordering::Relaxed);
        slot.tag.store(tag, Ordering::Relaxed);
    }

    /// Returns total allocated bytes (always a multiple of 4096).
    #[inline(always)]
    pub fn byte_size(&self) -> usize {
        self.region.byte_size()
    }
}

/// 16-byte slot in the open-addressing flat key index (4 slots per 64B cache line).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct KeyIndexSlot {
    pub tag: u64,
    pub record_idx: u32,
    pub key_len: u8,
    pub _pad: [u8; 3],
}

/// 80-byte contiguous key record holding integer-packed secret words, expiration, and header slice range.
#[repr(C)]
pub struct KeyRecord {
    pub words: [u64; 8],
    pub expires_at: u64,
    pub hdr_start: u16,
    pub hdr_count: u16,
    pub key_len: u8,
    pub _pad: [u8; 3],
}

/// Input descriptor for compiling a single key entry into [`ContiguousKeyStore`].
pub struct CompiledKeyInput {
    pub key_bytes: Vec<u8>,
    pub expires_at: u64,
    pub inject_headers: Vec<(HeaderName, HeaderValue)>,
}

/// Read-only contiguous key store backed by `k * 4096`-byte page-aligned regions.
pub struct ContiguousKeyStore {
    index: ContiguousPageRegion<KeyIndexSlot>,
    index_mask: usize,
    records: Box<[KeyRecord]>,
    headers: Box<[(HeaderName, HeaderValue)]>,
    pub min_key_len: usize,
    pub max_key_len: usize,
}

impl ContiguousKeyStore {
    /// Compiles key entries into contiguous `k * 4096`-byte open-addressing index & record tables.
    pub fn compile(entries: Vec<CompiledKeyInput>) -> Self {
        let index_slots = (entries.len() * 2).max(256).next_power_of_two();
        let mut index = ContiguousPageRegion::<KeyIndexSlot>::with_min_elements(index_slots);
        // Ensure capacity is power-of-two for bitmask probing
        let mask = index_slots - 1;

        let mut records = Vec::with_capacity(entries.len());
        let mut headers = Vec::new();
        let mut min_len = usize::MAX;
        let mut max_len = 0usize;

        let idx_slice = index.as_mut_slice();

        for (rec_idx, entry) in entries.into_iter().enumerate() {
            let key_bytes = entry.key_bytes;
            let expires_at = entry.expires_at;
            let hdrs = entry.inject_headers;
            let klen = key_bytes.len();
            min_len = min_len.min(klen);
            max_len = max_len.max(klen);

            let (words, tag) = encode_key_words(&key_bytes);
            let hdr_start = headers.len() as u16;
            let hdr_count = hdrs.len() as u16;
            headers.extend(hdrs);

            records.push(KeyRecord {
                words,
                expires_at,
                hdr_start,
                hdr_count,
                key_len: klen as u8,
                _pad: [0; 3],
            });

            // Insert into open-addressing linear-probing flat index
            let mut pos = (tag as usize) & mask;
            loop {
                if idx_slice[pos].tag == 0 {
                    idx_slice[pos] = KeyIndexSlot {
                        tag,
                        record_idx: rec_idx as u32,
                        key_len: klen as u8,
                        _pad: [0; 3],
                    };
                    break;
                }
                pos = (pos + 1) & mask;
            }
        }

        if min_len == usize::MAX {
            min_len = 0;
        }

        Self {
            index,
            index_mask: mask,
            records: records.into_boxed_slice(),
            headers: headers.into_boxed_slice(),
            min_key_len: min_len,
            max_key_len: max_len,
        }
    }

    /// Probes the open-addressing index and verifies constant-time integer equality against `words`.
    #[inline]
    pub fn find_record(&self, tag: u64, words: &[u64; 8], key_len: u8) -> Option<u32> {
        let slots = self.index.as_slice();
        let mut pos = (tag as usize) & self.index_mask;
        loop {
            debug_assert!(pos < slots.len());
            // Safety: `pos` is masked by `index_mask` (`index_slots - 1`).
            let slot = unsafe { *slots.get_unchecked(pos) };
            if slot.tag == 0 {
                return None;
            }
            if slot.tag == tag && slot.key_len == key_len {
                let rec = self.record(slot.record_idx);
                if ct_eq_words(words, key_len, &rec.words, rec.key_len) {
                    return Some(slot.record_idx);
                }
            }
            pos = (pos + 1) & self.index_mask;
        }
    }

    #[inline(always)]
    pub fn record(&self, record_idx: u32) -> &KeyRecord {
        debug_assert!((record_idx as usize) < self.records.len());
        // Safety: `record_idx` originates strictly from compiled `ContiguousKeyStore` indices.
        unsafe { self.records.get_unchecked(record_idx as usize) }
    }

    #[inline(always)]
    pub fn headers_for(&self, rec: &KeyRecord) -> &[(HeaderName, HeaderValue)] {
        let start = rec.hdr_start as usize;
        let end = start + rec.hdr_count as usize;
        debug_assert!(end <= self.headers.len());
        // Safety: `hdr_start + hdr_count` is bounded during `compile()`.
        unsafe { self.headers.get_unchecked(start..end) }
    }
}
