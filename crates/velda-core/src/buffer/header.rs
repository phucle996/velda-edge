//! Contiguous Single-Allocation Header Workspace (`velda_core::buffer::header`).
//!
//! Replaces `http::HeaderMap` and dual-tier numeric staging with a single flat contiguous buffer
//! `[tags | slots]` recycled per worker via a lock-free `thread_local!` LIFO local pool:
//!
//! ```text
//!   HeaderWorkspace { storage, ingress_time_nanos, cache_hash }
//!     └─► FlatHeaderStorage (1 contiguous heap block, default cap = 32, cold grow -> 256):
//!           [ tag_0 .. tag_{cap-1} : u64 | slot_0 .. slot_{cap-1} : HeaderSlot ]
//!               └─ bit 63: TAG_BYPASS_BIT (0 = Discard, 1 = Preserve-on-Reject)
//!               └─ bits 0..62: 63-bit case-insensitive FNV-1a header name hash
//!
//!   Hot paths:
//!     ├─ insert / append ──► check len < cap -> write tag + slot (`#[cold] grow_cold` 32->64->128->256)
//!     ├─ get / remove    ──► sequential scan on tags[..len] -> match -> AGU access slots[i]
//!     ├─ prepare_for_rejection ──► 1-pass compact keeping `(tag & TAG_BYPASS_BIT) != 0` in place
//!     └─ Drop / reset    ──► return cap == 32 blocks to `thread_local!` LIFO pool (0 Mutex/Arc)
//! ```

use std::alloc::{Layout, alloc, dealloc};
use std::cell::RefCell;
use std::ptr::NonNull;

use http::header::{AsHeaderName, HeaderName, HeaderValue};

/// Canonical header name for the W3C TraceContext Request Identifier.
pub const X_REQUEST_ID: HeaderName = HeaderName::from_static("x-request-id");

/// Default initial capacity for each header workspace (32 slots -> 256B tags + contiguous slots).
pub const DEFAULT_HEADER_CAPACITY: usize = 32;

/// Hard ceiling on total headers per request/response (Anti-DoS, `u8` slot-indexable `0..256`).
pub const MAX_WIRE_HEADERS: usize = 256;

/// Bit 63 in each `u64` tag: `1` = Preserve-on-Reject (Bypass), `0` = Discard-on-Reject.
pub const TAG_BYPASS_BIT: u64 = 1u64 << 63;

/// Lower 63 bits of each `u64` tag storing the non-zero case-insensitive FNV-1a name hash.
const TAG_HASH_MASK: u64 = (1u64 << 63) - 1;

/// Maximum `DEFAULT_HEADER_CAPACITY` buffers cached per worker thread in the local LIFO pool.
const LOCAL_POOL_MAX_BUFFERS: usize = 64;

/// Computes a non-zero 63-bit case-insensitive FNV-1a key for an HTTP header name.
#[inline(always)]
pub const fn header_key(name: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    let mut i = 0;
    while i < name.len() {
        let b = name[i];
        let lower = if b >= b'A' && b <= b'Z' { b + 32 } else { b };
        hash ^= lower as u64;
        hash = hash.wrapping_mul(0x0100_0000_01b3);
        i += 1;
    }
    let masked = hash & TAG_HASH_MASK;
    if masked == 0 { 1 } else { masked }
}

const SMALL_U64_DIGITS_STR: &str = {
    // 10 + 180 + 2700 = 2890 ASCII bytes containing "0".."9", "10".."99", "100".."999".
    // Using a const byte buffer converted once guarantees static lifetime and zero runtime validation.
    const TOTAL: usize = 10 + 180 + 2700;
    const BUF: [u8; TOTAL] = {
        let mut buf = [0u8; TOTAL];
        let mut n = 0usize;
        while n < 10 {
            buf[n] = b'0' + (n as u8);
            n += 1;
        }
        while n < 100 {
            let idx = 10 + (n - 10) * 2;
            buf[idx] = b'0' + ((n / 10) as u8);
            buf[idx + 1] = b'0' + ((n % 10) as u8);
            n += 1;
        }
        while n < 1000 {
            let idx = 190 + (n - 100) * 3;
            buf[idx] = b'0' + ((n / 100) as u8);
            buf[idx + 1] = b'0' + (((n / 10) % 10) as u8);
            buf[idx + 2] = b'0' + ((n % 10) as u8);
            n += 1;
        }
        buf
    };
    match std::str::from_utf8(&BUF) {
        Ok(s) => s,
        Err(_) => unreachable!(),
    }
};

const fn const_substr(s: &'static str, start: usize, len: usize) -> &'static str {
    let (_, tail) = s.as_bytes().split_at(start);
    let (mid, _) = tail.split_at(len);
    match std::str::from_utf8(mid) {
        Ok(sub) => sub,
        Err(_) => unreachable!(),
    }
}

/// Pre-validated static `HeaderValue` table for `0..1000` (zero runtime validation, zero refcount ops).
static SMALL_U64_HEADERS: [HeaderValue; 1000] = {
    use std::mem::MaybeUninit;
    let mut table: [MaybeUninit<HeaderValue>; 1000] = [const { MaybeUninit::uninit() }; 1000];
    let mut n = 0usize;
    while n < 10 {
        table[n].write(HeaderValue::from_static(const_substr(
            SMALL_U64_DIGITS_STR,
            n,
            1,
        )));
        n += 1;
    }
    while n < 100 {
        let idx = 10 + (n - 10) * 2;
        table[n].write(HeaderValue::from_static(const_substr(
            SMALL_U64_DIGITS_STR,
            idx,
            2,
        )));
        n += 1;
    }
    while n < 1000 {
        let idx = 190 + (n - 100) * 3;
        table[n].write(HeaderValue::from_static(const_substr(
            SMALL_U64_DIGITS_STR,
            idx,
            3,
        )));
        n += 1;
    }
    // Safety: Every index in 0..1000 is partitioned cleanly across 0..10, 10..100, 100..1000 and initialized.
    unsafe { std::mem::transmute::<[MaybeUninit<HeaderValue>; 1000], [HeaderValue; 1000]>(table) }
};

const DIGITS_1: [u8; 10] = *b"0123456789";

const DIGITS_2: [u8; 180] = {
    let mut buf = [0u8; 180];
    let mut n = 10usize;
    while n < 100 {
        let idx = (n - 10) * 2;
        buf[idx] = b'0' + ((n / 10) as u8);
        buf[idx + 1] = b'0' + ((n % 10) as u8);
        n += 1;
    }
    buf
};

thread_local! {
    static U64_HEADER_CACHE: RefCell<(u64, HeaderValue)> = const {
        RefCell::new((0, HeaderValue::from_static("0")))
    };

    static LOCAL_HEADER_POOL: RefCell<Vec<RawHeaderBlock>> = const {
        RefCell::new(Vec::new())
    };
}

/// Resolves any unsigned 64-bit integer into an HTTP [`HeaderValue`].
#[inline(always)]
pub fn resolve_u64_header_value(val: u64) -> HeaderValue {
    if val < 1000 {
        return SMALL_U64_HEADERS[val as usize].clone();
    }
    resolve_u64_header_value_cold(val)
}

#[cold]
#[inline(never)]
fn resolve_u64_header_value_cold(val: u64) -> HeaderValue {
    U64_HEADER_CACHE.with(|cell| {
        let mut cache = cell.borrow_mut();
        if cache.0 == val {
            return cache.1.clone();
        }
        let mut buf = [0u8; 20];
        let mut n = val;
        let mut pos = 20usize;
        while n >= 100 {
            let rem = (n % 100) as usize;
            n /= 100;
            pos -= 2;
            if rem < 10 {
                buf[pos] = b'0';
                buf[pos + 1] = DIGITS_1[rem];
            } else {
                let d_idx = (rem - 10) * 2;
                buf[pos] = DIGITS_2[d_idx];
                buf[pos + 1] = DIGITS_2[d_idx + 1];
            }
        }
        if n < 10 {
            pos -= 1;
            buf[pos] = DIGITS_1[n as usize];
        } else {
            let d_idx = ((n as usize) - 10) * 2;
            pos -= 2;
            buf[pos] = DIGITS_2[d_idx];
            buf[pos + 1] = DIGITS_2[d_idx + 1];
        }
        let hv = HeaderValue::from_bytes(&buf[pos..20])
            .unwrap_or_else(|_| HeaderValue::from_static("0"));
        *cache = (val, hv.clone());
        hv
    })
}

/// Fast 32-character hex parser (`&[u8] -> Option<u128>`).
#[inline]
fn parse_hex_128(bytes: &[u8]) -> Option<u128> {
    if bytes.len() != 32 {
        return None;
    }
    let mut acc = 0u128;
    for &b in bytes {
        let nibble = match b {
            b'0'..=b'9' => (b - b'0') as u128,
            b'a'..=b'f' => (b - b'a' + 10) as u128,
            b'A'..=b'F' => (b - b'A' + 10) as u128,
            _ => return None,
        };
        acc = (acc << 4) | nibble;
    }
    Some(acc)
}

#[inline(always)]
fn format_u128_hex_header(id: u128) -> HeaderValue {
    let mut hex = [0u8; 32];
    let hi = (id >> 64) as u64;
    let lo = id as u64;
    let (hi_out, lo_out) = hex.split_at_mut(16);
    if let (Ok(hi_arr), Ok(lo_arr)) = (hi_out.try_into(), lo_out.try_into()) {
        crate::request_id::format_hex_16(hi, hi_arr);
        crate::request_id::format_hex_16(lo, lo_arr);
    }
    HeaderValue::from_bytes(&hex)
        .unwrap_or_else(|_| HeaderValue::from_static("00000000000000000000000000000000"))
}

#[inline(always)]
fn format_ip_header(ip: u128) -> HeaderValue {
    if let Some(v4) = crate::lut::ip::is_ipv4_mapped_u128(ip) {
        let (buf, len) = crate::lut::ip::format_ipv4_bytes(v4);
        HeaderValue::from_bytes(buf.get(..len).unwrap_or(&[]))
            .unwrap_or_else(|_| HeaderValue::from_static("0.0.0.0"))
    } else {
        let (buf, len) = crate::lut::ip::format_ipv6_bytes(ip);
        HeaderValue::from_bytes(buf.get(..len).unwrap_or(&[]))
            .unwrap_or_else(|_| HeaderValue::from_static("::"))
    }
}

/// Single flat header slot stored contiguously right after the `tags` region.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeaderSlot {
    pub name: HeaderName,
    pub value: HeaderValue,
}

/// Owned raw contiguous allocation laid out as `[tags: [u64; cap] | slots: [HeaderSlot; cap]]`.
struct RawHeaderBlock {
    ptr: NonNull<u8>,
    layout: Layout,
    slots_offset: usize,
    cap: u16,
}

impl Drop for RawHeaderBlock {
    #[inline]
    fn drop(&mut self) {
        if self.cap > 0 {
            unsafe { dealloc(self.ptr.as_ptr(), self.layout) };
        }
    }
}

unsafe impl Send for RawHeaderBlock {}
unsafe impl Sync for RawHeaderBlock {}

impl RawHeaderBlock {
    fn layout_for(cap: usize) -> (Layout, usize) {
        let slot_align = std::mem::align_of::<HeaderSlot>().max(std::mem::align_of::<u64>());
        let tags_bytes = cap * std::mem::size_of::<u64>();
        let slots_offset = (tags_bytes + slot_align - 1) & !(slot_align - 1);
        let slots_bytes = cap * std::mem::size_of::<HeaderSlot>();
        let total = slots_offset + slots_bytes;
        let layout = Layout::from_size_align(total, slot_align).expect("valid header block layout");
        (layout, slots_offset)
    }

    fn alloc_raw(cap: usize) -> Self {
        let clamped_cap = cap.clamp(DEFAULT_HEADER_CAPACITY, MAX_WIRE_HEADERS);
        let (layout, slots_offset) = Self::layout_for(clamped_cap);
        // Safety: `layout.size() > 0` because `clamped_cap >= 32`.
        let ptr = unsafe { alloc(layout) };
        let non_null = NonNull::new(ptr).expect("header workspace block allocation failed");
        Self {
            ptr: non_null,
            layout,
            slots_offset,
            cap: clamped_cap as u16,
        }
    }

    #[inline(always)]
    fn acquire_pooled(requested_cap: usize) -> Self {
        let target = if requested_cap <= DEFAULT_HEADER_CAPACITY {
            DEFAULT_HEADER_CAPACITY
        } else {
            requested_cap
                .next_power_of_two()
                .clamp(DEFAULT_HEADER_CAPACITY, MAX_WIRE_HEADERS)
        };

        if target == DEFAULT_HEADER_CAPACITY
            && let Some(block) = LOCAL_HEADER_POOL
                .try_with(|cell| cell.borrow_mut().pop())
                .ok()
                .flatten()
        {
            return block;
        }
        Self::alloc_raw(target)
    }

    #[inline(always)]
    fn recycle_or_free(self) {
        if self.cap == DEFAULT_HEADER_CAPACITY as u16 {
            let _ = LOCAL_HEADER_POOL.try_with(|cell| {
                let mut pool = cell.borrow_mut();
                if pool.len() < LOCAL_POOL_MAX_BUFFERS {
                    pool.push(self);
                }
            });
        }
    }
}

/// Manages initialization and lifetime of slots `0..len` inside a [`RawHeaderBlock`].
struct FlatHeaderStorage {
    block: RawHeaderBlock,
    len: u16,
}

impl Drop for FlatHeaderStorage {
    #[inline]
    fn drop(&mut self) {
        self.clear_slots();
        // Move `block` out cleanly for LIFO local pool return without running Drop recursively.
        let dummy_layout = Layout::new::<u8>();
        let placeholder = RawHeaderBlock {
            ptr: NonNull::dangling(),
            layout: dummy_layout,
            slots_offset: 0,
            cap: 0,
        };
        let block = std::mem::replace(&mut self.block, placeholder);
        if block.cap > 0 {
            block.recycle_or_free();
        }
    }
}

impl Clone for FlatHeaderStorage {
    fn clone(&self) -> Self {
        let len = self.len as usize;
        let block = RawHeaderBlock::acquire_pooled(len.max(DEFAULT_HEADER_CAPACITY));
        unsafe {
            std::ptr::copy_nonoverlapping(
                self.block.ptr.as_ptr() as *const u64,
                block.ptr.as_ptr() as *mut u64,
                len,
            );
            let src_slots = self.slots();
            let dst_slots_ptr = block.ptr.as_ptr().add(block.slots_offset) as *mut HeaderSlot;
            for (i, slot) in src_slots.iter().enumerate() {
                std::ptr::write(dst_slots_ptr.add(i), slot.clone());
            }
        }
        Self {
            block,
            len: self.len,
        }
    }
}

impl FlatHeaderStorage {
    #[inline(always)]
    fn new(capacity: usize) -> Self {
        Self {
            block: RawHeaderBlock::acquire_pooled(capacity),
            len: 0,
        }
    }

    #[inline(always)]
    fn tags(&self) -> &[u64] {
        unsafe {
            std::slice::from_raw_parts(self.block.ptr.as_ptr() as *const u64, self.len as usize)
        }
    }

    #[inline(always)]
    fn slots(&self) -> &[HeaderSlot] {
        unsafe {
            std::slice::from_raw_parts(
                self.block.ptr.as_ptr().add(self.block.slots_offset) as *const HeaderSlot,
                self.len as usize,
            )
        }
    }

    #[inline(always)]
    fn slots_mut(&mut self) -> &mut [HeaderSlot] {
        unsafe {
            std::slice::from_raw_parts_mut(
                self.block.ptr.as_ptr().add(self.block.slots_offset) as *mut HeaderSlot,
                self.len as usize,
            )
        }
    }

    #[inline(always)]
    fn clear_slots(&mut self) {
        let n = self.len as usize;
        if n == 0 {
            return;
        }
        unsafe {
            let slots_ptr = self.block.ptr.as_ptr().add(self.block.slots_offset) as *mut HeaderSlot;
            let slice = std::ptr::slice_from_raw_parts_mut(slots_ptr, n);
            std::ptr::drop_in_place(slice);
        }
        self.len = 0;
    }

    #[inline(always)]
    fn tag_at(&self, idx: usize) -> u64 {
        self.tags().get(idx).copied().unwrap_or(0)
    }

    #[inline(always)]
    fn set_tag_at(&mut self, idx: usize, tag: u64) {
        if idx < self.block.cap as usize {
            unsafe {
                let tags_ptr = self.block.ptr.as_ptr() as *mut u64;
                std::ptr::write(tags_ptr.add(idx), tag);
            }
        }
    }

    #[inline(always)]
    fn write_entry_at(&mut self, idx: usize, tag: u64, slot: HeaderSlot) {
        if idx < self.block.cap as usize {
            unsafe {
                let tags_ptr = self.block.ptr.as_ptr() as *mut u64;
                let slots_ptr =
                    self.block.ptr.as_ptr().add(self.block.slots_offset) as *mut HeaderSlot;
                std::ptr::write(tags_ptr.add(idx), tag);
                std::ptr::write(slots_ptr.add(idx), slot);
            }
        }
    }

    /// Grows allocation on the cold path: `32 -> 64 -> 128 -> 256` (capped at `MAX_WIRE_HEADERS`).
    #[cold]
    #[inline(never)]
    fn grow_cold(&mut self) -> bool {
        let cur_cap = self.block.cap as usize;
        if cur_cap >= MAX_WIRE_HEADERS {
            return false;
        }
        let next_cap = (cur_cap * 2).min(MAX_WIRE_HEADERS);
        let new_block = RawHeaderBlock::alloc_raw(next_cap);
        let n = self.len as usize;

        unsafe {
            std::ptr::copy_nonoverlapping(
                self.block.ptr.as_ptr() as *const u64,
                new_block.ptr.as_ptr() as *mut u64,
                n,
            );
            std::ptr::copy_nonoverlapping(
                self.block.ptr.as_ptr().add(self.block.slots_offset) as *const HeaderSlot,
                new_block.ptr.as_ptr().add(new_block.slots_offset) as *mut HeaderSlot,
                n,
            );
        }

        let old_block = std::mem::replace(&mut self.block, new_block);
        old_block.recycle_or_free();
        true
    }

    #[inline(never)]
    fn slot_name_matches(slots: &[HeaderSlot], idx: usize, name_str: &str) -> bool {
        slots
            .get(idx)
            .is_some_and(|s| s.name.as_str().eq_ignore_ascii_case(name_str))
    }

    #[inline]
    fn find_first(&self, key: u64, name_str: &str) -> Option<usize> {
        let target = key & TAG_HASH_MASK;
        let tags = self.tags();
        let slots = self.slots();
        for (idx, &tag) in tags.iter().enumerate() {
            if (tag & TAG_HASH_MASK) == target && Self::slot_name_matches(slots, idx, name_str) {
                return Some(idx);
            }
        }
        None
    }

    #[inline]
    fn remove_at_shift(&mut self, idx: usize) -> Option<HeaderSlot> {
        let n = self.len as usize;
        if idx >= n {
            return None;
        }
        unsafe {
            let tags_ptr = self.block.ptr.as_ptr() as *mut u64;
            let slots_ptr = self.block.ptr.as_ptr().add(self.block.slots_offset) as *mut HeaderSlot;
            let removed = std::ptr::read(slots_ptr.add(idx));
            let tail = n - idx - 1;
            if tail > 0 {
                std::ptr::copy(tags_ptr.add(idx + 1), tags_ptr.add(idx), tail);
                std::ptr::copy(slots_ptr.add(idx + 1), slots_ptr.add(idx), tail);
            }
            self.len = (n - 1) as u16;
            Some(removed)
        }
    }

    #[inline]
    fn insert_internal(
        &mut self,
        name: HeaderName,
        value: HeaderValue,
        bypass: bool,
    ) -> Option<HeaderValue> {
        let key = header_key(name.as_str().as_bytes());
        let tag = if bypass { key | TAG_BYPASS_BIT } else { key };
        let name_str = name.as_str();

        if let Some(first_idx) = self.find_first(key, name_str) {
            self.set_tag_at(first_idx, tag);
            let old = if let Some(slot) = self.slots_mut().get_mut(first_idx) {
                Some(std::mem::replace(&mut slot.value, value))
            } else {
                None
            };

            // Remove any duplicate slots for the same header name (matching HeaderMap::insert semantics).
            let mut i = first_idx + 1;
            while i < self.len as usize {
                let cur_tag = self.tag_at(i);
                let is_dup = (cur_tag & TAG_HASH_MASK) == key
                    && self
                        .slots()
                        .get(i)
                        .is_some_and(|s| s.name.as_str() == name_str);
                if is_dup {
                    let _ = self.remove_at_shift(i);
                } else {
                    i += 1;
                }
            }
            return old;
        }

        let len = self.len as usize;
        if len >= self.block.cap as usize && !self.grow_cold() {
            return None;
        }
        self.write_entry_at(len, tag, HeaderSlot { name, value });
        self.len = (len + 1) as u16;
        None
    }

    #[inline]
    fn append_internal(&mut self, name: HeaderName, value: HeaderValue, bypass: bool) -> bool {
        let len = self.len as usize;
        if len >= self.block.cap as usize && !self.grow_cold() {
            return false;
        }
        let key = header_key(name.as_str().as_bytes());
        let tag = if bypass { key | TAG_BYPASS_BIT } else { key };
        self.write_entry_at(len, tag, HeaderSlot { name, value });
        self.len = (len + 1) as u16;
        true
    }

    #[inline]
    fn remove_by_name(&mut self, name_str: &str) -> Option<HeaderValue> {
        let key = header_key(name_str.as_bytes());
        let first_idx = self.find_first(key, name_str)?;
        let first_val = self.remove_at_shift(first_idx).map(|s| s.value);

        let mut i = first_idx;
        while i < self.len as usize {
            let cur_tag = self.tag_at(i);
            let is_dup = (cur_tag & TAG_HASH_MASK) == key
                && self
                    .slots()
                    .get(i)
                    .is_some_and(|s| s.name.as_str().eq_ignore_ascii_case(name_str));
            if is_dup {
                let _ = self.remove_at_shift(i);
            } else {
                i += 1;
            }
        }
        first_val
    }

    /// Compacts entries in place keeping only slots whose `tag & TAG_BYPASS_BIT != 0`.
    #[inline]
    fn compact_bypass_only(&mut self) {
        let n = self.len as usize;
        let mut write_idx = 0usize;
        unsafe {
            let tags_ptr = self.block.ptr.as_ptr() as *mut u64;
            let slots_ptr = self.block.ptr.as_ptr().add(self.block.slots_offset) as *mut HeaderSlot;
            for read_idx in 0..n {
                let tag = *tags_ptr.add(read_idx);
                if (tag & TAG_BYPASS_BIT) != 0 {
                    if write_idx != read_idx {
                        std::ptr::write(tags_ptr.add(write_idx), tag);
                        let moved = std::ptr::read(slots_ptr.add(read_idx));
                        std::ptr::write(slots_ptr.add(write_idx), moved);
                    }
                    write_idx += 1;
                } else {
                    std::ptr::drop_in_place(slots_ptr.add(read_idx));
                }
            }
        }
        self.len = write_idx as u16;
    }

    /// Reclaims oversized buffers (`cap > 32`) back to a fresh 32-slot block from the local pool.
    #[inline]
    fn reset_storage(&mut self) {
        self.clear_slots();
        if self.block.cap > DEFAULT_HEADER_CAPACITY as u16 {
            let new_block = RawHeaderBlock::acquire_pooled(DEFAULT_HEADER_CAPACITY);
            let old_block = std::mem::replace(&mut self.block, new_block);
            old_block.recycle_or_free();
        }
    }
}

/// Iterator over matching header values for [`HeaderWorkspace::get_all`].
pub struct HeaderValues<'a> {
    tags: &'a [u64],
    slots: &'a [HeaderSlot],
    target_key: u64,
    match_name: Option<&'a HeaderName>,
    idx: usize,
}

impl<'a> HeaderValues<'a> {
    // BACKWARD-COMPAT: Drop-in adapter for `http::header::GetAll::iter()` callers; remove once call sites iterate `get_all(...)` directly.
    /// Returns self for drop-in compatibility with `http::header::GetAll::iter()`.
    #[inline(always)]
    pub fn iter(self) -> Self {
        self
    }
}

impl<'a> Iterator for HeaderValues<'a> {
    type Item = &'a HeaderValue;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let match_name = self.match_name?;
        while self.idx < self.tags.len() {
            let i = self.idx;
            self.idx += 1;
            let tag = self.tags.get(i).copied().unwrap_or(0);
            if (tag & TAG_HASH_MASK) == self.target_key
                && let Some(slot) = self.slots.get(i)
                && slot.name == *match_name
            {
                return Some(&slot.value);
            }
        }
        None
    }
}

/// Iterator over all `(HeaderName, HeaderValue)` entries in insertion order.
pub struct HeaderIter<'a> {
    inner: std::slice::Iter<'a, HeaderSlot>,
}

impl<'a> Iterator for HeaderIter<'a> {
    type Item = (&'a HeaderName, &'a HeaderValue);

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|s| (&s.name, &s.value))
    }

    #[inline(always)]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

/// Contiguous single-buffer header table with native Bit-63 Preserve-on-Reject tagging.
#[derive(Clone)]
pub struct HeaderWorkspace {
    storage: FlatHeaderStorage,
    pub ingress_time_nanos: u64,
    /// Optional 64-bit HTTP cache key hash passed locklessly from lookup to store (`0` = unset).
    pub cache_hash: u64,
}

impl std::fmt::Debug for HeaderWorkspace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HeaderWorkspace")
            .field("len", &self.storage.len)
            .field("cap", &self.storage.block.cap)
            .field("ingress_time_nanos", &self.ingress_time_nanos)
            .field("cache_hash", &self.cache_hash)
            .field("entries", &self.storage.slots())
            .finish()
    }
}

impl PartialEq for HeaderWorkspace {
    fn eq(&self, other: &Self) -> bool {
        self.ingress_time_nanos == other.ingress_time_nanos
            && self.cache_hash == other.cache_hash
            && self.storage.slots() == other.storage.slots()
    }
}

impl Eq for HeaderWorkspace {}

impl Default for HeaderWorkspace {
    #[inline(always)]
    fn default() -> Self {
        Self::new(DEFAULT_HEADER_CAPACITY)
    }
}

impl HeaderWorkspace {
    pub const X_REQUEST_ID: HeaderName = X_REQUEST_ID;
    pub const DEFAULT_CAPACITY: usize = DEFAULT_HEADER_CAPACITY;

    /// Returns the ingress timestamp in Unix epoch seconds.
    #[inline(always)]
    pub fn ingress_time_sec(&self) -> u64 {
        self.ingress_time_nanos / 1_000_000_000
    }

    /// Creates and pre-warms a flat header workspace backed by the worker's local pool.
    #[inline]
    pub fn new(capacity: usize) -> Self {
        let mut storage = FlatHeaderStorage::new(capacity);
        let ingress_time_nanos = crate::clock::now_unix_nanos();
        let (_id, req_id) = crate::request_id::next_request_id(ingress_time_nanos);
        let _ = storage.insert_internal(X_REQUEST_ID, req_id, true);
        Self {
            storage,
            ingress_time_nanos,
            cache_hash: 0,
        }
    }

    /// Creates an empty flat header workspace backed by the worker's local pool without generating a Request ID.
    #[inline(always)]
    pub fn empty(capacity: usize) -> Self {
        Self {
            storage: FlatHeaderStorage::new(capacity),
            ingress_time_nanos: 0,
            cache_hash: 0,
        }
    }

    /// Creates a flat header workspace seeded with an explicit Request ID.
    #[inline]
    pub fn with_request_id(req_id: HeaderValue, capacity: usize) -> Self {
        let ingress_time_nanos = parse_hex_128(req_id.as_bytes())
            .filter(|&id| id != 0)
            .map(|id| (id >> 64) as u64)
            .unwrap_or_else(crate::clock::now_unix_nanos);
        let mut storage = FlatHeaderStorage::new(capacity);
        let _ = storage.insert_internal(X_REQUEST_ID, req_id, true);
        Self {
            storage,
            ingress_time_nanos,
            cache_hash: 0,
        }
    }

    /// Issues hardware Tier-2 (`T1` / L2 Cache) Prefetch hints on the flat buffer.
    #[inline(always)]
    pub fn prefetch_l2_keep(&self) {
        let ptr = self.storage.block.ptr.as_ptr();
        let tags_bytes = (self.storage.len as usize) * std::mem::size_of::<u64>();
        let slots_end = self.storage.block.slots_offset
            + (self.storage.len as usize) * std::mem::size_of::<HeaderSlot>();
        let total = slots_end.max(tags_bytes);
        #[cfg(target_arch = "x86_64")]
        {
            use std::arch::x86_64::{_MM_HINT_T1, _mm_prefetch};
            let mut offset = 0usize;
            while offset < total {
                unsafe {
                    _mm_prefetch(ptr.add(offset) as *const i8, _MM_HINT_T1);
                }
                offset += 64;
            }
        }
        #[cfg(target_arch = "aarch64")]
        {
            let mut offset = 0usize;
            while offset < total {
                unsafe {
                    std::arch::asm!(
                        "prfm pldl2keep, [{0}]",
                        in(reg) ptr.add(offset),
                        options(nostack, preserves_flags, readonly)
                    );
                }
                offset += 64;
            }
        }
        #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
        let _ = (ptr, total);
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.storage.len as usize
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.storage.len == 0
    }

    #[inline(always)]
    pub fn capacity(&self) -> usize {
        self.storage.block.cap as usize
    }

    #[inline(always)]
    pub fn clear(&mut self) {
        self.storage.clear_slots();
    }

    #[inline]
    pub fn reserve(&mut self, additional: usize) {
        let needed = (self.storage.len as usize).saturating_add(additional);
        while (self.storage.block.cap as usize) < needed
            && (self.storage.block.cap as usize) < MAX_WIRE_HEADERS
        {
            if !self.storage.grow_cold() {
                break;
            }
        }
    }

    #[inline]
    pub fn get<K: AsHeaderName>(&self, name: K) -> Option<&HeaderValue> {
        let name_str = name.as_str();
        let idx = self
            .storage
            .find_first(header_key(name_str.as_bytes()), name_str)?;
        self.storage.slots().get(idx).map(|s| &s.value)
    }

    #[inline]
    pub fn get_all<K: AsHeaderName>(&self, name: K) -> HeaderValues<'_> {
        let name_str = name.as_str();
        let target_key = header_key(name_str.as_bytes());
        match self.storage.find_first(target_key, name_str) {
            Some(first_idx) => HeaderValues {
                tags: self.storage.tags(),
                slots: self.storage.slots(),
                target_key,
                match_name: self.storage.slots().get(first_idx).map(|s| &s.name),
                idx: first_idx,
            },
            None => HeaderValues {
                tags: &[],
                slots: &[],
                target_key: 0,
                match_name: None,
                idx: 0,
            },
        }
    }

    #[inline]
    pub fn contains_key<K: AsHeaderName>(&self, name: K) -> bool {
        let name_str = name.as_str();
        self.storage
            .find_first(header_key(name_str.as_bytes()), name_str)
            .is_some()
    }

    #[inline(always)]
    pub fn insert<K: IntoHeaderKey>(&mut self, name: K, value: HeaderValue) -> Option<HeaderValue> {
        self.storage
            .insert_internal(name.into_header_name(), value, false)
    }

    #[inline(always)]
    pub fn append<K: IntoHeaderKey>(&mut self, name: K, value: HeaderValue) -> bool {
        self.storage
            .append_internal(name.into_header_name(), value, false)
    }

    #[inline]
    pub fn remove<K: AsHeaderName>(&mut self, name: K) -> Option<HeaderValue> {
        self.storage.remove_by_name(name.as_str())
    }

    #[inline]
    pub fn insert_bypass(&mut self, name: HeaderName, value: HeaderValue) -> bool {
        let _ = self.storage.insert_internal(name, value, true);
        true
    }

    /// Appends a header directly using a pre-computed 63-bit case-insensitive FNV-1a key hash.
    ///
    /// Intended for protocol wire decoders that compute name hashes alongside SIMD validation.
    #[inline(always)]
    pub fn push_prehashed(&mut self, key_hash: u64, name: HeaderName, value: HeaderValue) -> bool {
        let len = self.storage.len as usize;
        if len >= self.storage.block.cap as usize && !self.storage.grow_cold() {
            return false;
        }
        let tag = (key_hash & TAG_HASH_MASK).max(1);
        self.storage
            .write_entry_at(len, tag, HeaderSlot { name, value });
        self.storage.len = (len + 1) as u16;
        true
    }

    /// Appends a Bit-63 bypass header directly using a pre-computed 63-bit key hash without scanning.
    #[inline(always)]
    pub fn push_prehashed_bypass(
        &mut self,
        key_hash: u64,
        name: HeaderName,
        value: HeaderValue,
    ) -> bool {
        let len = self.storage.len as usize;
        if len >= self.storage.block.cap as usize && !self.storage.grow_cold() {
            return false;
        }
        let tag = (key_hash & TAG_HASH_MASK).max(1) | TAG_BYPASS_BIT;
        self.storage
            .write_entry_at(len, tag, HeaderSlot { name, value });
        self.storage.len = (len + 1) as u16;
        true
    }

    /// Overwrites the value of an already-populated slot index in place without hashing or scanning.
    #[inline(always)]
    pub fn set_value_at(&mut self, idx: usize, value: HeaderValue) {
        if let Some(slot) = self.storage.slots_mut().get_mut(idx) {
            slot.value = value;
        }
    }

    #[inline(always)]
    pub fn set_u64(&mut self, name: HeaderName, val: u64) {
        let hv = resolve_u64_header_value(val);
        self.storage.insert_internal(name, hv, false);
    }

    #[inline(always)]
    pub fn append_u64(&mut self, name: HeaderName, val: u64) {
        let hv = resolve_u64_header_value(val);
        self.storage.append_internal(name, hv, false);
    }

    #[inline]
    pub fn insert_bypass_u64(&mut self, name: HeaderName, val: u64) -> bool {
        let hv = resolve_u64_header_value(val);
        self.storage.insert_internal(name, hv, true);
        true
    }

    #[inline(always)]
    pub fn set_hex128(&mut self, name: HeaderName, id: u128) {
        self.storage
            .insert_internal(name, format_u128_hex_header(id), false);
    }

    #[inline(always)]
    pub fn append_hex128(&mut self, name: HeaderName, id: u128) {
        self.storage
            .append_internal(name, format_u128_hex_header(id), false);
    }

    #[inline(always)]
    pub fn set_ip(&mut self, name: HeaderName, ip: u128) {
        self.set_ip_u128(name, ip);
    }

    #[inline(always)]
    pub fn append_ip(&mut self, name: HeaderName, ip: u128) {
        self.append_ip_u128(name, ip);
    }

    #[inline(always)]
    pub fn set_ip_u128(&mut self, name: HeaderName, ip: u128) {
        self.storage
            .insert_internal(name, format_ip_header(ip), false);
    }

    #[inline(always)]
    pub fn append_ip_u128(&mut self, name: HeaderName, ip: u128) {
        self.storage
            .append_internal(name, format_ip_header(ip), false);
    }

    #[inline(always)]
    pub fn set_inline_ascii(&mut self, name: HeaderName, bytes: &[u8]) {
        let slice = bytes.get(..bytes.len().min(16)).unwrap_or(bytes);
        if let Ok(hv) = HeaderValue::from_bytes(slice) {
            self.storage.insert_internal(name, hv, false);
        }
    }

    #[inline(always)]
    pub fn append_inline_ascii(&mut self, name: HeaderName, bytes: &[u8]) {
        let slice = bytes.get(..bytes.len().min(16)).unwrap_or(bytes);
        if let Ok(hv) = HeaderValue::from_bytes(slice) {
            self.storage.append_internal(name, hv, false);
        }
    }

    /// Parses a header as an unsigned 64-bit integer from the flat slot slice.
    #[inline]
    pub fn get_u64<K: AsHeaderName>(&self, name: K) -> Option<u64> {
        let bytes = self.get(name)?.as_bytes();
        if bytes.is_empty() || bytes.len() > 20 {
            return None;
        }
        let mut acc = 0u64;
        for &b in bytes {
            let d = b.wrapping_sub(b'0');
            if d > 9 {
                return None;
            }
            acc = acc.checked_mul(10)?.checked_add(d as u64)?;
        }
        Some(acc)
    }

    /// Reads a header value parsed as a 128-bit hex identifier.
    #[inline]
    pub fn get_hex128<K: AsHeaderName>(&self, name: K) -> Option<u128> {
        parse_hex_128(self.get(name)?.as_bytes())
    }

    /// Parses a header value as an RFC 4291 `u128` IP integer.
    #[inline]
    pub fn get_ip_u128<K: AsHeaderName>(&self, name: K) -> Option<u128> {
        let bytes = self.get(name)?.as_bytes();
        let first = match bytes.iter().position(|&b| b == b',') {
            Some(idx) => bytes.get(..idx).unwrap_or(bytes),
            None => bytes,
        };
        crate::lut::ip::parse_ip_u128(first.trim_ascii())
    }

    #[inline(always)]
    pub fn iter(&self) -> HeaderIter<'_> {
        HeaderIter {
            inner: self.storage.slots().iter(),
        }
    }

    #[inline(always)]
    pub fn keys(&self) -> impl Iterator<Item = &HeaderName> {
        self.storage.slots().iter().map(|s| &s.name)
    }

    /// Iterates over all entries marked with Bit-63 (`TAG_BYPASS_BIT`).
    #[inline]
    pub fn bypass_headers(&self) -> impl Iterator<Item = (&HeaderName, &HeaderValue)> {
        let tags = self.storage.tags();
        let slots = self.storage.slots();
        tags.iter().enumerate().filter_map(move |(i, &tag)| {
            if (tag & TAG_BYPASS_BIT) != 0 {
                slots.get(i).map(|s| (&s.name, &s.value))
            } else {
                None
            }
        })
    }

    #[inline(always)]
    pub fn request_id(&self) -> &HeaderValue {
        if let Some(val) = self.get(&X_REQUEST_ID) {
            val
        } else {
            static FALLBACK_ID: HeaderValue =
                HeaderValue::from_static("0000000000000000-0000000000000000");
            &FALLBACK_ID
        }
    }

    #[inline]
    pub fn set_request_id(&mut self, req_id: HeaderValue) {
        let _ = self.storage.insert_internal(X_REQUEST_ID, req_id, true);
    }

    /// In-place single-pass compaction preserving Bit-63 bypass headers on early rejection.
    pub fn prepare_for_rejection(
        &mut self,
        meta: &'static crate::lut::RejectStaticMeta,
        include_content_headers: bool,
    ) {
        self.storage.compact_bypass_only();

        self.insert(
            http::header::CACHE_CONTROL,
            HeaderValue::from_static("no-store"),
        );

        if include_content_headers {
            self.insert(
                http::header::CONTENT_TYPE,
                HeaderValue::from_static("text/plain; charset=utf-8"),
            );
            self.insert(
                http::header::CONTENT_LENGTH,
                resolve_u64_header_value(meta.body.len() as u64),
            );
        }

        const KEY_RETRY_AFTER: u64 = header_key(b"retry-after");
        if meta.status == http::StatusCode::TOO_MANY_REQUESTS
            && self
                .storage
                .find_first(KEY_RETRY_AFTER, "retry-after")
                .is_none()
        {
            self.insert(http::header::RETRY_AFTER, HeaderValue::from_static("1"));
        }
    }

    /// In-place compaction preserving Bit-63 bypass headers for gRPC Trailers-Only rejection.
    pub fn prepare_for_grpc_rejection(
        &mut self,
        status: crate::lut::GrpcStatus,
        message: Option<&str>,
    ) {
        self.storage.compact_bypass_only();

        self.insert(
            http::header::CONTENT_TYPE,
            HeaderValue::from_static("application/grpc"),
        );
        self.insert(
            HeaderName::from_static("grpc-status"),
            HeaderValue::from_static(status.code_str()),
        );
        if let Some(msg) = message
            && let Ok(val) = HeaderValue::from_str(msg)
        {
            self.insert(HeaderName::from_static("grpc-message"), val);
        }
    }

    /// Resets workspace for reuse, returning oversized spike buffers to a clean 32-slot block.
    #[inline(always)]
    pub fn reset(&mut self) {
        self.storage.reset_storage();
        let ingress_nanos = crate::clock::now_unix_nanos();
        let (_id, req_id) = crate::request_id::next_request_id(ingress_nanos);
        self.ingress_time_nanos = ingress_nanos;
        self.cache_hash = 0;
        let _ = self.storage.insert_internal(X_REQUEST_ID, req_id, true);
    }
}

/// Zero-allocation header key conversion for [`HeaderWorkspace::insert`] and [`HeaderWorkspace::append`].
pub trait IntoHeaderKey {
    /// Converts the key into an owned [`HeaderName`] without allocating a temporary `HeaderMap`.
    fn into_header_name(self) -> HeaderName;
}

impl IntoHeaderKey for HeaderName {
    #[inline(always)]
    fn into_header_name(self) -> HeaderName {
        self
    }
}

impl IntoHeaderKey for &HeaderName {
    #[inline(always)]
    fn into_header_name(self) -> HeaderName {
        self.clone()
    }
}

impl IntoHeaderKey for &str {
    #[inline(always)]
    fn into_header_name(self) -> HeaderName {
        HeaderName::from_bytes(self.as_bytes()).unwrap_or(X_REQUEST_ID)
    }
}

impl FromIterator<(HeaderName, HeaderValue)> for HeaderWorkspace {
    fn from_iter<T: IntoIterator<Item = (HeaderName, HeaderValue)>>(iter: T) -> Self {
        let mut ws = Self::empty(DEFAULT_HEADER_CAPACITY);
        for (name, val) in iter {
            if name == X_REQUEST_ID {
                ws.insert_bypass(name, val);
            } else {
                ws.append(name, val);
            }
        }
        ws
    }
}

impl<'a> IntoIterator for &'a HeaderWorkspace {
    type Item = (&'a HeaderName, &'a HeaderValue);
    type IntoIter = HeaderIter<'a>;

    #[inline(always)]
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}
