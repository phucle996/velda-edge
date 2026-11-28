//! Contiguous Single-Allocation Header Workspace (`velda_core::buffer::header`).
//!
//! Replaces `http::HeaderMap` and nested block wrappers with a single flat contiguous
//! `[tags: [u8; cap] | slots: [HeaderSlot; cap]]` buffer recycled per worker via a
//! lock-free `thread_local!` LIFO pool:
//!
//! ```text
//!   HeaderWorkspace { ptr, layout, slots_offset, len, cap, ingress_time_nanos }
//!     └─► Contiguous allocation (default cap = 32, exponential cold grow 32->64->128->256):
//!           [ tag_0 .. tag_{cap-1} : u8 | padding | slot_0 .. slot_{cap-1} : HeaderSlot ]
//!               └─ bit 7 (0x80): TAG_BYTE_BYPASS_BIT (0 = Discard, 1 = Preserve-on-Reject)
//!               └─ bits 0..6   : non-zero 7-bit folded case-insensitive name hash (1..=127)
//!
//!   Hot paths:
//!     ├─ insert / append ──► check len < cap -> write u8 tag + slot (`#[cold] grow_cold` x2)
//!     ├─ get / contains  ──► SIMD u8 tag scan (`AVX2 32B` / `SSE2 16B` / `NEON 16B` / `SWAR 8B`)
//!     │                        └─► mask tail to `len` -> exact `HeaderName` verify on tag hit
//!     ├─ retain_bypass_only   ──► 1-pass in-place compact keeping `(tag & 0x80) != 0`
//!     └─ Drop / reset    ──► drop slots only (zero memset) -> return cap == 32 to LIFO pool
//! ```

use std::alloc::{Layout, alloc, dealloc};
use std::cell::RefCell;
use std::ptr::NonNull;

use http::header::{AsHeaderName, HeaderName, HeaderValue};

/// Canonical header name for the W3C TraceContext Request Identifier.
pub const X_REQUEST_ID: HeaderName = HeaderName::from_static("x-request-id");

/// Default initial capacity for each header workspace (32 slots -> 32B u8 tags + contiguous slots).
pub const DEFAULT_HEADER_CAPACITY: usize = 32;

/// Hard ceiling on total headers per request/response (Anti-DoS, `0..=256`).
pub const MAX_WIRE_HEADERS: usize = 256;

/// Public Bit-63 constant preserving legacy `u64` tag polarity (`1 = Preserve-on-Reject`).
pub const TAG_BYPASS_BIT: u64 = 1u64 << 63;

/// Lower 63 bits mask for 64-bit precomputed header keys.
const TAG_HASH_MASK: u64 = (1u64 << 63) - 1;

/// Compact 1-byte tag MSB (`bit 7`): `1` = Preserve-on-Reject (Bypass), `0` = Discard-on-Reject.
pub const TAG_BYTE_BYPASS_BIT: u8 = 1u8 << 7;

/// Lower 7 bits of each `u8` tag storing the non-zero folded header name fingerprint (`1..=127`).
const TAG_BYTE_HASH_MASK: u8 = 0x7F;

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

/// Folds a 64-bit header name hash into a non-zero 7-bit SIMD tag fingerprint (`1..=127`).
#[inline(always)]
pub const fn fold_tag_hash7(hash64: u64) -> u8 {
    let folded =
        ((hash64 ^ (hash64 >> 8) ^ (hash64 >> 16) ^ (hash64 >> 24)) as u8) & TAG_BYTE_HASH_MASK;
    if folded == 0 { 1 } else { folded }
}

/// Pre-hashed lookup key pairing a compile-time `tag7` fingerprint with an owned [`HeaderName`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HeaderKey {
    pub hash64: u64,
    pub tag7: u8,
    pub name: HeaderName,
}

impl HeaderKey {
    /// Builds a compile-time constant [`HeaderKey`] with zero runtime hashing.
    ///
    /// Note: `lower_ascii` must be a valid lowercase HTTP header name (enforced by [`HeaderName::from_static`]).
    #[inline(always)]
    pub const fn from_static(lower_ascii: &'static str) -> Self {
        let hash64 = header_key(lower_ascii.as_bytes());
        Self {
            hash64,
            tag7: fold_tag_hash7(hash64),
            name: HeaderName::from_static(lower_ascii),
        }
    }

    /// Computes a [`HeaderKey`] at runtime from a [`HeaderName`].
    #[inline(always)]
    pub fn from_name(name: HeaderName) -> Self {
        let hash64 = header_key(name.as_str().as_bytes());
        Self {
            hash64,
            tag7: fold_tag_hash7(hash64),
            name,
        }
    }
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

    static LOCAL_HEADER_POOL: RefCell<Vec<PooledHeaderBlock>> = const {
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

/// Owned raw contiguous allocation cached in the worker-local pool (`[tags: [u8; cap] | slots: [HeaderSlot; cap]]`).
struct PooledHeaderBlock {
    ptr: NonNull<u8>,
    layout: Layout,
    slots_offset: usize,
    cap: u16,
}

impl Drop for PooledHeaderBlock {
    #[inline]
    fn drop(&mut self) {
        if self.cap > 0 {
            unsafe { dealloc(self.ptr.as_ptr(), self.layout) };
        }
    }
}

unsafe impl Send for PooledHeaderBlock {}
unsafe impl Sync for PooledHeaderBlock {}

impl PooledHeaderBlock {
    #[inline(always)]
    fn layout_for(cap: usize) -> (Layout, usize) {
        let slot_align = std::mem::align_of::<HeaderSlot>().max(std::mem::align_of::<u64>());
        let tags_bytes = cap;
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
        // Zero-initialize `tags[0..clamped_cap]` once at allocation so SIMD chunk reads across
        // `[0..cap]` never observe uninitialized bytes.
        unsafe { std::ptr::write_bytes(non_null.as_ptr(), 0, clamped_cap) };
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

/// Iterator over matching header values for [`HeaderWorkspace::get_all`] and [`HeaderWorkspace::get_all_prehashed`].
pub struct HeaderValues<'a> {
    tags: &'a [u8],
    slots: &'a [HeaderSlot],
    target_tag7: u8,
    match_name: Option<&'a HeaderName>,
    idx: usize,
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
            if (tag & TAG_BYTE_HASH_MASK) == self.target_tag7
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

/// Portable 64-bit SWAR scanner (8 `u8` tags per register) used on non-SIMD targets and verified in tests.
#[cfg(any(
    test,
    not(any(
        all(
            target_arch = "x86_64",
            any(target_feature = "avx2", target_feature = "sse2")
        ),
        target_arch = "aarch64"
    ))
))]
#[inline(always)]
pub(crate) fn find_first_tag7_swar(
    tags_ptr: *const u8,
    len: usize,
    slots: &[HeaderSlot],
    target7: u8,
    mut eq: impl FnMut(&HeaderName) -> bool,
) -> Option<usize> {
    const LO_ONE: u64 = 0x0101_0101_0101_0101;
    const HI_BIT: u64 = 0x8080_8080_8080_8080;
    const HASH_MASK_8: u64 = 0x7F7F_7F7F_7F7F_7F7F;

    let target_splat = (target7 as u64).wrapping_mul(LO_ONE);
    let mut base = 0usize;
    while base < len {
        // Safety: `cap` is clamped to powers of two in `32..=256` (multiples of 8), so `base + 8 <= cap`.
        let word =
            u64::from_le(unsafe { std::ptr::read_unaligned(tags_ptr.add(base) as *const u64) });
        let x = (word & HASH_MASK_8) ^ target_splat;
        // Zero-false-positive 7-bit byte match: for `0 <= x <= 127`, `x + 0x7F` never carries
        // across lanes and has bit 7 == 0 iff `x == 0`.
        let mut match_mask = !(x.wrapping_add(HASH_MASK_8) | x) & HI_BIT;
        let rem = len - base;
        if rem < 8 {
            match_mask &= (1u64 << (rem * 8)) - 1;
        }
        while match_mask != 0 {
            let local_idx = (match_mask.trailing_zeros() as usize) >> 3;
            let idx = base + local_idx;
            if let Some(slot) = slots.get(idx)
                && eq(&slot.name)
            {
                return Some(idx);
            }
            match_mask &= match_mask - 1;
        }
        base += 8;
    }
    None
}

/// Unified contiguous single-buffer header table (`[u8 tags | HeaderSlot slots]`) with SIMD lookup
/// and MSB (`bit 7`) Preserve-on-Reject tagging.
pub struct HeaderWorkspace {
    ptr: NonNull<u8>,
    layout: Layout,
    slots_offset: usize,
    len: u16,
    cap: u16,
    pub ingress_time_nanos: u64,
}

unsafe impl Send for HeaderWorkspace {}
unsafe impl Sync for HeaderWorkspace {}

impl Drop for HeaderWorkspace {
    #[inline]
    fn drop(&mut self) {
        self.clear();
        if self.cap == 0 {
            return;
        }
        let block = PooledHeaderBlock {
            ptr: self.ptr,
            layout: self.layout,
            slots_offset: self.slots_offset,
            cap: self.cap,
        };
        // Mark empty before moving the allocation to the pool or freeing it so `Drop` never double-frees.
        self.cap = 0;
        block.recycle_or_free();
    }
}

impl Clone for HeaderWorkspace {
    fn clone(&self) -> Self {
        let len = self.len as usize;
        let mut block = PooledHeaderBlock::acquire_pooled(len.max(DEFAULT_HEADER_CAPACITY));
        unsafe {
            std::ptr::copy_nonoverlapping(self.ptr.as_ptr(), block.ptr.as_ptr(), len);
            let src_slots = self.slots();
            let dst_slots_ptr = block.ptr.as_ptr().add(block.slots_offset) as *mut HeaderSlot;
            for (i, slot) in src_slots.iter().enumerate() {
                std::ptr::write(dst_slots_ptr.add(i), slot.clone());
            }
        }
        let cloned = Self {
            ptr: block.ptr,
            layout: block.layout,
            slots_offset: block.slots_offset,
            len: self.len,
            cap: block.cap,
            ingress_time_nanos: self.ingress_time_nanos,
        };
        block.cap = 0;
        cloned
    }
}

impl std::fmt::Debug for HeaderWorkspace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HeaderWorkspace")
            .field("len", &self.len)
            .field("cap", &self.cap)
            .field("ingress_time_nanos", &self.ingress_time_nanos)
            .field("entries", &self.slots())
            .finish()
    }
}

impl PartialEq for HeaderWorkspace {
    fn eq(&self, other: &Self) -> bool {
        self.ingress_time_nanos == other.ingress_time_nanos && self.slots() == other.slots()
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

    #[inline(always)]
    fn from_block(mut block: PooledHeaderBlock, ingress_time_nanos: u64) -> Self {
        let ws = Self {
            ptr: block.ptr,
            layout: block.layout,
            slots_offset: block.slots_offset,
            len: 0,
            cap: block.cap,
            ingress_time_nanos,
        };
        block.cap = 0;
        ws
    }

    #[inline(always)]
    fn tags(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len as usize) }
    }

    #[inline(always)]
    fn slots(&self) -> &[HeaderSlot] {
        unsafe {
            std::slice::from_raw_parts(
                self.ptr.as_ptr().add(self.slots_offset) as *const HeaderSlot,
                self.len as usize,
            )
        }
    }

    #[inline(always)]
    fn slots_mut(&mut self) -> &mut [HeaderSlot] {
        unsafe {
            std::slice::from_raw_parts_mut(
                self.ptr.as_ptr().add(self.slots_offset) as *mut HeaderSlot,
                self.len as usize,
            )
        }
    }

    #[inline(always)]
    fn tag_at(&self, idx: usize) -> u8 {
        self.tags().get(idx).copied().unwrap_or(0)
    }

    #[inline(always)]
    fn set_tag_at(&mut self, idx: usize, tag: u8) {
        if idx < self.cap as usize {
            unsafe {
                std::ptr::write(self.ptr.as_ptr().add(idx), tag);
            }
        }
    }

    #[inline(always)]
    fn write_entry_at(&mut self, idx: usize, tag: u8, slot: HeaderSlot) {
        if idx < self.cap as usize {
            unsafe {
                let slots_ptr = self.ptr.as_ptr().add(self.slots_offset) as *mut HeaderSlot;
                std::ptr::write(self.ptr.as_ptr().add(idx), tag);
                std::ptr::write(slots_ptr.add(idx), slot);
            }
        }
    }

    /// Grows allocation on the cold path: `32 -> 64 -> 128 -> 256` (capped at `MAX_WIRE_HEADERS`).
    #[cold]
    #[inline(never)]
    fn grow_cold(&mut self) -> bool {
        let cur_cap = self.cap as usize;
        if cur_cap >= MAX_WIRE_HEADERS {
            return false;
        }
        let next_cap = (cur_cap * 2).min(MAX_WIRE_HEADERS);
        let mut new_block = PooledHeaderBlock::alloc_raw(next_cap);
        let n = self.len as usize;

        unsafe {
            std::ptr::copy_nonoverlapping(self.ptr.as_ptr(), new_block.ptr.as_ptr(), n);
            std::ptr::copy_nonoverlapping(
                self.ptr.as_ptr().add(self.slots_offset) as *const HeaderSlot,
                new_block.ptr.as_ptr().add(new_block.slots_offset) as *mut HeaderSlot,
                n,
            );
        }

        let old_block = PooledHeaderBlock {
            ptr: self.ptr,
            layout: self.layout,
            slots_offset: self.slots_offset,
            cap: self.cap,
        };
        self.ptr = new_block.ptr;
        self.layout = new_block.layout;
        self.slots_offset = new_block.slots_offset;
        self.cap = new_block.cap;
        new_block.cap = 0;
        old_block.recycle_or_free();
        true
    }

    #[inline(always)]
    fn find_first_tag7(
        &self,
        target7: u8,
        mut eq: impl FnMut(&HeaderName) -> bool,
    ) -> Option<usize> {
        let len = self.len as usize;
        if len == 0 {
            return None;
        }
        let tags_ptr = self.ptr.as_ptr();
        let slots = self.slots();

        #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
        unsafe {
            use std::arch::x86_64::{
                __m256i, _mm256_and_si256, _mm256_cmpeq_epi8, _mm256_loadu_si256,
                _mm256_movemask_epi8, _mm256_set1_epi8,
            };
            let target_vec = _mm256_set1_epi8(target7 as i8);
            let mask_vec = _mm256_set1_epi8(TAG_BYTE_HASH_MASK as i8);
            let mut base = 0usize;
            // Safety: `cap` is in `{32, 64, 128, 256}` (multiples of 32) and `tags[0..cap]` was
            // zero-initialized at allocation, so 32-byte loads starting at `base < len` stay in bounds.
            while base < len {
                let v = _mm256_loadu_si256(tags_ptr.add(base) as *const __m256i);
                let masked = _mm256_and_si256(v, mask_vec);
                let cmp = _mm256_cmpeq_epi8(masked, target_vec);
                let mut m = _mm256_movemask_epi8(cmp) as u32;
                let rem = len - base;
                if rem < 32 {
                    m &= (1u32 << rem) - 1;
                }
                while m != 0 {
                    let idx = base + m.trailing_zeros() as usize;
                    if eq(&slots.get_unchecked(idx).name) {
                        return Some(idx);
                    }
                    m &= m - 1;
                }
                base += 32;
            }
            None
        }

        #[cfg(all(
            target_arch = "x86_64",
            not(target_feature = "avx2"),
            target_feature = "sse2"
        ))]
        unsafe {
            use std::arch::x86_64::{
                __m128i, _mm_and_si128, _mm_cmpeq_epi8, _mm_loadu_si128, _mm_movemask_epi8,
                _mm_set1_epi8,
            };
            let target_vec = _mm_set1_epi8(target7 as i8);
            let mask_vec = _mm_set1_epi8(TAG_BYTE_HASH_MASK as i8);
            let mut base = 0usize;
            while base < len {
                let v = _mm_loadu_si128(tags_ptr.add(base) as *const __m128i);
                let masked = _mm_and_si128(v, mask_vec);
                let cmp = _mm_cmpeq_epi8(masked, target_vec);
                let mut m = _mm_movemask_epi8(cmp) as u32;
                let rem = len - base;
                if rem < 16 {
                    m &= (1u32 << rem) - 1;
                }
                while m != 0 {
                    let idx = base + m.trailing_zeros() as usize;
                    if eq(&slots.get_unchecked(idx).name) {
                        return Some(idx);
                    }
                    m &= m - 1;
                }
                base += 16;
            }
            None
        }

        #[cfg(target_arch = "aarch64")]
        unsafe {
            use std::arch::aarch64::{
                uint8x16_t, vaddv_u8, vandq_u8, vceqq_u8, vdupq_n_u8, vget_high_u8, vget_low_u8,
                vld1q_u8,
            };
            const BIT_WEIGHTS: [u8; 16] = [
                0x01, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40, 0x80, 0x01, 0x02, 0x04, 0x08, 0x10, 0x20,
                0x40, 0x80,
            ];
            let target_vec = vdupq_n_u8(target7);
            let mask_vec = vdupq_n_u8(TAG_BYTE_HASH_MASK);
            let weights: uint8x16_t = vld1q_u8(BIT_WEIGHTS.as_ptr());
            let mut base = 0usize;
            while base < len {
                let v = vld1q_u8(tags_ptr.add(base));
                let masked = vandq_u8(v, mask_vec);
                let cmp = vceqq_u8(masked, target_vec);
                let bits = vandq_u8(cmp, weights);
                let mut m: u32 = (vaddv_u8(vget_low_u8(bits)) as u32)
                    | ((vaddv_u8(vget_high_u8(bits)) as u32) << 8);
                let rem = len - base;
                if rem < 16 {
                    m &= (1u32 << rem) - 1;
                }
                while m != 0 {
                    let idx = base + m.trailing_zeros() as usize;
                    if eq(&slots.get_unchecked(idx).name) {
                        return Some(idx);
                    }
                    m &= m - 1;
                }
                base += 16;
            }
            None
        }

        #[cfg(not(any(
            all(
                target_arch = "x86_64",
                any(target_feature = "avx2", target_feature = "sse2")
            ),
            target_arch = "aarch64"
        )))]
        {
            find_first_tag7_swar(tags_ptr, len, slots, target7, eq)
        }
    }

    #[inline(always)]
    fn find_first(&self, target7: u8, name_str: &str) -> Option<usize> {
        self.find_first_tag7(target7, |slot_name| {
            slot_name.as_str().eq_ignore_ascii_case(name_str)
        })
    }

    #[inline(always)]
    fn find_first_name(&self, target7: u8, target: &HeaderName) -> Option<usize> {
        self.find_first_tag7(target7, |slot_name| slot_name == target)
    }

    #[inline]
    fn remove_at_shift(&mut self, idx: usize) -> Option<HeaderSlot> {
        let n = self.len as usize;
        if idx >= n {
            return None;
        }
        unsafe {
            let tags_ptr = self.ptr.as_ptr();
            let slots_ptr = self.ptr.as_ptr().add(self.slots_offset) as *mut HeaderSlot;
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
    fn insert_internal_tag7(
        &mut self,
        tag7: u8,
        name: HeaderName,
        value: HeaderValue,
        bypass: bool,
    ) -> Option<HeaderValue> {
        let tag = if bypass {
            tag7 | TAG_BYTE_BYPASS_BIT
        } else {
            tag7
        };

        if let Some(first_idx) = self.find_first_name(tag7, &name) {
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
                let is_dup = (cur_tag & TAG_BYTE_HASH_MASK) == tag7
                    && self.slots().get(i).is_some_and(|s| s.name == name);
                if is_dup {
                    let _ = self.remove_at_shift(i);
                } else {
                    i += 1;
                }
            }
            return old;
        }

        let len = self.len as usize;
        if len >= self.cap as usize && !self.grow_cold() {
            return None;
        }
        self.write_entry_at(len, tag, HeaderSlot { name, value });
        self.len = (len + 1) as u16;
        None
    }

    #[inline]
    fn append_internal(&mut self, name: HeaderName, value: HeaderValue, bypass: bool) -> bool {
        let len = self.len as usize;
        if len >= self.cap as usize && !self.grow_cold() {
            return false;
        }
        let tag7 = fold_tag_hash7(header_key(name.as_str().as_bytes()));
        let tag = if bypass {
            tag7 | TAG_BYTE_BYPASS_BIT
        } else {
            tag7
        };
        self.write_entry_at(len, tag, HeaderSlot { name, value });
        self.len = (len + 1) as u16;
        true
    }

    #[inline]
    fn remove_by_name(&mut self, name_str: &str) -> Option<HeaderValue> {
        let tag7 = fold_tag_hash7(header_key(name_str.as_bytes()));
        let first_idx = self.find_first(tag7, name_str)?;
        let first_val = self.remove_at_shift(first_idx).map(|s| s.value);

        let mut i = first_idx;
        while i < self.len as usize {
            let cur_tag = self.tag_at(i);
            let is_dup = (cur_tag & TAG_BYTE_HASH_MASK) == tag7
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

    /// Returns the ingress timestamp in Unix epoch seconds.
    #[inline(always)]
    pub fn ingress_time_sec(&self) -> u64 {
        self.ingress_time_nanos / 1_000_000_000
    }

    /// Creates and pre-warms a flat header workspace backed by the worker's local pool.
    #[inline]
    pub fn new(capacity: usize) -> Self {
        let ingress_time_nanos = crate::clock::now_unix_nanos();
        let (_id, req_id) = crate::request_id::next_request_id(ingress_time_nanos);
        let mut ws = Self::from_block(
            PooledHeaderBlock::acquire_pooled(capacity),
            ingress_time_nanos,
        );
        const TAG7_REQ_ID: u8 = fold_tag_hash7(header_key(b"x-request-id"));
        let _ = ws.insert_internal_tag7(TAG7_REQ_ID, X_REQUEST_ID, req_id, true);
        ws
    }

    /// Creates an empty flat header workspace backed by the worker's local pool without generating a Request ID.
    #[inline(always)]
    pub fn empty(capacity: usize) -> Self {
        Self::from_block(PooledHeaderBlock::acquire_pooled(capacity), 0)
    }

    /// Creates a flat header workspace seeded with an explicit Request ID.
    #[inline]
    pub fn with_request_id(req_id: HeaderValue, capacity: usize) -> Self {
        let ingress_time_nanos = parse_hex_128(req_id.as_bytes())
            .filter(|&id| id != 0)
            .map(|id| (id >> 64) as u64)
            .unwrap_or_else(crate::clock::now_unix_nanos);
        let mut ws = Self::from_block(
            PooledHeaderBlock::acquire_pooled(capacity),
            ingress_time_nanos,
        );
        const TAG7_REQ_ID: u8 = fold_tag_hash7(header_key(b"x-request-id"));
        let _ = ws.insert_internal_tag7(TAG7_REQ_ID, X_REQUEST_ID, req_id, true);
        ws
    }

    /// Issues hardware Tier-2 (`T1` / L2 Cache) Prefetch hints on the flat buffer.
    #[inline(always)]
    pub fn prefetch_l2_keep(&self) {
        let ptr = self.ptr.as_ptr();
        let tags_bytes = self.len as usize;
        let slots_end = self.slots_offset + (self.len as usize) * std::mem::size_of::<HeaderSlot>();
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
        self.len as usize
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline(always)]
    pub fn capacity(&self) -> usize {
        self.cap as usize
    }

    #[inline(always)]
    pub fn clear(&mut self) {
        let n = self.len as usize;
        if n == 0 {
            return;
        }
        unsafe {
            let slots_ptr = self.ptr.as_ptr().add(self.slots_offset) as *mut HeaderSlot;
            let slice = std::ptr::slice_from_raw_parts_mut(slots_ptr, n);
            std::ptr::drop_in_place(slice);
        }
        self.len = 0;
    }

    #[inline]
    pub fn reserve(&mut self, additional: usize) {
        let needed = (self.len as usize).saturating_add(additional);
        while (self.cap as usize) < needed && (self.cap as usize) < MAX_WIRE_HEADERS {
            if !self.grow_cold() {
                break;
            }
        }
    }

    #[inline]
    pub fn get<K: AsHeaderName>(&self, name: K) -> Option<&HeaderValue> {
        let name_str = name.as_str();
        let tag7 = fold_tag_hash7(header_key(name_str.as_bytes()));
        let idx = self.find_first(tag7, name_str)?;
        self.slots().get(idx).map(|s| &s.value)
    }

    /// Looks up the first header value using a pre-hashed [`HeaderKey`] (zero runtime hash).
    #[inline(always)]
    pub fn get_prehashed(&self, key: &HeaderKey) -> Option<&HeaderValue> {
        let idx = self.find_first_name(key.tag7, &key.name)?;
        self.slots().get(idx).map(|s| &s.value)
    }

    #[inline]
    pub fn get_all<K: AsHeaderName>(&self, name: K) -> HeaderValues<'_> {
        let name_str = name.as_str();
        let target_tag7 = fold_tag_hash7(header_key(name_str.as_bytes()));
        match self.find_first(target_tag7, name_str) {
            Some(first_idx) => HeaderValues {
                tags: self.tags(),
                slots: self.slots(),
                target_tag7,
                match_name: self.slots().get(first_idx).map(|s| &s.name),
                idx: first_idx,
            },
            None => HeaderValues {
                tags: &[],
                slots: &[],
                target_tag7: 0,
                match_name: None,
                idx: 0,
            },
        }
    }

    /// Iterates all matching values using a pre-hashed [`HeaderKey`] (zero runtime hash).
    #[inline(always)]
    pub fn get_all_prehashed(&self, key: &HeaderKey) -> HeaderValues<'_> {
        match self.find_first_name(key.tag7, &key.name) {
            Some(first_idx) => HeaderValues {
                tags: self.tags(),
                slots: self.slots(),
                target_tag7: key.tag7,
                match_name: self.slots().get(first_idx).map(|s| &s.name),
                idx: first_idx,
            },
            None => HeaderValues {
                tags: &[],
                slots: &[],
                target_tag7: 0,
                match_name: None,
                idx: 0,
            },
        }
    }

    #[inline]
    pub fn contains_key<K: AsHeaderName>(&self, name: K) -> bool {
        let name_str = name.as_str();
        let tag7 = fold_tag_hash7(header_key(name_str.as_bytes()));
        self.find_first(tag7, name_str).is_some()
    }

    /// Checks header existence using a pre-hashed [`HeaderKey`] (zero runtime hash).
    #[inline(always)]
    pub fn contains_prehashed(&self, key: &HeaderKey) -> bool {
        self.find_first_name(key.tag7, &key.name).is_some()
    }

    #[inline(always)]
    pub fn insert<K: IntoHeaderKey>(&mut self, name: K, value: HeaderValue) -> Option<HeaderValue> {
        let hname = name.into_header_name();
        let tag7 = fold_tag_hash7(header_key(hname.as_str().as_bytes()));
        self.insert_internal_tag7(tag7, hname, value, false)
    }

    /// Inserts or overwrites a header using a pre-hashed [`HeaderKey`] (zero runtime hash).
    #[inline(always)]
    pub fn insert_prehashed(&mut self, key: &HeaderKey, value: HeaderValue) -> Option<HeaderValue> {
        self.insert_internal_tag7(key.tag7, key.name.clone(), value, false)
    }

    #[inline(always)]
    pub fn append<K: IntoHeaderKey>(&mut self, name: K, value: HeaderValue) -> bool {
        self.append_internal(name.into_header_name(), value, false)
    }

    #[inline]
    pub fn remove<K: AsHeaderName>(&mut self, name: K) -> Option<HeaderValue> {
        self.remove_by_name(name.as_str())
    }

    #[inline]
    pub fn insert_bypass(&mut self, name: HeaderName, value: HeaderValue) -> bool {
        let tag7 = fold_tag_hash7(header_key(name.as_str().as_bytes()));
        let _ = self.insert_internal_tag7(tag7, name, value, true);
        true
    }

    /// Appends a header directly using a pre-computed 63-bit case-insensitive FNV-1a key hash.
    ///
    /// Intended for protocol wire decoders that compute name hashes alongside SIMD validation.
    #[inline(always)]
    pub fn push_prehashed(&mut self, key_hash: u64, name: HeaderName, value: HeaderValue) -> bool {
        let len = self.len as usize;
        if len >= self.cap as usize && !self.grow_cold() {
            return false;
        }
        let tag = fold_tag_hash7(key_hash);
        self.write_entry_at(len, tag, HeaderSlot { name, value });
        self.len = (len + 1) as u16;
        true
    }

    /// Appends a Bit-7 bypass header directly using a pre-computed 63-bit key hash without scanning.
    #[inline(always)]
    pub fn push_prehashed_bypass(
        &mut self,
        key_hash: u64,
        name: HeaderName,
        value: HeaderValue,
    ) -> bool {
        let len = self.len as usize;
        if len >= self.cap as usize && !self.grow_cold() {
            return false;
        }
        let tag = fold_tag_hash7(key_hash) | TAG_BYTE_BYPASS_BIT;
        self.write_entry_at(len, tag, HeaderSlot { name, value });
        self.len = (len + 1) as u16;
        true
    }

    /// Overwrites the value of an already-populated slot index in place without hashing or scanning.
    #[inline(always)]
    pub fn set_value_at(&mut self, idx: usize, value: HeaderValue) {
        if let Some(slot) = self.slots_mut().get_mut(idx) {
            slot.value = value;
        }
    }

    #[inline(always)]
    pub fn set_u64(&mut self, name: HeaderName, val: u64) {
        let hv = resolve_u64_header_value(val);
        let tag7 = fold_tag_hash7(header_key(name.as_str().as_bytes()));
        self.insert_internal_tag7(tag7, name, hv, false);
    }

    #[inline(always)]
    pub fn append_u64(&mut self, name: HeaderName, val: u64) {
        let hv = resolve_u64_header_value(val);
        self.append_internal(name, hv, false);
    }

    #[inline]
    pub fn insert_bypass_u64(&mut self, name: HeaderName, val: u64) -> bool {
        let hv = resolve_u64_header_value(val);
        let tag7 = fold_tag_hash7(header_key(name.as_str().as_bytes()));
        self.insert_internal_tag7(tag7, name, hv, true);
        true
    }

    #[inline(always)]
    pub fn set_hex128(&mut self, name: HeaderName, id: u128) {
        let tag7 = fold_tag_hash7(header_key(name.as_str().as_bytes()));
        self.insert_internal_tag7(tag7, name, format_u128_hex_header(id), false);
    }

    #[inline(always)]
    pub fn append_hex128(&mut self, name: HeaderName, id: u128) {
        self.append_internal(name, format_u128_hex_header(id), false);
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
        let tag7 = fold_tag_hash7(header_key(name.as_str().as_bytes()));
        self.insert_internal_tag7(tag7, name, format_ip_header(ip), false);
    }

    #[inline(always)]
    pub fn append_ip_u128(&mut self, name: HeaderName, ip: u128) {
        self.append_internal(name, format_ip_header(ip), false);
    }

    #[inline(always)]
    pub fn set_inline_ascii(&mut self, name: HeaderName, bytes: &[u8]) {
        let slice = bytes.get(..bytes.len().min(16)).unwrap_or(bytes);
        if let Ok(hv) = HeaderValue::from_bytes(slice) {
            let tag7 = fold_tag_hash7(header_key(name.as_str().as_bytes()));
            self.insert_internal_tag7(tag7, name, hv, false);
        }
    }

    #[inline(always)]
    pub fn append_inline_ascii(&mut self, name: HeaderName, bytes: &[u8]) {
        let slice = bytes.get(..bytes.len().min(16)).unwrap_or(bytes);
        if let Ok(hv) = HeaderValue::from_bytes(slice) {
            self.append_internal(name, hv, false);
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
            inner: self.slots().iter(),
        }
    }

    #[inline(always)]
    pub fn keys(&self) -> impl Iterator<Item = &HeaderName> {
        self.slots().iter().map(|s| &s.name)
    }

    /// Iterates over all entries marked with MSB bypass bit (`TAG_BYTE_BYPASS_BIT`).
    #[inline]
    pub fn bypass_headers(&self) -> impl Iterator<Item = (&HeaderName, &HeaderValue)> {
        let tags = self.tags();
        let slots = self.slots();
        tags.iter().enumerate().filter_map(move |(i, &tag)| {
            if (tag & TAG_BYTE_BYPASS_BIT) != 0 {
                slots.get(i).map(|s| (&s.name, &s.value))
            } else {
                None
            }
        })
    }

    #[inline(always)]
    pub fn request_id(&self) -> &HeaderValue {
        const KEY_REQ_ID: HeaderKey = HeaderKey::from_static("x-request-id");
        if let Some(val) = self.get_prehashed(&KEY_REQ_ID) {
            val
        } else {
            static FALLBACK_ID: HeaderValue =
                HeaderValue::from_static("0000000000000000-0000000000000000");
            &FALLBACK_ID
        }
    }

    #[inline]
    pub fn set_request_id(&mut self, req_id: HeaderValue) {
        const TAG7_REQ_ID: u8 = fold_tag_hash7(header_key(b"x-request-id"));
        let _ = self.insert_internal_tag7(TAG7_REQ_ID, X_REQUEST_ID, req_id, true);
    }

    /// Compacts entries in place, retaining only slots marked with [`TAG_BYTE_BYPASS_BIT`].
    ///
    /// Protocol-blind primitive used by HTTP/gRPC rejection handlers before stamping protocol headers.
    #[inline]
    pub fn retain_bypass_only(&mut self) {
        let n = self.len as usize;
        let mut write_idx = 0usize;
        unsafe {
            let tags_ptr = self.ptr.as_ptr();
            let slots_ptr = self.ptr.as_ptr().add(self.slots_offset) as *mut HeaderSlot;
            for read_idx in 0..n {
                let tag = *tags_ptr.add(read_idx);
                if (tag & TAG_BYTE_BYPASS_BIT) != 0 {
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

    /// Resets workspace for reuse, returning oversized spike buffers to a clean 32-slot block.
    #[inline(always)]
    pub fn reset(&mut self) {
        self.clear();
        if self.cap > DEFAULT_HEADER_CAPACITY as u16 {
            let mut new_block = PooledHeaderBlock::acquire_pooled(DEFAULT_HEADER_CAPACITY);
            let old_block = PooledHeaderBlock {
                ptr: self.ptr,
                layout: self.layout,
                slots_offset: self.slots_offset,
                cap: self.cap,
            };
            self.ptr = new_block.ptr;
            self.layout = new_block.layout;
            self.slots_offset = new_block.slots_offset;
            self.cap = new_block.cap;
            new_block.cap = 0;
            old_block.recycle_or_free();
        }
        let ingress_nanos = crate::clock::now_unix_nanos();
        let (_id, req_id) = crate::request_id::next_request_id(ingress_nanos);
        self.ingress_time_nanos = ingress_nanos;
        const TAG7_REQ_ID: u8 = fold_tag_hash7(header_key(b"x-request-id"));
        let _ = self.insert_internal_tag7(TAG7_REQ_ID, X_REQUEST_ID, req_id, true);
    }

    #[cfg(test)]
    fn assert_swar_parity(&self, target7: u8, name_str: &str) {
        let simd_res = self.find_first(target7, name_str);
        let swar_res = find_first_tag7_swar(
            self.ptr.as_ptr(),
            self.len as usize,
            self.slots(),
            target7,
            |s| s.as_str().eq_ignore_ascii_case(name_str),
        );
        assert_eq!(simd_res, swar_res);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simd_swar_parity_and_chunk_boundaries() {
        for n in [
            1usize, 7, 8, 9, 15, 16, 17, 31, 32, 33, 63, 64, 65, 127, 128, 256,
        ] {
            let mut ws = HeaderWorkspace::empty(DEFAULT_HEADER_CAPACITY);
            for i in 0..n {
                let s = format!("x-boundary-{i:03}");
                let nm = HeaderName::from_bytes(s.as_bytes()).unwrap();
                let kh = header_key(s.as_bytes());
                if i % 5 == 0 {
                    assert!(ws.push_prehashed_bypass(kh, nm, HeaderValue::from_static("1")));
                } else {
                    assert!(ws.push_prehashed(kh, nm, HeaderValue::from_static("0")));
                }
            }
            for i in 0..n {
                let s = format!("x-boundary-{i:03}");
                let tag7 = fold_tag_hash7(header_key(s.as_bytes()));
                ws.assert_swar_parity(tag7, &s);
            }
            let miss = "x-boundary-missing";
            ws.assert_swar_parity(fold_tag_hash7(header_key(miss.as_bytes())), miss);
        }
    }
}
