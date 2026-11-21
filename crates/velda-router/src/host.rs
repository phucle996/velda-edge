//! Zero-hash, 256-bit AVX2 / 64-bit SWAR host and authority matching (`velda_router::host`).
//!
//! # Microarchitectural Rationale
//! - **100% Zero-Hash Raw `u64` Label Blocks**: Domain labels are packed right-to-left
//!   (TLD -> SLD -> Subdomain) directly into 64-bit integers (`u64`), with ASCII letters
//!   uppercased. Labels longer than 8 bytes split into consecutive `u64` chunks marked with
//!   a continuation bit (`1 << 63`) so there is zero hashing and zero collision risk.
//! - **4KB Page-Aligned [`HostTable`]**: All unique [`HostPattern`] entries for a router are
//!   packed into an independent 4KB page-aligned contiguous slab (`align = 4096, size = k * 4096`),
//!   separating 64-byte host patterns from the 16-byte path radix nodes and 8-byte [`RouteEntry`](crate::arena::RouteEntry)
//!   descriptors in [`RouteArena`](crate::arena::RouteArena).
//! - **Anti-Subdomain Spoofing + RFC 6125 Wildcard Semantics**:
//!   - Unused slots are padded with [`EMPTY_LABEL`] (`0_u64`).
//!   - Wildcard `*` is represented by [`WILDCARD_LABEL`] (`u64::MAX`).
//!   - All 8 slots are verified: if `req` has a label where `pat` is `EMPTY_LABEL`, it is a
//!     mismatch (blocks extra-subdomain spoofing). If `pat` has `WILDCARD_LABEL` where `req` is
//!     `EMPTY_LABEL`, it is a mismatch (enforces RFC 6125).
//! - **256-bit AVX2 Acceleration (`vpcmpeqq`)**: Compares 4 `u64` label blocks per instruction
//!   on x86_64 AVX2 CPUs (zero AVX-512), with a portable 64-bit SWAR fallback for ARM/non-AVX2.

use std::alloc::{Layout, alloc_zeroed, dealloc};
use std::ptr::NonNull;

/// Physical OS memory page size (4 KiB).
pub const HOST_PAGE_SIZE: usize = 4096;

/// Empty / unused label slot sentinel.
pub const EMPTY_LABEL: u64 = 0;

/// Wildcard `*` label sentinel (cannot appear in any ASCII request hostname).
pub const WILDCARD_LABEL: u64 = u64::MAX;

/// Overflow sentinel when a request host exceeds 8 `u64` blocks (prevents truncation spoofing).
pub const OVERFLOW_LABEL: u64 = u64::MAX - 1;

/// Continuation bit (bit 63, always 0 in ASCII) marking subsequent 8-byte chunks of a >8-byte label.
const CONT_BIT: u64 = 1_u64 << 63;

/// Packs up to 8 ASCII bytes into a little-endian `u64` with ASCII letters uppercased.
#[inline(always)]
fn pack_u64_upper(b: &[u8]) -> u64 {
    let mut buf = [0u8; 8];
    let n = b.len().min(8);
    let mut i = 0;
    while i < n {
        buf[i] = b[i].to_ascii_uppercase();
        i += 1;
    }
    u64::from_le_bytes(buf)
}

/// Encodes a single DNS label right-to-left into `blocks[idx..]` as raw 64-bit chunks.
/// Returns `false` if the 8-slot buffer overflows.
#[inline(always)]
fn push_label_chunks(blocks: &mut [u64; 8], idx: &mut usize, seg: &[u8]) -> bool {
    if seg.is_empty() {
        return true;
    }
    let mut offset = 0;
    while offset < seg.len() {
        if *idx >= 8 {
            return false;
        }
        let end = (offset + 8).min(seg.len());
        let mut word = pack_u64_upper(&seg[offset..end]);
        if offset > 0 {
            word |= CONT_BIT;
        }
        blocks[*idx] = word;
        *idx += 1;
        offset = end;
    }
    true
}

/// 64-byte, 32-byte aligned right-to-left raw `u64` representation of an incoming request host.
#[repr(C, align(32))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct HostBlocks {
    pub blocks: [u64; 8],
}

const _: () = assert!(std::mem::size_of::<HostBlocks>() == 64);
const _: () = assert!(std::mem::align_of::<HostBlocks>() == 32);

impl HostBlocks {
    /// Returns `true` if no host was parsed (`blocks[0] == EMPTY_LABEL`).
    #[inline(always)]
    pub const fn is_empty(&self) -> bool {
        self.blocks[0] == EMPTY_LABEL
    }

    /// Parses an incoming request host string (stripping port/brackets) right-to-left into raw `u64` blocks.
    #[inline]
    pub fn parse(host: &str) -> Self {
        let clean = clean_host(host);
        let bytes = clean.as_bytes();
        let len = bytes.len();
        if len == 0 {
            return Self::default();
        }

        let mut blocks = [EMPTY_LABEL; 8];
        let mut idx = 0usize;

        // Unbracketed IPv6 (contains colons) or single label (e.g. "localhost")
        if clean.contains(':') || !clean.contains('.') {
            if !push_label_chunks(&mut blocks, &mut idx, bytes) {
                blocks[7] = OVERFLOW_LABEL;
            }
            return Self { blocks };
        }

        // Scan backwards: TLD -> SLD -> Subdomain
        let mut end = len;
        let mut i = len;
        while i > 0 {
            i -= 1;
            if bytes[i] == b'.' {
                let seg = &bytes[i + 1..end];
                if !push_label_chunks(&mut blocks, &mut idx, seg) {
                    blocks[7] = OVERFLOW_LABEL;
                    return Self { blocks };
                }
                end = i;
            }
        }

        if end > 0 && !push_label_chunks(&mut blocks, &mut idx, &bytes[..end]) {
            blocks[7] = OVERFLOW_LABEL;
        }

        Self { blocks }
    }
}

/// 64-byte, 32-byte aligned compiled host pattern stored in [`HostTable`].
///
/// Unused slots are [`EMPTY_LABEL`] (`0`), wildcard `*` is [`WILDCARD_LABEL`] (`u64::MAX`).
#[repr(C, align(32))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HostPattern {
    pub blocks: [u64; 8],
}

const _: () = assert!(std::mem::size_of::<HostPattern>() == 64);
const _: () = assert!(std::mem::align_of::<HostPattern>() == 32);

impl HostPattern {
    /// Compiles an optional host rule into `Some(HostPattern)`, or `None` for catch-all (`*` / `None`).
    #[inline]
    pub fn compile(host: Option<&str>) -> Option<Self> {
        let host = host?;
        if host.is_empty() || host == "*" {
            return None;
        }

        let clean = clean_host(host);
        if clean.is_empty() || clean == "*" {
            return None;
        }

        if let Some(suffix) = clean.strip_prefix("*.") {
            let parsed = HostBlocks::parse(suffix);
            let mut blocks = parsed.blocks;
            if let Some(slot) = blocks.iter().position(|&b| b == EMPTY_LABEL) {
                blocks[slot] = WILDCARD_LABEL;
            } else {
                blocks[7] = OVERFLOW_LABEL;
            }
            Some(Self { blocks })
        } else {
            let parsed = HostBlocks::parse(clean);
            Some(Self {
                blocks: parsed.blocks,
            })
        }
    }

    /// Matches this pattern against pre-parsed request [`HostBlocks`] using 256-bit AVX2 (x86_64)
    /// or 64-bit SWAR fallback.
    #[inline(always)]
    pub fn matches(&self, req: &HostBlocks) -> bool {
        if req.is_empty() || req.blocks[7] == OVERFLOW_LABEL {
            return false;
        }

        if match_blocks_fast(&req.blocks, &self.blocks) {
            return true;
        }

        // Slow path: only if pattern ends in WILDCARD_LABEL and request has a >8-byte single label
        // (continuation chunks marked with CONT_BIT) at that wildcard position.
        match_wildcard_long_label(&req.blocks, &self.blocks)
    }
}

/// Fast-path 8-slot block comparison: `true` iff for every lane `i in 0..8`,
/// `(req[i] == pat[i]) || (pat[i] == WILDCARD_LABEL && req[i] != EMPTY_LABEL)`.
#[inline(always)]
fn match_blocks_fast(req: &[u64; 8], pat: &[u64; 8]) -> bool {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    {
        // SAFETY: target_feature = "avx2" is statically enabled at compile time.
        unsafe { match_blocks_avx2(req, pat) }
    }
    #[cfg(not(all(target_arch = "x86_64", target_feature = "avx2")))]
    {
        #[cfg(target_arch = "x86_64")]
        {
            (*MATCH_BLOCKS_FN)(req, pat)
        }
        #[cfg(not(target_arch = "x86_64"))]
        {
            match_blocks_swar(req, pat)
        }
    }
}

#[cfg(all(target_arch = "x86_64", not(target_feature = "avx2")))]
type MatchBlocksFn = fn(&[u64; 8], &[u64; 8]) -> bool;

#[cfg(all(target_arch = "x86_64", not(target_feature = "avx2")))]
static MATCH_BLOCKS_FN: std::sync::LazyLock<MatchBlocksFn> = std::sync::LazyLock::new(|| {
    if std::arch::is_x86_feature_detected!("avx2") {
        match_blocks_avx2_wrapper
    } else {
        match_blocks_swar
    }
});

#[cfg(all(target_arch = "x86_64", not(target_feature = "avx2")))]
fn match_blocks_avx2_wrapper(req: &[u64; 8], pat: &[u64; 8]) -> bool {
    // SAFETY: Verified AVX2 support once at initialization via LazyLock.
    unsafe { match_blocks_avx2(req, pat) }
}

/// 256-bit AVX2 vector host matcher (`_mm256_cmpeq_epi64` / `vpcmpeqq`):
/// evaluates 4 `u64` label blocks per vector instruction (zero AVX-512).
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn match_blocks_avx2(req: &[u64; 8], pat: &[u64; 8]) -> bool {
    use std::arch::x86_64::*;

    if req[7] == OVERFLOW_LABEL {
        return false;
    }

    // SAFETY: Both `HostBlocks` and `HostPattern` are `#[repr(C, align(32))]`.
    let r0 = unsafe { _mm256_load_si256(req.as_ptr() as *const __m256i) };
    let p0 = unsafe { _mm256_load_si256(pat.as_ptr() as *const __m256i) };
    let r1 = unsafe { _mm256_load_si256(req.as_ptr().add(4) as *const __m256i) };
    let p1 = unsafe { _mm256_load_si256(pat.as_ptr().add(4) as *const __m256i) };

    let zero = _mm256_setzero_si256();
    let wc = _mm256_set1_epi64x(WILDCARD_LABEL as i64);

    // Half 0 (lanes 0..4)
    let eq0 = _mm256_cmpeq_epi64(r0, p0);
    let is_wc0 = _mm256_cmpeq_epi64(p0, wc);
    let r0_zero = _mm256_cmpeq_epi64(r0, zero);
    let wc_ok0 = _mm256_andnot_si256(r0_zero, is_wc0);
    let ok0 = _mm256_or_si256(eq0, wc_ok0);

    // Half 1 (lanes 4..8)
    let eq1 = _mm256_cmpeq_epi64(r1, p1);
    let is_wc1 = _mm256_cmpeq_epi64(p1, wc);
    let r1_zero = _mm256_cmpeq_epi64(r1, zero);
    let wc_ok1 = _mm256_andnot_si256(r1_zero, is_wc1);
    let ok1 = _mm256_or_si256(eq1, wc_ok1);

    let both_ok = _mm256_and_si256(ok0, ok1);
    _mm256_movemask_epi8(both_ok) == -1
}

/// Portable 64-bit SWAR host matcher for ARM / non-AVX2 targets.
#[cfg(any(not(all(target_arch = "x86_64", target_feature = "avx2")), test))]
#[inline(always)]
fn match_blocks_swar(req: &[u64; 8], pat: &[u64; 8]) -> bool {
    if req[7] == OVERFLOW_LABEL {
        return false;
    }
    let mut ok = true;
    let mut i = 0;
    while i < 8 {
        let r = req[i];
        let p = pat[i];
        ok &= (r == p) | ((p == WILDCARD_LABEL) & (r != EMPTY_LABEL));
        i += 1;
    }
    ok
}

/// Handles the rare case where a wildcard `*.domain` matches a request whose leftmost label
/// exceeds 8 bytes (represented as a start chunk followed by `CONT_BIT`-marked chunks).
#[inline(never)]
fn match_wildcard_long_label(req: &[u64; 8], pat: &[u64; 8]) -> bool {
    let Some(wc_idx) = pat.iter().position(|&p| p == WILDCARD_LABEL) else {
        return false;
    };

    // Suffix before wildcard must match 1:1
    for i in 0..wc_idx {
        if req[i] != pat[i] {
            return false;
        }
    }

    // Request must have a non-empty label at `wc_idx` (and not overflow)
    if req[wc_idx] == EMPTY_LABEL || req[wc_idx] == OVERFLOW_LABEL {
        return false;
    }

    // Any remaining non-empty slots in `req` after `wc_idx` MUST be continuation chunks of that
    // same single label (CONT_BIT set); a new label (CONT_BIT == 0) is subdomain spoofing -> false.
    for &r in &req[wc_idx + 1..] {
        if r == EMPTY_LABEL {
            return true;
        }
        if r == OVERFLOW_LABEL || (r & CONT_BIT) == 0 {
            return false;
        }
    }
    true
}

/// 64-byte cache-aligned header at offset 0 of [`HostTable`].
#[repr(C, align(64))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct HostTableHeader {
    count: u32,
    patterns_offset: u32,
    _pad: [u8; 56],
}

const _: () = assert!(std::mem::size_of::<HostTableHeader>() == 64);

/// Independent 4KB Page-Aligned Host Pattern Table (`align = 4096, size = k * 4096`).
///
/// Stores deduplicated 64-byte [`HostPattern`] entries contiguously so [`RouteEntry`](crate::arena::RouteEntry)
/// only needs a 2-byte `host_id: u16` (`0xFFFF` = ANY).
#[derive(Debug)]
pub struct HostTable {
    ptr: NonNull<u8>,
    capacity: usize,
}

unsafe impl Send for HostTable {}
unsafe impl Sync for HostTable {}

impl Drop for HostTable {
    fn drop(&mut self) {
        let layout = Layout::from_size_align(self.capacity, HOST_PAGE_SIZE)
            .expect("HostTable layout must be valid");
        unsafe {
            dealloc(self.ptr.as_ptr(), layout);
        }
    }
}

impl Clone for HostTable {
    fn clone(&self) -> Self {
        let layout = Layout::from_size_align(self.capacity, HOST_PAGE_SIZE)
            .expect("HostTable layout must be valid");
        let raw = unsafe { alloc_zeroed(layout) };
        let ptr = NonNull::new(raw).expect("HostTable clone allocation failed");
        unsafe {
            std::ptr::copy_nonoverlapping(self.ptr.as_ptr(), ptr.as_ptr(), self.capacity);
        }
        Self {
            ptr,
            capacity: self.capacity,
        }
    }
}

impl Default for HostTable {
    fn default() -> Self {
        Self::from_patterns(&[])
    }
}

impl HostTable {
    /// Allocates a 4KB page-aligned [`HostTable`] containing `patterns`.
    pub fn from_patterns(patterns: &[HostPattern]) -> Self {
        let patterns_offset = std::mem::size_of::<HostTableHeader>(); // 64B (32B aligned)
        let total_required = patterns_offset + std::mem::size_of_val(patterns);
        let pages = total_required.div_ceil(HOST_PAGE_SIZE).max(1);
        let capacity = pages * HOST_PAGE_SIZE;

        let layout = Layout::from_size_align(capacity, HOST_PAGE_SIZE)
            .expect("HostTable page layout must succeed");
        let raw = unsafe { alloc_zeroed(layout) };
        let ptr = NonNull::new(raw).expect("HostTable allocation failed");

        let header = HostTableHeader {
            count: patterns.len() as u32,
            patterns_offset: patterns_offset as u32,
            _pad: [0; 56],
        };

        unsafe {
            let base = ptr.as_ptr();
            std::ptr::write(base as *mut HostTableHeader, header);
            if !patterns.is_empty() {
                std::ptr::copy_nonoverlapping(
                    patterns.as_ptr(),
                    base.add(patterns_offset) as *mut HostPattern,
                    patterns.len(),
                );
            }
        }

        Self { ptr, capacity }
    }

    /// Returns the contiguous slice of 64-byte [`HostPattern`] entries.
    #[inline(always)]
    pub fn patterns(&self) -> &[HostPattern] {
        let h = unsafe { &*(self.ptr.as_ptr() as *const HostTableHeader) };
        if h.count == 0 {
            return &[];
        }
        unsafe {
            std::slice::from_raw_parts(
                self.ptr.as_ptr().add(h.patterns_offset as usize) as *const HostPattern,
                h.count as usize,
            )
        }
    }

    /// Returns the allocated page-aligned size in bytes (`k * 4096`).
    #[inline(always)]
    pub fn allocated_bytes(&self) -> usize {
        self.capacity
    }

    /// Matches `host_id` against pre-parsed request [`HostBlocks`].
    #[inline(always)]
    pub fn matches(&self, host_id: u16, req: &HostBlocks) -> bool {
        let patterns = self.patterns();
        let idx = host_id as usize;
        if idx >= patterns.len() {
            return false;
        }
        patterns[idx].matches(req)
    }
}

/// Pre-compiled host rule wrapper (used on rule builders before packing into [`HostTable`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompiledHost {
    /// Matches any host (`"*"` or `None`).
    Any,
    /// Fixed or wildcard [`HostPattern`].
    Pattern(HostPattern),
}

impl CompiledHost {
    /// Compiles an optional host pattern into a [`CompiledHost`].
    #[inline]
    pub fn compile(host: Option<&str>) -> Self {
        match HostPattern::compile(host) {
            Some(pat) => Self::Pattern(pat),
            None => Self::Any,
        }
    }

    /// Returns `Some(HostPattern)` or `None` for [`CompiledHost::Any`].
    #[inline(always)]
    pub const fn as_pattern(&self) -> Option<HostPattern> {
        match self {
            Self::Any => None,
            Self::Pattern(p) => Some(*p),
        }
    }

    /// Matches this compiled host against pre-tokenized request [`HostBlocks`].
    #[inline(always)]
    pub fn matches(&self, req: &HostBlocks) -> bool {
        match self {
            Self::Any => true,
            Self::Pattern(p) => p.matches(req),
        }
    }
}

// ============================================================================
// Port Stripping & Host Cleaning Utilities
// ============================================================================

/// Scans for the ASCII colon (`':'`, 0x3A) using 64-bit and 32-bit SWAR chunking
/// with hardware TZCNT zero-byte isolation.
#[inline]
pub fn swar_find_colon(bytes: &[u8]) -> Option<usize> {
    let len = bytes.len();
    let mut i = 0;

    while i + 8 <= len {
        let w = u64::from_le_bytes(bytes[i..i + 8].try_into().unwrap());
        let x = w ^ 0x3A3A_3A3A_3A3A_3A3A_u64;
        let has_zero = (x.wrapping_sub(0x0101_0101_0101_0101_u64)) & !x & 0x8080_8080_8080_8080_u64;
        if has_zero != 0 {
            return Some(i + (has_zero.trailing_zeros() / 8) as usize);
        }
        i += 8;
    }

    if i + 4 <= len {
        let w = u32::from_le_bytes(bytes[i..i + 4].try_into().unwrap());
        let x = w ^ 0x3A3A_3A3A_u32;
        let has_zero = (x.wrapping_sub(0x0101_0101_u32)) & !x & 0x8080_8080_u32;
        if has_zero != 0 {
            return Some(i + (has_zero.trailing_zeros() / 8) as usize);
        }
        i += 4;
    }

    while i < len {
        if bytes[i] == b':' {
            return Some(i);
        }
        i += 1;
    }

    None
}

/// Extracts the hostname without port or IPv6 enclosing brackets.
#[inline]
pub fn clean_host(host: &str) -> &str {
    if host.starts_with('[') {
        if let Some(close_bracket) = host.find(']') {
            &host[1..close_bracket]
        } else {
            host
        }
    } else {
        let bytes = host.as_bytes();
        if let Some(first_colon) = swar_find_colon(bytes) {
            if swar_find_colon(&bytes[first_colon + 1..]).is_some() {
                host
            } else {
                &host[..first_colon]
            }
        } else {
            host
        }
    }
}

/// Compares two ASCII domain byte slices case-insensitively using Dual 64-bit SWAR registers.
#[inline]
pub fn eq_ignore_ascii_case_dual64_swar(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let len = a.len();
    if len == 0 {
        return true;
    }

    let mut i = 0;
    let dual_len = len & !15;

    while i < dual_len {
        let wa0 = u64::from_ne_bytes(a[i..i + 8].try_into().unwrap());
        let wb0 = u64::from_ne_bytes(b[i..i + 8].try_into().unwrap());
        let wa1 = u64::from_ne_bytes(a[i + 8..i + 16].try_into().unwrap());
        let wb1 = u64::from_ne_bytes(b[i + 8..i + 16].try_into().unwrap());

        let d0 = wa0 ^ wb0;
        let d1 = wa1 ^ wb1;

        if (d0 | d1) != 0 {
            if ((d0 | d1) & 0xDFDF_DFDF_DFDF_DFDF_u64) != 0 {
                return false;
            }
            if !validate_ascii_letter_diff_64(wa0, d0) || !validate_ascii_letter_diff_64(wa1, d1) {
                return false;
            }
        }
        i += 16;
    }

    if i + 8 <= len {
        let wa = u64::from_ne_bytes(a[i..i + 8].try_into().unwrap());
        let wb = u64::from_ne_bytes(b[i..i + 8].try_into().unwrap());
        let d = wa ^ wb;
        if d != 0 {
            if (d & 0xDFDF_DFDF_DFDF_DFDF_u64) != 0 {
                return false;
            }
            if !validate_ascii_letter_diff_64(wa, d) {
                return false;
            }
        }
        i += 8;
    }

    if i + 4 <= len {
        let wa = u32::from_ne_bytes(a[i..i + 4].try_into().unwrap());
        let wb = u32::from_ne_bytes(b[i..i + 4].try_into().unwrap());
        let d = wa ^ wb;
        if d != 0 {
            if (d & 0xDFDF_DFDF_u32) != 0 {
                return false;
            }
            let mut mask = d;
            while mask != 0 {
                let byte_idx = (mask.trailing_zeros() / 8) as usize;
                let byte_val = ((wa >> (byte_idx * 8)) & 0xFF) as u8;
                let lower = byte_val | 0x20;
                if !lower.is_ascii_lowercase() {
                    return false;
                }
                mask &= !(0xFF_u32 << (byte_idx * 8));
            }
        }
        i += 4;
    }

    while i < len {
        if !a[i].eq_ignore_ascii_case(&b[i]) {
            return false;
        }
        i += 1;
    }

    true
}

#[inline(always)]
fn validate_ascii_letter_diff_64(word: u64, diff: u64) -> bool {
    if diff == 0 {
        return true;
    }
    let mut mask = diff;
    while mask != 0 {
        let byte_idx = (mask.trailing_zeros() / 8) as usize;
        let byte_val = ((word >> (byte_idx * 8)) & 0xFF) as u8;
        let lower = byte_val | 0x20;
        if !lower.is_ascii_lowercase() {
            return false;
        }
        mask &= !(0xFF_u64 << (byte_idx * 8));
    }
    true
}

/// Returns the specificity rank for a host pattern to enforce deterministic precedence:
/// - 2: Exact host (e.g. `"api.example.com"`)
/// - 1: Suffix wildcard (e.g. `"*.example.com"`)
/// - 0: Catch-all wildcard (`"*"` or `None`)
#[inline]
pub fn host_specificity(host: Option<&str>) -> u8 {
    match host {
        Some("*") | None => 0,
        Some(h) if clean_host(h).starts_with("*.") => 1,
        Some(_) => 2,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_clean_host() {
        assert_eq!(clean_host("example.com"), "example.com");
        assert_eq!(clean_host("example.com:8080"), "example.com");
        assert_eq!(clean_host("[::1]"), "::1");
        assert_eq!(clean_host("[::1]:8080"), "::1");
        assert_eq!(clean_host("[2001:db8::1]:443"), "2001:db8::1");
    }

    #[test]
    fn test_host_blocks_parsing_zero_hash() {
        let rev = HostBlocks::parse("api.velda.io");
        assert_eq!(rev.blocks[0], pack_u64_upper(b"io"));
        assert_eq!(rev.blocks[1], pack_u64_upper(b"velda"));
        assert_eq!(rev.blocks[2], pack_u64_upper(b"api"));
        assert_eq!(rev.blocks[3], EMPTY_LABEL);

        // Labels > 8 bytes are split into continuation chunks without hashing
        let long_a = HostBlocks::parse("abcdefghi");
        let two_labels = HostBlocks::parse("i.abcdefgh");
        assert_ne!(long_a, two_labels);
    }

    #[test]
    fn test_anti_subdomain_spoofing_and_rfc6125() {
        let any = CompiledHost::compile(None);
        let any_star = CompiledHost::compile(Some("*"));
        let exact = CompiledHost::compile(Some("api.example.com"));
        let wildcard = CompiledHost::compile(Some("*.example.com"));

        let req_api = HostBlocks::parse("api.example.com:8080");
        let req_upper = HostBlocks::parse("API.EXAMPLE.COM");
        let req_sub = HostBlocks::parse("sub.api.example.com");
        let req_root = HostBlocks::parse("example.com");
        let req_other = HostBlocks::parse("api.other.com");
        let req_long_sub = HostBlocks::parse("very-long-subdomain.example.com");
        let req_two_long = HostBlocks::parse("a.very-long-subdomain.example.com");

        assert!(any.matches(&req_api));
        assert!(any_star.matches(&req_api));

        assert!(exact.matches(&req_api));
        assert!(exact.matches(&req_upper));
        assert!(!exact.matches(&req_sub));
        assert!(!exact.matches(&req_root));
        assert!(!exact.matches(&req_other));

        assert!(wildcard.matches(&req_api));
        assert!(wildcard.matches(&req_long_sub));
        assert!(!wildcard.matches(&req_sub)); // Anti-subdomain spoofing: 1 label per '*'
        assert!(!wildcard.matches(&req_two_long));
        assert!(!wildcard.matches(&req_root)); // RFC 6125: missing subdomain -> mismatch
        assert!(!wildcard.matches(&req_other));

        // Verify SWAR fallback produces identical results
        if let CompiledHost::Pattern(pat) = wildcard {
            assert!(match_blocks_swar(&req_api.blocks, &pat.blocks));
            assert!(!match_blocks_swar(&req_sub.blocks, &pat.blocks));
            assert!(!match_blocks_swar(&req_root.blocks, &pat.blocks));
        }

        assert_eq!(host_specificity(Some("api.velda.io")), 2);
        assert_eq!(host_specificity(Some("*.velda.io")), 1);
        assert_eq!(host_specificity(Some("*")), 0);
        assert_eq!(host_specificity(None), 0);
    }

    #[test]
    fn test_strict_subdomain_level_and_long_label_boundaries() {
        let wc_abc = HostPattern::compile(Some("*.abc.velda.com")).unwrap();
        assert!(!wc_abc.matches(&HostBlocks::parse("abc.velda.com"))); // missing level (RFC 6125)
        assert!(!wc_abc.matches(&HostBlocks::parse("evil.node.abc.velda.com"))); // extra level (spoofing)
        assert!(wc_abc.matches(&HostBlocks::parse("node.abc.velda.com")));
        assert!(wc_abc.matches(&HostBlocks::parse(
            "very-long-subdomain-label.abc.velda.com"
        )));
        assert!(!wc_abc.matches(&HostBlocks::parse(
            "a.very-long-subdomain-label.abc.velda.com"
        )));

        let exact_api = HostPattern::compile(Some("api.velda.com")).unwrap();
        assert!(exact_api.matches(&HostBlocks::parse("api.velda.com")));
        assert!(!exact_api.matches(&HostBlocks::parse("sub.api.velda.com")));
        assert!(!exact_api.matches(&HostBlocks::parse("velda.com")));

        let exact_long = HostPattern::compile(Some("authentication.velda.com")).unwrap();
        assert!(exact_long.matches(&HostBlocks::parse("authentication.velda.com")));
        assert!(!exact_long.matches(&HostBlocks::parse("cation.authenti.velda.com")));
        assert!(!exact_long.matches(&HostBlocks::parse("sub.authentication.velda.com")));
    }

    #[test]
    fn test_overflow_label_rejected_even_with_8th_slot_wildcard() {
        // Pattern has 7 suffix labels + wildcard at slot 7 (index 7)
        let pat = HostPattern::compile(Some("*.b.c.d.e.f.g.h")).unwrap();
        assert_eq!(pat.blocks[7], WILDCARD_LABEL);

        // Valid 8-label request matches slot 7 wildcard
        let valid_8 = HostBlocks::parse("a.b.c.d.e.f.g.h");
        assert!(pat.matches(&valid_8));

        // 9-label request overflows into OVERFLOW_LABEL at slot 7 -> MUST be rejected
        let overflow_9 = HostBlocks::parse("x.a.b.c.d.e.f.g.h");
        assert_eq!(overflow_9.blocks[7], OVERFLOW_LABEL);
        assert!(!pat.matches(&overflow_9));
    }

    #[test]
    fn test_avx2_swar_equivalence() {
        let patterns = [
            "api.velda.io",
            "*.velda.io",
            "*.abc.velda.com",
            "*.b.c.d.e.f.g.h",
            "authentication.velda.com",
            "localhost",
            "[::1]:8080",
        ];
        let requests = [
            "api.velda.io",
            "API.VELDA.IO:443",
            "sub.api.velda.io",
            "velda.io",
            "node.abc.velda.com",
            "abc.velda.com",
            "evil.node.abc.velda.com",
            "a.b.c.d.e.f.g.h",
            "x.a.b.c.d.e.f.g.h",
            "authentication.velda.com",
            "cation.authenti.velda.com",
            "very-long-subdomain-label.velda.io",
            "localhost",
            "127.0.0.1:8080",
            "[::1]:443",
            "",
        ];

        for pat_str in patterns {
            let pat = HostPattern::compile(Some(pat_str)).unwrap();
            for req_str in requests {
                let req = HostBlocks::parse(req_str);
                let swar_res = match_blocks_swar(&req.blocks, &pat.blocks);
                #[cfg(target_arch = "x86_64")]
                if std::arch::is_x86_feature_detected!("avx2") {
                    let avx2_res = unsafe { match_blocks_avx2(&req.blocks, &pat.blocks) };
                    assert_eq!(
                        avx2_res, swar_res,
                        "AVX2 vs SWAR mismatch for pat={pat_str:?} req={req_str:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn test_host_table_4kb_page_aligned() {
        let p0 = HostPattern::compile(Some("api.velda.io")).unwrap();
        let p1 = HostPattern::compile(Some("*.velda.io")).unwrap();
        let table = HostTable::from_patterns(&[p0, p1]);

        assert_eq!((table.ptr.as_ptr() as usize) % HOST_PAGE_SIZE, 0);
        assert_eq!(table.allocated_bytes() % HOST_PAGE_SIZE, 0);
        assert_eq!(table.patterns().len(), 2);

        let req = HostBlocks::parse("api.velda.io");
        assert!(table.matches(0, &req));
        assert!(table.matches(1, &req));
        assert!(!table.matches(2, &req));
    }
}
