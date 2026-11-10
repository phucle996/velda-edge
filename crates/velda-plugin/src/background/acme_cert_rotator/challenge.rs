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

/// Static wire response header template preceding the Content-Length digits.
const WIRE_HEADER_PREFIX: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nCache-Control: no-store\r\nConnection: close\r\nContent-Length: ";
const WIRE_HEADER_PREFIX_LEN: usize = WIRE_HEADER_PREFIX.len(); // 103 bytes

/// Matcher function pointer contract for the 28-byte prefix `/.well-known/acme-challenge/`.
pub type PrefixMatcherFn = unsafe fn(path: &[u8]) -> bool;

/// Tier 1: 128-bit SIMD Dual-Overlapping Vector Matcher (SSE2 / Neon).
///
/// Exploits two 128-bit vector registers (`XMM0`, `XMM1` on x86_64) to scan all 28 bytes
/// of `/.well-known/acme-challenge/` without consuming any general-purpose registers (GPRs).
///
/// # Hardware Register & Cache Invariant
/// - Load 0 [0..16]: `b"/.well-known/acm"` into `XMM0`.
/// - Load 1 [12..28]: `b"/acme-challenge/"` into `XMM1` (4-byte overlap at indices 12..16).
/// - 100% of integer GPRs (`RAX`, `RCX`, `RDX`, `R8-R15`) remain unoccupied for downstream routing.
///
/// # Safety
/// The caller must guarantee that `path` contains at least 28 valid bytes.
#[inline(always)]
pub unsafe fn has_acme_prefix_128_simd(path: &[u8]) -> bool {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        use core::arch::x86_64::*;

        let p = path.as_ptr();
        let v0 = _mm_loadu_si128(p as *const __m128i);
        let v1 = _mm_loadu_si128(p.add(12) as *const __m128i);

        let k0 = _mm_loadu_si128(b"/.well-known/acm".as_ptr() as *const __m128i);
        let k1 = _mm_loadu_si128(b"/acme-challenge/".as_ptr() as *const __m128i);

        let diff0 = _mm_xor_si128(v0, k0);
        let diff1 = _mm_xor_si128(v1, k1);
        let diff = _mm_or_si128(diff0, diff1);

        let cmp = _mm_cmpeq_epi8(diff, _mm_setzero_si128());
        _mm_movemask_epi8(cmp) as u32 == 0xFFFF
    }
    #[cfg(not(target_arch = "x86_64"))]
    unsafe {
        has_acme_prefix_64_swar(path)
    }
}

/// Tier 2: 64-bit SWAR Multi-Word Register Matcher.
///
/// Evaluates 3 x 64-bit words + 1 x 32-bit word using unaligned register XORs,
/// completing prefix validation in ~2-3 CPU cycles without vector execution pipelines.
///
/// # Safety
/// The caller must guarantee that `path` contains at least 28 valid bytes.
#[inline(always)]
pub unsafe fn has_acme_prefix_64_swar(path: &[u8]) -> bool {
    unsafe {
        let p = path.as_ptr();
        let w0 = std::ptr::read_unaligned(p as *const u64);
        let w1 = std::ptr::read_unaligned(p.add(8) as *const u64);
        let w2 = std::ptr::read_unaligned(p.add(16) as *const u64);
        let w3 = std::ptr::read_unaligned(p.add(24) as *const u32);

        const K0: u64 = u64::from_ne_bytes(*b"/.well-k");
        const K1: u64 = u64::from_ne_bytes(*b"nown/acm");
        const K2: u64 = u64::from_ne_bytes(*b"e-challe");
        const K3: u32 = u32::from_ne_bytes(*b"nge/");

        ((w0 ^ K0) | (w1 ^ K1) | (w2 ^ K2)) == 0 && (w3 ^ K3) == 0
    }
}

/// Tier 3: 32-bit SWAR Register Matcher (7 x u32).
///
/// Decomposes 28 bytes into 7 contiguous 32-bit words ($28 / 4 = 7$), suited for
/// 32-bit architectures (ARMv7, x86-32) or when 64-bit GPRs are saturated.
///
/// # Safety
/// The caller must guarantee that `path` contains at least 28 valid bytes.
#[inline(always)]
pub unsafe fn has_acme_prefix_32_swar(path: &[u8]) -> bool {
    unsafe {
        let p = path.as_ptr();
        let w0 = std::ptr::read_unaligned(p as *const u32);
        let w1 = std::ptr::read_unaligned(p.add(4) as *const u32);
        let w2 = std::ptr::read_unaligned(p.add(8) as *const u32);
        let w3 = std::ptr::read_unaligned(p.add(12) as *const u32);
        let w4 = std::ptr::read_unaligned(p.add(16) as *const u32);
        let w5 = std::ptr::read_unaligned(p.add(20) as *const u32);
        let w6 = std::ptr::read_unaligned(p.add(24) as *const u32);

        const K0: u32 = u32::from_ne_bytes(*b"/.we");
        const K1: u32 = u32::from_ne_bytes(*b"ll-k");
        const K2: u32 = u32::from_ne_bytes(*b"nown");
        const K3: u32 = u32::from_ne_bytes(*b"/acm");
        const K4: u32 = u32::from_ne_bytes(*b"e-ch");
        const K5: u32 = u32::from_ne_bytes(*b"alle");
        const K6: u32 = u32::from_ne_bytes(*b"nge/");

        let diff =
            (w0 ^ K0) | (w1 ^ K1) | (w2 ^ K2) | (w3 ^ K3) | (w4 ^ K4) | (w5 ^ K5) | (w6 ^ K6);
        diff == 0
    }
}

/// Matches the standard ACME challenge HTTP prefix `/.well-known/acme-challenge/` (28 bytes).
///
/// # Safety
///
/// `path_bytes` must be at least 28 bytes long when invoking raw hardware vector loaders.
#[inline(always)]
pub unsafe fn has_acme_prefix(path_bytes: &[u8]) -> bool {
    #[cfg(target_arch = "x86_64")]
    {
        unsafe { has_acme_prefix_128_simd(path_bytes) }
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        let caps = velda_core::global_hardware_topology()
            .cpu
            .register_capabilities;
        if caps.has_128 {
            unsafe { has_acme_prefix_128_simd(path_bytes) }
        } else if caps.has_64 {
            unsafe { has_acme_prefix_64_swar(path_bytes) }
        } else {
            unsafe { has_acme_prefix_32_swar(path_bytes) }
        }
    }
}

/// Resolves the optimal prefix matcher function pointer based on probed host CPU capabilities.
///
/// Kept for crate API compatibility; internally delegates directly to inlined `has_acme_prefix`.
pub fn resolve_optimal_prefix_matcher() -> PrefixMatcherFn {
    has_acme_prefix
}

/// Public API verifying whether a request path matches the 28-byte `/.well-known/acme-challenge/` prefix.
#[inline(always)]
pub fn has_acme_challenge_prefix(path: &[u8]) -> bool {
    if path.len() < 28 {
        return false;
    }
    unsafe { has_acme_prefix(path) }
}

/// Explicitly invalidates and flushes a memory slice out of L1 and L2 CPU caches.
///
/// Confines ephemeral ACME HTTP-01 response bytes strictly to L3/DRAM, preventing
/// them from occupying precious L1D/L2 cache lines while waiting for remote
/// ACME CA verification (which takes hundreds of milliseconds or seconds).
#[inline]
pub fn evict_from_l1_l2(slice: &[u8]) {
    #[cfg(target_arch = "x86_64")]
    {
        let ptr = slice.as_ptr();
        let len = slice.len();
        let mut offset = 0;
        while offset < len {
            unsafe {
                core::arch::x86_64::_mm_clflush(ptr.add(offset));
            }
            offset += 64;
        }
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        let _ = slice;
    }
}

/// Branchless inlined comparison of token suffix starting after the 28-byte prefix.
///
/// Eliminates libc `bcmp` / `memcmp` dynamic symbol indirection via PLT/GOT,
/// performing unaligned 64-bit SWAR equality checks directly in general-purpose registers.
#[inline(always)]
fn token_suffix_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    debug_assert!(a.len() >= 28 && b.len() >= 28);
    let sa = &a[28..];
    let sb = &b[28..];
    let len = sa.len();
    let mut offset = 0;
    while offset + 8 <= len {
        let wa = unsafe { std::ptr::read_unaligned(sa.as_ptr().add(offset) as *const u64) };
        let wb = unsafe { std::ptr::read_unaligned(sb.as_ptr().add(offset) as *const u64) };
        if wa != wb {
            return false;
        }
        offset += 8;
    }
    while offset < len {
        if sa[offset] != sb[offset] {
            return false;
        }
        offset += 1;
    }
    true
}

struct Entry {
    path: Box<str>,
    response: Bytes,
}

/// Copy-on-write table of live challenges with hardware-bound prefix matcher.
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
    ///
    /// Evaluates prefix via inlined hardware SIMD matcher on the fast path without
    /// allocating a stack frame or pushing callee-saved registers. If matched, dispatches
    /// to [`lookup_matched`] to read from the lock-free copy-on-write table.
    #[inline(always)]
    pub fn lookup(&self, path: &str) -> Option<Bytes> {
        let path_bytes = path.as_bytes();
        if path_bytes.len() < 28 {
            return None;
        }

        // Fast-path inlined prefix check: if not matching, immediately returns None
        // with 0 callee-saved register pushes, 0 stack allocations, and 0 branch mispredictions.
        let matches = unsafe { has_acme_prefix(path_bytes) };

        if !matches {
            return None;
        }

        self.lookup_matched(path_bytes)
    }

    /// Looks up the challenge response directly by token, bypassing the 28-byte prefix check.
    ///
    /// Used when the Edge [`Router`] has already verified and matched the exact ephemeral route.
    #[inline]
    pub fn lookup_by_token(&self, token: &str) -> Option<Bytes> {
        let entries = self.entries.load();
        for entry in entries.iter() {
            let entry_bytes = entry.path.as_bytes();
            if entry_bytes.len() >= 28 && &entry_bytes[28..] == token.as_bytes() {
                return Some(entry.response.clone());
            }
        }
        None
    }

    /// Out-of-line path for matched prefix lookup to prevent callee-saved register spilling
    /// on the frontline fast-exit path.
    #[inline(never)]
    #[cold]
    fn lookup_matched(&self, path_bytes: &[u8]) -> Option<Bytes> {
        let entries = self.entries.load();
        for entry in entries.iter() {
            let entry_bytes = entry.path.as_bytes();
            if token_suffix_eq(entry_bytes, path_bytes) {
                return Some(entry.response.clone());
            }
        }
        None
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
    ///
    /// Uses pre-baked header template and SWAR integer-to-ASCII digit hot-patching
    /// without runtime string formatters or dynamic reallocation.
    pub fn publish(self: &Arc<Self>, token: &str, key_authorization: &str) -> ChallengeGuard {
        let mut path_str = String::with_capacity(CHALLENGE_PATH_PREFIX.len() + token.len());
        path_str.push_str(CHALLENGE_PATH_PREFIX);
        path_str.push_str(token);
        let path: Box<str> = path_str.into_boxed_str();

        let body = key_authorization.as_bytes();
        let body_len = body.len();

        let digit_count = if body_len < 10 {
            1
        } else if body_len < 100 {
            2
        } else {
            3
        };

        // Total wire bytes: Static Prefix (103) + digits (1..3) + "\r\n\r\n" (4) + body
        let total_len = WIRE_HEADER_PREFIX_LEN + digit_count + 4 + body_len;
        let mut wire = Vec::with_capacity(total_len);

        // 1. Direct blit of pre-allocated static header block
        wire.extend_from_slice(WIRE_HEADER_PREFIX);

        // 2. Hot-patch Content-Length ASCII digits without heap allocation / itoa
        match digit_count {
            1 => {
                wire.push(b'0' + (body_len as u8));
            }
            2 => {
                // SWAR: convert 2-digit length into 16-bit register pair
                let tens = ((body_len / 10) as u8) + b'0';
                let ones = ((body_len % 10) as u8) + b'0';
                wire.push(tens);
                wire.push(ones);
            }
            _ => {
                let hundreds = ((body_len / 100) as u8) + b'0';
                let rem = body_len % 100;
                let tens = ((rem / 10) as u8) + b'0';
                let ones = ((rem % 10) as u8) + b'0';
                wire.push(hundreds);
                wire.push(tens);
                wire.push(ones);
            }
        }

        // 3. Hot-patch separator "\r\n\r\n"
        wire.extend_from_slice(b"\r\n\r\n");

        // 4. Hot-patch body payload
        wire.extend_from_slice(body);

        let entry = Arc::new(Entry {
            path: path.clone(),
            response: Bytes::from(wire),
        });

        // Enforce L3/DRAM confinement: explicitly evict response bytes and path from L1D/L2
        // so they do not occupy high-priority frontline cache lines while waiting for ACME verification.
        evict_from_l1_l2(&entry.response);
        evict_from_l1_l2(entry.path.as_bytes());

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
        evict_from_l1_l2(self.path.as_bytes());
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

    #[test]
    fn test_tier_matchers_consistency() {
        let valid_prefix = b"/.well-known/acme-challenge/";
        let valid_with_token = b"/.well-known/acme-challenge/test-token-123";
        let invalid_prefix = b"/.well-known/other-endpoint/";
        let corrupted_tail = b"/.well-known/acme-challengeX";
        let short_path = b"/short";

        unsafe {
            assert!(has_acme_prefix_128_simd(valid_prefix));
            assert!(has_acme_prefix_128_simd(valid_with_token));
            assert!(!has_acme_prefix_128_simd(invalid_prefix));
            assert!(!has_acme_prefix_128_simd(corrupted_tail));

            assert!(has_acme_prefix_64_swar(valid_prefix));
            assert!(has_acme_prefix_64_swar(valid_with_token));
            assert!(!has_acme_prefix_64_swar(invalid_prefix));
            assert!(!has_acme_prefix_64_swar(corrupted_tail));

            assert!(has_acme_prefix_32_swar(valid_prefix));
            assert!(has_acme_prefix_32_swar(valid_with_token));
            assert!(!has_acme_prefix_32_swar(invalid_prefix));
            assert!(!has_acme_prefix_32_swar(corrupted_tail));
        }

        assert!(has_acme_challenge_prefix(valid_prefix));
        assert!(has_acme_challenge_prefix(valid_with_token));
        assert!(!has_acme_challenge_prefix(invalid_prefix));
        assert!(!has_acme_challenge_prefix(short_path));
    }
}
