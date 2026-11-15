//! In-memory Contiguous 8-Slot JWKS Key Table and Shared Store (`JwksKeyTable`).
//!
//! ```text
//!                         JWKS KEY TABLE MEMORY LAYOUT (1.1 KB)
//!
//! +-----------------------------------------------------------------------------------------+
//! | JwksKeyTable (repr(C, align(64)))                                                       |
//! |                                                                                         |
//! | [L1D Cache Line 0: 64-Byte Integer Index Header]                                        |
//! | +-------------------------------------------------------------------------------------+ |
//! | | kid_index: [u64; 8]                                                                 | |
//! | | [ slot 0 ]  [ slot 1 ]  [ slot 2 ]  [ slot 3 ]  [ slot 4 ]  [ slot 5 ]  [ slot 6 ]  [ slot 7] |
//! | |  (8B int)    (8B int)    (8B int)    (8B int)    (8B int)    (8B int)    (8B int)    (8B int) |
//! | +-------------------------------------------------------------------------------------+ |
//! | | default_slot: u8 (Slot 0 for static key)    | active_mask: u8 (1 bit per active slot)| |
//! | +-------------------------------------------------------------------------------------+ |
//! |                                                                                         |
//! | [Contiguous Slots Array: 8 x JwksKeySlot (repr(C, align(64)))]                         |
//! | +-------------------------------------------------------------------------------------+ |
//! | | Slot 0: [internal_kid | raw_kid_hash | raw_kid: [u8; 32] | alg | key (VerifierKey)]  | |
//! | | Slot 1: [internal_kid | raw_kid_hash | raw_kid: [u8; 32] | alg | key (VerifierKey)]  | |
//! | | ...                                                                                 | |
//! | | Slot 7: [internal_kid | raw_kid_hash | raw_kid: [u8; 32] | alg | key (VerifierKey)]  | |
//! | +-------------------------------------------------------------------------------------+ |
//! +-----------------------------------------------------------------------------------------+
//!
//!                          MULTI-CORE HARDWARE & COHERENCE
//!
//!         [Background Rotator Thread]                     [Frontline Serving Hot Path]
//!                      |                                                |
//!             Prepares new table                                 Token with kid
//!                      |                                                |
//!          Atomic ArcSwap::store()                               fn hash_kid() -> u64
//!                      |                                                |
//!                      v                                                v
//!          +------------------------+                     +---------------------------+
//!          | RAM: Arc<JwksKeyTable> |                     | 1x 64B L1D Cache Line:    |
//!          +------------------------+                     | Integer Match kid_index   |
//!                      |                                  +---------------------------+
//!        Pushed to L1D across cores                                     |
//!                      v                                                | (Local L1D Hit)
//!     +----------------------------------+                              v
//!     | Core 0 L1D (S) | Core 1 L1D (S)  |                  Direct Index into Slot
//!     | (Shared state) | (Shared state)  |                              |
//!     | Low LFB load   | 0 False Sharing |                              v
//!     +----------------------------------+                     Run Signature Check
//! ```
//!
//! # Architecture & Hardware-Mechanical Sympathy
//! - **Single Source of Truth (SSOT)**: Both static pre-configured keys and dynamic rotated keys
//!   reside within the same contiguous 8-slot array.
//! - **1 Cache Line Index (64 Bytes)**: `kid_index: [u64; 8]` packs all 8 key identifiers into
//!   a single 64-byte L1D cache line for flat branchless integer comparison without pointer chasing.
//! - **Core-Local Cache Residency**: A single compact table in RAM is cached across worker cores
//!   in the MESI `Shared` state (read-only on serving path), eliminating cross-core bus traffic
//!   and avoiding false sharing.
//! - **Lock-Free Double-Buffering**: Writer updates use `arc_swap::ArcSwap` atomic pointer swaps
//!   with `Release`/`Acquire` semantics, never blocking frontline traffic serving threads.

use std::sync::Arc;

use arc_swap::ArcSwap;

use super::crypto::{JwtAlgorithm, VerifierKey};

/// Maximum capacity of the contiguous JWKS key table (8 slots).
pub const MAX_JWKS_SLOTS: usize = 8;

/// Computes a 64-bit FNV-1a integer representation of an external `kid` string.
#[inline(always)]
pub const fn hash_kid(raw_kid: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf29ce484222325;
    let mut i = 0;
    while i < raw_kid.len() {
        hash ^= raw_kid[i] as u64;
        hash = hash.wrapping_mul(0x100000001b3);
        i += 1;
    }
    hash
}

/// Single 64-byte aligned key slot inside [`JwksKeyTable`].
#[repr(C, align(64))]
#[derive(Clone)]
pub struct JwksKeySlot {
    /// Internal unique integer identifier assigned to this key.
    pub internal_kid: u64,
    /// 64-bit integer representation of the external `kid` string.
    pub raw_kid_hash: u64,
    /// Raw ASCII `kid` bytes (inline, up to 32 bytes, zero heap allocation).
    pub raw_kid: [u8; 32],
    /// Length of `raw_kid` in bytes (`0..=32`).
    pub raw_kid_len: u8,
    /// Cryptographic signing algorithm.
    pub alg: JwtAlgorithm,
    /// Whether this slot contains an active, valid key (1 = active, 0 = inactive).
    pub is_active: u8,
    /// Explicit padding to maintain strict 64-byte alignment boundaries.
    pub _reserved: [u8; 5],
    /// Concrete cryptographic key (RSA / ECDSA / HMAC).
    pub key: Option<VerifierKey>,
}

impl JwksKeySlot {
    /// Creates an empty, inactive key slot.
    pub const fn empty() -> Self {
        Self {
            internal_kid: 0,
            raw_kid_hash: 0,
            raw_kid: [0u8; 32],
            raw_kid_len: 0,
            alg: JwtAlgorithm::HS256,
            is_active: 0,
            _reserved: [0u8; 5],
            key: None,
        }
    }

    /// Populates this slot with an active key.
    pub fn new(internal_kid: u64, raw_kid_str: &str, alg: JwtAlgorithm, key: VerifierKey) -> Self {
        let mut raw_kid = [0u8; 32];
        let bytes = raw_kid_str.as_bytes();
        let copy_len = bytes.len().min(32);
        raw_kid[..copy_len].copy_from_slice(&bytes[..copy_len]);

        Self {
            internal_kid,
            raw_kid_hash: hash_kid(bytes),
            raw_kid,
            raw_kid_len: copy_len as u8,
            alg,
            is_active: 1,
            _reserved: [0u8; 5],
            key: Some(key),
        }
    }
}

/// Contiguous 8-slot JWKS key table holding pre-parsed verification keys.
#[repr(C, align(64))]
#[derive(Clone)]
pub struct JwksKeyTable {
    /// 64-byte array containing the `raw_kid_hash` for each slot.
    /// Resides in exactly 1 L1D cache line for single-pass branchless integer matching.
    pub kid_index: [u64; MAX_JWKS_SLOTS],
    /// Array of 8 concrete key slots.
    pub slots: [JwksKeySlot; MAX_JWKS_SLOTS],
    /// Number of active keys currently held in the table.
    pub active_count: u8,
    /// Default slot index to use when a JWT token omits the `kid` header (defaults to 0).
    pub default_slot: u8,
}

impl JwksKeyTable {
    /// Creates an empty table with 8 inactive slots.
    pub const fn empty() -> Self {
        Self {
            kid_index: [0u64; MAX_JWKS_SLOTS],
            slots: [const { JwksKeySlot::empty() }; MAX_JWKS_SLOTS],
            active_count: 0,
            default_slot: 0,
        }
    }

    /// Creates a table initialized with a single static key at Slot 0.
    pub fn with_static_key(key: VerifierKey, alg: JwtAlgorithm) -> Self {
        let mut table = Self::empty();
        let slot = JwksKeySlot::new(1, "default", alg, key);
        let mut hashes = [0u64; MAX_JWKS_SLOTS];
        hashes[0] = slot.raw_kid_hash;
        table.slots[0] = slot;
        table.active_count = 1;
        table.default_slot = 0;
        table.set_kid_index_batch(&hashes);
        table
    }

    /// Writes all 8 key identifier hashes (64 bytes = 1 L1D cache line) in a single batch.
    ///
    /// On x86_64 targets with AVX2 support, this utilizes two 256-bit aligned vector stores
    /// (`_mm256_store_si256`), committing 32 bytes per instruction into the 64-byte aligned
    /// `kid_index` array without generating 8 individual scalar `movq` store micro-ops.
    #[inline]
    pub fn set_kid_index_batch(&mut self, hashes: &[u64; MAX_JWKS_SLOTS]) {
        #[cfg(target_arch = "x86_64")]
        {
            if is_x86_feature_detected!("avx2") {
                // SAFETY: `self` is `#[repr(C, align(64))]`, so `self.kid_index` is 64-byte aligned.
                // Both `src` and `dst` hold exactly `MAX_JWKS_SLOTS` (8) u64 elements (64 bytes).
                unsafe {
                    Self::write_kid_index_avx2(&mut self.kid_index, hashes);
                }
                return;
            }
        }
        self.kid_index.copy_from_slice(hashes);
    }

    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "avx2")]
    #[inline]
    unsafe fn write_kid_index_avx2(dst: &mut [u64; MAX_JWKS_SLOTS], src: &[u64; MAX_JWKS_SLOTS]) {
        use core::arch::x86_64::*;
        let s = src.as_ptr() as *const __m256i;
        let d = dst.as_mut_ptr() as *mut __m256i;

        // SAFETY: Caller guarantees `src` and `dst` point to 64 bytes (8x u64).
        // `dst` is 64-byte aligned (due to `#[repr(C, align(64))]`).
        unsafe {
            let v0 = _mm256_loadu_si256(s);
            let v1 = _mm256_loadu_si256(s.add(1));

            _mm256_store_si256(d, v0);
            _mm256_store_si256(d.add(1), v1);
        }
    }

    /// Batch-populates multiple slots and commits their 64-byte hash index in a single operation.
    pub fn populate_batch(
        &mut self,
        slots: [Option<JwksKeySlot>; MAX_JWKS_SLOTS],
        default_slot: u8,
    ) {
        let mut hashes = [0u64; MAX_JWKS_SLOTS];
        let mut count = 0;

        for (idx, slot_opt) in slots.into_iter().enumerate() {
            if let Some(slot) = slot_opt {
                hashes[idx] = slot.raw_kid_hash;
                self.slots[idx] = slot;
                count += 1;
            } else {
                hashes[idx] = 0;
                self.slots[idx] = JwksKeySlot::empty();
            }
        }

        self.set_kid_index_batch(&hashes);
        self.active_count = count;
        self.default_slot = default_slot;
    }

    /// Finds a key slot matching the raw `kid` string from the token's JOSE header.
    #[inline(always)]
    pub fn find_by_raw_kid(&self, raw_kid: &str) -> Option<&JwksKeySlot> {
        let target_hash = hash_kid(raw_kid.as_bytes());
        let target_bytes = raw_kid.as_bytes();

        // 1. Fast match across 64-byte kid_index (1 Cache Line in L1D!)
        for i in 0..MAX_JWKS_SLOTS {
            if self.kid_index[i] == target_hash {
                let slot = &self.slots[i];
                if slot.is_active != 0 {
                    let len = slot.raw_kid_len as usize;
                    if &slot.raw_kid[..len] == target_bytes {
                        return Some(slot);
                    }
                }
            }
        }
        None
    }

    /// Finds a key slot by internal integer ID.
    #[inline(always)]
    pub fn find_by_internal_kid(&self, internal_kid: u64) -> Option<&JwksKeySlot> {
        for i in 0..MAX_JWKS_SLOTS {
            let slot = &self.slots[i];
            if slot.is_active != 0 && slot.internal_kid == internal_kid {
                return Some(slot);
            }
        }
        None
    }

    /// Returns the default key slot (used when token omits `kid`).
    #[inline(always)]
    pub fn default_key(&self) -> Option<&JwksKeySlot> {
        if self.active_count > 0 {
            let slot = &self.slots[self.default_slot as usize];
            if slot.is_active != 0 {
                return Some(slot);
            }
        }
        None
    }
}

/// Thread-safe, lock-free container wrapping an atomic reference to [`JwksKeyTable`].
pub struct SharedJwksStore {
    inner: ArcSwap<JwksKeyTable>,
}

impl SharedJwksStore {
    /// Creates a new shared store with the initial key table.
    pub fn new(initial_table: JwksKeyTable) -> Self {
        Self {
            inner: ArcSwap::from_pointee(initial_table),
        }
    }

    /// Atomically loads the current active key table for reading on the hot path (lock-free).
    #[inline(always)]
    pub fn load(&self) -> arc_swap::Guard<Arc<JwksKeyTable>> {
        self.inner.load()
    }

    /// Atomically loads a cloned Arc of the active key table.
    #[inline(always)]
    pub fn load_full(&self) -> Arc<JwksKeyTable> {
        self.inner.load_full()
    }

    /// Atomically replaces the active key table with a newly prepared table (from rotator).
    pub fn update(&self, new_table: JwksKeyTable) {
        self.inner.store(Arc::new(new_table));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_kid_index_batch() {
        let mut table = JwksKeyTable::empty();
        let hashes: [u64; MAX_JWKS_SLOTS] = [
            0x1111_2222_3333_4444,
            0x5555_6666_7777_8888,
            0x9999_aaaa_bbbb_cccc,
            0xdddd_eeee_ffff_0000,
            0x1234_5678_9abc_def0,
            0x0fed_cba9_8765_4321,
            0xaaaa_bbbb_cccc_dddd,
            0xeeee_ffff_0000_1111,
        ];

        table.set_kid_index_batch(&hashes);
        assert_eq!(table.kid_index, hashes);
    }

    #[test]
    fn test_populate_batch() {
        let mut table = JwksKeyTable::empty();
        let key = VerifierKey::new(JwtAlgorithm::HS256, Some("secret"), None).unwrap();
        let slot0 = JwksKeySlot::new(1, "key-0", JwtAlgorithm::HS256, key.clone());
        let slot1 = JwksKeySlot::new(2, "key-1", JwtAlgorithm::HS256, key);

        let slots: [Option<JwksKeySlot>; MAX_JWKS_SLOTS] =
            [Some(slot0), Some(slot1), None, None, None, None, None, None];

        table.populate_batch(slots, 0);

        assert_eq!(table.active_count, 2);
        assert_eq!(table.default_slot, 0);
        assert_ne!(table.kid_index[0], 0);
        assert_ne!(table.kid_index[1], 0);
        assert_eq!(table.kid_index[2], 0);

        let found = table.find_by_raw_kid("key-0").expect("key-0 must be found");
        assert_eq!(found.internal_kid, 1);
        let found2 = table.find_by_raw_kid("key-1").expect("key-1 must be found");
        assert_eq!(found2.internal_kid, 2);
        assert!(table.find_by_raw_kid("key-unknown").is_none());
    }
}
