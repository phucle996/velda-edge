//! In-memory Contiguous 8-Slot JWKS Key Table and Epoch-Tracked Shared Store (`jwt_auth::table`).
//!
//! ```text
//!                         JWKS KEY TABLE MEMORY LAYOUT (align(64))
//!
//! +-----------------------------------------------------------------------------------------+
//! | JwksKeyTable (repr(C, align(64)))                                                       |
//! |                                                                                         |
//! | [L1D Cache Line 0: 64-Byte Integer Index Header]                                        |
//! | +-------------------------------------------------------------------------------------+ |
//! | | kid_index: [u64; 8]                                                                 | |
//! | | [ slot 0 ]  [ slot 1 ]  [ slot 2 ]  [ slot 3 ]  [ slot 4 ]  [ slot 5 ]  [ slot 6 ]  [ slot 7 ] |
//! | |  (8B int)    (8B int)    (8B int)    (8B int)    (8B int)    (8B int)    (8B int)    (8B int)  |
//! | +-------------------------------------------------------------------------------------+ |
//! |                                                                                         |
//! | [Contiguous Slots Array: 8 x JwksKeySlot (repr(C, align(64)))]                         |
//! | +-------------------------------------------------------------------------------------+ |
//! | | Slot 0..7: [internal_kid | raw_kid_hash | raw_kid: [u8; 128] | alg | VerifierKey]   | |
//! | +-------------------------------------------------------------------------------------+ |
//! +-----------------------------------------------------------------------------------------+
//!
//!                          EPOCH-COORDINATED ROTATION & CACHE COHERENCE
//!
//!         [Background Rotator Thread]                     [Frontline Serving Hot Path]
//!                      |                                                |
//!             Prepares new table                                 SharedJwksStore::epoch()
//!                      |                                                | (Acquire)
//!          ArcSwap::store(Arc<Table>)                                   v
//!          epoch.fetch_add(1, Release)                    JwtCache tags entries w/ epoch
//!                      |                                  (Stale-epoch tokens auto-miss!)
//!                      v                                                |
//!          1x 64B L1D Cache Line 0: <-----------------------------------+
//!          Integer Match `kid_index`
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arc_swap::ArcSwap;

use super::crypto::{JwtAlgorithm, JwtVerifyError, VerifierKey};
use crate::error::PluginError;

/// Maximum capacity of the contiguous JWKS key table (8 slots = 1 64-byte cache line of `u64` hashes).
pub const MAX_JWKS_SLOTS: usize = 8;

/// Maximum inline UTF-8 byte length of an external `kid` identifier (covers UUIDv4 36B, SHA-256 43B, SHA-512 86B, ARNs).
pub const MAX_KID_BYTES: usize = 128;

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
    /// 64-bit FNV-1a hash of the external `kid` string.
    pub raw_kid_hash: u64,
    /// Inline `kid` bytes (`0..=128` bytes, zero heap allocation, zero silent truncation).
    pub raw_kid: [u8; MAX_KID_BYTES],
    /// Length of `raw_kid` in bytes (`0..=128`).
    pub raw_kid_len: u8,
    /// Cryptographic signing algorithm bound to this slot.
    pub alg: JwtAlgorithm,
    /// Whether this slot contains an active verification key (`1` = active, `0` = inactive).
    pub is_active: u8,
    /// `1` when created from a static un-keyed config (`secret_key` / `public_key_pem`), allowing
    /// single-key mode to verify tokens carrying arbitrary IdP `kid` headers.
    pub is_wildcard_kid: u8,
    /// Explicit padding to align subsequent fields.
    pub _reserved: [u8; 4],
    /// Concrete cryptographic verification key (HMAC / RSA / ECDSA / Ed25519).
    pub key: Option<VerifierKey>,
}

impl JwksKeySlot {
    /// Creates an empty, inactive key slot.
    pub const fn empty() -> Self {
        Self {
            internal_kid: 0,
            raw_kid_hash: 0,
            raw_kid: [0u8; MAX_KID_BYTES],
            raw_kid_len: 0,
            alg: JwtAlgorithm::HS256,
            is_active: 0,
            is_wildcard_kid: 0,
            _reserved: [0u8; 4],
            key: None,
        }
    }

    /// Populates this slot with an active key, rejecting `kid` values exceeding [`MAX_KID_BYTES`].
    pub fn try_new(
        internal_kid: u64,
        raw_kid_str: &str,
        alg: JwtAlgorithm,
        key: VerifierKey,
    ) -> Result<Self, PluginError> {
        let bytes = raw_kid_str.as_bytes();
        if bytes.len() > MAX_KID_BYTES {
            return Err(PluginError::Config(format!(
                "JWK kid length ({} bytes) exceeds maximum supported {MAX_KID_BYTES} bytes",
                bytes.len()
            )));
        }
        let mut raw_kid = [0u8; MAX_KID_BYTES];
        raw_kid[..bytes.len()].copy_from_slice(bytes);

        Ok(Self {
            internal_kid,
            raw_kid_hash: hash_kid(bytes),
            raw_kid,
            raw_kid_len: bytes.len() as u8,
            alg,
            is_active: 1,
            is_wildcard_kid: 0,
            _reserved: [0u8; 4],
            key: Some(key),
        })
    }

    /// Populates this slot with an active key (`raw_kid_str.len() <= 128`).
    pub fn new(internal_kid: u64, raw_kid_str: &str, alg: JwtAlgorithm, key: VerifierKey) -> Self {
        Self::try_new(internal_kid, raw_kid_str, alg, key)
            .expect("raw_kid_str must not exceed MAX_KID_BYTES (128 bytes)")
    }

    /// Creates a static key slot (Slot 0) configured via `secret_key` or `public_key_pem`.
    pub fn new_static(alg: JwtAlgorithm, key: VerifierKey) -> Self {
        let mut slot = Self::new(1, "default", alg, key);
        slot.is_wildcard_kid = 1;
        slot
    }
}

/// Contiguous 8-slot JWKS key table holding pre-parsed verification keys.
#[repr(C, align(64))]
#[derive(Clone)]
pub struct JwksKeyTable {
    /// 64-byte array containing the `raw_kid_hash` for each slot (Cache Line 0).
    pub kid_index: [u64; MAX_JWKS_SLOTS],
    /// Contiguous array of 8 concrete key slots.
    pub slots: [JwksKeySlot; MAX_JWKS_SLOTS],
    /// Number of active keys currently held in the table (`0..=8`).
    pub active_count: u8,
    /// Default slot index used when a JWT omits the `kid` header (defaults to `0`).
    pub default_slot: u8,
}

const _: () = assert!(std::mem::align_of::<JwksKeyTable>() == 64);
const _: () = assert!(std::mem::offset_of!(JwksKeyTable, kid_index) == 0);

impl JwksKeyTable {
    /// Creates an empty table with 8 inactive slots (used when awaiting `jwks_rotator` boot fetch).
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
        let slot = JwksKeySlot::new_static(alg, key);
        let mut hashes = [0u64; MAX_JWKS_SLOTS];
        hashes[0] = slot.raw_kid_hash;
        table.slots[0] = slot;
        table.active_count = 1;
        table.default_slot = 0;
        table.set_kid_index_batch(&hashes);
        table
    }

    /// Writes all 8 key identifier hashes (64 bytes = 1 L1D cache line) in a single batch.
    #[inline]
    pub fn set_kid_index_batch(&mut self, hashes: &[u64; MAX_JWKS_SLOTS]) {
        self.kid_index.copy_from_slice(hashes);
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

    /// Finds a key slot matching the exact raw `kid` string from the token's JOSE header.
    #[inline(always)]
    pub fn find_by_raw_kid(&self, raw_kid: &str) -> Option<&JwksKeySlot> {
        let target_bytes = raw_kid.as_bytes();
        if target_bytes.len() > MAX_KID_BYTES {
            return None;
        }
        let target_hash = hash_kid(target_bytes);

        // Scan 64-byte kid_index residing in Cache Line 0
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

    /// Resolves the target key slot for an inbound JOSE `kid` header.
    ///
    /// - Returns `Err(JwtVerifyError::NoActiveKeys)` if the table is completely empty.
    /// - Matches exact `kid` across Cache Line 0 when `kid` is present.
    /// - In pure single-static-key mode (`active_count == 1` and `is_wildcard_kid == 1`),
    ///   uses the configured static key even if the IdP attaches a `kid` header.
    #[inline(always)]
    pub fn resolve_key(&self, header_kid: Option<&str>) -> Result<&JwksKeySlot, JwtVerifyError> {
        if self.active_count == 0 {
            return Err(JwtVerifyError::NoActiveKeys);
        }
        if let Some(kid_str) = header_kid {
            if let Some(slot) = self.find_by_raw_kid(kid_str) {
                return Ok(slot);
            }
            if self.active_count == 1
                && self.slots[0].is_active != 0
                && self.slots[0].is_wildcard_kid != 0
            {
                return Ok(&self.slots[0]);
            }
            Err(JwtVerifyError::UnknownKid)
        } else {
            self.default_key().ok_or(JwtVerifyError::UnknownKid)
        }
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
            let idx = (self.default_slot as usize) & (MAX_JWKS_SLOTS - 1);
            let slot = &self.slots[idx];
            if slot.is_active != 0 {
                return Some(slot);
            }
        }
        None
    }
}

/// Thread-safe, lock-free container wrapping an atomic reference to [`JwksKeyTable`] and its rotation epoch.
pub struct SharedJwksStore {
    inner: ArcSwap<JwksKeyTable>,
    epoch: AtomicU64,
}

impl SharedJwksStore {
    /// Creates a new shared store with the initial key table (`epoch = 1`).
    pub fn new(initial_table: JwksKeyTable) -> Self {
        Self {
            inner: ArcSwap::from_pointee(initial_table),
            epoch: AtomicU64::new(1),
        }
    }

    /// Returns the current key-table generation epoch (`Acquire` ordering) for cache invalidation.
    #[inline(always)]
    pub fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::Acquire)
    }

    /// Atomically loads the current active key table for reading on the hot path (lock-free).
    #[inline(always)]
    pub fn load(&self) -> arc_swap::Guard<Arc<JwksKeyTable>> {
        self.inner.load()
    }

    /// Atomically loads a cloned `Arc` of the active key table.
    #[inline(always)]
    pub fn load_full(&self) -> Arc<JwksKeyTable> {
        self.inner.load_full()
    }

    /// Atomically replaces the active key table and increments the generation epoch (`Release` ordering).
    pub fn update(&self, new_table: JwksKeyTable) {
        self.inner.store(Arc::new(new_table));
        self.epoch.fetch_add(1, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_long_uuid_and_sha256_thumbprint_kids_match_without_truncation() {
        let key = VerifierKey::new(JwtAlgorithm::HS256, Some("secret-key"), None).unwrap();
        let uuid_kid = "550e8400-e29b-41d4-a716-446655440000"; // 36 bytes
        let sha256_kid = "dGhpcy1pcy1hLTQzLWNoYXItc2hhMjU2LXRodW1icHJpbnQ"; // 47 bytes

        let slot0 = JwksKeySlot::try_new(1, uuid_kid, JwtAlgorithm::HS256, key.clone()).unwrap();
        let slot1 = JwksKeySlot::try_new(2, sha256_kid, JwtAlgorithm::HS256, key).unwrap();

        let mut table = JwksKeyTable::empty();
        table.populate_batch(
            [Some(slot0), Some(slot1), None, None, None, None, None, None],
            0,
        );

        assert_eq!(table.find_by_raw_kid(uuid_kid).unwrap().internal_kid, 1);
        assert_eq!(table.find_by_raw_kid(sha256_kid).unwrap().internal_kid, 2);

        let overlong = "x".repeat(MAX_KID_BYTES + 1);
        assert!(
            JwksKeySlot::try_new(
                3,
                &overlong,
                JwtAlgorithm::HS256,
                VerifierKey::new(JwtAlgorithm::HS256, Some("secret"), None).unwrap()
            )
            .is_err()
        );
    }

    #[test]
    fn test_static_key_matches_arbitrary_idp_kid_only_when_unpaired() {
        let key = VerifierKey::new(JwtAlgorithm::HS256, Some("secret"), None).unwrap();
        let static_table = JwksKeyTable::with_static_key(key.clone(), JwtAlgorithm::HS256);
        assert!(static_table.resolve_key(Some("auth0-prod-2026")).is_ok());

        // Once dynamic rotated keys are present alongside static key, unknown kids must reject
        let mut rotated = JwksKeyTable::empty();
        let s0 = JwksKeySlot::new_static(JwtAlgorithm::HS256, key.clone());
        let s1 = JwksKeySlot::new(2, "rotated-1", JwtAlgorithm::HS256, key);
        rotated.populate_batch([Some(s0), Some(s1), None, None, None, None, None, None], 0);
        assert_eq!(
            rotated.resolve_key(Some("rotated-1")).unwrap().internal_kid,
            2
        );
        assert_eq!(
            rotated.resolve_key(Some("unknown-kid")).err(),
            Some(JwtVerifyError::UnknownKid)
        );
    }

    #[test]
    fn test_shared_store_epoch_increments_on_update() {
        let store = SharedJwksStore::new(JwksKeyTable::empty());
        assert_eq!(store.epoch(), 1);
        assert_eq!(
            store.load().resolve_key(Some("k1")).err(),
            Some(JwtVerifyError::NoActiveKeys)
        );

        let key = VerifierKey::new(JwtAlgorithm::HS256, Some("secret"), None).unwrap();
        store.update(JwksKeyTable::with_static_key(key, JwtAlgorithm::HS256));
        assert_eq!(store.epoch(), 2);
    }
}
