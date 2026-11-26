//! Hardware-Aligned 16-Slot L1D/L3 JWKS Key Store (`jwt_auth::store`).
//!
//! ```text
//!                     JWKS STORE 3-TIER MEMORY ARCHITECTURE
//!
//!   [T1: L1D Cache Line 0 — `DirectoryLine` (#[repr(C, align(64))], Exact 64B)]
//!   +-------------------------------------------------------------------------+
//!   | 0x00..0x10  tags[16]: 8-bit kid tag per slot (0x00 = Empty)             |
//!   | 0x10..0x18  algs_packed (u64): 16 x u4 algorithm nibbles                |
//!   | 0x18..0x1A  valid_mask (u16)  | 0x1A primary_slot (u8) | 0x1B flags(u8) |
//!   | 0x1C        tag_shift (u8)    | 0x20..0x28 table_version_tag (u64)      |
//!   +-------------------------------------------------------------------------+
//!          │  (SIMD/SWAR scan `&valid_mask` + `_MM_HINT_T2` prefetch on L3)
//!          ▼
//!   [T2: L3 Cache Lines — Integer Metadata (#[repr(C, align(64))])]
//!   +-------------------------------------------------------------------------+
//!   | kid_ints:  [u64; 16]  (128B = 2 L3 lines: Trunc64(SHA256(salt || kid))) |
//!   | key_tags:  [u64; 16]  (128B = 2 L3 lines: hash(kid_u64 || alg || key))  |
//!   | key_spans: [KeySpan; 16] (128B = 2 L3 lines: { offset, len, sig_len })  |
//!   +-------------------------------------------------------------------------+
//!          │  (Only on JwtCache MISS)
//!          ▼
//!   [T3: Key Byte Arena]
//!   └── key_bytes: Box<[u8]> (Contiguous PKCS#1 RSA DER | EC Point | Ed25519 | HMAC)
//! ```

use std::sync::Arc;

use arc_swap::ArcSwap;
use ring::digest::{Context as DigestContext, SHA256};
use ring::hmac;

use super::crypto::{ALG_INVALID, JwtAlgorithm, JwtVerifyError, VerifierKey};
use crate::error::PluginError;

/// Maximum key capacity (`1` primary active key + up to `15` standby/overlap keys).
pub const MAX_JWKS_SLOTS: usize = 16;

/// `DirectoryLine::flags` bit 0: allows single static key without `kid` filter to verify any IdP `kid`.
pub const DIR_FLAG_WILDCARD_SINGLE: u8 = 1 << 0;

/// `DirectoryLine::flags` bit 1: set when key store operates in `external` mode (`etp_id`).
pub const DIR_FLAG_EXTERNAL_MODE: u8 = 1 << 1;

const DEFAULT_TAG_SHIFT: u8 = 56;

/// Computes a boot-salted 64-bit integer representation of a JOSE `kid` string.
///
/// Uses `Trunc64(SHA-256(boot_salt || b"kid:" || raw_kid))` clamped `>= 2` so `0`/`1` remain reserved.
#[inline]
pub fn hash_kid_salted(raw_kid: &[u8], boot_salt: &[u8; 32]) -> u64 {
    let mut ctx = DigestContext::new(&SHA256);
    ctx.update(boot_salt);
    ctx.update(b"kid:");
    ctx.update(raw_kid);
    let digest = ctx.finish();
    let bytes: [u8; 8] = digest.as_ref()[..8].try_into().expect("SHA-256 >= 8B");
    u64::from_le_bytes(bytes).max(2)
}

#[inline(always)]
const fn tag_from_kid_u64(kid_u64: u64, shift: u8) -> u8 {
    let t = ((kid_u64 >> shift) & 0xFF) as u8;
    if t == 0 { 1 } else { t }
}

/// Computes a 64-bit FNV-1a version tag over `(kid_u64, alg_u4, raw_bytes)` clamped `>= 2`.
fn compute_key_tag(kid_u64: u64, alg_u4: u8, raw_bytes: &[u8]) -> u64 {
    const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0100_0000_01b3;
    let mut h = FNV_OFFSET ^ kid_u64;
    h = h.wrapping_mul(FNV_PRIME) ^ (alg_u4 as u64);
    h = h.wrapping_mul(FNV_PRIME);
    for &b in raw_bytes {
        h ^= b as u64;
        h = h.wrapping_mul(FNV_PRIME);
    }
    h.max(2)
}

/// Single L1D 64-byte control directory containing bit-packed masks, `u4` algorithms, and 8-bit kid tags.
#[repr(C, align(64))]
#[derive(Clone, Copy)]
pub struct DirectoryLine {
    pub tags: [u8; MAX_JWKS_SLOTS],
    pub algs_packed: u64,
    pub valid_mask: u16,
    pub primary_slot: u8,
    pub flags: u8,
    pub tag_shift: u8,
    pub _pad: [u8; 3],
    pub table_version_tag: u64,
    pub _reserved: [u8; 24],
}

const _: () = assert!(std::mem::size_of::<DirectoryLine>() == 64);
const _: () = assert!(std::mem::align_of::<DirectoryLine>() == 64);

impl DirectoryLine {
    const fn empty() -> Self {
        Self {
            tags: [0u8; MAX_JWKS_SLOTS],
            algs_packed: u64::MAX,
            valid_mask: 0,
            primary_slot: 0,
            flags: 0,
            tag_shift: DEFAULT_TAG_SHIFT,
            _pad: [0u8; 3],
            table_version_tag: 2,
            _reserved: [0u8; 24],
        }
    }
}

/// 8-byte pointerless slice span into [`JwksKeyTable::key_bytes`].
#[repr(C)]
#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub struct KeySpan {
    pub offset: u32,
    pub len: u16,
    pub expected_sig_len: u16,
}

#[repr(C, align(64))]
#[derive(Clone, Copy)]
pub struct AlignedU64x16(pub [u64; MAX_JWKS_SLOTS]);

#[repr(C, align(64))]
#[derive(Clone, Copy)]
pub struct AlignedSpans16(pub [KeySpan; MAX_JWKS_SLOTS]);

/// Input specification used when constructing a [`JwksKeyTable`].
#[derive(Clone)]
pub struct KeyEntrySpec {
    pub raw_kid: Option<Box<str>>,
    pub alg: JwtAlgorithm,
    pub key: VerifierKey,
}

/// Cache-line aligned 16-slot key table (`1` primary active key + up to `15` standby keys).
#[repr(C, align(64))]
#[derive(Clone)]
pub struct JwksKeyTable {
    pub dir: DirectoryLine,
    pub kid_ints: AlignedU64x16,
    pub key_tags: AlignedU64x16,
    pub key_spans: AlignedSpans16,
    pub key_bytes: Box<[u8]>,
    pub hmac_keys: Option<Box<[Option<hmac::Key>; MAX_JWKS_SLOTS]>>,
}

const _: () = assert!(std::mem::align_of::<JwksKeyTable>() == 64);
const _: () = assert!(std::mem::offset_of!(JwksKeyTable, dir) == 0);

impl JwksKeyTable {
    /// Creates an empty key table (`valid_mask == 0`).
    pub fn empty() -> Self {
        Self {
            dir: DirectoryLine::empty(),
            kid_ints: AlignedU64x16([0u64; MAX_JWKS_SLOTS]),
            key_tags: AlignedU64x16([0u64; MAX_JWKS_SLOTS]),
            key_spans: AlignedSpans16([KeySpan::default(); MAX_JWKS_SLOTS]),
            key_bytes: Box::new([]),
            hmac_keys: None,
        }
    }

    /// Creates a table containing a single static key at Slot 0 (primary, wildcard `kid`).
    pub fn with_static_key(key: VerifierKey, alg: JwtAlgorithm, boot_salt: &[u8; 32]) -> Self {
        let entry = KeyEntrySpec {
            raw_kid: None,
            alg,
            key,
        };
        Self::build_from_entries(&[entry], 0, true, false, boot_salt)
            .expect("single static key always fits in 16 slots")
    }

    /// Builds a hardware-aligned [`JwksKeyTable`] from up to 16 key specifications.
    pub fn build_from_entries(
        entries: &[KeyEntrySpec],
        primary_slot: u8,
        wildcard_single: bool,
        external_mode: bool,
        boot_salt: &[u8; 32],
    ) -> Result<Self, PluginError> {
        if entries.is_empty() {
            let mut empty = Self::empty();
            if external_mode {
                empty.dir.flags |= DIR_FLAG_EXTERNAL_MODE;
            }
            return Ok(empty);
        }

        let count = entries.len().min(MAX_JWKS_SLOTS);
        let total_bytes: usize = entries[..count]
            .iter()
            .map(|e| e.key.raw_bytes().len())
            .sum();
        let mut arena = Vec::with_capacity(total_bytes);

        let mut dir = DirectoryLine::empty();
        dir.primary_slot = (primary_slot as usize).min(count - 1) as u8;
        if wildcard_single && count == 1 {
            dir.flags |= DIR_FLAG_WILDCARD_SINGLE;
        }
        if external_mode {
            dir.flags |= DIR_FLAG_EXTERNAL_MODE;
        }

        let mut kid_ints = [0u64; MAX_JWKS_SLOTS];
        let mut key_tags = [0u64; MAX_JWKS_SLOTS];
        let mut key_spans = [KeySpan::default(); MAX_JWKS_SLOTS];
        let mut hmac_arr: Option<[Option<hmac::Key>; MAX_JWKS_SLOTS]> = None;

        let mut ver_acc: u64 = 0xcbf2_9ce4_8422_2325;

        for (idx, entry) in entries[..count].iter().enumerate() {
            if !entry.key.is_compatible_with(entry.alg) {
                return Err(PluginError::Config(format!(
                    "Cryptographic key material at slot {idx} is incompatible with algorithm '{}'",
                    entry.alg.as_str()
                )));
            }

            let raw_bytes = entry.key.raw_bytes();
            let offset = u32::try_from(arena.len())
                .map_err(|_| PluginError::Config("Key Byte Arena exceeded 4 GiB offset".into()))?;
            let len = u16::try_from(raw_bytes.len()).map_err(|_| {
                PluginError::Config("Individual key material exceeded 64 KiB".into())
            })?;
            let sig_len = u16::try_from(entry.key.expected_sig_len()).unwrap_or(u16::MAX);
            arena.extend_from_slice(raw_bytes);

            let kid_u64 = match entry.raw_kid.as_deref() {
                Some(k) if !k.is_empty() => hash_kid_salted(k.as_bytes(), boot_salt),
                _ => hash_kid_salted(format!("__slot_{idx}").as_bytes(), boot_salt),
            };

            let alg_u4 = entry.alg.to_u4() & 0x0F;
            let shift_bits = (idx * 4) as u32;
            dir.algs_packed =
                (dir.algs_packed & !(0x0Fu64 << shift_bits)) | ((alg_u4 as u64) << shift_bits);
            dir.tags[idx] = tag_from_kid_u64(kid_u64, dir.tag_shift);
            dir.valid_mask |= 1u16 << idx;

            let k_tag = compute_key_tag(kid_u64, alg_u4, raw_bytes);
            kid_ints[idx] = kid_u64;
            key_tags[idx] = k_tag;
            key_spans[idx] = KeySpan {
                offset,
                len,
                expected_sig_len: sig_len,
            };

            if let VerifierKey::Hmac { key, .. } = &entry.key {
                let arr = hmac_arr.get_or_insert_with(|| std::array::from_fn(|_| None));
                arr[idx] = Some(key.clone());
            }

            ver_acc = (ver_acc ^ k_tag).wrapping_mul(0x0100_0000_01b3);
        }

        ver_acc ^= (dir.valid_mask as u64) << 32;
        dir.table_version_tag = ver_acc.max(2);

        Ok(Self {
            dir,
            kid_ints: AlignedU64x16(kid_ints),
            key_tags: AlignedU64x16(key_tags),
            key_spans: AlignedSpans16(key_spans),
            key_bytes: arena.into_boxed_slice(),
            hmac_keys: hmac_arr.map(Box::new),
        })
    }

    /// Returns `true` when at least one key slot is populated.
    #[inline(always)]
    pub const fn has_keys(&self) -> bool {
        self.dir.valid_mask != 0
    }

    /// Extracts the 4-bit algorithm code (`0..14`) for `slot_idx` directly from L1D `algs_packed`.
    #[inline(always)]
    pub const fn alg_u4(&self, slot_idx: usize) -> u8 {
        if slot_idx >= MAX_JWKS_SLOTS {
            return ALG_INVALID;
        }
        ((self.dir.algs_packed >> ((slot_idx & 0x0F) * 4)) & 0x0F) as u8
    }

    /// Prefetches `kid_ints` L3 cache line when moving beyond L1D `DirectoryLine`.
    #[inline(always)]
    fn prefetch_l3_metadata(&self) {
        let ptr = &self.kid_ints as *const AlignedU64x16 as *const u8;
        #[cfg(target_arch = "x86_64")]
        unsafe {
            core::arch::x86_64::_mm_prefetch(ptr as *const i8, core::arch::x86_64::_MM_HINT_T2);
        }
        #[cfg(target_arch = "aarch64")]
        unsafe {
            core::arch::asm!(
                "prfm pldl3keep, [{0}]",
                in(reg) ptr,
                options(nostack, preserves_flags, readonly)
            );
        }
        #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
        let _ = ptr;
    }

    /// Resolves a slot index via L1D tag-bitmap scan followed by L3 integer `kid_u64` equality.
    #[inline(always)]
    pub fn resolve_slot_by_kid_u64(&self, kid_u64: u64) -> Result<usize, JwtVerifyError> {
        let valid = self.dir.valid_mask;
        if valid == 0 {
            return Err(JwtVerifyError::NoActiveKeys);
        }

        self.prefetch_l3_metadata();
        let needle = tag_from_kid_u64(kid_u64, self.dir.tag_shift);
        let mut candidate_mask = match_tag_mask16(&self.dir.tags, needle) & valid;
        while candidate_mask != 0 {
            let idx = candidate_mask.trailing_zeros() as usize;
            if self.kid_ints.0[idx] == kid_u64 {
                return Ok(idx);
            }
            candidate_mask &= candidate_mask - 1;
        }

        // Unpaired single static key (`popcnt(valid) == 1` + wildcard flag) accepts arbitrary `kid`
        if (valid & (valid - 1)) == 0 && (self.dir.flags & DIR_FLAG_WILDCARD_SINGLE) != 0 {
            return Ok(self.dir.primary_slot as usize);
        }

        Err(JwtVerifyError::UnknownKid)
    }

    /// Returns the primary active slot index (used when JWT omits JOSE `kid`).
    #[inline(always)]
    pub fn primary_slot_idx(&self) -> Result<usize, JwtVerifyError> {
        let valid = self.dir.valid_mask;
        if valid == 0 {
            return Err(JwtVerifyError::NoActiveKeys);
        }
        let idx = (self.dir.primary_slot as usize) & (MAX_JWKS_SLOTS - 1);
        if (valid & (1u16 << idx)) != 0 {
            Ok(idx)
        } else {
            Ok(valid.trailing_zeros() as usize)
        }
    }

    /// Checks whether a cached L3 token entry's `key_tag` remains valid in this table.
    ///
    /// - Positive tokens carry their signing slot's `key_tag`: standby keys keep matching across rotation,
    ///   while retired or modified keys miss immediately.
    /// - Unbound rejects (e.g. `UnknownKid`) carry `table_version_tag`, invalidating on any table update.
    #[inline(always)]
    pub fn is_cache_tag_valid(&self, cached_tag: u64) -> bool {
        let valid = self.dir.valid_mask;
        if valid == 0 {
            return false;
        }
        if cached_tag == self.dir.table_version_tag {
            return true;
        }
        // Fast path: 99.9% active traffic hits the primary key tag in 1 comparison
        let p_idx = (self.dir.primary_slot as usize) & (MAX_JWKS_SLOTS - 1);
        if self.key_tags.0[p_idx] == cached_tag && (valid & (1u16 << p_idx)) != 0 {
            return true;
        }
        let mut standby_mask = valid & !(1u16 << p_idx);
        while standby_mask != 0 {
            let idx = standby_mask.trailing_zeros() as usize;
            if self.key_tags.0[idx] == cached_tag {
                return true;
            }
            standby_mask &= standby_mask - 1;
        }
        false
    }

    /// Returns contiguous verification key bytes for `slot_idx` from the Key Byte Arena.
    #[inline(always)]
    pub fn key_bytes_for(&self, slot_idx: usize) -> (&[u8], usize) {
        let span = self.key_spans.0[slot_idx];
        let start = span.offset as usize;
        let end = start + (span.len as usize);
        (&self.key_bytes[start..end], span.expected_sig_len as usize)
    }

    /// Returns optional precomputed HMAC key handle for `slot_idx`.
    #[inline(always)]
    pub fn hmac_key_for(&self, slot_idx: usize) -> Option<&hmac::Key> {
        self.hmac_keys
            .as_deref()
            .and_then(|arr| arr[slot_idx].as_ref())
    }
}

/// Scans a 16-byte tag directory inside L1D `DirectoryLine` returning a 16-bit lane match mask.
#[inline(always)]
fn match_tag_mask16(tags: &[u8; MAX_JWKS_SLOTS], needle: u8) -> u16 {
    #[cfg(all(target_arch = "x86_64", target_feature = "sse2"))]
    unsafe {
        use core::arch::x86_64::{
            __m128i, _mm_cmpeq_epi8, _mm_loadu_si128, _mm_movemask_epi8, _mm_set1_epi8,
        };
        let vec = _mm_loadu_si128(tags.as_ptr() as *const __m128i);
        let target = _mm_set1_epi8(needle as i8);
        _mm_movemask_epi8(_mm_cmpeq_epi8(vec, target)) as u16
    }

    #[cfg(target_arch = "aarch64")]
    unsafe {
        use core::arch::aarch64::{vceqq_u8, vdupq_n_u8, vld1q_u8};
        const WEIGHTS_U64: u64 = 0x8040_2010_0804_0201;
        let vec = vld1q_u8(tags.as_ptr());
        let cmp = vceqq_u8(vec, vdupq_n_u8(needle));
        let mut bytes = [0u8; 16];
        core::arch::aarch64::vst1q_u8(bytes.as_mut_ptr(), cmp);
        let lo = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
        let hi = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
        let _ = WEIGHTS_U64;
        let m_lo = ((lo & 0x8080_8080_8080_8080).wrapping_mul(0x0002_0408_1020_4081) >> 56) as u16;
        let m_hi = ((hi & 0x8080_8080_8080_8080).wrapping_mul(0x0002_0408_1020_4081) >> 56) as u16;
        m_lo | (m_hi << 8)
    }

    #[cfg(not(any(
        all(target_arch = "x86_64", target_feature = "sse2"),
        target_arch = "aarch64"
    )))]
    {
        let mut mask = 0u16;
        let spread = 0x0101_0101_0101_0101u64.wrapping_mul(needle as u64);
        for chunk in 0..2 {
            let word = u64::from_le_bytes(tags[chunk * 8..(chunk + 1) * 8].try_into().unwrap());
            let x = word ^ spread;
            let zero_bytes = x.wrapping_sub(0x0101_0101_0101_0101) & !x & 0x8080_8080_8080_8080;
            let lane_bits = (zero_bytes.wrapping_mul(0x0002_0408_1020_4081) >> 56) as u16;
            mask |= (lane_bits & 0xFF) << (chunk * 8);
        }
        mask
    }
}

/// Plugin-internal lock-free JWKS store wrapping [`JwksKeyTable`].
pub struct JwksStore {
    inner: ArcSwap<JwksKeyTable>,
}

impl JwksStore {
    /// Initializes store with an initial [`JwksKeyTable`].
    pub fn new(initial_table: JwksKeyTable) -> Self {
        Self {
            inner: ArcSwap::from_pointee(initial_table),
        }
    }

    /// Lock-free read guard on active [`JwksKeyTable`].
    #[inline(always)]
    pub fn load(&self) -> arc_swap::Guard<Arc<JwksKeyTable>> {
        self.inner.load()
    }

    /// Clones active `Arc<JwksKeyTable>`.
    #[inline(always)]
    pub fn load_full(&self) -> Arc<JwksKeyTable> {
        self.inner.load_full()
    }

    /// Atomically swaps in `new_table` only when key material, masks, or algorithms changed.
    pub fn update(&self, new_table: JwksKeyTable) -> bool {
        let cur = self.inner.load();
        if cur.dir.table_version_tag == new_table.dir.table_version_tag
            && cur.dir.valid_mask == new_table.dir.valid_mask
            && cur.dir.algs_packed == new_table.dir.algs_packed
            && cur.key_tags.0 == new_table.key_tags.0
            && cur.key_bytes == new_table.key_bytes
        {
            return false;
        }
        self.inner.store(Arc::new(new_table));
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_salted_kid_lookup_standby_retention_and_material_change_invalidation() {
        let salt = [0x42u8; 32];
        let k1 = VerifierKey::new(JwtAlgorithm::HS256, Some("secret-1"), None).unwrap();
        let k2 =
            VerifierKey::new(JwtAlgorithm::HS512, Some("secret-2-very-long-key"), None).unwrap();
        let k1_new = VerifierKey::new(JwtAlgorithm::HS256, Some("secret-1-rotated"), None).unwrap();

        let entries1 = vec![KeyEntrySpec {
            raw_kid: Some("kid-alpha".into()),
            alg: JwtAlgorithm::HS256,
            key: k1.clone(),
        }];
        let t1 = JwksKeyTable::build_from_entries(&entries1, 0, false, true, &salt).unwrap();
        let store = JwksStore::new(t1);

        let guard1 = store.load();
        let idx1 = guard1
            .resolve_slot_by_kid_u64(hash_kid_salted(b"kid-alpha", &salt))
            .unwrap();
        let tag_alpha = guard1.key_tags.0[idx1];
        assert_eq!(guard1.alg_u4(idx1), JwtAlgorithm::HS256.to_u4());
        drop(guard1);

        // Rotate: kid-beta becomes primary (Slot 0), kid-alpha stays standby (Slot 1)
        let entries2 = vec![
            KeyEntrySpec {
                raw_kid: Some("kid-beta".into()),
                alg: JwtAlgorithm::HS512,
                key: k2.clone(),
            },
            KeyEntrySpec {
                raw_kid: Some("kid-alpha".into()),
                alg: JwtAlgorithm::HS256,
                key: k1,
            },
        ];
        let t2 = JwksKeyTable::build_from_entries(&entries2, 0, false, true, &salt).unwrap();
        assert!(store.update(t2));

        let guard2 = store.load();
        // Standby kid-alpha cache tag stays valid across rotation!
        assert!(guard2.is_cache_tag_valid(tag_alpha));
        assert_eq!(guard2.alg_u4(0), JwtAlgorithm::HS512.to_u4());
        assert_eq!(guard2.alg_u4(1), JwtAlgorithm::HS256.to_u4());
        // Calling update again with identical table returns false (zero swap churn)
        let t2_dup = JwksKeyTable::build_from_entries(&entries2, 0, false, true, &salt).unwrap();
        drop(guard2);
        assert!(!store.update(t2_dup));

        // Rotating secret bytes under the SAME kid-alpha changes key_tag -> invalidates old cache
        let entries3 = vec![KeyEntrySpec {
            raw_kid: Some("kid-alpha".into()),
            alg: JwtAlgorithm::HS256,
            key: k1_new,
        }];
        let t3 = JwksKeyTable::build_from_entries(&entries3, 0, false, true, &salt).unwrap();
        assert!(store.update(t3));
        assert!(!store.load().is_cache_tag_valid(tag_alpha));
    }

    #[test]
    fn test_static_single_key_wildcard_kid_turns_off_with_standby() {
        let salt = [0x11u8; 32];
        let k = VerifierKey::new(JwtAlgorithm::HS256, Some("secret"), None).unwrap();
        let t_single = JwksKeyTable::with_static_key(k.clone(), JwtAlgorithm::HS256, &salt);
        assert!(
            t_single
                .resolve_slot_by_kid_u64(hash_kid_salted(b"any-kid", &salt))
                .is_ok()
        );

        let t_multi = JwksKeyTable::build_from_entries(
            &[
                KeyEntrySpec {
                    raw_kid: Some("default".into()),
                    alg: JwtAlgorithm::HS256,
                    key: k.clone(),
                },
                KeyEntrySpec {
                    raw_kid: Some("rot-1".into()),
                    alg: JwtAlgorithm::HS256,
                    key: k,
                },
            ],
            1,
            true,
            true,
            &salt,
        )
        .unwrap();
        assert_eq!(
            t_multi
                .resolve_slot_by_kid_u64(hash_kid_salted(b"unknown", &salt))
                .err(),
            Some(JwtVerifyError::UnknownKid)
        );
    }
}
