//! Lock-Striped Flat Shard Index Table (`CacheShard`).
//!
//! Each shard owns two flat heap-allocated arrays indexed via AGU bitmask (`hash & SLOT_MASK`):
//!
//! ```text
//!   hash ──► tag = normalize_tag(hash) [0 reserved as empty]
//!        ──► base = tag & (SLOTS_PER_SHARD - 1)
//!        ──► probe p in 0..8: tags[(base + p) & SLOT_MASK]
//!              └─ tag match ──► AGU jump slots[idx]
//!                                 ├─ lookup_variant ─► negotiate_variant_kind ─► arena.pin_span
//!                                 └─ insert/evict   ─► release_spans() -> return slabs to pool
//! ```

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

use bytes::Bytes;
use http::StatusCode;
use velda_core::clock::coarse_sec;

use crate::http_cache::entity::{
    CacheEntry, CompressedVariants, StoreBytesParams, VARIANT_BIT_BROTLI, VARIANT_BIT_GZIP,
    VARIANT_BIT_ZSTD, VariantKind, negotiate_variant_kind,
};
use crate::http_cache::slab::{SlabArena, SlabPool, SpanHandle};

/// Number of lock-striped index shards (power of two).
pub const NUM_SHARDS: usize = 64;
pub(crate) const SHARD_MASK: usize = NUM_SHARDS - 1;

const SLOTS_PER_SHARD: usize = 1024;
const SLOT_MASK: usize = SLOTS_PER_SHARD - 1;
const PROBE_WINDOW: usize = 8;

/// Outcome of a fast-path cache variant lookup.
#[derive(Debug)]
pub enum LookupOutcome {
    Hit {
        status: StatusCode,
        payload: Bytes,
        content_type: Option<http::HeaderValue>,
        encoding: Option<http::HeaderValue>,
        age_secs: u64,
        has_variants: bool,
    },
    AuthBypass,
    Miss,
}

#[derive(Debug)]
struct IndexSlot {
    raw: SpanHandle,
    gzip: SpanHandle,
    brotli: SpanHandle,
    zstd: SpanHandle,
    content_type: Option<http::HeaderValue>,
    created_at_sec: u32,
    expires_at_sec: u32,
    status: StatusCode,
    variant_mask: u8,
    is_public: bool,
    accessed: AtomicBool,
}

impl Default for IndexSlot {
    fn default() -> Self {
        Self {
            raw: SpanHandle::default(),
            gzip: SpanHandle::default(),
            brotli: SpanHandle::default(),
            zstd: SpanHandle::default(),
            content_type: None,
            created_at_sec: 0,
            expires_at_sec: 0,
            status: StatusCode::OK,
            variant_mask: 0,
            is_public: false,
            accessed: AtomicBool::new(false),
        }
    }
}

impl IndexSlot {
    #[inline(always)]
    fn is_live(&self, arena: &SlabArena, now: u32) -> bool {
        if now >= self.expires_at_sec {
            return false;
        }
        [self.raw, self.gzip, self.brotli, self.zstd]
            .into_iter()
            .any(|s| s.is_present() && arena.generation(s.buf_id()) == s.generation)
    }

    fn release_spans(&mut self, pool: &mut SlabPool) {
        pool.release_span(self.raw);
        pool.release_span(self.gzip);
        pool.release_span(self.brotli);
        pool.release_span(self.zstd);
        *self = Self::default();
    }

    fn store_variant_spans(
        &mut self,
        variants: &CompressedVariants,
        expires_at_sec: u32,
        pool: &mut SlabPool,
    ) {
        let targets: [(&Option<Bytes>, &mut SpanHandle, u8); 3] = [
            (&variants.gzip, &mut self.gzip, VARIANT_BIT_GZIP),
            (&variants.brotli, &mut self.brotli, VARIANT_BIT_BROTLI),
            (&variants.zstd, &mut self.zstd, VARIANT_BIT_ZSTD),
        ];
        for (opt_bytes, slot_span, bit) in targets {
            if let Some(bytes) = opt_bytes.as_deref()
                && let Some(new_span) = pool.alloc_and_write(bytes, expires_at_sec)
            {
                pool.release_span(*slot_span);
                *slot_span = new_span;
                self.variant_mask |= bit;
            }
        }
    }
}

#[inline(always)]
fn normalize_tag(hash: u64) -> u64 {
    if hash == 0 { 1 } else { hash }
}

/// Open-addressed flat index table inside a single cache shard.
#[derive(Debug)]
struct ShardInner {
    tags: Box<[u64]>,
    slots: Box<[IndexSlot]>,
}

impl ShardInner {
    fn new() -> Self {
        let tags = vec![0u64; SLOTS_PER_SHARD].into_boxed_slice();
        let slots = (0..SLOTS_PER_SHARD)
            .map(|_| IndexSlot::default())
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self { tags, slots }
    }

    #[inline]
    fn find_slot_idx(&self, hash: u64) -> Option<usize> {
        let tag = normalize_tag(hash);
        let base = (tag as usize) & SLOT_MASK;
        for probe in 0..PROBE_WINDOW {
            let idx = (base + probe) & SLOT_MASK;
            if self.tags.get(idx) == Some(&tag) {
                return Some(idx);
            }
        }
        None
    }

    fn acquire_write_slot(&mut self, hash: u64, pool: &mut SlabPool, now: u32) -> (usize, bool) {
        let tag = normalize_tag(hash);
        let base = (tag as usize) & SLOT_MASK;
        let mut first_empty = None;
        let mut first_dead = None;

        for probe in 0..PROBE_WINDOW {
            let idx = (base + probe) & SLOT_MASK;
            let current_tag = self.tags.get(idx).copied().unwrap_or(0);
            if current_tag == tag {
                return (idx, true);
            }
            if current_tag == 0 {
                if first_empty.is_none() {
                    first_empty = Some(idx);
                }
            } else if first_dead.is_none()
                && self
                    .slots
                    .get(idx)
                    .is_some_and(|s| !s.is_live(&pool.arena, now))
            {
                first_dead = Some(idx);
            }
        }

        if let Some(empty_idx) = first_empty {
            if let Some(slot_tag) = self.tags.get_mut(empty_idx) {
                *slot_tag = tag;
            }
            return (empty_idx, false);
        }

        if let Some(dead_idx) = first_dead {
            if let Some(slot) = self.slots.get_mut(dead_idx) {
                slot.release_spans(pool);
            }
            if let Some(slot_tag) = self.tags.get_mut(dead_idx) {
                *slot_tag = tag;
            }
            return (dead_idx, false);
        }

        let mut victim_idx = base;
        for pass in 0..(PROBE_WINDOW * 2) {
            let idx = (base + pass) & SLOT_MASK;
            if let Some(slot) = self.slots.get(idx)
                && !slot.accessed.swap(false, Ordering::Relaxed)
            {
                victim_idx = idx;
                break;
            }
        }

        if let Some(slot) = self.slots.get_mut(victim_idx) {
            slot.release_spans(pool);
        }
        if let Some(slot_tag) = self.tags.get_mut(victim_idx) {
            *slot_tag = tag;
        }
        (victim_idx, false)
    }

    #[inline]
    fn lookup_variant(
        &self,
        hash: u64,
        arena: &Arc<SlabArena>,
        accept_encoding: Option<&http::HeaderValue>,
        bypass_auth: bool,
    ) -> LookupOutcome {
        let Some(idx) = self.find_slot_idx(hash) else {
            return LookupOutcome::Miss;
        };
        let Some(slot) = self.slots.get(idx) else {
            return LookupOutcome::Miss;
        };

        let now = coarse_sec() as u32;
        if now >= slot.expires_at_sec {
            return LookupOutcome::Miss;
        }

        if bypass_auth && !slot.is_public {
            return LookupOutcome::AuthBypass;
        }

        let Some(kind) =
            negotiate_variant_kind(slot.variant_mask, slot.raw.is_present(), accept_encoding)
        else {
            return LookupOutcome::Miss;
        };

        let span = match kind {
            VariantKind::Zstd => slot.zstd,
            VariantKind::Brotli => slot.brotli,
            VariantKind::Gzip => slot.gzip,
            VariantKind::Raw => slot.raw,
        };

        let Some(payload) = arena.pin_span(span) else {
            return LookupOutcome::Miss;
        };

        slot.accessed.store(true, Ordering::Relaxed);
        LookupOutcome::Hit {
            status: slot.status,
            payload,
            content_type: slot.content_type.clone(),
            encoding: kind.content_encoding_header(),
            age_secs: now.saturating_sub(slot.created_at_sec) as u64,
            has_variants: slot.variant_mask != 0,
        }
    }

    #[inline]
    fn get_entry(&self, hash: u64, arena: &Arc<SlabArena>) -> Option<CacheEntry> {
        let idx = self.find_slot_idx(hash)?;
        let slot = self.slots.get(idx)?;
        let now = coarse_sec() as u32;
        if now >= slot.expires_at_sec {
            return None;
        }

        let raw_body = arena.pin_span(slot.raw);
        let zstd = arena.pin_span(slot.zstd);
        let brotli = arena.pin_span(slot.brotli);
        let gzip = arena.pin_span(slot.gzip);

        if raw_body.is_none() && zstd.is_none() && brotli.is_none() && gzip.is_none() {
            return None;
        }

        slot.accessed.store(true, Ordering::Relaxed);
        Some(CacheEntry {
            key: Arc::from(""),
            status: slot.status,
            content_type: slot.content_type.clone(),
            raw_body,
            variants: CompressedVariants { gzip, brotli, zstd },
            is_public: slot.is_public,
            created_at_sec: slot.created_at_sec,
            expires_at_sec: slot.expires_at_sec,
            accessed: std::sync::atomic::AtomicBool::new(true),
        })
    }

    fn store_entry(&mut self, hash: u64, entry: CacheEntry, pool: &mut SlabPool) {
        let now = coarse_sec() as u32;
        let (idx, matched) = self.acquire_write_slot(hash, pool, now);
        let Some(slot) = self.slots.get_mut(idx) else {
            return;
        };
        if matched {
            slot.release_spans(pool);
        }

        let raw = entry
            .raw_body
            .as_deref()
            .and_then(|b| pool.alloc_and_write(b, entry.expires_at_sec))
            .unwrap_or_default();

        *slot = IndexSlot {
            raw,
            content_type: entry.content_type,
            created_at_sec: entry.created_at_sec,
            expires_at_sec: entry.expires_at_sec,
            status: entry.status,
            is_public: entry.is_public,
            accessed: AtomicBool::new(true),
            ..IndexSlot::default()
        };
        slot.store_variant_spans(&entry.variants, entry.expires_at_sec, pool);
    }

    fn store_bytes(&mut self, params: StoreBytesParams<'_>, pool: &mut SlabPool) {
        let now = coarse_sec() as u32;
        let ttl_secs = params.ttl.as_secs().max(1) as u32;
        let expires_at_sec = now.saturating_add(ttl_secs);

        let Some(new_span) = pool.alloc_and_write(params.payload, expires_at_sec) else {
            return;
        };

        let (idx, matched) = self.acquire_write_slot(params.hash, pool, now);
        let Some(slot) = self.slots.get_mut(idx) else {
            pool.release_span(new_span);
            return;
        };

        if matched && !slot.is_live(&pool.arena, now) {
            slot.release_spans(pool);
        }

        slot.status = params.status;
        if params.content_type.is_some() {
            slot.content_type = params.content_type;
        }
        slot.created_at_sec = now;
        slot.expires_at_sec = expires_at_sec;
        if params.is_public {
            slot.is_public = true;
        }

        match params.encoding {
            Some("gzip") => {
                pool.release_span(slot.gzip);
                slot.gzip = new_span;
                slot.variant_mask |= VARIANT_BIT_GZIP;
            }
            Some("br") => {
                pool.release_span(slot.brotli);
                slot.brotli = new_span;
                slot.variant_mask |= VARIANT_BIT_BROTLI;
            }
            Some("zstd") => {
                pool.release_span(slot.zstd);
                slot.zstd = new_span;
                slot.variant_mask |= VARIANT_BIT_ZSTD;
            }
            _ => {
                pool.release_span(slot.raw);
                slot.raw = new_span;
            }
        }
        slot.accessed.store(true, Ordering::Relaxed);
    }

    fn merge_variants(
        &mut self,
        hash: u64,
        variants: &CompressedVariants,
        pool: &mut SlabPool,
    ) -> bool {
        let Some(idx) = self.find_slot_idx(hash) else {
            return false;
        };
        let now = coarse_sec() as u32;
        let Some(slot) = self.slots.get_mut(idx) else {
            return false;
        };
        if !slot.is_live(&pool.arena, now) {
            return false;
        }

        let expires_at_sec = slot.expires_at_sec;
        slot.store_variant_spans(variants, expires_at_sec, pool);
        slot.accessed.store(true, Ordering::Relaxed);
        true
    }

    fn remove(&mut self, hash: u64, pool: &mut SlabPool) -> bool {
        let Some(idx) = self.find_slot_idx(hash) else {
            return false;
        };
        if let Some(tag) = self.tags.get_mut(idx) {
            *tag = 0;
        }
        if let Some(slot) = self.slots.get_mut(idx) {
            slot.release_spans(pool);
        }
        true
    }

    fn clear_slots(&mut self) {
        self.tags.fill(0);
        for slot in self.slots.iter_mut() {
            *slot = IndexSlot::default();
        }
    }
}

/// Lock-striped cache shard aligned to 64 bytes to prevent cross-core false sharing.
#[repr(align(64))]
#[derive(Debug)]
pub struct CacheShard {
    inner: RwLock<ShardInner>,
}

impl CacheShard {
    pub(crate) fn new() -> Self {
        Self {
            inner: RwLock::new(ShardInner::new()),
        }
    }

    #[inline]
    pub(crate) fn lookup_variant(
        &self,
        hash: u64,
        arena: &Arc<SlabArena>,
        accept_encoding: Option<&http::HeaderValue>,
        bypass_auth: bool,
    ) -> LookupOutcome {
        let Ok(guard) = self.inner.read() else {
            return LookupOutcome::Miss;
        };
        guard.lookup_variant(hash, arena, accept_encoding, bypass_auth)
    }

    #[inline]
    pub(crate) fn get_entry(&self, hash: u64, arena: &Arc<SlabArena>) -> Option<CacheEntry> {
        let guard = self.inner.read().ok()?;
        guard.get_entry(hash, arena)
    }

    #[inline]
    pub(crate) fn store_entry(&self, hash: u64, entry: CacheEntry, pool: &mut SlabPool) {
        if let Ok(mut guard) = self.inner.write() {
            guard.store_entry(hash, entry, pool);
        }
    }

    #[inline]
    pub(crate) fn store_bytes(&self, params: StoreBytesParams<'_>, pool: &mut SlabPool) {
        if let Ok(mut guard) = self.inner.write() {
            guard.store_bytes(params, pool);
        }
    }

    #[inline]
    pub(crate) fn merge_variants(
        &self,
        hash: u64,
        variants: &CompressedVariants,
        pool: &mut SlabPool,
    ) -> bool {
        if let Ok(mut guard) = self.inner.write() {
            guard.merge_variants(hash, variants, pool)
        } else {
            false
        }
    }

    pub(crate) fn remove(&self, hash: u64, pool: &mut SlabPool) -> bool {
        if let Ok(mut guard) = self.inner.write() {
            guard.remove(hash, pool)
        } else {
            false
        }
    }

    pub(crate) fn clear_slots(&self) {
        if let Ok(mut guard) = self.inner.write() {
            guard.clear_slots();
        }
    }
}
