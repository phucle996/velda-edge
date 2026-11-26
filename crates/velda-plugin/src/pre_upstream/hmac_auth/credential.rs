//! Zero-Hash Static Credential Lookup via Distinguishing Bit Extraction & AGU Scaling (`hmac_auth::credential`).
//!
//! Credentials are immutable across the lifetime of an [`HmacAuthPlugin`](super::HmacAuthPlugin)
//! instance and replaced atomically only on plugin config reload. Rather than hashing
//! `access_key_id` per request in a HashMap, init-time analysis extracts the minimal
//! distinguishing bits across the static key set so runtime lookup computes `idx` purely
//! via bit-shift / bit-mask (`idx << 3` AGU pointer indexing) into sparse `Option<Box<CredentialEntry>>`
//! slots without requiring contiguous inline storage of credential payloads.
//!
//! ```text
//!   access_key_id: &[u8]
//!     ├─► K == 1: Direct compare against single Box<CredentialEntry> (0 index math)
//!     ├─► BitWindow (contiguous bits in byte/u64):
//!     │     idx = ((extract_word(key) >> shift) & mask) ─► slots[idx] via AGU [base + idx*8]
//!     ├─► BitGather (minimal distinguishing bit positions across 1..=8 byte offsets):
//!     │     [x86_64 + bmi2] _pext_u64(packed_bytes, bitmask)
//!     │     [portable]      unrolled bit-mask & shift gather
//!     │     ─► slots[idx & mask] ─► compare exact access_key_id bytes
//!     └─► RadixBuckets (large/dense fallback):
//!           [(len ^ b0 ^ b_last) & mask] ─► Box<[CredentialEntry]> scan (0 hash loop)
//! ```

use http::header::HeaderValue;
use ring::hmac;

/// Pre-compiled credential record allocated per entry (does not require contiguous array layout).
pub struct CredentialEntry {
    pub access_key_id: Box<[u8]>,
    pub key: hmac::Key,
    pub consumer_header: Option<HeaderValue>,
}

/// Sparse pointer slot: `8` bytes (`Option<Box<CredentialEntry>>` null-pointer optimization).
type Slot = Option<Box<CredentialEntry>>;

enum DispatchPlan {
    Empty,
    Single(Box<CredentialEntry>),
    /// Contiguous bit window extracted from one byte at `byte_pos` (index into key).
    ByteWindow {
        byte_pos: usize,
        shift: u8,
        mask: usize,
        slots: Box<[Slot]>,
    },
    /// Contiguous bit window extracted from bytes from end (`key.len() - 1 - rev_pos`).
    TailByteWindow {
        rev_pos: usize,
        shift: u8,
        mask: usize,
        slots: Box<[Slot]>,
    },
    /// Up to 8 selected byte offsets packed into `u64`, reduced by minimal distinguishing `bitmask`.
    BitGather {
        offsets: [u8; 8],
        num_offsets: u8,
        bitmask: u64,
        mask: usize,
        slots: Box<[Slot]>,
    },
    /// Multi-entry bucket fallback when > 10 distinguishing bits would exceed `1024` pointer slots.
    RadixBuckets {
        pos: usize,
        mask: usize,
        buckets: Box<[Option<Box<[CredentialEntry]>>]>,
    },
}

/// Static immutable credential lookup table swapped atomically on plugin reload.
pub struct CredentialTable {
    plan: DispatchPlan,
}

impl CredentialTable {
    /// Compiles static credentials into the tightest zero-hash AGU index plan.
    pub fn build(mut entries: Vec<CredentialEntry>) -> Self {
        let n = entries.len();
        if n == 0 {
            return Self {
                plan: DispatchPlan::Empty,
            };
        }
        if n == 1 {
            return Self {
                plan: DispatchPlan::Single(Box::new(entries.pop().unwrap())),
            };
        }

        let min_len = entries
            .iter()
            .map(|e| e.access_key_id.len())
            .min()
            .unwrap_or(0);

        // 1. Try a single byte position from start (`pos < min_len`) with contiguous shift + mask
        let min_bits = (n.next_power_of_two().trailing_zeros() as u8).max(1);
        for bits in min_bits..=8 {
            let table_len = 1usize << bits;
            let mask = table_len - 1;
            for pos in 0..min_len {
                for shift in 0..=(8 - bits) {
                    if is_collision_free(&entries, |k| ((k[pos] >> shift) as usize) & mask) {
                        let slots = place_into_slots(entries, table_len, |k| {
                            ((k[pos] >> shift) as usize) & mask
                        });
                        return Self {
                            plan: DispatchPlan::ByteWindow {
                                byte_pos: pos,
                                shift,
                                mask,
                                slots,
                            },
                        };
                    }
                }
            }
            // Also try from the tail (`rev_pos` bytes from end), very common for `client_01..client_99`
            for rev_pos in 0..min_len {
                for shift in 0..=(8 - bits) {
                    if is_collision_free(&entries, |k| {
                        ((k[k.len() - 1 - rev_pos] >> shift) as usize) & mask
                    }) {
                        let slots = place_into_slots(entries, table_len, |k| {
                            ((k[k.len() - 1 - rev_pos] >> shift) as usize) & mask
                        });
                        return Self {
                            plan: DispatchPlan::TailByteWindow {
                                rev_pos,
                                shift,
                                mask,
                                slots,
                            },
                        };
                    }
                }
            }
        }

        // 2. Greedy minimal distinguishing bit selection across up to 8 differing byte offsets (< min_len)
        if min_len > 0
            && let Some((offsets, num_offsets, bitmask, bits)) =
                find_bit_gather_plan(&entries, min_len)
        {
            let table_len = 1usize << bits;
            let mask = table_len - 1;
            let slots = place_into_slots(entries, table_len, |k| {
                let word = pack_bytes(k, &offsets, num_offsets);
                gather_bits(word, bitmask) & mask
            });
            return Self {
                plan: DispatchPlan::BitGather {
                    offsets,
                    num_offsets,
                    bitmask,
                    mask,
                    slots,
                },
            };
        }

        // 3. Universal non-hashing fallback by length + distinguishing byte bits into small buckets
        let best_pos = if min_len > 0 {
            best_byte_pos(&entries, min_len)
        } else {
            0
        };
        let table_len = (n * 2).next_power_of_two().clamp(16, 1024);
        let mask = table_len - 1;
        let mut grouped: Vec<Vec<CredentialEntry>> = (0..table_len).map(|_| Vec::new()).collect();
        for entry in entries {
            let idx = radix_idx(&entry.access_key_id, best_pos, mask);
            grouped[idx].push(entry);
        }
        let buckets = grouped
            .into_iter()
            .map(|v| {
                if v.is_empty() {
                    None
                } else {
                    Some(v.into_boxed_slice())
                }
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();

        Self {
            plan: DispatchPlan::RadixBuckets {
                pos: best_pos,
                mask,
                buckets,
            },
        }
    }

    /// Resolves a credential entry in O(1) via AGU bit-shift indexing without runtime hashing.
    #[inline(always)]
    pub fn lookup(&self, access_key_id: &[u8]) -> Option<&CredentialEntry> {
        match &self.plan {
            DispatchPlan::Empty => None,
            DispatchPlan::Single(entry) => {
                (entry.access_key_id.as_ref() == access_key_id).then_some(entry.as_ref())
            }
            DispatchPlan::ByteWindow {
                byte_pos,
                shift,
                mask,
                slots,
            } => {
                let &b = access_key_id.get(*byte_pos)?;
                let idx = ((b >> *shift) as usize) & *mask;
                let entry = slots[idx].as_deref()?;
                (entry.access_key_id.as_ref() == access_key_id).then_some(entry)
            }
            DispatchPlan::TailByteWindow {
                rev_pos,
                shift,
                mask,
                slots,
            } => {
                let len = access_key_id.len();
                if len <= *rev_pos {
                    return None;
                }
                let idx = ((access_key_id[len - 1 - *rev_pos] >> *shift) as usize) & *mask;
                let entry = slots[idx].as_deref()?;
                (entry.access_key_id.as_ref() == access_key_id).then_some(entry)
            }
            DispatchPlan::BitGather {
                offsets,
                num_offsets,
                bitmask,
                mask,
                slots,
            } => {
                let last_off = offsets[(*num_offsets as usize).saturating_sub(1)] as usize;
                if access_key_id.len() <= last_off {
                    return None;
                }
                let word = pack_bytes(access_key_id, offsets, *num_offsets);
                let idx = gather_bits(word, *bitmask) & *mask;
                let entry = slots[idx].as_deref()?;
                (entry.access_key_id.as_ref() == access_key_id).then_some(entry)
            }
            DispatchPlan::RadixBuckets { pos, mask, buckets } => {
                let idx = radix_idx(access_key_id, *pos, *mask);
                let bucket = buckets[idx].as_deref()?;
                bucket
                    .iter()
                    .find(|entry| entry.access_key_id.as_ref() == access_key_id)
            }
        }
    }
}

#[inline(always)]
fn radix_idx(key: &[u8], pos: usize, mask: usize) -> usize {
    let b0 = key.get(pos).copied().unwrap_or(0) as usize;
    let blast = key.last().copied().unwrap_or(0) as usize;
    (b0 ^ (blast << 3) ^ (key.len() << 1)) & mask
}

#[inline(always)]
fn pack_bytes(key: &[u8], offsets: &[u8; 8], num_offsets: u8) -> u64 {
    let mut w = 0u64;
    let mut i = 0usize;
    let n = num_offsets as usize;
    while i < n {
        let off = offsets[i] as usize;
        // Guarded by caller checking `access_key_id.len() > max_offset`, `.get` stays safe.
        let b = key.get(off).copied().unwrap_or(0) as u64;
        w |= b << (i * 8);
        i += 1;
    }
    w
}

/// Extracts bits selected by `bitmask` into contiguous LSB positions (`[0..popcnt)`).
#[inline(always)]
fn gather_bits(word: u64, bitmask: u64) -> usize {
    #[cfg(all(target_arch = "x86_64", target_feature = "bmi2"))]
    {
        // SAFETY: `bmi2` is verified statically via `#[cfg(target_feature = "bmi2")]`.
        unsafe { core::arch::x86_64::_pext_u64(word, bitmask) as usize }
    }
    #[cfg(not(all(target_arch = "x86_64", target_feature = "bmi2")))]
    {
        let mut mask = bitmask;
        let mut out = 0usize;
        let mut out_bit = 0u32;
        while mask != 0 {
            let tz = mask.trailing_zeros();
            out |= (((word >> tz) & 1) as usize) << out_bit;
            out_bit += 1;
            mask &= mask - 1;
        }
        out
    }
}

fn is_collision_free<F: Fn(&[u8]) -> usize>(entries: &[CredentialEntry], idx_fn: F) -> bool {
    let mut seen = Vec::with_capacity(entries.len());
    for entry in entries {
        let idx = idx_fn(&entry.access_key_id);
        if seen.contains(&idx) {
            return false;
        }
        seen.push(idx);
    }
    true
}

fn place_into_slots<F: Fn(&[u8]) -> usize>(
    entries: Vec<CredentialEntry>,
    table_len: usize,
    idx_fn: F,
) -> Box<[Slot]> {
    let mut slots: Vec<Slot> = (0..table_len).map(|_| None).collect();
    for entry in entries {
        let idx = idx_fn(&entry.access_key_id);
        slots[idx] = Some(Box::new(entry));
    }
    slots.into_boxed_slice()
}

fn best_byte_pos(entries: &[CredentialEntry], min_len: usize) -> usize {
    let mut best_pos = 0usize;
    let mut best_unique = 0usize;
    for pos in 0..min_len.min(64) {
        let mut present = [false; 256];
        let mut count = 0usize;
        for e in entries {
            let b = e.access_key_id[pos] as usize;
            if !present[b] {
                present[b] = true;
                count += 1;
            }
        }
        if count > best_unique {
            best_unique = count;
            best_pos = pos;
        }
    }
    best_pos
}

/// Selects up to 8 varying byte positions (< min_len, capped at offset 255) and up to 10
/// distinguishing bits separating all entries into unique slots.
fn find_bit_gather_plan(
    entries: &[CredentialEntry],
    min_len: usize,
) -> Option<([u8; 8], u8, u64, u8)> {
    const MAX_SLOTS_BITS: usize = 10; // <= 1024 pointer slots (8 KB)
    let limit = min_len.min(256);

    // Collect candidate byte offsets where keys actually differ, ranked by distinct byte count
    let mut byte_scores: Vec<(usize, usize)> = Vec::new();
    for pos in 0..limit {
        let first = entries[0].access_key_id[pos];
        let mut seen = [false; 256];
        let mut distinct = 0usize;
        for e in entries {
            let b = e.access_key_id[pos];
            if !seen[b as usize] {
                seen[b as usize] = true;
                distinct += 1;
            }
        }
        if distinct > 1 || entries.iter().any(|e| e.access_key_id[pos] != first) {
            byte_scores.push((distinct, pos));
        }
    }
    if byte_scores.is_empty() {
        return None;
    }
    byte_scores.sort_unstable_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.cmp(&b.1)));

    let mut offsets = [0u8; 8];
    let num_offsets = byte_scores.len().min(8);
    let mut chosen: Vec<usize> = byte_scores
        .iter()
        .take(num_offsets)
        .map(|&(_, p)| p)
        .collect();
    // Sort selected offsets ascending so runtime check `len > offsets[num_offsets - 1]` covers all
    chosen.sort_unstable();
    for (i, &pos) in chosen.iter().enumerate() {
        offsets[i] = pos as u8;
    }

    let words: Vec<u64> = entries
        .iter()
        .map(|e| pack_bytes(&e.access_key_id, &offsets, num_offsets as u8))
        .collect();

    // Greedy distinguishing bit selection (equivalence-class splitting)
    let mut bitmask = 0u64;
    let mut selected_bits = 0usize;
    let mut groups: Vec<Vec<usize>> = vec![(0..entries.len()).collect()];

    while groups.iter().any(|g| g.len() > 1) {
        if selected_bits >= MAX_SLOTS_BITS {
            return None;
        }

        let mut best_bit = None;
        let mut best_score = usize::MAX;
        let total_bits = num_offsets * 8;

        for bit in 0..total_bits {
            if (bitmask & (1u64 << bit)) != 0 {
                continue;
            }
            // Minimize max split bucket size sum-of-squares after adding `bit`
            let mut cost = 0usize;
            for g in &groups {
                if g.len() <= 1 {
                    continue;
                }
                let ones = g
                    .iter()
                    .filter(|&&idx| ((words[idx] >> bit) & 1) != 0)
                    .count();
                let zeros = g.len() - ones;
                cost += ones * ones + zeros * zeros;
            }
            if cost < best_score {
                best_score = cost;
                best_bit = Some(bit);
            }
        }

        let bit = best_bit?;
        bitmask |= 1u64 << bit;
        selected_bits += 1;

        let mut next_groups = Vec::with_capacity(groups.len() * 2);
        for g in groups {
            if g.len() <= 1 {
                next_groups.push(g);
                continue;
            }
            let (ones, zeros): (Vec<usize>, Vec<usize>) = g
                .into_iter()
                .partition(|&idx| ((words[idx] >> bit) & 1) != 0);
            if !zeros.is_empty() {
                next_groups.push(zeros);
            }
            if !ones.is_empty() {
                next_groups.push(ones);
            }
        }
        groups = next_groups;
    }

    Some((
        offsets,
        num_offsets as u8,
        bitmask,
        (selected_bits as u8).max(1),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mk_entry(kid: &str) -> CredentialEntry {
        CredentialEntry {
            access_key_id: kid.as_bytes().to_vec().into_boxed_slice(),
            key: hmac::Key::new(hmac::HMAC_SHA256, b"secret_key_for_unit_test_123"),
            consumer_header: Some(HeaderValue::from_str(kid).unwrap()),
        }
    }

    #[test]
    fn test_single_and_shared_prefix_keys_lookup() {
        let single = CredentialTable::build(vec![mk_entry("only_key")]);
        assert!(single.lookup(b"only_key").is_some());
        assert!(single.lookup(b"other_key").is_none());

        let multi = CredentialTable::build(vec![
            mk_entry("partner_alpha_01"),
            mk_entry("partner_alpha_02"),
            mk_entry("partner_beta_03"),
            mk_entry("partner_gamma_99"),
        ]);
        assert_eq!(
            multi
                .lookup(b"partner_alpha_01")
                .unwrap()
                .consumer_header
                .as_ref()
                .unwrap(),
            "partner_alpha_01"
        );
        assert_eq!(
            multi
                .lookup(b"partner_gamma_99")
                .unwrap()
                .consumer_header
                .as_ref()
                .unwrap(),
            "partner_gamma_99"
        );
        assert!(multi.lookup(b"partner_alpha_03").is_none());
        assert!(multi.lookup(b"short").is_none());
    }

    #[test]
    fn test_many_keys_varying_lengths_radix_fallback() {
        let entries: Vec<CredentialEntry> = (0..120)
            .map(|i| mk_entry(&format!("key_{i:04}_end")))
            .collect();
        let table = CredentialTable::build(entries);
        for i in 0..120 {
            let k = format!("key_{i:04}_end");
            assert!(table.lookup(k.as_bytes()).is_some(), "missing {k}");
        }
        assert!(table.lookup(b"key_9999_end").is_none());
    }
}
