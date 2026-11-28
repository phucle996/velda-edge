//! Two-Tier Progressive Escalation Memory: L1 Single-Cache-Line 448-Bit Bitfilter + L2 64-Shard Exact `(client_ip, ja4)` CLOCK Arena.
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────────────────────┐
//! │            L1 448-BIT BITFILTER + EPOCH (STRICT 64B = 1 CACHE LINE) + L2 ARENA          │
//! ├─────────────────────────────────────────────────────────────────────────────────────────┤
//! │  mix64(client_ip, ja4)                                                                  │
//! │    │                                                                                    │
//! │    ▼ L1BitFilter: [AtomicU64; 7] (448b) + epoch_tick_100ms: AtomicU64 (8B) = 64 bytes   │
//! │    │   Lemire fast-range maps u32 halves into [0, 448) -> word = b >> 6, bit = b & 63   │
//! │    ├── L1 miss (0) AND not suspicious (score < delay_score):                            │
//! │    │     └──► Bypass L2 completely (1 cache-line load, zero writes, zero L2 contention) │
//! │    └── L1 hit (1) OR suspicious (score >= delay_score):                                 │
//! │          └──► L2 64-Shard CLOCK Arena (stores exact `client_ip: u128` + `ja4: u64`):    │
//! │                 ├── Exact match + active window:                                        │
//! │                 │     decayed_strike = strike - elapsed_100ms / decay_100ms             │
//! │                 │     suspicious -> strike = min(decayed + 1, 15), set L1 bits          │
//! │                 │     clean      -> keep decayed strike (stops mixed-request evasion)   │
//! │                 └── Miss -> if suspicious: insert/CLOCK evict + set L1 bits             │
//! └─────────────────────────────────────────────────────────────────────────────────────────┘
//! ```

use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::pre_upstream::bot_detection::config::BotEscalationConfig;

const L1_WORDS: usize = 7;
const L1_BITS: u64 = (L1_WORDS * 64) as u64;
const NUM_SHARDS: usize = 64;
const SET_WAYS: usize = 8;

/// Strict single 64-byte cache-line L1 Bloom-style bitfilter (`448` filter bits + `8`-byte epoch).
///
/// Clean requests only execute `AtomicU64::load(Relaxed)` within this single cache line, keeping it
/// in the MESI `Shared` state across all worker cores with zero cross-core cache bouncing.
#[repr(C, align(64))]
struct L1BitFilter {
    words: [AtomicU64; L1_WORDS],
    epoch_tick_100ms: AtomicU64,
}

const _: () = assert!(std::mem::size_of::<L1BitFilter>() == 64);
const _: () = assert!(std::mem::align_of::<L1BitFilter>() == 64);

impl L1BitFilter {
    fn new(now_tick_100ms: u64) -> Self {
        Self {
            words: std::array::from_fn(|_| AtomicU64::new(0)),
            epoch_tick_100ms: AtomicU64::new(now_tick_100ms),
        }
    }

    #[inline(always)]
    fn probe_coords(key_hash: u64) -> (usize, u64, usize, u64) {
        let b1 = (((key_hash & 0xffff_ffff).wrapping_mul(L1_BITS)) >> 32) as usize;
        let b2 = ((((key_hash >> 32) & 0xffff_ffff).wrapping_mul(L1_BITS)) >> 32) as usize;
        (b1 >> 6, 1u64 << (b1 & 63), b2 >> 6, 1u64 << (b2 & 63))
    }

    #[inline(always)]
    fn maybe_present(&self, key_hash: u64, now_tick_100ms: u64, window_ticks: u64) -> bool {
        let epoch = self.epoch_tick_100ms.load(Ordering::Relaxed);
        if now_tick_100ms.saturating_sub(epoch) >= window_ticks {
            // Best-effort generation clear on window rollover so stale bits do not saturate 448 bits.
            if self
                .epoch_tick_100ms
                .compare_exchange(epoch, now_tick_100ms, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                for w in &self.words {
                    w.store(0, Ordering::Relaxed);
                }
            }
            return false;
        }

        let (w1, m1, w2, m2) = Self::probe_coords(key_hash);
        let v1 = self.words[w1].load(Ordering::Relaxed);
        if (v1 & m1) == 0 {
            return false;
        }
        let v2 = if w2 == w1 {
            v1
        } else {
            self.words[w2].load(Ordering::Relaxed)
        };
        (v2 & m2) != 0
    }

    #[inline(always)]
    fn mark(&self, key_hash: u64) {
        let (w1, m1, w2, m2) = Self::probe_coords(key_hash);
        if w1 == w2 {
            let mask = m1 | m2;
            if (self.words[w1].load(Ordering::Relaxed) & mask) != mask {
                self.words[w1].fetch_or(mask, Ordering::Relaxed);
            }
        } else {
            if (self.words[w1].load(Ordering::Relaxed) & m1) == 0 {
                self.words[w1].fetch_or(m1, Ordering::Relaxed);
            }
            if (self.words[w2].load(Ordering::Relaxed) & m2) == 0 {
                self.words[w2].fetch_or(m2, Ordering::Relaxed);
            }
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct L2Slot {
    client_ip: u128,
    ja4: u64,
    last_tick_100ms: u32,
    strike: u8,
    occupied: bool,
}

#[derive(Debug, Clone)]
struct L2Set {
    slots: [L2Slot; SET_WAYS],
    ref_mask: u8,
    clock_hand: u8,
}

impl Default for L2Set {
    fn default() -> Self {
        Self {
            slots: [L2Slot::default(); SET_WAYS],
            ref_mask: 0,
            clock_hand: 0,
        }
    }
}

struct L2Shard {
    sets: Box<[L2Set]>,
    set_mask: usize,
}

#[derive(Clone, Copy)]
struct L2Timing {
    now_tick: u32,
    window_ticks: u32,
    decay_ticks: u32,
}

impl L2Shard {
    fn new(min_slots: usize) -> Self {
        let num_sets = min_slots.div_ceil(SET_WAYS).max(1).next_power_of_two();
        Self {
            sets: vec![L2Set::default(); num_sets].into_boxed_slice(),
            set_mask: num_sets - 1,
        }
    }

    /// Evaluates and updates L2 state cleanly for exact `(client_ip, ja4)`.
    /// Returns `(current_strike, is_repeat_offender)`.
    fn record_or_check(
        &mut self,
        key_hash: u64,
        client_ip: u128,
        ja4: u64,
        suspicious: bool,
        timing: L2Timing,
    ) -> (u8, bool) {
        let now_tick_100ms = timing.now_tick;
        let window_ticks = timing.window_ticks;
        let decay_ticks = timing.decay_ticks;
        let set_idx = ((key_hash >> 6) as usize) & self.set_mask;
        let set = &mut self.sets[set_idx];

        let mut empty_way: Option<usize> = None;
        let mut expired_way: Option<usize> = None;

        for way in 0..SET_WAYS {
            let slot = &mut set.slots[way];
            if !slot.occupied {
                if empty_way.is_none() {
                    empty_way = Some(way);
                }
                continue;
            }

            let elapsed = now_tick_100ms.wrapping_sub(slot.last_tick_100ms);
            if elapsed >= window_ticks {
                if slot.client_ip == client_ip && slot.ja4 == ja4 {
                    if !suspicious {
                        slot.occupied = false;
                        return (0, false);
                    }
                    slot.last_tick_100ms = now_tick_100ms;
                    slot.strike = 0;
                    set.ref_mask |= 1u8 << way;
                    return (0, true);
                }
                if expired_way.is_none() {
                    expired_way = Some(way);
                }
                continue;
            }

            if slot.client_ip == client_ip && slot.ja4 == ja4 {
                let decay_steps = (elapsed / decay_ticks.max(1)).min(15) as u8;
                let decayed = slot.strike.saturating_sub(decay_steps);
                set.ref_mask |= 1u8 << way;

                if suspicious {
                    let next = (decayed.saturating_add(1)).min(15);
                    slot.strike = next;
                    slot.last_tick_100ms = now_tick_100ms;
                    return (next, true);
                } else {
                    // Preserve active penalty while honoring decay so mixed clean/dirty traffic still slows down.
                    return (decayed, decayed > 0);
                }
            }
        }

        if !suspicious {
            return (0, false);
        }

        // First active strike for this `(client_ip, ja4)` inside window: allocate empty, expired, or CLOCK-evicted slot.
        let target_way = if let Some(w) = empty_way {
            w
        } else if let Some(w) = expired_way {
            w
        } else {
            let mut hand = set.clock_hand as usize;
            let chosen = loop {
                let bit = 1u8 << hand;
                if (set.ref_mask & bit) == 0 {
                    break hand;
                }
                set.ref_mask &= !bit;
                hand = (hand + 1) & (SET_WAYS - 1);
            };
            set.clock_hand = ((chosen + 1) & (SET_WAYS - 1)) as u8;
            chosen
        };

        set.slots[target_way] = L2Slot {
            client_ip,
            ja4,
            last_tick_100ms: now_tick_100ms,
            strike: 0,
            occupied: true,
        };
        set.ref_mask |= 1u8 << target_way;
        (0, true)
    }
}

/// Bounded L1 (448-bit Cache-Line Bitfilter + 8B Epoch = 64B) + L2 (64-Shard Exact `(client_ip, ja4)` Arena).
pub(crate) struct EscalationArena {
    l1: L1BitFilter,
    shards: Box<[Mutex<L2Shard>]>,
    window_ticks_100ms: u32,
    decay_ticks_100ms: u32,
}

impl std::fmt::Debug for EscalationArena {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EscalationArena")
            .field("shards", &self.shards.len())
            .field("window_ticks_100ms", &self.window_ticks_100ms)
            .field("decay_ticks_100ms", &self.decay_ticks_100ms)
            .finish()
    }
}

impl EscalationArena {
    pub(crate) fn new(cfg: &BotEscalationConfig) -> Self {
        let now_tick = monotonic_tick_100ms();
        let per_shard = (cfg.max_entries / NUM_SHARDS).max(SET_WAYS);
        let shards = (0..NUM_SHARDS)
            .map(|_| Mutex::new(L2Shard::new(per_shard)))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            l1: L1BitFilter::new(now_tick),
            shards,
            window_ticks_100ms: (cfg.window_secs * 10).min(u32::MAX as u64) as u32,
            decay_ticks_100ms: (cfg.decay_secs * 10).min(u32::MAX as u64) as u32,
        }
    }

    /// Evaluates L1 448-bit filter first; touches L2 exact arena only when `suspicious` or L1 bit hits.
    ///
    /// Returns `(strike_level_0_to_15, has_active_history)`.
    #[inline]
    pub(crate) fn observe(
        &self,
        client_ip: u128,
        ja4: u64,
        suspicious: bool,
        now_tick_100ms: u64,
    ) -> (u8, bool) {
        let h = mix_ip_ja4(client_ip, ja4);
        let maybe_in_l2 = self
            .l1
            .maybe_present(h, now_tick_100ms, self.window_ticks_100ms as u64);

        if !suspicious && !maybe_in_l2 {
            return (0, false);
        }

        let shard_idx = (h as usize) & (NUM_SHARDS - 1);
        let (strike, active) = {
            let mut shard = self.shards[shard_idx].lock().unwrap();
            shard.record_or_check(
                h,
                client_ip,
                ja4,
                suspicious,
                L2Timing {
                    now_tick: now_tick_100ms as u32,
                    window_ticks: self.window_ticks_100ms,
                    decay_ticks: self.decay_ticks_100ms,
                },
            )
        };

        if suspicious || active {
            self.l1.mark(h);
        }
        (strike, active)
    }
}

/// Returns coarse monotonic `100ms` ticks from `velda_core::clock`.
#[inline(always)]
pub(crate) fn monotonic_tick_100ms() -> u64 {
    velda_core::clock::now_unix_nanos() / 100_000_000
}

#[inline(always)]
fn mix_ip_ja4(client_ip: u128, ja4: u64) -> u64 {
    let mut x =
        (client_ip as u64) ^ ((client_ip >> 64) as u64).rotate_left(17) ^ ja4.rotate_left(31);
    x ^= x >> 30;
    x = x.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^= x >> 31;
    x
}
