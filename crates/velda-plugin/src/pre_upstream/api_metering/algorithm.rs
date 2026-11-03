//! High-performance in-memory algorithms for API metering, billing tiers, and monetization.
//!
//! # Objective & Mission
//! Manages tenant organization tiers (`free`, `pro`, `enterprise`), prepaid wallet balances,
//! and macro-level quotas (e.g. 1,000,000 requests/month, 50GB bandwidth) with authoritative
//! rejection (`402 Payment Required` or `429 Too Many Requests`).
//!
//! # Hardware & Performance Invariants
//! - Zero Float on Hot Path: All currency values are strictly integers in micro-credits ($10^{-6}$),
//!   completely eliminating IEEE-754 precision drift and rounding exploit vectors.
//! - Lock-Striped Cache-Aligned Sharding: 64 independent cache-aligned stripes (`#[repr(align(64))]`)
//!   distribute accounting operations across CPU cores with zero cross-core cache invalidations.
//! - Bounded Storage: Shards lazily prune stale accounts inactive for > 30 days to defend against
//!   cardinality bombs.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use rustc_hash::FxHashMap;

use crate::pre_upstream::consumer_quota::{QuotaAlgorithm, QuotaWindowConfig, WindowState};

/// Scaling factor for micro-credits (1 credit = 1,000,000 micro-credits).
pub const MICRO_CREDITS_PER_UNIT: u64 = 1_000_000;

/// Number of independent cache-aligned memory shards (power of 2).
const NUM_SHARDS: usize = 64;

/// Maximum tracked accounts per shard before lazy cleanup (~2 million accounts total).
pub const MAX_ACCOUNTS_PER_SHARD: usize = 32_768;

/// Inactivity threshold for pruning stale account states (30 days in seconds).
const ACCOUNT_INACTIVITY_TTL_SEC: u64 = 30 * 86_400;

/// Action taken when an account exhausts its tier quota or credit balance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ExhaustedAction {
    /// Return HTTP 402 Payment Required (Subscription or wallet exhausted).
    #[default]
    Reject402,
    /// Return HTTP 429 Too Many Requests (Temporary window cooldown).
    Reject429,
    /// Allow request through while recording debt (Pay-as-you-go overage billing).
    Passthrough,
}

impl ExhaustedAction {
    /// Parses an action from configuration string.
    pub fn parse(s: &str) -> Self {
        match s.trim().to_ascii_lowercase().as_str() {
            "429" | "reject_429" | "too_many_requests" => Self::Reject429,
            "allow" | "passthrough" | "overage" => Self::Passthrough,
            _ => Self::Reject402,
        }
    }
}

/// Static configuration for an individual service tier (e.g. "free", "pro", "enterprise").
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TierConfig {
    /// Canonical tier identifier (e.g. "free", "pro", "enterprise").
    pub name: String,
    /// Array of sliding or fixed quota windows bounding this tier.
    pub windows: Vec<QuotaWindowConfig>,
    /// Base cost deducted per request in micro-credits ($10^{-6}$).
    pub cost_per_req_micro_credits: u64,
    /// Policy executed when any window or credit balance is exhausted.
    pub exhausted_action: ExhaustedAction,
}

impl Default for TierConfig {
    fn default() -> Self {
        Self {
            name: "free".to_string(),
            windows: vec![QuotaWindowConfig {
                name: "monthly_cap".to_string(),
                window_seconds: 30 * 86_400,
                limit: 10_000,
                algorithm: QuotaAlgorithm::FixedWindow,
            }],
            cost_per_req_micro_credits: 0,
            exhausted_action: ExhaustedAction::Reject402,
        }
    }
}

/// In-memory state tracking an organization account's balance and windows.
#[derive(Debug, Clone)]
pub struct AccountState {
    pub last_access_sec: u64,
    /// Wallet credit balance in micro-credits (can become negative on overage pass-through).
    pub balance_micro_credits: i64,
    /// States corresponding to the tier's configured windows.
    pub windows: Vec<WindowState>,
}

/// Decision output resulting from evaluating an account against its tier policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MeteringDecision {
    /// Request permitted.
    Allowed {
        min_remaining: u64,
        window_name: String,
        balance_micro_credits: i64,
    },
    /// Request rejected due to quota or credit exhaustion.
    Exhausted {
        action: ExhaustedAction,
        window_name: String,
        reset_seconds: u64,
    },
    /// Request permitted under overage / debt recording policy.
    OverageAllowed { negative_balance: i64 },
}

/// Cache-aligned memory shard preventing cacheline bouncing across CPU cores.
#[repr(align(64))]
struct CacheAlignedShard {
    lock: std::sync::Mutex<FxHashMap<u64, AccountState>>,
    last_cleanup: AtomicU64,
}

/// Thread-safe, lock-striped concurrent storage container for tenant accounts.
pub struct ShardedMeteringState {
    shards: [CacheAlignedShard; NUM_SHARDS],
}

impl Default for ShardedMeteringState {
    fn default() -> Self {
        Self::new()
    }
}

impl ShardedMeteringState {
    /// Creates a new sharded state container with 64 independent cache-aligned lock stripes.
    pub fn new() -> Self {
        Self {
            shards: std::array::from_fn(|_| CacheAlignedShard {
                lock: std::sync::Mutex::new(FxHashMap::default()),
                last_cleanup: AtomicU64::new(0),
            }),
        }
    }

    /// Evaluates account quotas and deducts cost atomically.
    pub fn evaluate(
        &self,
        account_hash: u64,
        tier_cfg: &TierConfig,
        initial_balance_micro: i64,
        now_sec: u64,
        now_instant: Instant,
    ) -> MeteringDecision {
        let shard_idx = (account_hash as usize) & (NUM_SHARDS - 1);
        let shard = &self.shards[shard_idx];

        let mut map = shard.lock.lock().unwrap();

        // 1. Lazy cleanup of inactive accounts every 60s if capacity exceeded
        let last_clean = shard.last_cleanup.load(Ordering::Relaxed);
        if map.len() > MAX_ACCOUNTS_PER_SHARD && now_sec.saturating_sub(last_clean) > 60 {
            map.retain(|_, v| {
                now_sec.saturating_sub(v.last_access_sec) < ACCOUNT_INACTIVITY_TTL_SEC
            });
            shard.last_cleanup.store(now_sec, Ordering::Relaxed);
        }

        // 2. Fetch or initialize account state
        let state = map.entry(account_hash).or_insert_with(|| {
            let win_states = tier_cfg
                .windows
                .iter()
                .map(|cfg| WindowState::new(cfg, now_sec, now_instant))
                .collect();
            AccountState {
                last_access_sec: now_sec,
                balance_micro_credits: initial_balance_micro,
                windows: win_states,
            }
        });

        state.last_access_sec = now_sec;

        // Synchronize window state count if tier changed dynamically
        if state.windows.len() != tier_cfg.windows.len() {
            state.windows = tier_cfg
                .windows
                .iter()
                .map(|cfg| WindowState::new(cfg, now_sec, now_instant))
                .collect();
        }

        // 3. Check wallet credit balance if tier has a cost per request
        let cost = tier_cfg.cost_per_req_micro_credits as i64;
        if cost > 0 && state.balance_micro_credits < cost {
            match tier_cfg.exhausted_action {
                ExhaustedAction::Passthrough => {
                    state.balance_micro_credits -= cost;
                    return MeteringDecision::OverageAllowed {
                        negative_balance: state.balance_micro_credits,
                    };
                }
                action => {
                    return MeteringDecision::Exhausted {
                        action,
                        window_name: "credit_balance".to_string(),
                        reset_seconds: 0,
                    };
                }
            }
        }

        // 4. Dry-run window evaluation across all configured tier windows
        let mut min_remaining = u64::MAX;
        let mut min_remaining_win = String::new();

        for (i, cfg) in tier_cfg.windows.iter().enumerate() {
            let mut clone_state = state.windows[i].clone();
            let (allowed, remaining, reset_secs) = clone_state.evaluate(cfg, now_sec, now_instant);
            if !allowed {
                match tier_cfg.exhausted_action {
                    ExhaustedAction::Passthrough => {
                        // Overage permitted under contract
                        break;
                    }
                    action => {
                        return MeteringDecision::Exhausted {
                            action,
                            window_name: cfg.name.clone(),
                            reset_seconds: reset_secs,
                        };
                    }
                }
            }
            if remaining < min_remaining {
                min_remaining = remaining;
                min_remaining_win = cfg.name.clone();
            }
        }

        // 5. Commit pass: deduct cost and increment window counters
        if cost > 0 {
            state.balance_micro_credits -= cost;
        }

        for (i, cfg) in tier_cfg.windows.iter().enumerate() {
            state.windows[i].evaluate(cfg, now_sec, now_instant);
        }

        MeteringDecision::Allowed {
            min_remaining,
            window_name: min_remaining_win,
            balance_micro_credits: state.balance_micro_credits,
        }
    }
}
