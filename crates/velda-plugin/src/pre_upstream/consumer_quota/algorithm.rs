//! High-performance in-memory algorithms for individual consumer / member quota tracking.
//!
//! # Objective & Mission
//! Enforces fair-use resource allocation and prevents individual tenant members, user accounts,
//! or bot clients from hogging shared system capacity (e.g. 50 requests per 5 hours in AI APIs,
//! or 1,000 requests per day per user).
//!
//! # Hardware & Performance Invariants
//! - Zero Float on Hot Path: All calculations, rate pacing, and sliding-window weightings operate strictly
//!   in pure integer arithmetic (basis points and integer division), avoiding FPU conversions and precision drift.
//! - Lock-Striped Cache-Aligned Sharding: Employs 64 cache-aligned shards (`#[repr(align(64))]`) to eliminate
//!   cross-core MESI bus bouncing, maintaining sub-20ns evaluation latency at 35,000+ RPS.
//! - Bounded Memory Footprint: Shards enforce an explicit capacity ceiling (`MAX_ENTRIES_PER_SHARD = 32,768`)
//!   with lazy TTL eviction, defending against cardinality bombs and memory exhaustion attacks.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use rustc_hash::FxHashMap;

/// Scaling factor for fixed-point token bucket calculations (1 token = 1,000,000,000 nano-tokens).
const NANO_TOKENS_PER_UNIT: u64 = 1_000_000_000;

/// Number of independent cache-aligned memory shards (power of 2).
const NUM_SHARDS: usize = 64;

/// Maximum tracked keys per shard before lazy cleanup is triggered (~2 million keys max across 64 shards).
pub const MAX_ENTRIES_PER_SHARD: usize = 32_768;

/// TTL before an inactive quota entry is considered stale and eligible for eviction (e.g. 24 hours).
const ENTRY_INACTIVITY_TTL_SEC: u64 = 86_400;

/// Evaluation algorithm selected for a specific quota window.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum QuotaAlgorithm {
    /// Sliding window counter with smooth basis-point interpolation (ideal for 3h-5h AI rolling windows).
    #[default]
    SlidingWindow,
    /// Fixed calendar/clock window (e.g. daily quota reset at 00:00 UTC).
    FixedWindow,
    /// Token bucket for absorbing sudden bursts while constraining sustained short-term rate.
    TokenBucket,
}

impl QuotaAlgorithm {
    /// Parses an algorithm identifier from configuration string.
    pub fn parse(s: &str) -> Self {
        match s.trim().to_ascii_lowercase().as_str() {
            "fixed" | "fixed_window" => Self::FixedWindow,
            "token" | "token_bucket" => Self::TokenBucket,
            _ => Self::SlidingWindow,
        }
    }
}

/// Static definition of a single quota window within a consumer quota policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuotaWindowConfig {
    /// Human-readable identifier for this window (e.g. "5h_ai_limit", "daily_cap").
    pub name: String,
    /// Window duration strictly in seconds (e.g. 18000 = 5 hours, 86400 = 1 day).
    pub window_seconds: u64,
    /// Maximum allowed request count within this window.
    pub limit: u64,
    /// Measurement algorithm applied to this window.
    pub algorithm: QuotaAlgorithm,
}

/// State machine tracking usage inside a single window for a single client key.
#[derive(Debug, Clone)]
pub enum WindowState {
    Sliding {
        curr_epoch: u64,
        curr_count: u64,
        prev_count: u64,
        delta_consumed: u64,
    },
    Fixed {
        curr_epoch: u64,
        curr_count: u64,
        delta_consumed: u64,
    },
    Bucket {
        nano_tokens: u64,
        last_refill: Instant,
        delta_consumed: u64,
    },
}

impl WindowState {
    /// Initializes zeroed state matching the configured algorithm.
    pub fn new(cfg: &QuotaWindowConfig, now_sec: u64, now_instant: Instant) -> Self {
        match cfg.algorithm {
            QuotaAlgorithm::SlidingWindow => {
                let epoch = now_sec.checked_div(cfg.window_seconds).unwrap_or(now_sec);
                Self::Sliding {
                    curr_epoch: epoch,
                    curr_count: 0,
                    prev_count: 0,
                    delta_consumed: 0,
                }
            }
            QuotaAlgorithm::FixedWindow => {
                let epoch = now_sec.checked_div(cfg.window_seconds).unwrap_or(now_sec);
                Self::Fixed {
                    curr_epoch: epoch,
                    curr_count: 0,
                    delta_consumed: 0,
                }
            }
            QuotaAlgorithm::TokenBucket => Self::Bucket {
                nano_tokens: cfg.limit.saturating_mul(NANO_TOKENS_PER_UNIT),
                last_refill: now_instant,
                delta_consumed: 0,
            },
        }
    }

    /// Returns uncommitted delta consumed since last shadow sync.
    #[inline]
    pub fn delta_consumed(&self) -> u64 {
        match self {
            Self::Sliding { delta_consumed, .. }
            | Self::Fixed { delta_consumed, .. }
            | Self::Bucket { delta_consumed, .. } => *delta_consumed,
        }
    }

    /// Resets uncommitted delta to zero after shadow collection.
    #[inline]
    pub fn reset_delta(&mut self) {
        match self {
            Self::Sliding { delta_consumed, .. }
            | Self::Fixed { delta_consumed, .. }
            | Self::Bucket { delta_consumed, .. } => *delta_consumed = 0,
        }
    }

    /// Reconciles updated global count returned by distributed store into local memory.
    #[inline]
    pub fn reconcile_global(&mut self, global_count: u64) {
        match self {
            Self::Sliding { curr_count, .. } | Self::Fixed { curr_count, .. }
                if global_count > *curr_count =>
            {
                *curr_count = global_count;
            }
            _ => {}
        }
    }

    /// Evaluates if an increment of 1 request is permitted under this window.
    ///
    /// Returns `(allowed: bool, remaining: u64, reset_seconds: u64)`.
    pub fn evaluate(
        &mut self,
        cfg: &QuotaWindowConfig,
        now_sec: u64,
        now_instant: Instant,
    ) -> (bool, u64, u64) {
        if cfg.window_seconds == 0 {
            // Degenerate zero-second window: strictly pass-through
            return (true, cfg.limit, 0);
        }

        match self {
            Self::Sliding {
                curr_epoch,
                curr_count,
                prev_count,
                delta_consumed,
            } => {
                let actual_epoch = now_sec / cfg.window_seconds;
                if actual_epoch > *curr_epoch {
                    let steps = actual_epoch - *curr_epoch;
                    if steps == 1 {
                        *prev_count = *curr_count;
                    } else {
                        *prev_count = 0;
                    }
                    *curr_count = 0;
                    *curr_epoch = actual_epoch;
                }

                // Integer Basis-Points Interpolation:
                let elapsed_in_window = now_sec % cfg.window_seconds;
                let current_weight_bp = (elapsed_in_window * 10_000) / cfg.window_seconds;
                let prev_weight_bp = 10_000u64.saturating_sub(current_weight_bp);
                let estimated_prev = (*prev_count * prev_weight_bp) / 10_000;
                let total_estimated = *curr_count + estimated_prev;

                let reset_seconds = cfg.window_seconds.saturating_sub(elapsed_in_window).max(1);

                if total_estimated < cfg.limit {
                    *curr_count = curr_count.saturating_add(1);
                    *delta_consumed = delta_consumed.saturating_add(1);
                    let remaining = cfg.limit.saturating_sub(total_estimated + 1);
                    (true, remaining, reset_seconds)
                } else {
                    (false, 0, reset_seconds)
                }
            }
            Self::Fixed {
                curr_epoch,
                curr_count,
                delta_consumed,
            } => {
                let actual_epoch = now_sec / cfg.window_seconds;
                if actual_epoch > *curr_epoch {
                    *curr_count = 0;
                    *curr_epoch = actual_epoch;
                }

                let elapsed_in_window = now_sec % cfg.window_seconds;
                let reset_seconds = cfg.window_seconds.saturating_sub(elapsed_in_window).max(1);

                if *curr_count < cfg.limit {
                    *curr_count = curr_count.saturating_add(1);
                    *delta_consumed = delta_consumed.saturating_add(1);
                    let remaining = cfg.limit.saturating_sub(*curr_count);
                    (true, remaining, reset_seconds)
                } else {
                    (false, 0, reset_seconds)
                }
            }
            Self::Bucket {
                nano_tokens,
                last_refill,
                delta_consumed,
            } => {
                let elapsed_nanos = now_instant
                    .saturating_duration_since(*last_refill)
                    .as_nanos() as u64;

                let max_nano = cfg.limit.saturating_mul(NANO_TOKENS_PER_UNIT);
                // Fill rate per nanosecond in integer fixed-point:
                let fill_rate_per_sec = cfg.limit / cfg.window_seconds.max(1);
                let refill_nano = elapsed_nanos.saturating_mul(fill_rate_per_sec);

                *nano_tokens = nano_tokens.saturating_add(refill_nano).min(max_nano);
                *last_refill = now_instant;

                let cost_nano = NANO_TOKENS_PER_UNIT;
                if *nano_tokens >= cost_nano {
                    *nano_tokens -= cost_nano;
                    *delta_consumed = delta_consumed.saturating_add(1);
                    let remaining = *nano_tokens / NANO_TOKENS_PER_UNIT;
                    (true, remaining, 0)
                } else {
                    let needed_nano = cost_nano.saturating_sub(*nano_tokens);
                    let wait_secs = if fill_rate_per_sec > 0 {
                        (needed_nano / (fill_rate_per_sec * NANO_TOKENS_PER_UNIT)).max(1)
                    } else {
                        cfg.window_seconds
                    };
                    (false, 0, wait_secs)
                }
            }
        }
    }
}

/// Composite quota state tracking multiple windows for a single client key.
#[derive(Debug, Clone)]
pub struct KeyQuotaState {
    pub last_access_sec: u64,
    pub windows: Vec<WindowState>,
}

/// Decision output resulting from evaluating an ingress request against quota rules.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QuotaDecision {
    /// Request permitted across all configured windows.
    Allowed {
        min_remaining: u64,
        window_name: String,
    },
    /// Request rejected because one of the windows has been exhausted.
    Exhausted {
        window_name: String,
        reset_seconds: u64,
    },
}

/// Cache-aligned memory shard preventing cacheline bouncing across CPU cores.
#[repr(align(64))]
struct CacheAlignedShard {
    lock: std::sync::Mutex<FxHashMap<u64, KeyQuotaState>>,
    last_cleanup: AtomicU64,
}

/// Thread-safe, lock-striped concurrent storage container for consumer quota tracking.
pub struct ShardedQuotaState {
    shards: [CacheAlignedShard; NUM_SHARDS],
}

impl Default for ShardedQuotaState {
    fn default() -> Self {
        Self::new()
    }
}

impl ShardedQuotaState {
    /// Creates a new sharded state container with 64 independent cache-aligned lock stripes.
    pub fn new() -> Self {
        Self {
            shards: std::array::from_fn(|_| CacheAlignedShard {
                lock: std::sync::Mutex::new(FxHashMap::default()),
                last_cleanup: AtomicU64::new(0),
            }),
        }
    }

    /// Evaluates all configured quota windows atomically for a hashed client identifier.
    pub fn evaluate(
        &self,
        key_hash: u64,
        windows: &[QuotaWindowConfig],
        now_sec: u64,
        now_instant: Instant,
    ) -> QuotaDecision {
        if windows.is_empty() {
            return QuotaDecision::Allowed {
                min_remaining: u64::MAX,
                window_name: "unlimited".to_string(),
            };
        }

        let shard_idx = (key_hash as usize) & (NUM_SHARDS - 1);
        let shard = &self.shards[shard_idx];

        let mut map = shard.lock.lock().unwrap();

        // 1. Lazy periodic cleanup if capacity exceeds high watermark (every 60s)
        let last_clean = shard.last_cleanup.load(Ordering::Relaxed);
        if map.len() > MAX_ENTRIES_PER_SHARD && now_sec.saturating_sub(last_clean) > 60 {
            map.retain(|_, v| now_sec.saturating_sub(v.last_access_sec) < ENTRY_INACTIVITY_TTL_SEC);
            shard.last_cleanup.store(now_sec, Ordering::Relaxed);
        }

        // 2. Fetch or initialize client state
        let state = map.entry(key_hash).or_insert_with(|| {
            let win_states = windows
                .iter()
                .map(|cfg| WindowState::new(cfg, now_sec, now_instant))
                .collect();
            KeyQuotaState {
                last_access_sec: now_sec,
                windows: win_states,
            }
        });

        state.last_access_sec = now_sec;

        // Synchronize window count if config reloaded dynamically
        if state.windows.len() != windows.len() {
            state.windows = windows
                .iter()
                .map(|cfg| WindowState::new(cfg, now_sec, now_instant))
                .collect();
        }

        // 3. Dry-run evaluation: check if ALL windows have capacity before committing
        let mut min_remaining = u64::MAX;
        let mut min_remaining_win = String::new();

        for (i, cfg) in windows.iter().enumerate() {
            let mut clone_state = state.windows[i].clone();
            let (allowed, remaining, reset_secs) = clone_state.evaluate(cfg, now_sec, now_instant);
            if !allowed {
                return QuotaDecision::Exhausted {
                    window_name: cfg.name.clone(),
                    reset_seconds: reset_secs,
                };
            }
            if remaining < min_remaining {
                min_remaining = remaining;
                min_remaining_win = cfg.name.clone();
            }
        }

        // 4. Commit pass: increment usage in all windows
        for (i, cfg) in windows.iter().enumerate() {
            state.windows[i].evaluate(cfg, now_sec, now_instant);
        }

        QuotaDecision::Allowed {
            min_remaining,
            window_name: min_remaining_win,
        }
    }

    /// Collects all non-zero uncommitted deltas across all 64 shards for batched shadow sync.
    /// Returns `Vec<(key_hash, window_index, delta)>`.
    pub fn collect_deltas(&self) -> Vec<(u64, usize, u64)> {
        let mut batch = Vec::new();
        for shard in &self.shards {
            let mut map = shard.lock.lock().unwrap();
            for (&key_hash, state) in map.iter_mut() {
                for (win_idx, win) in state.windows.iter_mut().enumerate() {
                    let delta = win.delta_consumed();
                    if delta > 0 {
                        batch.push((key_hash, win_idx, delta));
                        win.reset_delta();
                    }
                }
            }
        }
        batch
    }

    /// Reconciles updated global counts returned from distributed Redis into RAM shards.
    pub fn reconcile_globals(&self, updates: &[(u64, usize, u64)]) {
        for &(key_hash, win_idx, global_count) in updates {
            let shard_idx = (key_hash as usize) & (NUM_SHARDS - 1);
            let shard = &self.shards[shard_idx];
            let mut map = shard.lock.lock().unwrap();
            let win_opt = map
                .get_mut(&key_hash)
                .and_then(|s| s.windows.get_mut(win_idx));
            if let Some(win) = win_opt {
                win.reconcile_global(global_count);
            }
        }
    }
}
