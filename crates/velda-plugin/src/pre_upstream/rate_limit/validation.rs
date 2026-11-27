//! Configuration schema and strict semantic validation for the `rate_limit` plugin.
//!
//! ```text
//!   PluginConfig ("rate_limit") ──► serde_json::from_value(RateLimitConfig)
//!         │                           [deny_unknown_fields: clean ETP contract]
//!         ├─► etp_id: None         ──► Local node-only mode (pbww = 0, ShardArena only)
//!         ├─► etp_id: Some(id)     ──► Multi-SoT ETP distributed ledger + Shadow sync
//!         └─► Validate rates, burst, overdraft_percent, key_strategy, sync_interval_ms
//!
//!   KeyStrategy Modes (Ordered Header / Multi-Tier Cascading):
//!   ┌────────────────────────────────────────────────────────────────────────────┐
//!   │ client_ip    | Client remote IP only                                       │
//!   │ header       | First non-empty configured key_header                       │
//!   │ route        | Canonical HTTP/gRPC URI path                                │
//!   │ priority     | First present source across key_headers / client_ip         │
//!   │ composite    | Single joined digest across all present key_headers (+ IP)  │
//!   │ hierarchical | Cumulative multi-tier cascade [H0, H0|H1, H0|H1|H2, ...]    │
//!   └────────────────────────────────────────────────────────────────────────────┘
//! ```

use serde::{Deserialize, Serialize};

use crate::error::PluginError;

/// Rate-limiting enforcement algorithm evaluated against local shards and cluster SoT ledgers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RateLimitAlgorithm {
    #[default]
    TokenBucket,
    SlidingWindow,
    FixedWindow,
}

/// Request identity extraction and multi-header composition strategy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KeyStrategy {
    ClientIp,
    Header,
    Route,
    #[default]
    Priority,
    Composite,
    /// Evaluates cumulative cascading tiers built from `key_headers`
    /// (`[tier0, tier0|tier1, tier0|tier1|tier2, ...]`) to block both tenant-wide
    /// rotation attacks and per-key floods simultaneously.
    Hierarchical,
}

/// Behavior when an attached distributed SoT storage ETP becomes temporarily unreachable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FallbackBehavior {
    #[default]
    Memory,
    Allow,
    Deny,
}

/// Declarative configuration for the `rate_limit` pre-upstream plugin.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RateLimitConfig {
    /// Rate-limiting algorithm (`token_bucket`, `sliding_window`, `fixed_window`).
    #[serde(default)]
    pub algorithm: RateLimitAlgorithm,

    /// Optional External Third-Party SoT storage pool ID from `etp.bin`.
    /// `None` runs purely in-memory on local `CoreLane` + `ShardArena` (`pbww = 0`).
    #[serde(default)]
    pub etp_id: Option<String>,

    /// Optional explicit ETP kind hint validated by `velda-sync` against the SoT whitelist.
    #[serde(default)]
    pub etp_kind: Option<String>,

    /// Requests permitted per second per resolved bucket key.
    #[serde(default = "default_rate_per_second")]
    pub rate_per_second: u64,

    /// Baseline burst capacity (`>= rate_per_second` when omitted in constructor, validated `>= 1`).
    #[serde(default = "default_burst_capacity")]
    pub burst_capacity: u64,

    /// Bounded soft overdraft / grace percentage (`0..=100`, default `5`) permitted above
    /// `burst_capacity` to absorb transient multi-core lease skew and async SoT sync jitter
    /// without false-positive drops, while blocking genuine spikes and spam cleanly.
    #[serde(default = "default_overdraft_percent")]
    pub overdraft_percent: u8,

    /// Key extraction strategy (`client_ip`, `header`, `route`, `priority`, `composite`, `hierarchical`).
    #[serde(default)]
    pub key_strategy: KeyStrategy,

    /// Ordered source names (`client_ip`, `route`, or HTTP header names such as `x-tenant-id`,
    /// `x-api-key`) used by `header`, `priority`, `composite`, and `hierarchical` strategies.
    #[serde(default)]
    pub key_headers: Vec<String>,

    /// Action when an attached SoT ETP fails health/IO checks (`memory`, `allow`, `deny`).
    #[serde(default)]
    pub fallback_behavior: FallbackBehavior,

    /// Namespace prefix applied to distributed keys persisted in SoT storage ETPs.
    #[serde(default = "default_key_prefix")]
    pub key_prefix: String,

    /// Interval in milliseconds between Shadow Worker SoT push+reconcile cycles (`10..=60000`).
    #[serde(default = "default_sync_interval_ms")]
    pub sync_interval_ms: u64,

    /// Maximum bounded entry capacity in local `ShardArena` across all 64 shards.
    #[serde(default = "default_max_entries")]
    pub max_entries: usize,

    /// Plugin Background Workload Weight (`0..=100`). Automatically `0` when `etp_id` is `None`.
    #[serde(default = "default_pbww")]
    pub pbww: u32,
}

fn default_rate_per_second() -> u64 {
    1000
}

fn default_burst_capacity() -> u64 {
    1000
}

fn default_overdraft_percent() -> u8 {
    5
}

fn default_key_prefix() -> String {
    "velda:rl:".to_string()
}

fn default_sync_interval_ms() -> u64 {
    200
}

fn default_max_entries() -> usize {
    65_536
}

/// Hard upper bound on `max_entries` per `rate_limit` instance to prevent accidental OOM reloads.
pub const MAX_ALLOWED_ENTRIES: usize = 4_194_304;

fn default_pbww() -> u32 {
    20
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            algorithm: RateLimitAlgorithm::TokenBucket,
            etp_id: None,
            etp_kind: None,
            rate_per_second: default_rate_per_second(),
            burst_capacity: default_burst_capacity(),
            overdraft_percent: default_overdraft_percent(),
            key_strategy: KeyStrategy::Priority,
            key_headers: Vec::new(),
            fallback_behavior: FallbackBehavior::Memory,
            key_prefix: default_key_prefix(),
            sync_interval_ms: default_sync_interval_ms(),
            max_entries: default_max_entries(),
            pbww: default_pbww(),
        }
    }
}

/// Validates and parses raw `rate_limit` JSON configuration.
pub fn validate_rate_limit_config(raw: &serde_json::Value) -> Result<RateLimitConfig, PluginError> {
    let mut cfg: RateLimitConfig = serde_json::from_value(raw.clone())
        .map_err(|e| PluginError::Config(format!("Invalid rate_limit config: {e}")))?;

    if cfg.rate_per_second == 0 {
        return Err(PluginError::Config(
            "rate_limit 'rate_per_second' must be >= 1".to_string(),
        ));
    }
    if cfg.burst_capacity == 0 {
        return Err(PluginError::Config(
            "rate_limit 'burst_capacity' must be >= 1".to_string(),
        ));
    }
    if cfg.overdraft_percent > 100 {
        return Err(PluginError::Config(format!(
            "rate_limit 'overdraft_percent' ({}) must be in 0..=100",
            cfg.overdraft_percent
        )));
    }
    if !(10..=60_000).contains(&cfg.sync_interval_ms) {
        return Err(PluginError::Config(format!(
            "rate_limit 'sync_interval_ms' ({}) must be in 10..=60000",
            cfg.sync_interval_ms
        )));
    }
    if cfg.max_entries < 64 || cfg.max_entries > MAX_ALLOWED_ENTRIES {
        return Err(PluginError::Config(format!(
            "rate_limit 'max_entries' ({}) must be in 64..={MAX_ALLOWED_ENTRIES}",
            cfg.max_entries
        )));
    }

    if let Some(ref etp_id) = cfg.etp_id {
        let trimmed = etp_id.trim();
        if trimmed.is_empty() {
            return Err(PluginError::Config(
                "rate_limit 'etp_id' cannot be blank when specified; omit or set null for local-only mode".to_string(),
            ));
        }
        cfg.etp_id = Some(trimmed.to_string());
    }

    cfg.key_headers.retain(|s| !s.trim().is_empty());
    if matches!(
        cfg.key_strategy,
        KeyStrategy::Header | KeyStrategy::Composite | KeyStrategy::Hierarchical
    ) && cfg.key_headers.is_empty()
    {
        return Err(PluginError::Config(format!(
            "rate_limit key_strategy '{:?}' requires non-empty 'key_headers'",
            cfg.key_strategy
        )));
    }

    cfg.key_prefix = cfg.key_prefix.trim().to_string();
    if cfg.key_prefix.is_empty() {
        cfg.key_prefix = default_key_prefix();
    }

    if cfg.etp_id.is_none() {
        cfg.pbww = 0;
    } else if cfg.pbww > 100 {
        return Err(PluginError::Config(format!(
            "rate_limit 'pbww' ({}) must be in 0..=100",
            cfg.pbww
        )));
    }

    Ok(cfg)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rejects_legacy_fields_and_accepts_clean_etp_config() {
        let legacy = serde_json::json!({
            "backend": "redis",
            "redis_url": "redis://127.0.0.1:6379",
            "rate_per_second": 100
        });
        assert!(validate_rate_limit_config(&legacy).is_err());

        let valid_local = serde_json::json!({
            "algorithm": "token_bucket",
            "rate_per_second": 500,
            "burst_capacity": 1000,
            "overdraft_percent": 5,
            "key_strategy": "hierarchical",
            "key_headers": ["x-tenant-id", "x-api-key", "client_ip"]
        });
        let parsed = validate_rate_limit_config(&valid_local).unwrap();
        assert_eq!(parsed.pbww, 0);
        assert_eq!(parsed.key_strategy, KeyStrategy::Hierarchical);

        let valid_etp = serde_json::json!({
            "etp_id": "pg-rl-sot",
            "rate_per_second": 500,
            "burst_capacity": 1000,
            "key_strategy": "composite",
            "key_headers": ["x-tenant-id", "client_ip"],
            "fallback_behavior": "memory",
            "sync_interval_ms": 150
        });
        let parsed_etp = validate_rate_limit_config(&valid_etp).unwrap();
        assert_eq!(parsed_etp.etp_id.as_deref(), Some("pg-rl-sot"));
        assert_eq!(parsed_etp.pbww, 20);

        let too_large = serde_json::json!({
            "max_entries": MAX_ALLOWED_ENTRIES + 1
        });
        assert!(validate_rate_limit_config(&too_large).is_err());
    }
}
