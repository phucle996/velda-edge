//! Configuration schema for `openai_token_quota` (`post_response`).
//!
//! ```text
//!   OpenAiTokenQuotaConfig
//!     ├── mode                : "local" (default) | "distributed"
//!     ├── consumer_id_headers : ["x-tenant-id", "x-user-id"] (required, hierarchical)
//!     ├── ledger_slots        : power-of-2 in 64..=1_048_576 (128B/slot, k * 4096B)
//!     ├── local_default_quota : i64 (initial local quota per consumer in local mode)
//!     ├── redis_url           : Option<String> (required when mode == "distributed")
//!     ├── redis_password      : Option<String>
//!     ├── micro_quota_batch   : u64 (micro-quota chunk leased from Redis per refill)
//!     ├── refill_watermark    : i64 (triggers shadow worker refill when remaining <= watermark)
//!     └── sync_interval_ms    : u64 (background shadow sweep interval)
//! ```

use serde::{Deserialize, Serialize};

use crate::config::OutboundTlsConfig;

/// Operating mode for the token quota ledger.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum QuotaMode {
    /// Standalone single-node mode: all counting and quota deductions stay in local RAM.
    #[default]
    Local,
    /// Distributed mode: frontline deducts from local micro-quota; shadow worker leases
    /// new micro-quota chunks from Redis (`velda:plugin:<plugin_id>:<consumer_key>`).
    Distributed,
}

#[inline(always)]
const fn default_ledger_slots() -> usize {
    4096
}

#[inline(always)]
const fn default_local_quota() -> i64 {
    0
}

#[inline(always)]
const fn default_micro_quota_batch() -> u64 {
    5_000
}

#[inline(always)]
const fn default_refill_watermark() -> i64 {
    1_000
}

#[inline(always)]
const fn default_sync_interval_ms() -> u64 {
    500
}

#[inline(always)]
const fn default_shadow_weight() -> u32 {
    25
}

#[inline(always)]
const fn default_batch_users() -> usize {
    64
}

/// Declarative configuration for the `openai_token_quota` post-response plugin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OpenAiTokenQuotaConfig {
    /// `"local"` or `"distributed"`. Defaults to `"local"`.
    #[serde(default)]
    pub mode: QuotaMode,

    /// Ordered list of request header names combined hierarchically (`seg1.seg2...`)
    /// to resolve consumer/tenant identity. Required; no default.
    pub consumer_id_headers: Vec<String>,

    /// Total number of 128-byte usage slots in the shared contiguous arena
    /// (power-of-2 in `64..=1_048_576`). Default `4096` slots = `524,288` bytes (`512 KB`).
    #[serde(default = "default_ledger_slots")]
    pub ledger_slots: usize,

    /// Initial local token quota assigned to new slots in `local` mode (`0` = pure counter).
    #[serde(default = "default_local_quota")]
    pub local_default_quota: i64,

    /// Redis endpoint (`redis://host:port` or `rediss://host:port`) in `distributed` mode.
    #[serde(default)]
    pub redis_url: Option<String>,

    /// Optional Redis password (`AUTH`).
    #[serde(default)]
    pub redis_password: Option<String>,

    /// Optional TLS / mTLS client configuration for Redis (`rediss://` or explicit TLS).
    #[serde(default)]
    pub redis_tls: Option<OutboundTlsConfig>,

    /// Micro-quota batch size leased from Redis per refill in `distributed` mode.
    #[serde(default = "default_micro_quota_batch")]
    pub micro_quota_batch: u64,

    /// Maximum number of consumer keys batched per multi-key Lua `EVAL` round-trip (`1..=512`).
    #[serde(default = "default_batch_users")]
    pub batch_users: usize,

    /// Low watermark in local remaining tokens that triggers an urgent shadow refill.
    #[serde(default = "default_refill_watermark")]
    pub refill_watermark: i64,

    /// Shadow worker background sweep interval in milliseconds.
    #[serde(default = "default_sync_interval_ms")]
    pub sync_interval_ms: u64,

    /// Background workload scheduling weight (`1..=100`, same scale as `BackgroundPlugin::weight`).
    #[serde(default = "default_shadow_weight")]
    pub shadow_weight: u32,
}
