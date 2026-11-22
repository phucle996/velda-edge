//! Configuration schema for `api_key_auth` (`pre_upstream`).
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────────────────────┐
//! │                     API KEY AUTH CONFIGURATION                                          │
//! ├─────────────────────────────────────────────────────────────────────────────────────────┤
//! │  JSON:                                                                                  │
//! │    ├── header_name          : "x-api-key" (inspected directly on HeaderWorkspace)       │
//! │    ├── hide_credentials     : bool (remove header_name before forwarding upstream)      │
//! │    ├── cache_ttl_secs       : u64 (1..=86400, default 300, via coarse_sec())            │
//! │    ├── cache_slots_per_core : usize (power-of-2 >= 256, 16B/slot -> k * 4096B/core)     │
//! │    └── keys                 : [{ key (1..=64B), expires_at (u64), inject_headers }]     │
//! └─────────────────────────────────────────────────────────────────────────────────────────┘
//! ```

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Hard upper bound for API key length in bytes (`[u64; 8]` = 64 bytes = 1 L1d cache line).
/// Protects the hot path from oversized credential header DoS payloads.
pub const MAX_API_KEY_LEN: usize = 64;

#[inline(always)]
const fn default_cache_ttl_secs() -> u64 {
    300
}

#[inline(always)]
const fn default_cache_slots_per_core() -> usize {
    256
}

/// Declarative entry mapping a single API key (`1..=64` bytes) to optional expiration and headers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApiKeyEntryConfig {
    /// Secret API key (`1..=64` ASCII/UTF-8 bytes).
    pub key: String,

    /// Unix epoch expiration timestamp in seconds (`0` = never expires).
    #[serde(default)]
    pub expires_at: u64,

    /// Headers written (overwriting any client-supplied value) into `HeaderWorkspace` on match.
    #[serde(default)]
    pub inject_headers: HashMap<String, String>,
}

/// Declarative configuration for the `api_key_auth` pre-upstream plugin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApiKeyAuthConfig {
    /// Request header name to inspect on `HeaderWorkspace` (shared across HTTP and gRPC).
    pub header_name: String,

    /// Whether to remove `header_name` before forwarding the request upstream.
    #[serde(default)]
    pub hide_credentials: bool,

    /// Positive cache TTL in seconds (`1..=86400`), evaluated with `velda_core::clock::coarse_sec()`.
    #[serde(default = "default_cache_ttl_secs")]
    pub cache_ttl_secs: u64,

    /// Number of 16-byte positive cache slots allocated per worker core (rounded up to power-of-2 >= 256).
    /// `256` slots = `4,096` bytes (`4 KB`), `512` = `8 KB`, etc.
    #[serde(default = "default_cache_slots_per_core")]
    pub cache_slots_per_core: usize,

    /// Configured valid API keys (`>= 1` entry).
    pub keys: Vec<ApiKeyEntryConfig>,
}
