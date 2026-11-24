//! Config Compilation & Validation (`api_metering::config`).
//!
//! ```text
//! PluginConfig (JSON) ──► parse_metering_config()
//!   ├── windows.len() > 4             -> Err(Config)
//!   ├── distributed && etp_id.is_none -> Err(Config)
//!   └── compile_tiers() ──► Box<[CompiledTier]> + default_tier_idx
//! ```

use std::hash::{Hash, Hasher};

use http::{HeaderName, HeaderValue};
use rustc_hash::{FxHashMap, FxHasher};

use super::arena::MAX_WINDOWS_PER_TIER;
use crate::config::PluginConfig;
use crate::error::PluginError;
use crate::pre_upstream::consumer_quota::{QuotaAlgorithm, QuotaWindowConfig};
use crate::pre_upstream::distributed::DistributedSyncConfig;

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
    /// Array of sliding, fixed, or token-bucket quota windows bounding this tier (up to 4).
    pub windows: Vec<QuotaWindowConfig>,
    /// Base cost deducted per request in micro-credits ($10^{-6}$).
    pub cost_per_req_micro_credits: u64,
    /// Policy executed when any window or credit balance is exhausted.
    pub exhausted_action: ExhaustedAction,
    /// Enables bounded node-level grace (`clamp(limit / 200, 2, 1000)`) to avoid false 402/429 under multi-core leases.
    pub grace_enabled: bool,
    /// Optional explicit node-level grace request budget (overrides `clamp(limit / 200, 2, 1000)` when `Some`).
    pub grace_requests: Option<u64>,
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
            grace_enabled: false,
            grace_requests: None,
        }
    }
}

/// Computes node-level grace request budget for a given window limit.
#[inline(always)]
pub const fn compute_node_grace(limit: u64, grace_enabled: bool, explicit: Option<u64>) -> u64 {
    if !grace_enabled && explicit.is_none() {
        return 0;
    }
    if let Some(n) = explicit {
        return n;
    }
    let pct = limit / 200; // 0.5%
    if pct < 2 {
        2
    } else if pct > 1000 {
        1000
    } else {
        pct
    }
}

/// Precompiled window configuration carrying a pre-validated [`HeaderValue`] and grace ceiling.
#[derive(Debug, Clone)]
pub struct CompiledWindow {
    pub cfg: QuotaWindowConfig,
    pub name_header_val: Option<HeaderValue>,
    pub node_grace_limit: u64,
}

/// Precompiled tier definition for zero-allocation hot-path resolution and evaluation.
#[derive(Debug, Clone)]
pub struct CompiledTier {
    pub name: String,
    pub tier_header_val: Option<HeaderValue>,
    pub tier_fingerprint: u64,
    pub windows: Box<[CompiledWindow]>,
    pub cost_i64: i64,
    pub exhausted_action: ExhaustedAction,
    pub has_token_bucket: bool,
    pub node_grace_requests: u64,
}

impl CompiledTier {
    /// Compiles a [`TierConfig`] into an allocation-free hot-path descriptor.
    pub fn compile(cfg: &TierConfig) -> Self {
        let count = cfg.windows.len().min(MAX_WINDOWS_PER_TIER);
        if cfg.windows.len() > MAX_WINDOWS_PER_TIER {
            tracing::warn!(
                tier = %cfg.name,
                configured = cfg.windows.len(),
                max = MAX_WINDOWS_PER_TIER,
                "Tier configured more windows than MAX_WINDOWS_PER_TIER; truncating excess windows"
            );
        }

        let mut windows = Vec::with_capacity(count);
        let mut has_token_bucket = false;
        let mut fp = FxHasher::default();
        cfg.name.as_bytes().hash(&mut fp);
        count.hash(&mut fp);
        cfg.grace_enabled.hash(&mut fp);
        cfg.grace_requests.hash(&mut fp);

        for w in &cfg.windows[..count] {
            if matches!(w.algorithm, QuotaAlgorithm::TokenBucket) {
                has_token_bucket = true;
            }
            (w.algorithm as u8).hash(&mut fp);
            w.window_seconds.hash(&mut fp);
            w.limit.hash(&mut fp);
            windows.push(CompiledWindow {
                cfg: w.clone(),
                name_header_val: HeaderValue::from_str(&w.name).ok(),
                node_grace_limit: compute_node_grace(
                    w.limit,
                    cfg.grace_enabled,
                    cfg.grace_requests,
                ),
            });
        }

        let node_grace_requests = if cfg.grace_enabled || cfg.grace_requests.is_some() {
            cfg.grace_requests.unwrap_or(2)
        } else {
            0
        };

        Self {
            name: cfg.name.clone(),
            tier_header_val: HeaderValue::from_str(&cfg.name).ok(),
            tier_fingerprint: fp.finish(),
            windows: windows.into_boxed_slice(),
            cost_i64: i64::try_from(cfg.cost_per_req_micro_credits).unwrap_or(i64::MAX),
            exhausted_action: cfg.exhausted_action,
            has_token_bucket,
            node_grace_requests,
        }
    }
}

/// Static configuration contract for [`super::plugin::ApiMeteringPlugin`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiMeteringConfig {
    /// Ordered list of header names evaluated as hierarchy levels (`L0 -> L1 -> L2`).
    pub account_headers: Vec<HeaderName>,
    /// Header name specifying client tier (default: `x-plan-tier`).
    pub tier_header: HeaderName,
    /// Fallback tier name when header is absent or unrecognized (default: `free`).
    pub default_tier: String,
    /// Initial wallet micro-credits credited to newly discovered accounts.
    pub initial_balance_micro_credits: i64,
    /// Tier definitions indexed by lowercase name.
    pub tiers: FxHashMap<String, TierConfig>,
    /// Identifier of the `redis` ETP entry in `EtpRegistry` when distributed sync is enabled.
    pub etp_id: Option<String>,
    /// Distributed multi-gateway synchronization parameters (sync interval, key prefix, pbww).
    pub distributed: Option<DistributedSyncConfig>,
}

impl Default for ApiMeteringConfig {
    fn default() -> Self {
        let mut tiers = FxHashMap::default();
        tiers.insert(
            "free".to_string(),
            TierConfig {
                name: "free".to_string(),
                windows: vec![QuotaWindowConfig {
                    name: "monthly_cap".to_string(),
                    window_seconds: 30 * 86_400,
                    limit: 10_000,
                    algorithm: QuotaAlgorithm::FixedWindow,
                }],
                cost_per_req_micro_credits: 0,
                exhausted_action: ExhaustedAction::Reject402,
                grace_enabled: false,
                grace_requests: None,
            },
        );
        tiers.insert(
            "pro".to_string(),
            TierConfig {
                name: "pro".to_string(),
                windows: vec![QuotaWindowConfig {
                    name: "monthly_cap".to_string(),
                    window_seconds: 30 * 86_400,
                    limit: 1_000_000,
                    algorithm: QuotaAlgorithm::SlidingWindow,
                }],
                cost_per_req_micro_credits: 0,
                exhausted_action: ExhaustedAction::Reject429,
                grace_enabled: false,
                grace_requests: None,
            },
        );
        tiers.insert(
            "enterprise".to_string(),
            TierConfig {
                name: "enterprise".to_string(),
                windows: vec![QuotaWindowConfig {
                    name: "monthly_cap".to_string(),
                    window_seconds: 30 * 86_400,
                    limit: 10_000_000,
                    algorithm: QuotaAlgorithm::SlidingWindow,
                }],
                cost_per_req_micro_credits: 0,
                exhausted_action: ExhaustedAction::Passthrough,
                grace_enabled: false,
                grace_requests: None,
            },
        );

        Self {
            account_headers: vec![
                HeaderName::from_static("x-tenant-id"),
                HeaderName::from_static("x-org-id"),
                HeaderName::from_static("x-account-id"),
            ],
            tier_header: HeaderName::from_static("x-plan-tier"),
            default_tier: "free".to_string(),
            initial_balance_micro_credits: 0,
            tiers,
            etp_id: None,
            distributed: None,
        }
    }
}

pub(crate) fn compile_tiers(config: &ApiMeteringConfig) -> (Box<[CompiledTier]>, usize) {
    let default_lower = config.default_tier.trim().to_ascii_lowercase();
    let mut list: Vec<CompiledTier> = config.tiers.values().map(CompiledTier::compile).collect();

    if let Some(pos) = list.iter().position(|t| t.name == default_lower) {
        return (list.into_boxed_slice(), pos);
    }

    if let Some(entry) = config.tiers.get(&default_lower) {
        list.push(CompiledTier::compile(entry));
        let idx = list.len() - 1;
        return (list.into_boxed_slice(), idx);
    }

    let mut fallback = TierConfig::default();
    if !default_lower.is_empty() {
        fallback.name = default_lower;
    }
    list.push(CompiledTier::compile(&fallback));
    let idx = list.len() - 1;
    (list.into_boxed_slice(), idx)
}

pub(crate) fn parse_metering_config(
    config: &PluginConfig,
) -> Result<ApiMeteringConfig, PluginError> {
    let account_headers = if let Some(arr) = config
        .config
        .get("account_headers")
        .and_then(|v| v.as_array())
    {
        let mut headers = Vec::with_capacity(arr.len());
        for v in arr {
            if let Some(s) = v.as_str() {
                let name = HeaderName::from_bytes(s.as_bytes()).map_err(|e| {
                    PluginError::Config(format!("Invalid account_header name '{s}': {e}"))
                })?;
                headers.push(name);
            }
        }
        headers
    } else if let Some(single) = config.config.get("account_header").and_then(|v| v.as_str()) {
        vec![HeaderName::from_bytes(single.as_bytes()).map_err(|e| {
            PluginError::Config(format!("Invalid account_header name '{single}': {e}"))
        })?]
    } else {
        vec![
            HeaderName::from_static("x-tenant-id"),
            HeaderName::from_static("x-org-id"),
            HeaderName::from_static("x-account-id"),
        ]
    };

    let tier_header = if let Some(s) = config.config.get("tier_header").and_then(|v| v.as_str()) {
        HeaderName::from_bytes(s.as_bytes())
            .map_err(|e| PluginError::Config(format!("Invalid tier_header name '{s}': {e}")))?
    } else {
        HeaderName::from_static("x-plan-tier")
    };

    let default_tier = config
        .config
        .get("default_tier")
        .and_then(|v| v.as_str())
        .unwrap_or("free")
        .trim()
        .to_ascii_lowercase();

    let initial_balance_micro_credits = config
        .config
        .get("initial_balance_micro_credits")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);

    let mut tiers = FxHashMap::default();
    if let Some(tier_map) = config.config.get("tiers").and_then(|v| v.as_object()) {
        for (tier_name, val) in tier_map {
            let name = tier_name.trim().to_ascii_lowercase();
            let cost_per_req_micro_credits = val
                .get("cost_per_req_micro_credits")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);

            let exhausted_action = val
                .get("exhausted_action")
                .and_then(|v| v.as_str())
                .map(ExhaustedAction::parse)
                .unwrap_or(ExhaustedAction::Reject402);

            let grace_enabled = val
                .get("grace_enabled")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            let grace_requests = val.get("grace_requests").and_then(|v| v.as_u64());

            let mut windows = Vec::new();
            if let Some(win_arr) = val.get("windows").and_then(|v| v.as_array()) {
                if win_arr.len() > MAX_WINDOWS_PER_TIER {
                    return Err(PluginError::Config(format!(
                        "Tier '{name}' defines {} windows, exceeding MAX_WINDOWS_PER_TIER ({MAX_WINDOWS_PER_TIER})",
                        win_arr.len()
                    )));
                }
                for (idx, w) in win_arr.iter().enumerate() {
                    let w_name = match w.get("name").and_then(|v| v.as_str()) {
                        Some(s) => s.to_string(),
                        None => format!("window_{idx}"),
                    };
                    let window_seconds = w
                        .get("window_seconds")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(30 * 86_400);
                    let limit = w.get("limit").and_then(|v| v.as_u64()).unwrap_or(10_000);
                    let algorithm = w
                        .get("algorithm")
                        .and_then(|v| v.as_str())
                        .map(QuotaAlgorithm::parse)
                        .unwrap_or(QuotaAlgorithm::FixedWindow);

                    windows.push(QuotaWindowConfig {
                        name: w_name,
                        window_seconds,
                        limit,
                        algorithm,
                    });
                }
            }

            tiers.insert(
                name.clone(),
                TierConfig {
                    name,
                    windows,
                    cost_per_req_micro_credits,
                    exhausted_action,
                    grace_enabled,
                    grace_requests,
                },
            );
        }
    }

    if tiers.is_empty() {
        tiers = ApiMeteringConfig::default().tiers;
    }

    let distributed = config
        .config
        .get("distributed")
        .map(|v| DistributedSyncConfig::from_json(Some(v)));

    let etp_id = config
        .config
        .get("etp_id")
        .or_else(|| {
            config
                .config
                .get("distributed")
                .and_then(|d| d.get("etp_id"))
        })
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(ToOwned::to_owned);

    if distributed.as_ref().is_some_and(|d| d.enabled) && etp_id.is_none() {
        return Err(PluginError::Config(format!(
            "Plugin '{}': 'etp_id' is required when distributed sync is enabled",
            config.id
        )));
    }

    Ok(ApiMeteringConfig {
        account_headers,
        tier_header,
        default_tier,
        initial_balance_micro_credits,
        tiers,
        etp_id,
        distributed,
    })
}
