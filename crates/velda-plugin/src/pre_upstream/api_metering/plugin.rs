//! API Metering & Monetization Plugin (`api_metering`).
//!
//! # Objective & Mission
//! Enforces tenant organization tiers (`free`, `pro`, `enterprise`), prepaid wallet balances,
//! and macro-level quotas (e.g. 1,000,000 requests/month, 50GB bandwidth) with authoritative
//! rejection (`402 Payment Required` or `429 Too Many Requests`).
//!
//! # Phase
//! Evaluated during the `pre_upstream` phase immediately before forwarding to backend backplanes.
//!
//! # Rejection Behavior
//! - `ExhaustedAction::Reject402`: Short-circuits with [`Action::Respond`] carrying `402 Payment Required`,
//!   an informative JSON payload, and `x-metering-tier` metadata.
//! - `ExhaustedAction::Reject429`: Rejects with [`Action::Reject(RejectReason::TooManyRequests)`] and
//!   tags Bit-1 bypass headers (`Retry-After`, `x-metering-exhausted`).
//! - `ExhaustedAction::Passthrough`: Permits request and records debt (negative micro-credit balance).

use bytes::Bytes;
use http::header::{CONTENT_LENGTH, CONTENT_TYPE, RETRY_AFTER};
use http::{HeaderName, HeaderValue, StatusCode};
use rustc_hash::FxHashMap;
use std::sync::Arc;
use std::time::Instant;
use velda_core::{Action, L7Request, L7Response, RejectReason};
use velda_sync::post_sync::plugin::PluginConfig;

use super::algorithm::{ExhaustedAction, MeteringDecision, ShardedMeteringState, TierConfig};
use super::key::resolve_account_and_tier;
use crate::capability::{AccessLevel, PluginCapability, SupportedProtocols};
use crate::error::PluginError;
use crate::pre_upstream::PreUpstreamPlugin;
use crate::pre_upstream::consumer_quota::{QuotaAlgorithm, QuotaWindowConfig};
use crate::pre_upstream::distributed::{DistributedSyncConfig, RedisClient};

/// Static configuration contract for [`ApiMeteringPlugin`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiMeteringConfig {
    /// Ordered list of header names evaluated to resolve tenant account identity.
    pub account_headers: Vec<HeaderName>,
    /// Header name specifying client tier (default: `x-plan-tier`).
    pub tier_header: HeaderName,
    /// Fallback tier name when header is absent or unrecognized (default: `free`).
    pub default_tier: String,
    /// Initial wallet micro-credits credited to newly discovered accounts.
    pub initial_balance_micro_credits: i64,
    /// Tier definitions indexed by lowercase name.
    pub tiers: FxHashMap<String, TierConfig>,
    /// Distributed multi-gateway synchronization configuration via Redis.
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
            distributed: None,
        }
    }
}

/// Pre-upstream multi-tenant API metering and billing enforcement engine.
pub struct ApiMeteringPlugin {
    pub id: String,
    pub config: ApiMeteringConfig,
    pub state: Arc<ShardedMeteringState>,
}

impl std::fmt::Debug for ApiMeteringPlugin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ApiMeteringPlugin")
            .field("id", &self.id)
            .field("config", &self.config)
            .finish()
    }
}

impl ApiMeteringPlugin {
    /// Constructs a new API metering plugin instance with an explicit configuration.
    pub fn new(id: impl Into<String>, config: ApiMeteringConfig) -> Self {
        let state = Arc::new(ShardedMeteringState::new());
        maybe_spawn_shadow_metering_sync(state.clone(), &config);
        Self {
            id: id.into(),
            config,
            state,
        }
    }

    /// Constructs an instance sharing an existing sharded metering state.
    pub fn with_shared_state(
        id: impl Into<String>,
        config: ApiMeteringConfig,
        state: Arc<ShardedMeteringState>,
    ) -> Self {
        maybe_spawn_shadow_metering_sync(state.clone(), &config);
        Self {
            id: id.into(),
            config,
            state,
        }
    }

    /// Compiles a configuration from control-plane JSON definition.
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
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

        let tier_header = if let Some(s) = config.config.get("tier_header").and_then(|v| v.as_str())
        {
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
                let name = tier_name.to_ascii_lowercase();
                let cost_per_req_micro_credits = val
                    .get("cost_per_req_micro_credits")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0);

                let exhausted_action = val
                    .get("exhausted_action")
                    .and_then(|v| v.as_str())
                    .map(ExhaustedAction::parse)
                    .unwrap_or(ExhaustedAction::Reject402);

                let mut windows = Vec::new();
                if let Some(win_arr) = val.get("windows").and_then(|v| v.as_array()) {
                    for (idx, w) in win_arr.iter().enumerate() {
                        let w_name = w
                            .get("name")
                            .and_then(|v| v.as_str())
                            .unwrap_or(&format!("window_{idx}"))
                            .to_string();

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
                    },
                );
            }
        }

        // Fallback default tier if user did not supply tiers map
        if tiers.is_empty() {
            tiers = ApiMeteringConfig::default().tiers;
        }

        let distributed = config
            .config
            .get("distributed")
            .map(|v| DistributedSyncConfig::from_json(Some(v)));

        Ok(Self::new(
            config.id.clone(),
            ApiMeteringConfig {
                account_headers,
                tier_header,
                default_tier,
                initial_balance_micro_credits,
                tiers,
                distributed,
            },
        ))
    }
}

static X_METERING_TIER: HeaderName = HeaderName::from_static("x-metering-tier");
static X_METERING_REMAINING: HeaderName = HeaderName::from_static("x-metering-remaining");
static X_METERING_WINDOW: HeaderName = HeaderName::from_static("x-metering-window");
static X_METERING_BALANCE: HeaderName = HeaderName::from_static("x-metering-balance");
static X_METERING_OVERAGE: HeaderName = HeaderName::from_static("x-metering-overage");
static X_METERING_EXHAUSTED_WINDOW: HeaderName =
    HeaderName::from_static("x-metering-exhausted-window");
static X_METERING_EXHAUSTED: HeaderName = HeaderName::from_static("x-metering-exhausted");

impl PreUpstreamPlugin for ApiMeteringPlugin {
    fn id(&self) -> &str {
        &self.id
    }

    fn capability(&self) -> PluginCapability {
        PluginCapability::header_only(AccessLevel::Read)
    }

    fn supported_protocols(&self) -> SupportedProtocols {
        SupportedProtocols::ALL_L7
    }

    fn on_upstream_request(&self, req: &mut L7Request, _upstream_target: &mut Arc<str>) -> Action {
        let (account_hash, tier_name) = resolve_account_and_tier(
            req,
            &self.config.account_headers,
            &self.config.tier_header,
            &self.config.default_tier,
        );

        let default_cfg = TierConfig::default();
        let tier_cfg = self
            .config
            .tiers
            .get(&tier_name)
            .or_else(|| self.config.tiers.get(&self.config.default_tier))
            .unwrap_or(&default_cfg);

        // Use authoritative pre-sampled ingress timestamp from HeaderWorkspace (0ns)
        let now_sec = if req.headers.ingress_time_sec != 0 {
            req.headers.ingress_time_sec
        } else {
            req.ingress_time_sec
        };
        let now_instant = Instant::now();

        match self.state.evaluate(
            account_hash,
            tier_cfg,
            self.config.initial_balance_micro_credits,
            now_sec,
            now_instant,
        ) {
            MeteringDecision::Allowed {
                min_remaining,
                window_name,
                balance_micro_credits,
            } => {
                if let Ok(val) = HeaderValue::from_str(&tier_cfg.name) {
                    req.headers.insert(&X_METERING_TIER, val);
                }
                req.headers
                    .set_u64(X_METERING_REMAINING.clone(), min_remaining);
                if let Ok(val) = HeaderValue::from_str(&window_name) {
                    req.headers.insert(&X_METERING_WINDOW, val);
                }
                if balance_micro_credits >= 0 {
                    req.headers
                        .set_u64(X_METERING_BALANCE.clone(), balance_micro_credits as u64);
                } else {
                    let mut itoa_buf = itoa::Buffer::new();
                    let balance_val = HeaderValue::from_str(itoa_buf.format(balance_micro_credits))
                        .unwrap_or_else(|_| HeaderValue::from_static("0"));
                    req.headers.insert(&X_METERING_BALANCE, balance_val);
                }
                Action::Continue
            }
            MeteringDecision::OverageAllowed { negative_balance } => {
                if let Ok(val) = HeaderValue::from_str(&tier_cfg.name) {
                    req.headers.insert(&X_METERING_TIER, val);
                }
                req.headers
                    .insert(&X_METERING_OVERAGE, HeaderValue::from_static("true"));
                let mut itoa_buf = itoa::Buffer::new();
                let balance_val = HeaderValue::from_str(itoa_buf.format(negative_balance))
                    .unwrap_or_else(|_| HeaderValue::from_static("0"));
                req.headers.insert(&X_METERING_BALANCE, balance_val);
                Action::Continue
            }
            MeteringDecision::Exhausted {
                action,
                window_name,
                reset_seconds,
            } => match action {
                ExhaustedAction::Reject402 => {
                    let json_payload = format!(
                        r#"{{"error":"payment_required","message":"API tier quota or prepaid credit balance exhausted","tier":"{}","window":"{}","reset_seconds":{}}}"#,
                        tier_cfg.name, window_name, reset_seconds
                    );
                    let body_bytes = Bytes::from(json_payload);
                    let len = body_bytes.len();

                    let mut resp = L7Response::from_bytes(StatusCode::PAYMENT_REQUIRED, body_bytes);
                    resp.headers.insert(
                        CONTENT_TYPE,
                        HeaderValue::from_static("application/json; charset=utf-8"),
                    );
                    resp.headers.set_u64(CONTENT_LENGTH, len as u64);
                    if let Ok(val) = HeaderValue::from_str(&tier_cfg.name) {
                        resp.headers.insert(&X_METERING_TIER, val);
                    }
                    if let Ok(val) = HeaderValue::from_str(&window_name) {
                        resp.headers.insert(&X_METERING_EXHAUSTED_WINDOW, val);
                    }
                    if reset_seconds > 0 {
                        resp.headers.set_u64(RETRY_AFTER, reset_seconds);
                    }
                    resp.headers.render();

                    Action::Respond(resp)
                }
                ExhaustedAction::Reject429 => {
                    if reset_seconds > 0 {
                        req.headers.insert_bypass_u64(RETRY_AFTER, reset_seconds);
                    }
                    if let Ok(val) = HeaderValue::from_str(&tier_cfg.name) {
                        req.headers.insert_bypass(X_METERING_TIER.clone(), val);
                    }
                    req.headers.insert_bypass(
                        X_METERING_EXHAUSTED.clone(),
                        HeaderValue::from_static("true"),
                    );

                    Action::Reject(RejectReason::TooManyRequests)
                }
                ExhaustedAction::Passthrough => Action::Continue,
            },
        }
    }
}

/// Spawns an autonomous background Shadow Logistics Worker loop reconciling local credit deltas with distributed Redis.
fn maybe_spawn_shadow_metering_sync(state: Arc<ShardedMeteringState>, config: &ApiMeteringConfig) {
    let dist = match &config.distributed {
        Some(d) if d.enabled => d,
        _ => return,
    };

    let dist_cfg = dist.clone();
    let initial_balance = config.initial_balance_micro_credits;
    let weak_state = Arc::downgrade(&state);

    let _ = std::thread::Builder::new()
        .name("velda-metering-sync".to_string())
        .spawn(move || {
            velda_core::hardware::set_current_thread_idle_priority();
            let rt = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(r) => r,
                Err(e) => {
                    tracing::error!(error = %e, "Failed to build tokio runtime for shadow metering sync");
                    return;
                }
            };

            rt.block_on(async move {
                let mut client = RedisClient::new(dist_cfg.redis_addr);
                let mut interval =
                    tokio::time::interval(std::time::Duration::from_millis(dist_cfg.sync_interval_ms));

                loop {
                    interval.tick().await;

                    let state = match weak_state.upgrade() {
                        Some(s) => s,
                        None => break,
                    };

                    let deltas = state.collect_deltas();
                    if deltas.is_empty() {
                        continue;
                    }

                    // Format HINCRBY {prefix}:metering:consumed {acc_hash} {delta}
                    let mut cmd_strings = Vec::with_capacity(deltas.len());
                    for &(acc_hash, delta) in &deltas {
                        let key = format!("{}:metering:consumed", dist_cfg.key_prefix);
                        let field = acc_hash.to_string();
                        // Track positive accumulated consumption
                        cmd_strings.push((key, field, delta, acc_hash));
                    }

                    let cmd_refs: Vec<(&str, &str, i64)> = cmd_strings
                        .iter()
                        .map(|(k, f, d, _)| (k.as_str(), f.as_str(), *d))
                        .collect();

                    match client.pipelined_hincrby(&cmd_refs).await {
                        Ok(results) => {
                            let mut updates = Vec::with_capacity(results.len());
                            for (idx, &res) in results.iter().enumerate() {
                                let (_, _, _, acc_hash) = cmd_strings[idx];
                                let remaining_balance = initial_balance - res;
                                updates.push((acc_hash, remaining_balance));
                            }
                            state.reconcile_balances(&updates);
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, "Shadow logistics failed to flush metering deltas to Redis");
                        }
                    }
                }
            });
        });
}
