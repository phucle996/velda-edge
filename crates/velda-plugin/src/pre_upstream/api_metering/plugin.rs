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
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use velda_core::{Action, L7Request, L7Response, RejectReason};
use velda_sync::post_sync::plugin::PluginConfig;

use super::algorithm::{ExhaustedAction, MeteringDecision, ShardedMeteringState, TierConfig};
use super::key::resolve_account_and_tier;
use crate::capability::{AccessLevel, PluginCapability, SupportedProtocols};
use crate::error::PluginError;
use crate::pre_upstream::PreUpstreamPlugin;
use crate::pre_upstream::consumer_quota::{QuotaAlgorithm, QuotaWindowConfig};

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
        Self {
            id: id.into(),
            config,
            state: Arc::new(ShardedMeteringState::new()),
        }
    }

    /// Constructs an instance sharing an existing sharded metering state.
    pub fn with_shared_state(
        id: impl Into<String>,
        config: ApiMeteringConfig,
        state: Arc<ShardedMeteringState>,
    ) -> Self {
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

        Ok(Self::new(
            config.id.clone(),
            ApiMeteringConfig {
                account_headers,
                tier_header,
                default_tier,
                initial_balance_micro_credits,
                tiers,
            },
        ))
    }
}

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

        let now_sec = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
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
                    req.headers.insert("x-metering-tier", val);
                }
                if let Ok(val) = HeaderValue::from_str(&min_remaining.to_string()) {
                    req.headers.insert("x-metering-remaining", val);
                }
                if let Ok(val) = HeaderValue::from_str(&window_name) {
                    req.headers.insert("x-metering-window", val);
                }
                if let Ok(val) = HeaderValue::from_str(&balance_micro_credits.to_string()) {
                    req.headers.insert("x-metering-balance", val);
                }
                Action::Continue
            }
            MeteringDecision::OverageAllowed { negative_balance } => {
                if let Ok(val) = HeaderValue::from_str(&tier_cfg.name) {
                    req.headers.insert("x-metering-tier", val);
                }
                req.headers
                    .insert("x-metering-overage", HeaderValue::from_static("true"));
                if let Ok(val) = HeaderValue::from_str(&negative_balance.to_string()) {
                    req.headers.insert("x-metering-balance", val);
                }
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
                    resp.headers.insert(
                        CONTENT_LENGTH,
                        HeaderValue::from_str(&len.to_string()).unwrap(),
                    );
                    if let Ok(val) = HeaderValue::from_str(&tier_cfg.name) {
                        resp.headers.insert("x-metering-tier", val);
                    }
                    if let Ok(val) = HeaderValue::from_str(&window_name) {
                        resp.headers.insert("x-metering-exhausted-window", val);
                    }
                    if reset_seconds > 0 {
                        let sec_str = reset_seconds.to_string();
                        if let Ok(val) = HeaderValue::from_str(&sec_str) {
                            resp.headers.insert(RETRY_AFTER, val);
                        }
                    }

                    Action::Respond(resp)
                }
                ExhaustedAction::Reject429 => {
                    if reset_seconds > 0 {
                        let sec_str = reset_seconds.to_string();
                        if let Ok(val) = HeaderValue::from_str(&sec_str) {
                            req.headers.insert_bypass(RETRY_AFTER, val);
                        }
                    }
                    if let Ok(val) = HeaderValue::from_str(&tier_cfg.name) {
                        req.headers
                            .insert_bypass(HeaderName::from_static("x-metering-tier"), val);
                    }
                    req.headers.insert_bypass(
                        HeaderName::from_static("x-metering-exhausted"),
                        HeaderValue::from_static("true"),
                    );

                    Action::Reject(RejectReason::TooManyRequests)
                }
                ExhaustedAction::Passthrough => Action::Continue,
            },
        }
    }
}
