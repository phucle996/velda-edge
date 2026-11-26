//! API Metering Plugin (`api_metering::plugin`).
//!
//! ```text
//! on_upstream_request(req)
//!   ├─► resolve_account_hash() ─► None -> #[cold] 400 Bad Request
//!   ├─► resolve_tier()         ─► &CompiledTier (0 alloc)
//!   └─► evaluate_compiled()
//!         ├── Allowed        ─► write x-metering-* headers -> Continue
//!         ├── OverageAllowed ─► #[cold] overage headers    -> Continue
//!         └── Exhausted      ─► #[cold] 402 JSON | 429 bypass
//! ```

use std::fmt::Write as _;
use std::sync::{Arc, OnceLock};
use std::time::Instant;

use bytes::Bytes;
use http::header::{CONTENT_LENGTH, CONTENT_TYPE, RETRY_AFTER};
use http::{HeaderName, HeaderValue, StatusCode};
use velda_core::{Action, L7Request, L7Response, RejectReason};
use velda_etp::EtpInstance;

use super::algorithm::{CompiledDecision, ShardedMeteringState};
use super::config::{
    ApiMeteringConfig, CompiledTier, ExhaustedAction, compile_tiers, parse_metering_config,
};
use super::key::{resolve_account_keys, resolve_tier};
use super::providers::{ApiMeteringProvider, from_etp_instance, run_sync_loop};
use crate::capability::{AccessLevel, PluginCapability, SupportedProtocols};
use crate::config::PluginConfig;
use crate::error::PluginError;
use crate::pre_upstream::PreUpstreamPlugin;

static X_METERING_TIER: HeaderName = HeaderName::from_static("x-metering-tier");
static X_METERING_REMAINING: HeaderName = HeaderName::from_static("x-metering-remaining");
static X_METERING_WINDOW: HeaderName = HeaderName::from_static("x-metering-window");
static X_METERING_BALANCE: HeaderName = HeaderName::from_static("x-metering-balance");
static X_METERING_OVERAGE: HeaderName = HeaderName::from_static("x-metering-overage");
static X_METERING_EXHAUSTED_WINDOW: HeaderName =
    HeaderName::from_static("x-metering-exhausted-window");
static X_METERING_EXHAUSTED: HeaderName = HeaderName::from_static("x-metering-exhausted");

/// Pre-upstream multi-tenant API metering and billing enforcement engine.
pub struct ApiMeteringPlugin {
    pub id: String,
    pub config: ApiMeteringConfig,
    pub state: Arc<ShardedMeteringState>,
    pub provider: OnceLock<ApiMeteringProvider>,
    compiled_tiers: Box<[CompiledTier]>,
    default_tier_idx: usize,
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
        Self::with_shared_state(id, config, state)
    }

    /// Constructs an instance sharing an existing sharded metering state.
    pub fn with_shared_state(
        id: impl Into<String>,
        config: ApiMeteringConfig,
        state: Arc<ShardedMeteringState>,
    ) -> Self {
        let (compiled_tiers, default_tier_idx) = compile_tiers(&config);
        Self {
            id: id.into(),
            config,
            state,
            provider: OnceLock::new(),
            compiled_tiers,
            default_tier_idx,
        }
    }

    /// Compiles a configuration from control-plane JSON definition.
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let parsed = parse_metering_config(config)?;
        Ok(Self::new(config.id.clone(), parsed))
    }
}

impl PreUpstreamPlugin for ApiMeteringPlugin {
    fn id(&self) -> &str {
        &self.id
    }

    #[inline(always)]
    fn pbww(&self) -> u32 {
        if self.config.etp_id.is_some() {
            self.config.pbww.clamp(1, 100)
        } else {
            0
        }
    }

    #[inline(always)]
    fn etp_id(&self) -> Option<&str> {
        self.config.etp_id.as_deref()
    }

    fn attach_etp(&self, instance: &EtpInstance) -> Result<(), PluginError> {
        let provider = from_etp_instance(&self.id, self.config.etp_id.as_deref(), instance)?;
        let _ = self.provider.set(provider);
        Ok(())
    }

    fn run_background(
        self: Arc<Self>,
        shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> Option<crate::BackgroundTaskFuture> {
        let etp_id = self.config.etp_id.as_deref()?;
        let provider = match self.provider.get() {
            Some(p) => p.clone(),
            None => {
                let msg = format!(
                    "Plugin '{}' (api_metering) has no attached KV ETP for etp_id '{}'",
                    self.id, etp_id
                );
                return Some(Box::pin(async move { Err(PluginError::Config(msg)) }));
            }
        };
        let state = Arc::clone(&self.state);
        let initial_balance = self.config.initial_balance_micro_credits;
        let sync_interval_ms = self.config.sync_interval_ms;
        let key_prefix = self.config.key_prefix.clone();
        Some(Box::pin(run_sync_loop(
            provider,
            state,
            sync_interval_ms,
            key_prefix,
            initial_balance,
            shutdown,
        )))
    }

    fn capability(&self) -> PluginCapability {
        // Injects `x-metering-*` telemetry headers onto `req.headers` on the forward path.
        PluginCapability::header_only(AccessLevel::Write)
    }

    fn supported_protocols(&self) -> SupportedProtocols {
        SupportedProtocols::ALL_L7
    }

    fn on_upstream_request(&self, req: &mut L7Request, _targets: &mut u32) -> Action {
        let Some(chain) = resolve_account_keys(req, &self.config.account_headers) else {
            return handle_missing_account();
        };
        let Some(default_tier) = self
            .compiled_tiers
            .get(self.default_tier_idx)
            .or_else(|| self.compiled_tiers.first())
        else {
            return Action::Continue;
        };
        let tier = resolve_tier(
            req,
            &self.config.tier_header,
            &self.compiled_tiers,
            default_tier,
        );

        let mut now_sec = req.headers.ingress_time_sec();
        if now_sec == 0 {
            now_sec = velda_core::clock::coarse_sec();
        }
        let now_instant = if tier.has_token_bucket {
            Instant::now()
        } else {
            self.state.start_instant()
        };

        match self.state.evaluate_chain_compiled(
            &chain,
            tier,
            self.config.initial_balance_micro_credits,
            now_sec,
            now_instant,
        ) {
            CompiledDecision::Allowed {
                min_remaining,
                min_window_idx,
                balance_micro_credits,
            } => {
                if let Some(ref val) = tier.tier_header_val {
                    req.headers.insert(&X_METERING_TIER, val.clone());
                }
                if let Some(w) = min_window_idx.and_then(|idx| tier.windows.get(idx)) {
                    req.headers
                        .set_u64(X_METERING_REMAINING.clone(), min_remaining);
                    if let Some(ref wv) = w.name_header_val {
                        req.headers.insert(&X_METERING_WINDOW, wv.clone());
                    }
                }
                if balance_micro_credits >= 0 {
                    req.headers
                        .set_u64(X_METERING_BALANCE.clone(), balance_micro_credits as u64);
                } else {
                    insert_negative_balance(req, balance_micro_credits);
                }
                Action::Continue
            }
            CompiledDecision::OverageAllowed { negative_balance } => {
                handle_overage_allowed(req, tier.tier_header_val.as_ref(), negative_balance)
            }
            CompiledDecision::Exhausted {
                action,
                window_idx,
                reset_seconds,
            } => handle_exhausted(req, action, tier, window_idx, reset_seconds),
        }
    }
}

#[cold]
#[inline(never)]
fn handle_missing_account() -> Action {
    const JSON_CT: HeaderValue = HeaderValue::from_static("application/json; charset=utf-8");
    const BODY: &[u8] =
        br#"{"error":"bad_request","message":"Missing required tenant account header for API metering"}"#;

    let mut resp = L7Response::from_bytes(StatusCode::BAD_REQUEST, Bytes::from_static(BODY));
    resp.headers.insert(CONTENT_TYPE, JSON_CT);
    resp.headers.set_u64(CONTENT_LENGTH, BODY.len() as u64);
    Action::Respond(resp)
}

#[cold]
#[inline(never)]
fn insert_negative_balance(req: &mut L7Request, balance_micro_credits: i64) {
    const ZERO_VAL: HeaderValue = HeaderValue::from_static("0");
    let mut itoa_buf = itoa::Buffer::new();
    let balance_val =
        HeaderValue::from_str(itoa_buf.format(balance_micro_credits)).unwrap_or(ZERO_VAL);
    req.headers.insert(&X_METERING_BALANCE, balance_val);
}

#[cold]
#[inline(never)]
fn handle_overage_allowed(
    req: &mut L7Request,
    tier_header_val: Option<&HeaderValue>,
    negative_balance: i64,
) -> Action {
    const TRUE_VAL: HeaderValue = HeaderValue::from_static("true");
    if let Some(val) = tier_header_val {
        req.headers.insert(&X_METERING_TIER, val.clone());
    }
    req.headers.insert(&X_METERING_OVERAGE, TRUE_VAL);
    insert_negative_balance(req, negative_balance);
    Action::Continue
}

#[cold]
#[inline(never)]
fn push_json_escaped(out: &mut String, s: &str) {
    for ch in s.chars() {
        match ch {
            '"' => out.push_str(r#"\""#),
            '\\' => out.push_str(r#"\\"#),
            '\n' => out.push_str(r#"\n"#),
            '\r' => out.push_str(r#"\r"#),
            '\t' => out.push_str(r#"\t"#),
            c if (c as u32) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
}

#[cold]
#[inline(never)]
fn handle_exhausted(
    req: &mut L7Request,
    action: ExhaustedAction,
    tier: &CompiledTier,
    window_idx: Option<usize>,
    reset_seconds: u64,
) -> Action {
    const JSON_CT: HeaderValue = HeaderValue::from_static("application/json; charset=utf-8");
    const TRUE_VAL: HeaderValue = HeaderValue::from_static("true");
    const CREDIT_BALANCE_VAL: HeaderValue = HeaderValue::from_static("credit_balance");

    match action {
        ExhaustedAction::Reject402 => {
            let window_str = window_idx
                .and_then(|idx| tier.windows.get(idx))
                .map(|w| w.cfg.name.as_str())
                .unwrap_or("credit_balance");

            let mut json_payload = String::with_capacity(140);
            json_payload.push_str(
                r#"{"error":"payment_required","message":"API tier quota or prepaid credit balance exhausted","tier":""#,
            );
            push_json_escaped(&mut json_payload, &tier.name);
            json_payload.push_str(r#"","window":""#);
            push_json_escaped(&mut json_payload, window_str);
            let _ = write!(json_payload, r#"","reset_seconds":{}}}"#, reset_seconds);

            let body_bytes = Bytes::from(json_payload);
            let len = body_bytes.len();

            let mut resp = L7Response::from_bytes(StatusCode::PAYMENT_REQUIRED, body_bytes);
            resp.headers.insert(CONTENT_TYPE, JSON_CT);
            resp.headers.set_u64(CONTENT_LENGTH, len as u64);
            if let Some(ref tv) = tier.tier_header_val {
                resp.headers.insert(&X_METERING_TIER, tv.clone());
            }
            match window_idx.and_then(|idx| tier.windows.get(idx)) {
                Some(w) => {
                    if let Some(ref wv) = w.name_header_val {
                        resp.headers
                            .insert(&X_METERING_EXHAUSTED_WINDOW, wv.clone());
                    }
                }
                None => {
                    resp.headers
                        .insert(&X_METERING_EXHAUSTED_WINDOW, CREDIT_BALANCE_VAL);
                }
            }
            if reset_seconds > 0 {
                resp.headers.set_u64(RETRY_AFTER, reset_seconds);
            }

            Action::Respond(resp)
        }
        ExhaustedAction::Reject429 => {
            if reset_seconds > 0 {
                req.headers.insert_bypass_u64(RETRY_AFTER, reset_seconds);
            }
            if let Some(ref tv) = tier.tier_header_val {
                req.headers
                    .insert_bypass(X_METERING_TIER.clone(), tv.clone());
            }
            req.headers
                .insert_bypass(X_METERING_EXHAUSTED.clone(), TRUE_VAL);

            Action::Reject(RejectReason::TooManyRequests)
        }
        ExhaustedAction::Passthrough => Action::Continue,
    }
}
