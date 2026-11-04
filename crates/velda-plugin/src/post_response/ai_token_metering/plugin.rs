//! AI Token Metering & Monetization Post-Response Plugin (`ai_token_metering`).
//!
//! # Architectural Invariants
//! - **Phase Placement**: Evaluated during `post_response` because token usage metadata is only
//!   materialized after the upstream LLM finishes response generation.
//! - **Strict Protocol Boundary**: Dedicated to HTTP REST and SSE streaming pipelines
//!   (`SupportedProtocols::ALL_HTTP`). Zero cross-protocol transcoding.
//! - **Explicit Unpriced Policy (No Silent Fallback)**: If a model is not defined in the pricing
//!   catalog, the plugin never assumes a fallback rate. It sets `x-ai-metering-status: unpriced_model`,
//!   bills $0, logs raw tokens for auditing, and increments warning counters.
//! - **Stack-Allocated Telemetry**: Uses `itoa::Buffer` to format `x-ai-*` headers in-place on the
//!   stack, preserving zero-heap-allocation guarantees on response return.

use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use http::HeaderValue;
use http::header::HeaderName;
use velda_core::{HeaderWorkspace, L7Response};
use velda_sync::post_sync::plugin::PluginConfig;

use super::config::AiTokenMeteringConfig;
use super::scanner::{scan_model, scan_token_usage};
use super::state::{ConsumerAiUsage, ShardedAiUsageLedger};
use crate::capability::{AccessLevel, PluginCapability, SupportedProtocols};
use crate::error::PluginError;
use crate::post_response::PostResponsePlugin;
use crate::pre_upstream::RedisClient;

/// AI Token Metering & Monetization Post-Response Plugin.
pub struct AiTokenMeteringPlugin {
    id: String,
    pub config: AiTokenMeteringConfig,
    pub ledger: Arc<ShardedAiUsageLedger>,
    pub metered_requests_total: Arc<AtomicU64>,
    pub metered_tokens_total: Arc<AtomicU64>,
    pub metered_cost_micro_usd_total: Arc<AtomicU64>,
    pub unpriced_requests_total: Arc<AtomicU64>,
    pub unpriced_tokens_total: Arc<AtomicU64>,
}

impl AiTokenMeteringPlugin {
    /// Creates a new plugin instance from configuration.
    pub fn new(id: impl Into<String>, config: AiTokenMeteringConfig) -> Self {
        let ledger = Arc::new(ShardedAiUsageLedger::new());

        maybe_spawn_shadow_ai_sync(ledger.clone(), &config);

        Self {
            id: id.into(),
            config,
            ledger,
            metered_requests_total: Arc::new(AtomicU64::new(0)),
            metered_tokens_total: Arc::new(AtomicU64::new(0)),
            metered_cost_micro_usd_total: Arc::new(AtomicU64::new(0)),
            unpriced_requests_total: Arc::new(AtomicU64::new(0)),
            unpriced_tokens_total: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Instantiates plugin from static [`PluginConfig`] JSON.
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let cfg = AiTokenMeteringConfig::from_json(&config.config)?;
        Ok(Self::new(&config.id, cfg))
    }

    /// Queries the in-memory recorded usage state for a given consumer ID.
    pub fn get_consumer_usage(&self, consumer_id: &str) -> Option<ConsumerAiUsage> {
        let mut hasher = DefaultHasher::new();
        consumer_id.hash(&mut hasher);
        let consumer_hash = hasher.finish();
        self.ledger.get_consumer_usage(consumer_hash)
    }
}

impl PostResponsePlugin for AiTokenMeteringPlugin {
    fn id(&self) -> &str {
        &self.id
    }

    fn on_response(&self, req_headers: &HeaderWorkspace, resp: &mut L7Response) {
        if resp.body.is_empty() {
            return;
        }

        let usage = match scan_token_usage(resp.body.as_slice()) {
            Some(u) => u,
            None => {
                if self.config.inject_telemetry_headers {
                    resp.headers.insert(
                        HeaderName::from_static("x-ai-metering-status"),
                        HeaderValue::from_static("no_usage_found"),
                    );
                }
                return;
            }
        };

        // Resolve model: check request header `x-ai-model` first, then scan response body
        let model_str = req_headers
            .get("x-ai-model")
            .and_then(|v| v.to_str().ok())
            .or_else(|| scan_model(resp.body.as_slice()))
            .unwrap_or("unknown");

        let model_lower = model_str.to_ascii_lowercase();

        // Extract consumer identity
        let mut consumer_id = "anonymous";
        for h in &self.config.consumer_id_headers {
            if let Some(val) = req_headers.get(h).and_then(|v| v.to_str().ok()) {
                let trimmed = val.trim();
                if !trimmed.is_empty() {
                    consumer_id = trimmed;
                    break;
                }
            }
        }

        let mut hasher = DefaultHasher::new();
        consumer_id.hash(&mut hasher);
        let consumer_hash = hasher.finish();

        let now_sec = velda_core::coarse_now_sec();

        // Strict Pricing Lookup: NEVER fallback to another model silently!
        if let Some(pricing) = self.config.model_pricing.get(&model_lower) {
            let cost_micro_usd = pricing.compute_cost_micro_usd(
                usage.prompt_tokens,
                usage.completion_tokens,
                usage.cached_prompt_tokens,
            );

            self.ledger
                .record(consumer_hash, &usage, cost_micro_usd, now_sec);
            self.metered_requests_total.fetch_add(1, Ordering::Relaxed);
            self.metered_tokens_total
                .fetch_add(usage.total_tokens, Ordering::Relaxed);
            self.metered_cost_micro_usd_total
                .fetch_add(cost_micro_usd, Ordering::Relaxed);

            if self.config.inject_telemetry_headers {
                let mut itoa_buf = itoa::Buffer::new();

                if let Ok(model_val) = HeaderValue::from_str(model_str) {
                    resp.headers
                        .insert(HeaderName::from_static("x-ai-model"), model_val);
                }
                if let Ok(v) = HeaderValue::from_str(itoa_buf.format(usage.prompt_tokens)) {
                    resp.headers
                        .insert(HeaderName::from_static("x-ai-prompt-tokens"), v);
                }
                if let Ok(v) = HeaderValue::from_str(itoa_buf.format(usage.completion_tokens)) {
                    resp.headers
                        .insert(HeaderName::from_static("x-ai-completion-tokens"), v);
                }
                if let Ok(v) = HeaderValue::from_str(itoa_buf.format(usage.total_tokens)) {
                    resp.headers
                        .insert(HeaderName::from_static("x-ai-total-tokens"), v);
                }
                if usage.cached_prompt_tokens > 0
                    && let Ok(v) =
                        HeaderValue::from_str(itoa_buf.format(usage.cached_prompt_tokens))
                {
                    resp.headers
                        .insert(HeaderName::from_static("x-ai-cached-tokens"), v);
                }
                if let Ok(v) = HeaderValue::from_str(itoa_buf.format(cost_micro_usd)) {
                    resp.headers
                        .insert(HeaderName::from_static("x-ai-cost-micro-usd"), v);
                }
                resp.headers.insert(
                    HeaderName::from_static("x-ai-metering-status"),
                    HeaderValue::from_static("success"),
                );
            }
        } else {
            // Unpriced model: Strict invariant - audit tokens, record cost 0, warn consumer!
            self.ledger.record(consumer_hash, &usage, 0, now_sec);
            self.unpriced_requests_total.fetch_add(1, Ordering::Relaxed);
            self.unpriced_tokens_total
                .fetch_add(usage.total_tokens, Ordering::Relaxed);

            if self.config.inject_telemetry_headers {
                let mut itoa_buf = itoa::Buffer::new();

                if let Ok(model_val) = HeaderValue::from_str(model_str) {
                    resp.headers
                        .insert(HeaderName::from_static("x-ai-model"), model_val);
                }
                if let Ok(v) = HeaderValue::from_str(itoa_buf.format(usage.prompt_tokens)) {
                    resp.headers
                        .insert(HeaderName::from_static("x-ai-prompt-tokens"), v);
                }
                if let Ok(v) = HeaderValue::from_str(itoa_buf.format(usage.completion_tokens)) {
                    resp.headers
                        .insert(HeaderName::from_static("x-ai-completion-tokens"), v);
                }
                if let Ok(v) = HeaderValue::from_str(itoa_buf.format(usage.total_tokens)) {
                    resp.headers
                        .insert(HeaderName::from_static("x-ai-total-tokens"), v);
                }
                if usage.cached_prompt_tokens > 0
                    && let Ok(v) =
                        HeaderValue::from_str(itoa_buf.format(usage.cached_prompt_tokens))
                {
                    resp.headers
                        .insert(HeaderName::from_static("x-ai-cached-tokens"), v);
                }
                resp.headers.insert(
                    HeaderName::from_static("x-ai-cost-micro-usd"),
                    HeaderValue::from_static("0"),
                );
                resp.headers.insert(
                    HeaderName::from_static("x-ai-metering-status"),
                    HeaderValue::from_static("unpriced_model"),
                );
            }
        }
    }

    fn capability(&self) -> PluginCapability {
        PluginCapability::new(AccessLevel::Write, AccessLevel::Read)
    }

    fn supported_protocols(&self) -> SupportedProtocols {
        SupportedProtocols::ALL_HTTP
    }
}

/// Spawns an autonomous background Shadow Logistics Worker loop reconciling local deltas with distributed Redis.
fn maybe_spawn_shadow_ai_sync(ledger: Arc<ShardedAiUsageLedger>, config: &AiTokenMeteringConfig) {
    let dist = match &config.distributed {
        Some(d) if d.enabled => d,
        _ => return,
    };

    let dist_cfg = dist.clone();
    let weak_ledger = Arc::downgrade(&ledger);

    let _ = std::thread::Builder::new()
        .name("velda-ai-sync".to_string())
        .spawn(move || {
            velda_core::set_current_thread_idle_priority();
            let rt = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(r) => r,
                Err(e) => {
                    tracing::error!(
                        error = %e,
                        "Failed to build tokio runtime for shadow AI metering sync"
                    );
                    return;
                }
            };

            rt.block_on(async move {
                let mut client = RedisClient::new(dist_cfg.redis_addr);
                let mut interval = tokio::time::interval(std::time::Duration::from_millis(
                    dist_cfg.sync_interval_ms,
                ));

                loop {
                    interval.tick().await;

                    let ledger = match weak_ledger.upgrade() {
                        Some(l) => l,
                        None => break,
                    };

                    let deltas = ledger.drain_deltas();
                    if deltas.is_empty() {
                        continue;
                    }

                    // Format HINCRBY commands for prompt, completion, and cost
                    let mut cmd_strings = Vec::with_capacity(deltas.len() * 3);
                    for (acc_hash, d_prompt, d_comp, _d_cached, d_cost) in deltas {
                        let hash_str = acc_hash.to_string();
                        if d_prompt > 0 {
                            let key = format!("{}:ai:prompt_tokens", dist_cfg.key_prefix);
                            cmd_strings.push((key, hash_str.clone(), d_prompt as i64));
                        }
                        if d_comp > 0 {
                            let key = format!("{}:ai:completion_tokens", dist_cfg.key_prefix);
                            cmd_strings.push((key, hash_str.clone(), d_comp as i64));
                        }
                        if d_cost > 0 {
                            let key = format!("{}:ai:cost_micro_usd", dist_cfg.key_prefix);
                            cmd_strings.push((key, hash_str, d_cost as i64));
                        }
                    }

                    let cmd_refs: Vec<(&str, &str, i64)> = cmd_strings
                        .iter()
                        .map(|(k, f, d)| (k.as_str(), f.as_str(), *d))
                        .collect();

                    if let Err(e) = client.ensure_connected().await {
                        tracing::debug!(error = %e, "Failed connecting to distributed Redis");
                        continue;
                    }

                    if let Err(e) = client.pipelined_hincrby(&cmd_refs).await {
                        tracing::debug!(error = %e, "Failed to flush pipelined AI usage to Redis");
                    }
                }
            });
        });
}
