//! Static Configuration Contract for [`AiTokenMeteringPlugin`].
//!
//! # Invariants
//! - **Strict Validation**: All model names are normalized to lowercase.
//! - **No Silent Fallback Model**: A default model fallback is strictly forbidden; unpriced models
//!   must explicitly fail resolution and raise warnings.

use http::header::HeaderName;
use rustc_hash::FxHashMap;
use serde_json::Value;

use super::pricing::ModelPricing;
use crate::error::PluginError;
use crate::pre_upstream::DistributedSyncConfig;

/// Configuration contract for [`super::AiTokenMeteringPlugin`].
#[derive(Debug, Clone)]
pub struct AiTokenMeteringConfig {
    /// Ordered list of header names evaluated to resolve consumer / tenant identity.
    pub consumer_id_headers: Vec<HeaderName>,
    /// Model catalog pricing rates mapped by lowercase model identifier.
    pub model_pricing: FxHashMap<String, ModelPricing>,
    /// Whether to inject `x-ai-*` telemetry headers into the response.
    pub inject_telemetry_headers: bool,
    /// Optional distributed Redis cluster synchronization configuration.
    pub distributed: Option<DistributedSyncConfig>,
}

impl Default for AiTokenMeteringConfig {
    fn default() -> Self {
        let mut model_pricing = FxHashMap::default();
        // Reference prices ($10^-6 micro-USD per 1M tokens)
        // gpt-4o: $2.50 prompt, $1.25 cached, $10.00 completion
        model_pricing.insert(
            "gpt-4o".to_string(),
            ModelPricing {
                prompt_micro_usd_per_million: 2_500_000,
                cached_prompt_micro_usd_per_million: 1_250_000,
                completion_micro_usd_per_million: 10_000_000,
            },
        );
        // gpt-4o-mini: $0.15 prompt, $0.075 cached, $0.60 completion
        model_pricing.insert(
            "gpt-4o-mini".to_string(),
            ModelPricing {
                prompt_micro_usd_per_million: 150_000,
                cached_prompt_micro_usd_per_million: 75_000,
                completion_micro_usd_per_million: 600_000,
            },
        );
        // deepseek-chat: $0.14 prompt, $0.014 cached, $0.28 completion
        model_pricing.insert(
            "deepseek-chat".to_string(),
            ModelPricing {
                prompt_micro_usd_per_million: 140_000,
                cached_prompt_micro_usd_per_million: 14_000,
                completion_micro_usd_per_million: 280_000,
            },
        );
        // deepseek-coder: $0.14 prompt, $0.014 cached, $0.28 completion
        model_pricing.insert(
            "deepseek-coder".to_string(),
            ModelPricing {
                prompt_micro_usd_per_million: 140_000,
                cached_prompt_micro_usd_per_million: 14_000,
                completion_micro_usd_per_million: 280_000,
            },
        );
        // claude-3-5-sonnet: $3.00 prompt, $0.30 cached, $15.00 completion
        model_pricing.insert(
            "claude-3-5-sonnet-20241022".to_string(),
            ModelPricing {
                prompt_micro_usd_per_million: 3_000_000,
                cached_prompt_micro_usd_per_million: 300_000,
                completion_micro_usd_per_million: 15_000_000,
            },
        );

        Self {
            consumer_id_headers: vec![
                HeaderName::from_static("x-consumer-id"),
                HeaderName::from_static("x-tenant-id"),
                HeaderName::from_static("x-user-id"),
            ],
            model_pricing,
            inject_telemetry_headers: true,
            distributed: None,
        }
    }
}

impl AiTokenMeteringConfig {
    /// Parses configuration from JSON payload.
    pub fn from_json(config: &Value) -> Result<Self, PluginError> {
        let mut cfg = Self::default();

        if let Some(headers) = config.get("consumer_id_headers").and_then(Value::as_array) {
            let mut parsed_headers = Vec::with_capacity(headers.len());
            for h in headers {
                if let Some(s) = h.as_str() {
                    let header_name = HeaderName::from_bytes(s.trim().as_bytes()).map_err(|e| {
                        PluginError::Config(format!("Invalid consumer_id_header '{s}': {e}"))
                    })?;
                    parsed_headers.push(header_name);
                }
            }
            if !parsed_headers.is_empty() {
                cfg.consumer_id_headers = parsed_headers;
            }
        }

        if let Some(models) = config.get("models").and_then(Value::as_object) {
            for (model_name, pricing_val) in models {
                let prompt_micro = pricing_val
                    .get("prompt_micro_usd_per_million")
                    .and_then(Value::as_u64)
                    .unwrap_or(0);
                let completion_micro = pricing_val
                    .get("completion_micro_usd_per_million")
                    .and_then(Value::as_u64)
                    .unwrap_or(0);
                let cached_micro = pricing_val
                    .get("cached_prompt_micro_usd_per_million")
                    .and_then(Value::as_u64)
                    .unwrap_or(0);

                cfg.model_pricing.insert(
                    model_name.trim().to_ascii_lowercase(),
                    ModelPricing {
                        prompt_micro_usd_per_million: prompt_micro,
                        completion_micro_usd_per_million: completion_micro,
                        cached_prompt_micro_usd_per_million: cached_micro,
                    },
                );
            }
        }

        if let Some(inject) = config
            .get("inject_telemetry_headers")
            .and_then(Value::as_bool)
        {
            cfg.inject_telemetry_headers = inject;
        }

        if let Some(dist_val) = config.get("distributed") {
            let dist = DistributedSyncConfig::from_json(Some(dist_val));
            if dist.enabled {
                cfg.distributed = Some(dist);
            }
        }

        Ok(cfg)
    }
}
