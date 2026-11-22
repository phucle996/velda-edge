//! Schema & semantic validation for `openai_token_quota` (`post_response`).
//!
//! ```text
//!   Raw JSON Value
//!     ├── Deserialize OpenAiTokenQuotaConfig (#[serde(deny_unknown_fields)])
//!     ├── Validate consumer_id_headers (non-empty RFC 9110 header tokens, lowercase)
//!     ├── Normalize ledger_slots (power-of-2 in 64..=1_048_576 -> k * 4096B shared arena)
//!     └── If mode == "distributed": validate redis_url, micro_quota_batch > 0, sync_interval_ms >= 50
//! ```

use serde_json::Value;

use crate::error::PluginError;
use crate::post_response::openai_token_quota::config::{OpenAiTokenQuotaConfig, QuotaMode};

fn is_valid_header_name(h: &str) -> bool {
    let trimmed = h.trim();
    if trimmed.is_empty() {
        return false;
    }
    trimmed.bytes().all(|b| {
        b.is_ascii_alphanumeric()
            || matches!(
                b,
                b'!' | b'#'
                    | b'$'
                    | b'%'
                    | b'&'
                    | b'\''
                    | b'*'
                    | b'+'
                    | b'-'
                    | b'.'
                    | b'^'
                    | b'_'
                    | b'`'
                    | b'|'
                    | b'~'
            )
    })
}

/// Validates and normalizes `openai_token_quota` plugin configuration.
pub fn validate_openai_token_quota_config(
    plugin_id: &str,
    raw_config: &Value,
) -> Result<OpenAiTokenQuotaConfig, PluginError> {
    let mut cfg: OpenAiTokenQuotaConfig =
        serde_json::from_value(raw_config.clone()).map_err(|e| {
            PluginError::Config(format!(
                "Plugin '{plugin_id}' has invalid openai_token_quota config: {e}"
            ))
        })?;

    if cfg.consumer_id_headers.is_empty() {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': consumer_id_headers cannot be empty"
        )));
    }
    for h in &mut cfg.consumer_id_headers {
        let lower = h.trim().to_ascii_lowercase();
        if !is_valid_header_name(&lower) {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': invalid consumer_id_header '{h}'"
            )));
        }
        *h = lower;
    }

    if cfg.ledger_slots < 64 || cfg.ledger_slots > 1_048_576 {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': ledger_slots ({}) must be in 64..=1048576",
            cfg.ledger_slots
        )));
    }
    cfg.ledger_slots = cfg.ledger_slots.next_power_of_two();

    if cfg.mode == QuotaMode::Distributed {
        let url = cfg
            .redis_url
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                PluginError::Config(format!(
                    "Plugin '{plugin_id}': redis_url is required when mode is 'distributed'"
                ))
            })?;
        if !url.starts_with("redis://") && !url.starts_with("rediss://") {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': redis_url '{url}' must start with 'redis://' or 'rediss://'"
            )));
        }
        cfg.redis_url = Some(url.to_string());

        if let Some(ref tls) = cfg.redis_tls {
            tls.build_client_config(&[]).map_err(|e| {
                PluginError::Config(format!(
                    "Plugin '{plugin_id}': invalid redis_tls config: {e}"
                ))
            })?;
        }

        if cfg.micro_quota_batch == 0 {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': micro_quota_batch must be > 0"
            )));
        }
        if cfg.batch_users == 0 || cfg.batch_users > 512 {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': batch_users ({}) must be in 1..=512",
                cfg.batch_users
            )));
        }
        if cfg.sync_interval_ms < 50 {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': sync_interval_ms ({}) must be >= 50",
                cfg.sync_interval_ms
            )));
        }
    }

    if cfg.shadow_weight == 0 || cfg.shadow_weight > 100 {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': shadow_weight ({}) must be in 1..=100",
            cfg.shadow_weight
        )));
    }

    Ok(cfg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_valid_config_normalizes_headers_and_slots() {
        let raw = json!({
            "consumer_id_headers": [" X-Tenant-ID ", " X-User-ID "],
            "ledger_slots": 100
        });
        let cfg = validate_openai_token_quota_config("openai_token_quota", &raw).unwrap();
        assert_eq!(cfg.mode, QuotaMode::Local);
        assert_eq!(cfg.consumer_id_headers, vec!["x-tenant-id", "x-user-id"]);
        assert_eq!(cfg.ledger_slots, 128);
    }

    #[test]
    fn test_rejects_missing_or_empty_consumer_id_headers_and_invalid_distributed() {
        let missing = json!({});
        assert!(validate_openai_token_quota_config("ai", &missing).is_err());

        let empty = json!({ "consumer_id_headers": [] });
        assert!(validate_openai_token_quota_config("ai", &empty).is_err());

        let bad_dist = json!({
            "mode": "distributed",
            "consumer_id_headers": ["x-tenant-id"]
        });
        assert!(validate_openai_token_quota_config("ai", &bad_dist).is_err());

        let valid_dist = json!({
            "mode": "distributed",
            "consumer_id_headers": ["x-tenant-id", "x-user-id"],
            "redis_url": "redis://127.0.0.1:6379",
            "micro_quota_batch": 5000,
            "refill_watermark": 1000
        });
        assert!(validate_openai_token_quota_config("ai", &valid_dist).is_ok());
    }
}
