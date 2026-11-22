//! `openai_token_quota` Post-Response Plugin (`phase: "post_response"`).
//!
//! Hot-Path Execution Mechanism (Zero Heap Allocation, Zero Mutex, Zero Blocking I/O):
//!
//! ```text
//!   Upstream L7Response
//!         │
//!         ▼
//!   [1] scan_token_usage(tail[..4096])  (register-only byte scan on last <= 4 KB)
//!         │
//!         ├── None ──► insert x-ai-metering-status: no_usage_found ──► return
//!         │
//!         ▼ Some(TokenUsage)
//!   [2] Hierarchical Streaming Hash + Stack Key Buffer (0 heap alloc, <= 62B)
//!         req_headers["x-tenant-id"] = "abc"  ─┐
//!         req_headers["x-user-id"]   = "123"  ─┴─► "abc.123" + FNV-1a -> u64 consumer_hash
//!         │
//!         ▼
//!   [3] SharedUsageArena.record(hash, key, prompt, completion, total, cached, coarse_sec())
//!         ├─► Deducts total_tokens from local_remaining (atomic fetch_sub)
//!         └─► In Distributed mode, if local_remaining <= refill_watermark:
//!               notify_one() wakes Shadow Worker to lease micro-quota from Redis
//!               (Redis down? Frontline keeps counting locally without blocking!)
//!         │
//!         ▼
//!   [4] Stack itoa::Buffer (0 alloc) ──► Inject 4 standard client headers + status:
//!         x-ai-prompt-tokens, x-ai-completion-tokens, x-ai-total-tokens,
//!         x-ai-cached-tokens (always "0" if 0), x-ai-metering-status: success | quota_exhausted
//! ```

use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use http::HeaderValue;
use http::header::HeaderName;
use tokio::sync::Notify;
use velda_core::clock::coarse_sec;
use velda_core::{HeaderWorkspace, L7Response};

use crate::BackgroundTaskFuture;
use crate::capability::{AccessLevel, PluginCapability, SupportedProtocols};
use crate::config::{OutboundTlsConfig, PluginConfig};
use crate::error::PluginError;
use crate::post_response::PostResponsePlugin;
use crate::post_response::openai_token_quota::config::QuotaMode;
use crate::post_response::openai_token_quota::local::{
    Fnv1aHasher, MAX_CONSUMER_KEY_LEN, SharedUsageArena, fnv1a_64,
};
use crate::post_response::openai_token_quota::redis::{RedisSyncParams, run_redis_sync_loop};
use crate::post_response::openai_token_quota::scanner::scan_token_usage;
use crate::post_response::openai_token_quota::validation::validate_openai_token_quota_config;

static HDR_PROMPT: HeaderName = HeaderName::from_static("x-ai-prompt-tokens");
static HDR_COMPLETION: HeaderName = HeaderName::from_static("x-ai-completion-tokens");
static HDR_TOTAL: HeaderName = HeaderName::from_static("x-ai-total-tokens");
static HDR_CACHED: HeaderName = HeaderName::from_static("x-ai-cached-tokens");
static HDR_STATUS: HeaderName = HeaderName::from_static("x-ai-metering-status");

/// Zero-allocation, shared-arena OpenAI token quota plugin (`local` & `distributed`).
pub struct OpenAiTokenQuotaPlugin {
    pub id: String,
    pub mode: QuotaMode,
    pub consumer_id_headers: Vec<HeaderName>,
    pub ledger: Arc<SharedUsageArena>,
    pub refill_notify: Arc<Notify>,
    pub redis_connected: Arc<AtomicBool>,
    pub redis_url: Option<String>,
    pub redis_password: Option<String>,
    pub redis_tls: Option<OutboundTlsConfig>,
    pub micro_quota_batch: u64,
    pub batch_users: usize,
    pub sync_interval_ms: u64,
    pub shadow_weight: u32,
    pub counted_requests_total: Arc<AtomicU64>,
    pub counted_tokens_total: Arc<AtomicU64>,
}

impl OpenAiTokenQuotaPlugin {
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let cfg = validate_openai_token_quota_config(&config.id, &config.config)?;

        let mut consumer_id_headers = Vec::with_capacity(cfg.consumer_id_headers.len());
        for h in &cfg.consumer_id_headers {
            let name = HeaderName::from_str(h).map_err(|e| {
                PluginError::Config(format!(
                    "Plugin '{}': invalid header name '{h}': {e}",
                    config.id
                ))
            })?;
            consumer_id_headers.push(name);
        }

        let initial_quota = match cfg.mode {
            QuotaMode::Local => cfg.local_default_quota,
            QuotaMode::Distributed => 0,
        };
        let ledger = Arc::new(SharedUsageArena::new(
            cfg.ledger_slots,
            initial_quota,
            cfg.refill_watermark,
        ));

        Ok(Self {
            id: config.id.clone(),
            mode: cfg.mode,
            consumer_id_headers,
            ledger,
            refill_notify: Arc::new(Notify::new()),
            redis_connected: Arc::new(AtomicBool::new(false)),
            redis_url: cfg.redis_url,
            redis_password: cfg.redis_password,
            redis_tls: cfg.redis_tls,
            micro_quota_batch: cfg.micro_quota_batch,
            batch_users: cfg.batch_users,
            sync_interval_ms: cfg.sync_interval_ms,
            shadow_weight: cfg.shadow_weight,
            counted_requests_total: Arc::new(AtomicU64::new(0)),
            counted_tokens_total: Arc::new(AtomicU64::new(0)),
        })
    }

    #[inline(always)]
    fn write_u64_header(
        headers: &mut HeaderWorkspace,
        name: HeaderName,
        val: u64,
        buf: &mut itoa::Buffer,
    ) {
        let s = buf.format(val);
        if let Ok(hv) = HeaderValue::from_bytes(s.as_bytes()) {
            headers.insert(name, hv);
        }
    }
}

impl PostResponsePlugin for OpenAiTokenQuotaPlugin {
    #[inline(always)]
    fn id(&self) -> &str {
        &self.id
    }

    #[inline(always)]
    fn pbww(&self) -> u32 {
        match self.mode {
            QuotaMode::Local => 0,
            QuotaMode::Distributed => self.shadow_weight,
        }
    }

    fn run_background(
        self: Arc<Self>,
        shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> Option<BackgroundTaskFuture> {
        if self.mode != QuotaMode::Distributed {
            return None;
        }
        let redis_url = self.redis_url.clone()?;
        let params = RedisSyncParams {
            plugin_id: self.id.clone(),
            arena: Arc::clone(&self.ledger),
            notify: Arc::clone(&self.refill_notify),
            connected: Arc::clone(&self.redis_connected),
            redis_url,
            redis_password: self.redis_password.clone(),
            redis_tls: self.redis_tls.clone(),
            micro_quota_batch: self.micro_quota_batch,
            batch_users: self.batch_users,
            sync_interval_ms: self.sync_interval_ms,
            pbww: self.shadow_weight,
        };
        Some(Box::pin(run_redis_sync_loop(params, shutdown)))
    }

    #[inline(always)]
    fn capability(&self) -> PluginCapability {
        PluginCapability::new(AccessLevel::Write, AccessLevel::Read)
    }

    #[inline(always)]
    fn supported_protocols(&self) -> SupportedProtocols {
        SupportedProtocols::ALL_HTTP
    }

    #[inline]
    fn on_response(&self, req_headers: &HeaderWorkspace, resp: &mut L7Response) {
        let Some(usage) = scan_token_usage(resp.body.as_slice()) else {
            resp.headers.insert(
                HDR_STATUS.clone(),
                HeaderValue::from_static("no_usage_found"),
            );
            return;
        };

        // Combine all present configured headers hierarchically as `seg1.seg2...`
        // in CPU registers + bounded 62B stack buffer (zero heap allocation).
        let mut hasher = Fnv1aHasher::new();
        let mut key_buf = [0u8; MAX_CONSUMER_KEY_LEN];
        let mut key_len = 0usize;
        let mut has_segment = false;

        for h in &self.consumer_id_headers {
            if let Some(val) = req_headers.get(h).and_then(|v| v.to_str().ok()) {
                let trimmed = val.trim();
                if !trimmed.is_empty() {
                    if has_segment {
                        hasher.write_byte(b'.');
                        if key_len < MAX_CONSUMER_KEY_LEN {
                            key_buf[key_len] = b'.';
                            key_len += 1;
                        }
                    }
                    let bytes = trimmed.as_bytes();
                    hasher.write_bytes(bytes);
                    let copy_len = bytes.len().min(MAX_CONSUMER_KEY_LEN - key_len);
                    key_buf[key_len..key_len + copy_len].copy_from_slice(&bytes[..copy_len]);
                    key_len += copy_len;
                    has_segment = true;
                }
            }
        }

        let (consumer_hash, consumer_key): (u64, &[u8]) = if has_segment {
            (hasher.finish_non_zero(), &key_buf[..key_len])
        } else {
            (fnv1a_64(b"anonymous"), b"anonymous")
        };

        let outcome = self
            .ledger
            .record(consumer_hash, consumer_key, usage, coarse_sec());
        self.counted_requests_total.fetch_add(1, Ordering::Relaxed);
        self.counted_tokens_total
            .fetch_add(usage.total_tokens, Ordering::Relaxed);

        if self.mode == QuotaMode::Distributed && outcome.needs_refill {
            self.refill_notify.notify_one();
        }

        // Always emit the 4 standard client token headers (branchless on 0).
        let mut itoa_buf = itoa::Buffer::new();
        Self::write_u64_header(
            &mut resp.headers,
            HDR_PROMPT.clone(),
            usage.prompt_tokens,
            &mut itoa_buf,
        );
        Self::write_u64_header(
            &mut resp.headers,
            HDR_COMPLETION.clone(),
            usage.completion_tokens,
            &mut itoa_buf,
        );
        Self::write_u64_header(
            &mut resp.headers,
            HDR_TOTAL.clone(),
            usage.total_tokens,
            &mut itoa_buf,
        );
        Self::write_u64_header(
            &mut resp.headers,
            HDR_CACHED.clone(),
            usage.cached_prompt_tokens,
            &mut itoa_buf,
        );

        let status = if outcome.exhausted {
            HeaderValue::from_static("quota_exhausted")
        } else {
            HeaderValue::from_static("success")
        };
        resp.headers.insert(HDR_STATUS.clone(), status);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{HeaderMap, StatusCode};
    use serde_json::json;

    #[test]
    fn test_openai_token_quota_emits_4_token_headers_without_cost() {
        velda_core::clock::init_clock();
        let cfg = PluginConfig {
            id: "openai_token_quota".to_string(),
            phase: "post_response".to_string(),
            config: json!({
                "consumer_id_headers": ["x-tenant-id"]
            }),
        };

        let plugin = OpenAiTokenQuotaPlugin::from_config(&cfg).unwrap();
        let mut req_hdrs = HeaderMap::new();
        req_hdrs.insert("x-tenant-id", HeaderValue::from_static("tenant-a"));
        let req_ws = HeaderWorkspace::from(req_hdrs);

        let body = br#"{"id":"1","model":"deepseek-chat","usage":{"prompt_tokens":1540,"completion_tokens":320,"total_tokens":1860}}"#;
        let mut resp = L7Response::from_bytes(StatusCode::OK, body.as_slice());
        plugin.on_response(&req_ws, &mut resp);

        assert_eq!(resp.headers.get("x-ai-prompt-tokens").unwrap(), "1540");
        assert_eq!(resp.headers.get("x-ai-completion-tokens").unwrap(), "320");
        assert_eq!(resp.headers.get("x-ai-total-tokens").unwrap(), "1860");
        assert_eq!(resp.headers.get("x-ai-cached-tokens").unwrap(), "0");
        assert_eq!(resp.headers.get("x-ai-metering-status").unwrap(), "success");
        assert!(resp.headers.get("x-ai-cost-micro-usd").is_none());
        assert_eq!(plugin.counted_requests_total.load(Ordering::Relaxed), 1);
        assert_eq!(plugin.counted_tokens_total.load(Ordering::Relaxed), 1860);
    }

    #[test]
    fn test_hierarchical_consumer_id_accumulates_and_deducts_local_quota() {
        velda_core::clock::init_clock();
        let cfg = PluginConfig {
            id: "openai_token_quota".to_string(),
            phase: "post_response".to_string(),
            config: json!({
                "mode": "local",
                "consumer_id_headers": ["x-tenant-id", "x-user-id"],
                "local_default_quota": 100
            }),
        };

        let plugin = OpenAiTokenQuotaPlugin::from_config(&cfg).unwrap();
        let mut req_hdrs = HeaderMap::new();
        req_hdrs.insert("x-tenant-id", HeaderValue::from_static("abc"));
        req_hdrs.insert("x-user-id", HeaderValue::from_static("123"));
        let req_ws = HeaderWorkspace::from(req_hdrs);

        let body1 = br#"{"usage":{"prompt_tokens":10,"completion_tokens":5,"total_tokens":15}}"#;
        let body2 = br#"{"usage":{"prompt_tokens":20,"completion_tokens":10,"total_tokens":30,"prompt_tokens_details":{"cached_tokens":4}}}"#;

        let mut resp1 = L7Response::from_bytes(StatusCode::OK, body1.as_slice());
        let mut resp2 = L7Response::from_bytes(StatusCode::OK, body2.as_slice());
        plugin.on_response(&req_ws, &mut resp1);
        plugin.on_response(&req_ws, &mut resp2);

        let (p, c, t, ca, rem) = plugin
            .ledger
            .read_consumer("abc.123")
            .expect("abc.123 must be recorded in shared arena");
        assert_eq!((p, c, t, ca, rem), (30, 15, 45, 4, 55));
    }
}
