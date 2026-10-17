//! Rate Limiting Pre-Upstream Plugin Skeleton (`phase: "pre_upstream"`).
//!
//! Enforces local token bucket or fixed-window request pacing on routed requests
//! before leasing upstream connections, short-circuiting with 429 Too Many Requests.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use http::header::RETRY_AFTER;
use http::{HeaderValue, StatusCode};
use velda_core::{Action, L7Request, L7Response};

use super::PreUpstreamPlugin;

/// Production skeleton for local in-memory token rate limiter.
#[derive(Debug)]
pub struct RateLimitPlugin {
    pub id: String,
    pub limit_per_second: u64,
    current_second: AtomicU64,
    current_count: AtomicU64,
}

impl RateLimitPlugin {
    /// Constructs a new rate limiter with a maximum request rate per second.
    pub fn new(id: impl Into<String>, limit_per_second: u64) -> Self {
        Self {
            id: id.into(),
            limit_per_second,
            current_second: AtomicU64::new(0),
            current_count: AtomicU64::new(0),
        }
    }

    #[inline]
    fn now_sec() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    }

    /// Deserializes and constructs an instance directly from a [`PluginConfig`].
    pub fn from_config(
        config: &velda_sync::post_sync::plugin::PluginConfig,
    ) -> Result<Self, crate::error::PluginError> {
        let limit = config
            .config
            .get("limit_per_second")
            .and_then(|v| v.as_u64())
            .unwrap_or(1000);

        Ok(Self::new(&config.id, limit))
    }
}

impl PreUpstreamPlugin for RateLimitPlugin {
    #[inline]
    fn id(&self) -> &str {
        &self.id
    }

    fn on_upstream_request(&self, _req: &mut L7Request) -> Action {
        let sec = Self::now_sec();
        let prev_sec = self.current_second.load(Ordering::Relaxed);

        if sec != prev_sec {
            // New second window: attempt to reset
            self.current_second.store(sec, Ordering::Relaxed);
            self.current_count.store(1, Ordering::Relaxed);
            return Action::Continue;
        }

        let current = self.current_count.fetch_add(1, Ordering::Relaxed);
        if current < self.limit_per_second {
            Action::Continue
        } else {
            Action::Respond(
                L7Response::from_bytes(
                    StatusCode::TOO_MANY_REQUESTS,
                    "Too Many Requests: rate limit exceeded on upstream target",
                )
                .with_header(RETRY_AFTER, HeaderValue::from_static("1")),
            )
        }
    }

    #[inline]
    fn capability(&self) -> crate::capability::PluginCapability {
        crate::capability::PluginCapability::header_only(crate::capability::AccessLevel::Read)
    }

    #[inline]
    fn supported_protocols(&self) -> crate::capability::SupportedProtocols {
        crate::capability::SupportedProtocols::ALL_L7
    }
}
