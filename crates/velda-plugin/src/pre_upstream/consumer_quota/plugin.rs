//! Consumer Quota Plugin Implementation (`consumer_quota`).
//!
//! # Objective & Mission
//! Enforces fair-use resource allocation and prevents individual tenant members, user accounts,
//! or bot clients from hogging shared system capacity (e.g. 50 requests per 5 hours in AI APIs,
//! or 1,000 requests per day per user).
//!
//! # Phase
//! Evaluated during the `pre_upstream` phase immediately before forwarding to backend backplanes.
//!
//! # Rejection Behavior
//! When exhausted, rejects with `Action::Reject(RejectReason::TooManyRequests)` and tags Bit-1
//! bypass response headers (`Retry-After`, `x-quota-remaining: 0`, `x-quota-window`).

use http::header::RETRY_AFTER;
use http::{HeaderName, HeaderValue};
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use velda_core::{Action, L7Request, RejectReason};
use velda_sync::post_sync::plugin::PluginConfig;

use super::algorithm::{QuotaAlgorithm, QuotaDecision, QuotaWindowConfig, ShardedQuotaState};
use super::key::resolve_quota_key;
use crate::capability::{AccessLevel, PluginCapability, SupportedProtocols};
use crate::error::PluginError;
use crate::pre_upstream::PreUpstreamPlugin;

/// Static configuration contract for [`ConsumerQuotaPlugin`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerQuotaConfig {
    /// Ordered list of header names evaluated to construct client identity (e.g. `["x-user-id", "x-consumer-id"]`).
    pub key_headers: Vec<HeaderName>,
    /// Array of sliding, fixed, or token bucket quota windows applied simultaneously.
    pub windows: Vec<QuotaWindowConfig>,
}

impl Default for ConsumerQuotaConfig {
    fn default() -> Self {
        Self {
            key_headers: vec![
                HeaderName::from_static("x-user-id"),
                HeaderName::from_static("x-consumer-id"),
            ],
            windows: vec![
                QuotaWindowConfig {
                    name: "ai_rolling_5h".to_string(),
                    window_seconds: 18_000, // 5 hours
                    limit: 50,
                    algorithm: QuotaAlgorithm::SlidingWindow,
                },
                QuotaWindowConfig {
                    name: "daily_cap".to_string(),
                    window_seconds: 86_400, // 1 day
                    limit: 1000,
                    algorithm: QuotaAlgorithm::FixedWindow,
                },
            ],
        }
    }
}

/// Pre-upstream fair-use and member quota enforcement engine.
pub struct ConsumerQuotaPlugin {
    pub id: String,
    pub config: ConsumerQuotaConfig,
    pub state: Arc<ShardedQuotaState>,
}

impl std::fmt::Debug for ConsumerQuotaPlugin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConsumerQuotaPlugin")
            .field("id", &self.id)
            .field("config", &self.config)
            .finish()
    }
}

impl ConsumerQuotaPlugin {
    /// Constructs a new consumer quota plugin instance with an explicit configuration.
    pub fn new(id: impl Into<String>, config: ConsumerQuotaConfig) -> Self {
        Self {
            id: id.into(),
            config,
            state: Arc::new(ShardedQuotaState::new()),
        }
    }

    /// Constructs an instance sharing an existing sharded quota state (useful for multi-route clusters).
    pub fn with_shared_state(
        id: impl Into<String>,
        config: ConsumerQuotaConfig,
        state: Arc<ShardedQuotaState>,
    ) -> Self {
        Self {
            id: id.into(),
            config,
            state,
        }
    }

    /// Compiles a configuration from control-plane JSON definition.
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let key_headers =
            if let Some(arr) = config.config.get("key_headers").and_then(|v| v.as_array()) {
                let mut headers = Vec::with_capacity(arr.len());
                for v in arr {
                    if let Some(s) = v.as_str() {
                        let name = HeaderName::from_bytes(s.as_bytes()).map_err(|e| {
                            PluginError::Config(format!("Invalid key_header name '{s}': {e}"))
                        })?;
                        headers.push(name);
                    }
                }
                headers
            } else if let Some(single) = config.config.get("key_header").and_then(|v| v.as_str()) {
                vec![HeaderName::from_bytes(single.as_bytes()).map_err(|e| {
                    PluginError::Config(format!("Invalid key_header name '{single}': {e}"))
                })?]
            } else {
                vec![
                    HeaderName::from_static("x-user-id"),
                    HeaderName::from_static("x-consumer-id"),
                ]
            };

        let mut windows = Vec::new();
        if let Some(win_arr) = config.config.get("windows").and_then(|v| v.as_array()) {
            for (idx, w) in win_arr.iter().enumerate() {
                let name = w
                    .get("name")
                    .and_then(|v| v.as_str())
                    .unwrap_or(&format!("window_{idx}"))
                    .to_string();

                let window_seconds = w
                    .get("window_seconds")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(3600);

                let limit = w.get("limit").and_then(|v| v.as_u64()).unwrap_or(100);

                let algorithm = w
                    .get("algorithm")
                    .and_then(|v| v.as_str())
                    .map(QuotaAlgorithm::parse)
                    .unwrap_or(QuotaAlgorithm::SlidingWindow);

                windows.push(QuotaWindowConfig {
                    name,
                    window_seconds,
                    limit,
                    algorithm,
                });
            }
        } else {
            // Default 5-hour rolling AI limit
            windows.push(QuotaWindowConfig {
                name: "5h_ai_rolling".to_string(),
                window_seconds: 18_000,
                limit: 50,
                algorithm: QuotaAlgorithm::SlidingWindow,
            });
        }

        Ok(Self::new(
            config.id.clone(),
            ConsumerQuotaConfig {
                key_headers,
                windows,
            },
        ))
    }
}

impl PreUpstreamPlugin for ConsumerQuotaPlugin {
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
        let key_hash = resolve_quota_key(req, &self.config.key_headers);

        let now_sec = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let now_instant = Instant::now();

        match self
            .state
            .evaluate(key_hash, &self.config.windows, now_sec, now_instant)
        {
            QuotaDecision::Allowed {
                min_remaining,
                window_name,
            } => {
                // Ingress Header Telemetry: Inject remaining capacity tag
                if let Ok(val) = HeaderValue::from_str(&min_remaining.to_string()) {
                    req.headers.insert("x-quota-remaining", val);
                }
                if let Ok(val) = HeaderValue::from_str(&window_name) {
                    req.headers.insert("x-quota-window", val);
                }
                Action::Continue
            }
            QuotaDecision::Exhausted {
                window_name,
                reset_seconds,
            } => {
                // Bit-1 Bypass Header Injection on Rejection (RFC 6585)
                if let Ok(val) = HeaderValue::from_str(&reset_seconds.to_string()) {
                    req.headers.insert_bypass(RETRY_AFTER, val);
                }
                req.headers.insert_bypass(
                    HeaderName::from_static("x-quota-remaining"),
                    HeaderValue::from_static("0"),
                );
                if let Ok(val) = HeaderValue::from_str(&window_name) {
                    req.headers
                        .insert_bypass(HeaderName::from_static("x-quota-window"), val);
                }

                Action::Reject(RejectReason::TooManyRequests)
            }
        }
    }
}
