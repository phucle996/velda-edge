//! Unified Fault Injection Plugin (`fault_injection`).
//!
//! ```text
//!   on_upstream_request(req)
//!     ├─► 1. Extract & strip `x-fault-delay-ms` and `x-fault-abort-status` (always sanitized)
//!     ├─► 2. Fast-path guard: `active_mask == 0` -> Action::Continue
//!     ├─► 3. Scope check (`IpRangeMatcher` + pre-hashed `FaultHeaderMatchRule`)
//!     ├─► 4. Single XorShift64* roll (shared across fractional delay and abort thresholds)
//!     └─► 5. On abort: stamp MSB bypass header `x-velda-fault: abort` in `req.headers`
//!            and return Action::Status(InternalStatus) (or Delay(dur, Action::Status(..)))
//!            -> Protocol crate (`velda-http1/2/3`, `velda-grpc`) renders wire status & body
//! ```

use std::cell::Cell;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use http::header::{HeaderName, HeaderValue};
use velda_core::{Action, InternalStatus, L7Request};

use crate::capability::{AccessLevel, PluginCapability, SupportedProtocols};
use crate::config::PluginConfig;
use crate::error::PluginError;
use crate::pre_upstream::PreUpstreamPlugin;

use super::condition::{
    FaultCondition, FaultHeaderMatchRule, IpRangeMatcher, KEY_FAULT_ABORT_STATUS,
    KEY_FAULT_DELAY_MS,
};

static GLOBAL_SEED: AtomicU64 = AtomicU64::new(0x853c_49e6_748f_ea9b);

thread_local! {
    static RNG_STATE: Cell<u64> = const { Cell::new(0) };
}

/// Single thread-local XorShift64* PRNG shared by delay and abort evaluation.
#[inline(always)]
fn fast_random_u64() -> u64 {
    RNG_STATE.with(|cell| {
        let mut x = cell.get();
        if x == 0 {
            let seed = GLOBAL_SEED.fetch_add(0x9e37_79b9_7f4a_7c15, Ordering::Relaxed);
            let mut z = seed ^ 0xbf58_476d_1ce4_e5b9;
            z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
            x = z ^ (z >> 31);
            if x == 0 {
                x = 0x853c_49e6_748f_ea9b;
            }
        }
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        cell.set(x);
        x.wrapping_mul(0x2545_f491_4f6c_dd1d)
    })
}

/// Converts percentage `[0.0, 100.0]` into an integer 32-bit probability threshold `[0, u32::MAX]`.
#[inline(always)]
fn percentage_to_threshold_32(percentage: f64) -> u32 {
    if percentage <= 0.0 {
        0
    } else if percentage >= 100.0 {
        u32::MAX
    } else {
        ((percentage / 100.0) * (u32::MAX as f64)) as u32
    }
}

/// Lemire fast range reduction for bounded random delay intervals.
#[inline(always)]
fn fast_reduce(val: u64, n: usize) -> usize {
    ((val as u128 * n as u128) >> 64) as usize
}

/// Compiled delay fault configuration (`[min_ms, max_ms]`).
#[derive(Debug, Clone, PartialEq)]
pub struct FaultDelayConfig {
    /// Configured probability percentage (`0.0..=100.0`).
    pub percentage: f64,
    /// Pre-computed integer threshold in `0..=u32::MAX`.
    threshold: u32,
    /// Minimum delay in milliseconds (inclusive).
    pub min_ms: u64,
    /// Maximum delay in milliseconds (inclusive).
    pub max_ms: u64,
    /// Pre-computed `Duration` when `min_ms == max_ms`.
    fixed_dur: Duration,
}

impl FaultDelayConfig {
    /// Creates a new validated delay configuration.
    pub fn new(percentage: f64, min_ms: u64, max_ms: u64) -> Result<Self, String> {
        if !(0.0..=100.0).contains(&percentage) {
            return Err(format!(
                "Delay percentage must be between 0.0 and 100.0, got {percentage}"
            ));
        }
        if min_ms > max_ms {
            return Err(format!(
                "min_delay_ms ({min_ms}) cannot be greater than max_delay_ms ({max_ms})"
            ));
        }

        let fixed_dur = if min_ms == max_ms {
            Duration::from_millis(min_ms)
        } else {
            Duration::ZERO
        };

        Ok(Self {
            percentage,
            threshold: percentage_to_threshold_32(percentage),
            min_ms,
            max_ms,
            fixed_dur,
        })
    }

    /// Evaluates delay using an externally sampled 32-bit entropy word.
    #[inline]
    pub fn evaluate_with_roll(&self, override_ms: Option<u64>, roll32: u32) -> Option<Duration> {
        if let Some(ms) = override_ms {
            return Some(Duration::from_millis(ms));
        }
        if self.threshold == 0 || (self.threshold < u32::MAX && roll32 >= self.threshold) {
            return None;
        }
        if self.min_ms == self.max_ms {
            return Some(self.fixed_dur);
        }
        let delta = (self.max_ms - self.min_ms) as usize;
        let ms = self.min_ms + (fast_reduce(fast_random_u64(), delta + 1) as u64);
        Some(Duration::from_millis(ms))
    }

    /// Evaluates whether this request should be delayed.
    #[inline]
    pub fn calculate_delay(&self, override_ms: Option<u64>) -> Option<Duration> {
        let roll32 = if override_ms.is_none() && self.threshold > 0 && self.threshold < u32::MAX {
            (fast_random_u64() >> 32) as u32
        } else {
            0
        };
        self.evaluate_with_roll(override_ms, roll32)
    }
}

/// Compiled protocol-blind abort fault configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FaultAbortConfig {
    /// Pre-computed integer threshold in `0..=u32::MAX`.
    threshold: u32,
    /// Protocol-agnostic internal status code rendered natively by downstream protocol crates.
    pub status: InternalStatus,
}

impl FaultAbortConfig {
    /// Creates a new validated abort configuration returning an aborted [`InternalStatus`].
    pub fn new(percentage: f64, status: InternalStatus) -> Result<Self, String> {
        if !(0.0..=100.0).contains(&percentage) {
            return Err(format!(
                "Abort percentage must be between 0.0 and 100.0, got {percentage}"
            ));
        }
        if !status.is_aborted() {
            return Err(format!(
                "Abort InternalStatus must be an aborted state, got '{}'",
                status.as_str()
            ));
        }
        Ok(Self {
            threshold: percentage_to_threshold_32(percentage),
            status,
        })
    }

    /// Evaluates abort using an externally sampled 32-bit entropy word.
    #[inline]
    pub fn evaluate_with_roll(
        &self,
        override_status: Option<InternalStatus>,
        roll32: u32,
    ) -> Option<InternalStatus> {
        if let Some(status) = override_status.filter(|s| s.is_aborted()) {
            return Some(status);
        }
        if self.threshold == 0 || (self.threshold < u32::MAX && roll32 >= self.threshold) {
            return None;
        }
        Some(self.status)
    }

    /// Evaluates whether this request should be aborted.
    #[inline]
    pub fn evaluate(&self, override_status: Option<InternalStatus>) -> Option<InternalStatus> {
        let roll32 = if override_status.is_none() && self.threshold > 0 && self.threshold < u32::MAX
        {
            fast_random_u64() as u32
        } else {
            0
        };
        self.evaluate_with_roll(override_status, roll32)
    }
}

/// Protocol-agnostic plugin for injecting artificial latency and canonical internal fault aborts.
///
/// Aligned to 64 bytes (`#[repr(C, align(64))]`) with `active_mask` at offset 0 on L1D cache line 0.
#[repr(C, align(64))]
#[derive(Debug, Clone)]
pub struct FaultInjectionPlugin {
    /// Non-zero when any static injection or dynamic override is active (`0` = inactive fast path).
    active_mask: u64,
    pub condition: FaultCondition,
    pub delay: Option<FaultDelayConfig>,
    pub abort: Option<FaultAbortConfig>,
    pub id: String,
}

impl FaultInjectionPlugin {
    /// Creates a new unified [`FaultInjectionPlugin`] instance.
    pub fn new(
        id: impl Into<String>,
        condition: FaultCondition,
        delay: Option<FaultDelayConfig>,
        abort: Option<FaultAbortConfig>,
    ) -> Self {
        let d_thresh = delay.as_ref().map_or(0, |d| d.threshold) as u64;
        let a_thresh = abort.as_ref().map_or(0, |a| a.threshold) as u64;
        let override_bit = u64::from(condition.allow_header_override);
        let active_mask = d_thresh | (a_thresh << 32) | override_bit;

        Self {
            active_mask,
            condition,
            delay,
            abort,
            id: id.into(),
        }
    }

    /// Instantiates a new [`FaultInjectionPlugin`] from compiled [`PluginConfig`].
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let cfg = super::validation::validate_fault_injection_config(&config.id, &config.config)?;

        let ip_matcher = if !cfg.scope.client_ips.is_empty() {
            Some(
                IpRangeMatcher::from_cidrs(&cfg.scope.client_ips).map_err(|e| {
                    PluginError::Config(format!(
                        "Plugin '{}': IP range matcher error: {e}",
                        config.id
                    ))
                })?,
            )
        } else {
            None
        };

        let header_match = if let Some(hm) = &cfg.scope.header_match {
            let name = HeaderName::from_bytes(hm.name.as_bytes()).map_err(|e| {
                PluginError::Config(format!(
                    "Plugin '{}': invalid header name '{}' in scope.header_match: {e}",
                    config.id, hm.name
                ))
            })?;
            Some(match &hm.exact_match {
                Some(val) => FaultHeaderMatchRule::exact(name, val.clone()),
                None => FaultHeaderMatchRule::present(name),
            })
        } else {
            None
        };

        let condition = FaultCondition {
            ip_matcher,
            header_match,
            allow_header_override: cfg.scope.allow_header_override,
            max_override_delay_ms: cfg.scope.max_override_delay_ms,
        };

        let delay_cfg = if cfg.delay.enabled && cfg.delay.percentage > 0 {
            Some(
                FaultDelayConfig::new(
                    cfg.delay.percentage as f64,
                    cfg.delay.fixed_delay_ms,
                    cfg.delay.fixed_delay_ms,
                )
                .map_err(|e| {
                    PluginError::Config(format!("Plugin '{}': delay error: {e}", config.id))
                })?,
            )
        } else if cfg.scope.allow_header_override {
            Some(
                FaultDelayConfig::new(0.0, 0, cfg.scope.max_override_delay_ms).map_err(|e| {
                    PluginError::Config(format!(
                        "Plugin '{}': delay override init error: {e}",
                        config.id
                    ))
                })?,
            )
        } else {
            None
        };

        let abort_cfg = if cfg.abort.enabled && cfg.abort.percentage > 0 {
            let status =
                InternalStatus::from_name(&cfg.abort.internal_status).ok_or_else(|| {
                    PluginError::Config(format!(
                        "Plugin '{}': invalid abort.internal_status '{}'",
                        config.id, cfg.abort.internal_status
                    ))
                })?;
            Some(
                FaultAbortConfig::new(cfg.abort.percentage as f64, status).map_err(|e| {
                    PluginError::Config(format!("Plugin '{}': abort error: {e}", config.id))
                })?,
            )
        } else if cfg.scope.allow_header_override {
            let default_status = InternalStatus::from_name(&cfg.abort.internal_status)
                .filter(|s| s.is_aborted())
                .unwrap_or(InternalStatus::ServiceUnavailable);
            Some(FaultAbortConfig::new(0.0, default_status).map_err(|e| {
                PluginError::Config(format!(
                    "Plugin '{}': abort override init error: {e}",
                    config.id
                ))
            })?)
        } else {
            None
        };

        Ok(Self::new(&config.id, condition, delay_cfg, abort_cfg))
    }
}

impl PreUpstreamPlugin for FaultInjectionPlugin {
    #[inline(always)]
    fn id(&self) -> &str {
        &self.id
    }

    #[inline(always)]
    fn pbww(&self) -> u32 {
        0
    }

    #[inline]
    fn on_upstream_request(&self, req: &mut L7Request, _targets: &mut u32) -> Action {
        // Step 1: Extract dynamic overrides (if scoped) and strip control headers before forwarding.
        let (override_delay, override_abort) = if self.condition.allow_header_override {
            self.condition.extract_overrides(req)
        } else {
            (None, None)
        };
        if req.headers.contains_prehashed(&KEY_FAULT_DELAY_MS) {
            req.headers.remove("x-fault-delay-ms");
        }
        if req.headers.contains_prehashed(&KEY_FAULT_ABORT_STATUS) {
            req.headers.remove("x-fault-abort-status");
        }

        // Step 2: Single-instruction pass-through if inactive.
        if self.active_mask == 0 {
            return Action::Continue;
        }

        // Step 3: Scope filter.
        if !self.condition.matches(req) {
            return Action::Continue;
        }

        // Step 4: Sample PRNG once only when a non-overridden fractional rule (1..99%) is active.
        let need_delay_roll = override_delay.is_none()
            && self
                .delay
                .as_ref()
                .is_some_and(|d| d.threshold > 0 && d.threshold < u32::MAX);
        let need_abort_roll = override_abort.is_none()
            && self
                .abort
                .as_ref()
                .is_some_and(|a| a.threshold > 0 && a.threshold < u32::MAX);

        let r = if need_delay_roll || need_abort_roll {
            fast_random_u64()
        } else {
            0
        };

        let delay_dur = self
            .delay
            .as_ref()
            .and_then(|d| d.evaluate_with_roll(override_delay, (r >> 32) as u32));
        let abort_status = self
            .abort
            .as_ref()
            .and_then(|a| a.evaluate_with_roll(override_abort, r as u32));

        // Step 5: Stamp MSB bypass header on abort and return Action::Status(InternalStatus).
        match (delay_dur, abort_status) {
            (Some(dur), Some(status)) => {
                req.headers.insert_bypass(
                    HeaderName::from_static("x-velda-fault"),
                    HeaderValue::from_static("abort"),
                );
                Action::delay(dur, Action::Status(status))
            }
            (Some(dur), None) => Action::delay(dur, Action::Continue),
            (None, Some(status)) => {
                req.headers.insert_bypass(
                    HeaderName::from_static("x-velda-fault"),
                    HeaderValue::from_static("abort"),
                );
                Action::Status(status)
            }
            (None, None) => Action::Continue,
        }
    }

    #[inline(always)]
    fn capability(&self) -> PluginCapability {
        PluginCapability::header_only(AccessLevel::Write)
    }

    #[inline(always)]
    fn supported_protocols(&self) -> SupportedProtocols {
        SupportedProtocols::ALL_HTTP.union(SupportedProtocols::GRPC)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{Method, Uri, Version};
    use velda_core::{Body, HeaderWorkspace};

    #[test]
    fn test_abort_config_validation() {
        assert!(FaultAbortConfig::new(101.0, InternalStatus::ServiceUnavailable).is_err());
        assert!(FaultAbortConfig::new(-0.1, InternalStatus::ServiceUnavailable).is_err());
        assert!(FaultAbortConfig::new(100.0, InternalStatus::Completed).is_err());

        let cfg = FaultAbortConfig::new(100.0, InternalStatus::ServiceUnavailable).unwrap();
        assert_eq!(cfg.evaluate(None), Some(InternalStatus::ServiceUnavailable));

        let zero = FaultAbortConfig::new(0.0, InternalStatus::InternalError).unwrap();
        assert_eq!(zero.evaluate(None), None);
        assert_eq!(
            zero.evaluate(Some(InternalStatus::UpstreamTimeout)),
            Some(InternalStatus::UpstreamTimeout)
        );
    }

    #[test]
    fn test_delay_config_validation_and_bounds() {
        assert!(FaultDelayConfig::new(101.0, 10, 20).is_err());
        assert!(FaultDelayConfig::new(50.0, 30, 20).is_err());

        let fixed = FaultDelayConfig::new(100.0, 250, 250).unwrap();
        assert_eq!(
            fixed.calculate_delay(None),
            Some(Duration::from_millis(250))
        );

        let ranged = FaultDelayConfig::new(100.0, 100, 300).unwrap();
        for _ in 0..50 {
            let ms = ranged.calculate_delay(None).unwrap().as_millis() as u64;
            assert!((100..=300).contains(&ms));
        }
    }

    #[test]
    fn test_scope_bypass_and_matrix_actions() {
        let condition = FaultCondition {
            ip_matcher: Some(IpRangeMatcher::from_cidrs(&["10.0.0.0/8".to_string()]).unwrap()),
            ..Default::default()
        };
        let abort = FaultAbortConfig::new(100.0, InternalStatus::ServiceUnavailable).unwrap();
        let scoped = FaultInjectionPlugin::new("fault-scoped", condition, None, Some(abort));

        let mut headers = HeaderWorkspace::empty(4);
        headers.insert(
            HeaderName::from_static("x-forwarded-for"),
            HeaderValue::from_static("203.0.113.1"),
        );
        let mut req_outside = L7Request::new(
            Method::GET,
            Uri::from_static("/api/users"),
            Version::HTTP_11,
            headers,
            Body::Empty,
        );
        let mut target = 1;
        assert!(matches!(
            scoped.on_upstream_request(&mut req_outside, &mut target),
            Action::Continue
        ));
        assert!(req_outside.headers.get("x-velda-fault").is_none());

        // Matching request receives bypass header + Action::Status(ServiceUnavailable)
        let mut req = L7Request::new(
            Method::POST,
            Uri::from_static("/pkg.Svc/Call"),
            Version::HTTP_2,
            HeaderWorkspace::empty(4),
            Body::Empty,
        );
        let both = FaultInjectionPlugin::new(
            "fault-both",
            FaultCondition::default(),
            Some(FaultDelayConfig::new(100.0, 300, 300).unwrap()),
            Some(FaultAbortConfig::new(100.0, InternalStatus::UpstreamTimeout).unwrap()),
        );
        match both.on_upstream_request(&mut req, &mut target) {
            Action::Delay { duration, and_then } => {
                assert_eq!(duration, Duration::from_millis(300));
                assert!(matches!(
                    *and_then,
                    Action::Status(InternalStatus::UpstreamTimeout)
                ));
            }
            other => panic!("Expected Action::Delay(Action::Status), got {other:?}"),
        }
        assert_eq!(req.headers.get("x-velda-fault").unwrap(), "abort");
        req.headers.retain_bypass_only();
        assert_eq!(req.headers.get("x-velda-fault").unwrap(), "abort");
    }

    #[test]
    fn test_dynamic_override_hygiene_and_protocol_support() {
        let condition = FaultCondition {
            allow_header_override: true,
            max_override_delay_ms: 5000,
            header_match: Some(FaultHeaderMatchRule::exact(
                HeaderName::from_static("x-chaos-test"),
                "enabled",
            )),
            ..Default::default()
        };
        let plugin = FaultInjectionPlugin::new(
            "fault-override",
            condition,
            Some(FaultDelayConfig::new(0.0, 100, 200).unwrap()),
            Some(FaultAbortConfig::new(0.0, InternalStatus::InternalError).unwrap()),
        );
        assert_eq!(
            plugin.supported_protocols(),
            SupportedProtocols::ALL_HTTP.union(SupportedProtocols::GRPC)
        );
        assert!(
            !plugin
                .supported_protocols()
                .contains(SupportedProtocols::RAW_TCP)
        );

        let mut headers = HeaderWorkspace::empty(4);
        headers.insert(
            HeaderName::from_static("x-chaos-test"),
            HeaderValue::from_static("enabled"),
        );
        headers.insert(
            HeaderName::from_static("x-fault-delay-ms"),
            HeaderValue::from_static("400"),
        );
        headers.insert(
            HeaderName::from_static("x-fault-abort-status"),
            HeaderValue::from_static("rate_limited"),
        );
        let mut req = L7Request::new(
            Method::GET,
            Uri::from_static("/api/users"),
            Version::HTTP_11,
            headers,
            Body::Empty,
        );
        let mut target = 1;
        match plugin.on_upstream_request(&mut req, &mut target) {
            Action::Delay { duration, and_then } => {
                assert_eq!(duration, Duration::from_millis(400));
                assert!(matches!(
                    *and_then,
                    Action::Status(InternalStatus::RateLimited)
                ));
            }
            other => panic!("Expected Action::Delay(Action::Status), got {other:?}"),
        }
        assert!(!req.headers.contains_key("x-fault-delay-ms"));
        assert!(!req.headers.contains_key("x-fault-abort-status"));
        assert_eq!(req.headers.get("x-velda-fault").unwrap(), "abort");
    }

    #[test]
    fn test_from_config_complete_with_scope() {
        let json_cfg = serde_json::json!({
            "scope": {
                "client_ips": ["10.0.0.0/8"],
                "header_match": {
                    "name": "x-canary-test",
                    "exact_match": "active"
                },
                "allow_header_override": true,
                "max_override_delay_ms": 4000
            },
            "delay": {
                "enabled": true,
                "fixed_delay_ms": 150,
                "percentage": 20
            },
            "abort": {
                "enabled": true,
                "internal_status": "service_unavailable",
                "percentage": 10
            }
        });

        let config = PluginConfig {
            id: "fault_injection".to_string(),
            config: json_cfg,
        };

        let plugin = FaultInjectionPlugin::from_config(&config).unwrap();
        assert_eq!(plugin.id, "fault_injection");
        assert!(plugin.condition.ip_matcher.is_some());
        assert!(plugin.condition.header_match.is_some());
        assert!(plugin.condition.allow_header_override);
        assert_eq!(plugin.condition.max_override_delay_ms, 4000);
        assert_ne!(plugin.active_mask, 0);
    }
}
