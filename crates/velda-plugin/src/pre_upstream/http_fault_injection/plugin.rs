//! HTTP Fault Injection Plugin (`http_fault_injection`).
//!
//! ```text
//!   on_upstream_request(req)
//!     ├─► 1. Fast-Path Guard: combined_mask == 0 (no overrides, 0% delay, 0% abort)
//!     │        └─► sanitize x-fault-* only if SIMD prehashed tag hits -> Action::Continue
//!     ├─► 2. Extract & strip dynamic overrides (`x-fault-delay-ms`, `x-fault-abort-code`)
//!     ├─► 3. Scope filter (`IpRangeMatcher` + pre-hashed `HeaderMatchRule`)
//!     ├─► 4. Single 64-bit XorShift64* roll (only when fractional 1..99% is active)
//!     └─► 5. Action composition: Delay(dur, Respond|Continue) / Respond / Continue
//! ```

use std::cell::Cell;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use http::header::{CONTENT_LENGTH, CONTENT_TYPE, HeaderName, HeaderValue};
use http::{StatusCode, Version};
use velda_core::{Action, Body, HeaderWorkspace, L7Request, L7Response};

use crate::capability::{AccessLevel, PluginCapability, SupportedProtocols};
use crate::config::PluginConfig;
use crate::error::PluginError;
use crate::pre_upstream::PreUpstreamPlugin;

use super::condition::{
    HeaderMatchRule, HttpFaultCondition, IpRangeMatcher, KEY_FAULT_ABORT_CODE, KEY_FAULT_DELAY_MS,
};

static GLOBAL_SEED: AtomicU64 = AtomicU64::new(0x853c_49e6_748f_ea9b);

thread_local! {
    static RNG_STATE: Cell<u64> = const { Cell::new(0) };
}

/// Single thread-local XorShift64* PRNG shared by delay and abort evaluation (~0.8ns).
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

/// Daniel Lemire's fast alternative to modulo reduction for uniform bounded random integers.
#[inline(always)]
fn fast_reduce(val: u64, n: usize) -> usize {
    ((val as u128 * n as u128) >> 64) as usize
}

/// Configuration for HTTP Delay Injection (`[min_ms, max_ms]`).
#[derive(Debug, Clone, PartialEq)]
pub struct HttpDelayConfig {
    /// Configured percentage of matching requests that receive delay (`0.0..=100.0`).
    pub percentage: f64,
    /// Pre-computed integer threshold in `0..=u32::MAX`.
    threshold: u32,
    /// Minimum delay in milliseconds (inclusive).
    pub min_ms: u64,
    /// Maximum delay in milliseconds (inclusive).
    pub max_ms: u64,
    /// Pre-computed duration when `min_ms == max_ms` (`Duration::ZERO` otherwise).
    fixed_dur: Duration,
}

impl HttpDelayConfig {
    /// Creates a new delay configuration with validated bounds.
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

    /// Evaluates delay using an externally sampled 32-bit entropy word (zero redundant TLS PRNG).
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

    /// Evaluates whether this request should be delayed, rolling thread-local PRNG if fractional.
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

/// Configuration for HTTP Abort Injection with pre-compiled default [`L7Response`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpAbortConfig {
    /// Pre-computed integer threshold in `0..=u32::MAX`.
    threshold: u32,
    /// Default HTTP status code to return when aborted (e.g. `503`).
    pub status: StatusCode,
    /// Pre-allocated raw response payload body.
    pub body: Bytes,
    /// Pre-computed Content-Length header value.
    content_length_val: HeaderValue,
    /// Pre-compiled response headers to inject on cold override status builds.
    pub headers: Vec<(HeaderName, HeaderValue)>,
    default_response: L7Response,
}

impl HttpAbortConfig {
    /// Creates a new abort configuration with validated status and pre-compiled response headers.
    pub fn new(
        percentage: f64,
        status_code: u16,
        custom_body: Option<String>,
        custom_headers: Option<Vec<(HeaderName, HeaderValue)>>,
    ) -> Result<Self, String> {
        if !(0.0..=100.0).contains(&percentage) {
            return Err(format!(
                "Abort percentage must be between 0.0 and 100.0, got {percentage}"
            ));
        }
        if !(400..=599).contains(&status_code) {
            return Err(format!(
                "Abort HTTP status code must be an error status between 400 and 599, got {status_code}"
            ));
        }

        let status = StatusCode::from_u16(status_code)
            .map_err(|e| format!("Invalid HTTP status code {status_code}: {e}"))?;

        let (body, content_length_val) = match custom_body {
            Some(text) => {
                let b = Bytes::from(text.into_bytes());
                let hv = HeaderValue::from_str(&b.len().to_string())
                    .unwrap_or_else(|_| HeaderValue::from_static("0"));
                (b, hv)
            }
            None => (
                Bytes::from_static(b"fault filter abort\n"),
                HeaderValue::from_static("19"),
            ),
        };

        let mut headers = vec![
            (
                CONTENT_TYPE,
                HeaderValue::from_static("text/plain; charset=utf-8"),
            ),
            (
                HeaderName::from_static("x-velda-fault"),
                HeaderValue::from_static("abort"),
            ),
        ];
        if let Some(customs) = custom_headers {
            headers.extend(customs);
        }

        let mut header_ws = HeaderWorkspace::new(headers.len() + 2);
        for (name, val) in &headers {
            header_ws.insert(name.clone(), val.clone());
        }
        header_ws.insert(CONTENT_LENGTH, content_length_val.clone());

        let default_response = L7Response::new(
            status,
            Version::HTTP_11,
            header_ws,
            Body::Bytes(body.clone()),
        );

        Ok(Self {
            threshold: percentage_to_threshold_32(percentage),
            status,
            body,
            content_length_val,
            headers,
            default_response,
        })
    }

    /// Evaluates abort using an externally sampled 32-bit entropy word.
    #[inline]
    pub fn evaluate_with_roll(
        &self,
        override_status: Option<u16>,
        roll32: u32,
    ) -> Option<L7Response> {
        if let Some(code) = override_status {
            return Some(self.resolve_override_response(code));
        }
        if self.threshold == 0 || (self.threshold < u32::MAX && roll32 >= self.threshold) {
            return None;
        }
        Some(self.default_response.clone())
    }

    /// Evaluates whether this request should be aborted, rolling thread-local PRNG if fractional.
    #[inline]
    pub fn evaluate(&self, override_status: Option<u16>) -> Option<L7Response> {
        let roll32 = if override_status.is_none() && self.threshold > 0 && self.threshold < u32::MAX
        {
            fast_random_u64() as u32
        } else {
            0
        };
        self.evaluate_with_roll(override_status, roll32)
    }

    #[cold]
    #[inline(never)]
    fn resolve_override_response(&self, code: u16) -> L7Response {
        let status = StatusCode::from_u16(code).unwrap_or(self.status);
        if status == self.status {
            return self.default_response.clone();
        }
        let mut header_ws = HeaderWorkspace::new(self.headers.len() + 2);
        for (name, val) in &self.headers {
            header_ws.insert(name.clone(), val.clone());
        }
        header_ws.insert(CONTENT_LENGTH, self.content_length_val.clone());
        L7Response::new(
            status,
            Version::HTTP_11,
            header_ws,
            Body::Bytes(self.body.clone()),
        )
    }
}

/// Plugin for injecting artificial delays and HTTP abort responses.
///
/// Aligned to 64 bytes (`#[repr(C, align(64))]`) with `active_mask` at offset 0 on the first L1D cache line.
#[repr(C, align(64))]
#[derive(Debug, Clone)]
pub struct HttpFaultInjectionPlugin {
    /// Non-zero when any static injection or dynamic override is active (`0` = inactive fast path).
    active_mask: u64,
    pub condition: HttpFaultCondition,
    pub delay: Option<HttpDelayConfig>,
    pub abort: Option<HttpAbortConfig>,
    pub id: String,
}

impl HttpFaultInjectionPlugin {
    /// Creates a new HTTP Fault Injection plugin instance.
    pub fn new(
        id: impl Into<String>,
        condition: HttpFaultCondition,
        delay: Option<HttpDelayConfig>,
        abort: Option<HttpAbortConfig>,
    ) -> Self {
        let d_thresh = delay.as_ref().map_or(0, |d| d.threshold) as u64;
        let a_thresh = abort.as_ref().map_or(0, |a| a.threshold) as u64;
        let override_bit = u64::from(condition.allow_header_override);
        let active_mask = d_thresh | (a_thresh << 32) | override_bit;

        Self {
            active_mask,
            id: id.into(),
            condition,
            delay,
            abort,
        }
    }

    /// Instantiates a new [`HttpFaultInjectionPlugin`] from compiled [`PluginConfig`].
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let cfg =
            super::validation::validate_http_fault_injection_config(&config.id, &config.config)
                .map_err(|e| {
                    PluginError::Config(format!(
                        "Invalid http_fault_injection configuration for '{}': {e}",
                        config.id
                    ))
                })?;

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
                Some(val) => HeaderMatchRule::exact(name, val.clone()),
                None => HeaderMatchRule::present(name),
            })
        } else {
            None
        };

        let condition = HttpFaultCondition {
            ip_matcher,
            header_match,
            allow_header_override: cfg.scope.allow_header_override,
            max_override_delay_ms: cfg.scope.max_override_delay_ms,
        };

        let delay_cfg = if cfg.delay.enabled && cfg.delay.percentage > 0 {
            Some(
                HttpDelayConfig::new(
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
                HttpDelayConfig::new(0.0, 0, cfg.scope.max_override_delay_ms).map_err(|e| {
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
            Some(
                HttpAbortConfig::new(
                    cfg.abort.percentage as f64,
                    cfg.abort.http_status,
                    None,
                    None,
                )
                .map_err(|e| {
                    PluginError::Config(format!("Plugin '{}': abort error: {e}", config.id))
                })?,
            )
        } else if cfg.scope.allow_header_override {
            Some(HttpAbortConfig::new(0.0, 500, None, None).map_err(|e| {
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

impl PreUpstreamPlugin for HttpFaultInjectionPlugin {
    #[inline(always)]
    fn id(&self) -> &str {
        &self.id
    }

    #[inline(always)]
    fn pbww(&self) -> u32 {
        0
    }

    /// Evaluates the request against fault injection rules.
    #[inline]
    fn on_upstream_request(&self, req: &mut L7Request, _targets: &mut u32) -> Action {
        // Step 1: Extract dynamic overrides (when allowed) and sanitize chaos headers once via pre-hashed SIMD checks.
        let (override_delay, override_abort) = if self.condition.allow_header_override {
            self.condition.extract_overrides(req)
        } else {
            (None, None)
        };
        if req.headers.contains_prehashed(&KEY_FAULT_DELAY_MS) {
            req.headers.remove("x-fault-delay-ms");
        }
        if req.headers.contains_prehashed(&KEY_FAULT_ABORT_CODE) {
            req.headers.remove("x-fault-abort-code");
        }

        // Step 2: Single-instruction pass-through if inactive.
        if self.active_mask == 0 {
            return Action::Continue;
        }

        // Step 3: Scope filter.
        if !self.condition.matches(req) {
            return Action::Continue;
        }

        // Step 4: Sample PRNG once only if any active fractional threshold requires entropy.
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
        let abort_resp = self
            .abort
            .as_ref()
            .and_then(|a| a.evaluate_with_roll(override_abort, r as u32));

        // Step 5: Action composition.
        match (delay_dur, abort_resp) {
            (Some(dur), Some(resp)) => Action::delay(dur, Action::Respond(resp)),
            (Some(dur), None) => Action::delay(dur, Action::Continue),
            (None, Some(resp)) => Action::Respond(resp),
            (None, None) => Action::Continue,
        }
    }

    #[inline(always)]
    fn capability(&self) -> PluginCapability {
        PluginCapability::header_only(AccessLevel::Read)
    }

    #[inline(always)]
    fn supported_protocols(&self) -> SupportedProtocols {
        SupportedProtocols::ALL_HTTP
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{HeaderValue, Method, StatusCode, Uri, Version};
    use velda_core::{Body, HeaderWorkspace};

    #[test]
    fn test_abort_config_validation_and_defaults() {
        assert!(HttpAbortConfig::new(101.0, 503, None, None).is_err());
        assert!(HttpAbortConfig::new(-0.1, 503, None, None).is_err());
        assert!(HttpAbortConfig::new(50.0, 999, None, None).is_err());
        assert!(HttpAbortConfig::new(50.0, 200, None, None).is_err());
        assert!(HttpAbortConfig::new(50.0, 302, None, None).is_err());

        let config = HttpAbortConfig::new(100.0, 503, None, None).unwrap();
        let resp = config.evaluate(None).unwrap();
        assert_eq!(resp.status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            resp.headers.get("content-type").unwrap(),
            "text/plain; charset=utf-8"
        );
        assert_eq!(resp.headers.get("content-length").unwrap(), "19");
        assert_eq!(resp.headers.get("x-velda-fault").unwrap(), "abort");
        assert_eq!(
            resp.body,
            Body::Bytes(Bytes::from_static(b"fault filter abort\n"))
        );

        let zero_cfg = HttpAbortConfig::new(0.0, 500, None, None).unwrap();
        assert!(zero_cfg.evaluate(None).is_none());
        assert_eq!(
            zero_cfg.evaluate(Some(504)).unwrap().status,
            StatusCode::GATEWAY_TIMEOUT
        );
    }

    #[test]
    fn test_delay_config_validation_and_bounds() {
        assert!(HttpDelayConfig::new(101.0, 10, 20).is_err());
        assert!(HttpDelayConfig::new(-1.0, 10, 20).is_err());
        assert!(HttpDelayConfig::new(50.0, 30, 20).is_err());

        let fixed = HttpDelayConfig::new(100.0, 250, 250).unwrap();
        assert_eq!(
            fixed.calculate_delay(None),
            Some(Duration::from_millis(250))
        );

        let zero = HttpDelayConfig::new(0.0, 100, 500).unwrap();
        assert_eq!(zero.calculate_delay(None), None);
        assert_eq!(
            zero.calculate_delay(Some(750)),
            Some(Duration::from_millis(750))
        );

        let ranged = HttpDelayConfig::new(100.0, 100, 300).unwrap();
        for _ in 0..100 {
            let ms = ranged.calculate_delay(None).unwrap().as_millis() as u64;
            assert!((100..=300).contains(&ms));
        }
    }

    #[test]
    fn test_fault_injection_scope_bypass() {
        let condition = HttpFaultCondition {
            ip_matcher: Some(IpRangeMatcher::from_cidrs(&["10.0.0.0/8".to_string()]).unwrap()),
            ..Default::default()
        };
        let abort = HttpAbortConfig::new(100.0, 503, None, None).unwrap();
        let plugin = HttpFaultInjectionPlugin::new("fault-test", condition, None, Some(abort));

        let mut headers = HeaderWorkspace::empty(4);
        headers.insert(
            HeaderName::from_static("x-forwarded-for"),
            HeaderValue::from_static("203.0.113.1"),
        );
        let mut req = L7Request::new(
            Method::GET,
            Uri::from_static("/api/users"),
            Version::HTTP_11,
            headers,
            Body::Empty,
        );

        let mut target = 1;
        assert!(matches!(
            plugin.on_upstream_request(&mut req, &mut target),
            Action::Continue
        ));
    }

    #[test]
    fn test_fault_injection_delay_and_abort_matrix() {
        let mut req = L7Request::new(
            Method::GET,
            Uri::from_static("/api/users"),
            Version::HTTP_11,
            HeaderWorkspace::empty(4),
            Body::Empty,
        );
        let mut target = 1;

        let delay_only = HttpFaultInjectionPlugin::new(
            "d",
            HttpFaultCondition::default(),
            Some(HttpDelayConfig::new(100.0, 200, 200).unwrap()),
            None,
        );
        match delay_only.on_upstream_request(&mut req, &mut target) {
            Action::Delay { duration, and_then } => {
                assert_eq!(duration, Duration::from_millis(200));
                assert!(matches!(*and_then, Action::Continue));
            }
            _ => panic!("Expected Action::Delay with Continue"),
        }

        let abort_only = HttpFaultInjectionPlugin::new(
            "a",
            HttpFaultCondition::default(),
            None,
            Some(HttpAbortConfig::new(100.0, 503, None, None).unwrap()),
        );
        match abort_only.on_upstream_request(&mut req, &mut target) {
            Action::Respond(resp) => assert_eq!(resp.status, StatusCode::SERVICE_UNAVAILABLE),
            _ => panic!("Expected Action::Respond"),
        }

        let both = HttpFaultInjectionPlugin::new(
            "b",
            HttpFaultCondition::default(),
            Some(HttpDelayConfig::new(100.0, 300, 300).unwrap()),
            Some(HttpAbortConfig::new(100.0, 504, None, None).unwrap()),
        );
        match both.on_upstream_request(&mut req, &mut target) {
            Action::Delay { duration, and_then } => {
                assert_eq!(duration, Duration::from_millis(300));
                match *and_then {
                    Action::Respond(resp) => assert_eq!(resp.status, StatusCode::GATEWAY_TIMEOUT),
                    _ => panic!("Expected Action::Respond inside Delay"),
                }
            }
            _ => panic!("Expected Action::Delay with Respond"),
        }
    }

    #[test]
    fn test_fault_injection_dynamic_override_and_hygiene_strip() {
        let condition = HttpFaultCondition {
            allow_header_override: true,
            max_override_delay_ms: 5000,
            header_match: Some(HeaderMatchRule::exact(
                HeaderName::from_static("x-chaos-test"),
                "enabled",
            )),
            ..Default::default()
        };
        let delay = HttpDelayConfig::new(0.0, 100, 200).unwrap();
        let abort = HttpAbortConfig::new(0.0, 500, None, None).unwrap();
        let plugin =
            HttpFaultInjectionPlugin::new("fault-test", condition, Some(delay), Some(abort));

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
            HeaderName::from_static("x-fault-abort-code"),
            HeaderValue::from_static("503"),
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
                match *and_then {
                    Action::Respond(resp) => {
                        assert_eq!(resp.status, StatusCode::SERVICE_UNAVAILABLE)
                    }
                    _ => panic!("Expected Action::Respond inside Delay"),
                }
            }
            _ => panic!("Expected Action::Delay with Respond"),
        }

        assert!(!req.headers.contains_key("x-fault-delay-ms"));
        assert!(!req.headers.contains_key("x-fault-abort-code"));
        assert!(req.headers.contains_key("x-chaos-test"));

        // Inactive plugin still sanitizes spoofed chaos headers when present
        let inactive =
            HttpFaultInjectionPlugin::new("inactive", HttpFaultCondition::default(), None, None);
        let mut spoofed = HeaderWorkspace::empty(4);
        spoofed.insert(
            HeaderName::from_static("x-fault-delay-ms"),
            HeaderValue::from_static("100"),
        );
        let mut req2 = L7Request::new(
            Method::GET,
            Uri::from_static("/"),
            Version::HTTP_11,
            spoofed,
            Body::Empty,
        );
        assert!(matches!(
            inactive.on_upstream_request(&mut req2, &mut target),
            Action::Continue
        ));
        assert!(!req2.headers.contains_key("x-fault-delay-ms"));
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
                "http_status": 503,
                "percentage": 10
            }
        });

        let config = PluginConfig {
            id: "http_fault_injection".to_string(),
            config: json_cfg,
        };

        let plugin = HttpFaultInjectionPlugin::from_config(&config).unwrap();
        assert_eq!(plugin.id, "http_fault_injection");
        assert!(plugin.condition.ip_matcher.is_some());
        assert!(plugin.condition.header_match.is_some());
        assert!(plugin.condition.allow_header_override);
        assert_eq!(plugin.condition.max_override_delay_ms, 4000);
        assert!(plugin.delay.is_some());
        assert!(plugin.abort.is_some());
        assert_ne!(plugin.active_mask, 0);
    }
}
