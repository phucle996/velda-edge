//! `bot_detection` Pre-Upstream Deep Bot Detection & Progressive Tarpit Plugin.
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────────────────────┐
//! │                 BOT_DETECTION WORKFLOW (DESIGNED AFTER `ja4_filter`)                    │
//! ├─────────────────────────────────────────────────────────────────────────────────────────┤
//! │  L7Request                                                                              │
//! │    ├── 1. Single-pass feature extract + fused SoA scan -> risk score (0..=100)          │
//! │    ├── 2. If `block_score` reached (enforce) -> Action::Reject(Forbidden)               │
//! │    ├── 3. If `escalation` enabled:                                                      │
//! │    │       ├── Clean + L1 448-bit Bitfilter == 0 -> Action::Continue (1 cache line, 0 L2)│
//! │    │       └── Suspicious or L1 == 1 -> L2 exact (client_ip, ja4) strike (0..=15)       │
//! │    │             ├── strike >= strike_block_threshold -> Action::Reject(Forbidden)      │
//! │    │             └── else -> Action::Delay((base << strike.min(4)).min(max_delay_ms))   │
//! │    └── 4. Else -> Action::Delay(linear score delay) or Action::Continue                 │
//! └─────────────────────────────────────────────────────────────────────────────────────────┘
//! ```

use std::sync::Arc;
use std::time::Duration;

use http::Version;
use velda_core::bitfield::*;
use velda_core::lut::{HttpMethod, classify_user_agent};
use velda_core::{Action, L7Request, RejectReason};

use crate::capability::{AccessLevel, PluginCapability, SupportedProtocols};
use crate::config::PluginConfig;
use crate::error::PluginError;
use crate::pre_upstream::PreUpstreamPlugin;
use crate::pre_upstream::bot_detection::arena::{EscalationArena, monotonic_tick_100ms};
use crate::pre_upstream::bot_detection::compiler::{
    BOOL_IS_TLS, BOOL_TLS_HAS_ALPS, BOOL_TLS_HAS_CERT_COMPRESS, BOOL_TLS_HAS_GREASE,
    BOOL_TLS_HAS_SNI, BOOL_WIRE_NO_RFC7540, BOOL_WIRE_ODD_SETTINGS, CompiledRuleSet,
    GRP_CIPHER_BKT_SHIFT, GRP_CONN_WIN_SHIFT, GRP_EXT_BKT_SHIFT, GRP_HTTP_VER_SHIFT,
    GRP_INIT_WIN_SHIFT, GRP_METHOD_SHIFT, GRP_PSEUDO_SHIFT, GRP_TLS_ALPN_SHIFT, GRP_TLS_VER_SHIFT,
    Ja4SetMatcher, compile_ruleset, pseudo_code_to_slot,
};
use crate::pre_upstream::bot_detection::config::{
    BotDetectionConfig, BotDetectionMode, parse_strict_bot_detection_config,
};
use crate::pre_upstream::bot_detection::validation::validate_thresholds_and_escalation;

/// Deep Layer 7 bot detection plugin combining boot-fused SoA anomaly scoring (0..=100)
/// with lock-free L1 448-bit (64B) + L2 exact `(client_ip, ja4)` progressive delay escalation.
#[derive(Debug, Clone)]
pub struct BotDetectionPlugin {
    id: String,
    mode: BotDetectionMode,
    delay_score: Option<u8>,
    min_delay_ms: u64,
    max_delay_ms: u64,
    block_score: Option<u8>,
    strike_block_threshold: Option<u8>,
    rules: CompiledRuleSet,
    arena: Option<Arc<EscalationArena>>,
}

impl BotDetectionPlugin {
    /// Builds and boot-compiles a [`BotDetectionPlugin`] from a validated [`BotDetectionConfig`].
    pub fn from_bot_config(
        id: impl Into<String>,
        cfg: &BotDetectionConfig,
    ) -> Result<Self, PluginError> {
        let id = id.into();
        validate_thresholds_and_escalation(&id, cfg)?;
        let rules = compile_ruleset(&id, cfg)?;
        let arena = cfg
            .escalation
            .as_ref()
            .map(|esc| Arc::new(EscalationArena::new(esc)));
        let strike_block_threshold = cfg
            .escalation
            .as_ref()
            .and_then(|esc| esc.strike_block_threshold);
        Ok(Self {
            id,
            mode: cfg.mode,
            delay_score: cfg.thresholds.delay_score,
            min_delay_ms: cfg.thresholds.min_delay_ms.unwrap_or(0),
            max_delay_ms: cfg.thresholds.max_delay_ms.unwrap_or(0),
            block_score: cfg.thresholds.block_score,
            strike_block_threshold,
            rules,
            arena,
        })
    }

    /// Instantiates [`BotDetectionPlugin`] from a declarative [`PluginConfig`].
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let parsed = parse_strict_bot_detection_config(&config.id, &config.config)?;
        Self::from_bot_config(config.id.clone(), &parsed)
    }

    /// Evaluates the `0..=100` anomaly risk score across the single-pass fused SoA ruleset.
    #[inline]
    pub fn evaluate_score(&self, req: &L7Request) -> u8 {
        let state = &req.state;
        let tls = state.tls_traits;
        let wire = state.wire_traits();

        let mut bool_bits = 0u64;
        if state.is_tls {
            bool_bits |= BOOL_IS_TLS;
        }
        if (tls & TLS_FLAG_HAS_SNI) != 0 {
            bool_bits |= BOOL_TLS_HAS_SNI;
        }
        if (tls & TLS_FLAG_HAS_GREASE) != 0 {
            bool_bits |= BOOL_TLS_HAS_GREASE;
        }
        if (tls & TLS_FLAG_HAS_CERT_COMPRESS) != 0 {
            bool_bits |= BOOL_TLS_HAS_CERT_COMPRESS;
        }
        if (tls & TLS_FLAG_HAS_ALPS) != 0 {
            bool_bits |= BOOL_TLS_HAS_ALPS;
        }
        if (wire & WIRE_FLAG_ODD_SETTINGS) != 0 {
            bool_bits |= BOOL_WIRE_ODD_SETTINGS;
        }
        if (wire & WIRE_FLAG_NO_RFC7540_PRIORITIES) != 0 {
            bool_bits |= BOOL_WIRE_NO_RFC7540;
        }

        let http_slot = match req.version {
            Version::HTTP_11 => 1u32,
            Version::HTTP_2 => 2,
            Version::HTTP_3 => 3,
            _ => 0,
        };
        let tls_ver_slot = (tls & TLS_VER_MASK) as u32;
        let tls_alpn_slot = ((tls & TLS_ALPN_MASK) >> TLS_ALPN_SHIFT) as u32;
        let cipher_bkt_slot = ((tls & TLS_CIPHER_BUCKET_MASK) >> TLS_CIPHER_BUCKET_SHIFT) as u32;
        let ext_bkt_slot = ((tls & TLS_EXT_BUCKET_MASK) >> TLS_EXT_BUCKET_SHIFT) as u32;
        let pseudo_slot = pseudo_code_to_slot((wire & WIRE_PSEUDO_ORDER_MASK) as u8);
        let init_win_slot = ((wire & WIRE_INIT_WIN_MASK) >> WIRE_INIT_WIN_SHIFT) as u32;
        let conn_win_slot = ((wire & WIRE_CONN_WIN_MASK) >> WIRE_CONN_WIN_SHIFT) as u32;
        let method_slot = HttpMethod::from_http_method(&req.method) as u32;

        let group_bit = (1u64 << (GRP_HTTP_VER_SHIFT + http_slot))
            | (1u64 << (GRP_TLS_VER_SHIFT + tls_ver_slot))
            | (1u64 << (GRP_TLS_ALPN_SHIFT + tls_alpn_slot))
            | (1u64 << (GRP_CIPHER_BKT_SHIFT + cipher_bkt_slot))
            | (1u64 << (GRP_EXT_BKT_SHIFT + ext_bkt_slot))
            | (1u64 << (GRP_PSEUDO_SHIFT + pseudo_slot))
            | (1u64 << (GRP_INIT_WIN_SHIFT + init_win_slot))
            | (1u64 << (GRP_CONN_WIN_SHIFT + conn_win_slot))
            | (1u64 << (GRP_METHOD_SHIFT + method_slot));

        let (ua_bit, ua_major) = if self.rules.needs_ua {
            let (fam, _os, major) = req
                .headers
                .get(http::header::USER_AGENT)
                .map(|v| classify_user_agent(v.as_bytes()))
                .unwrap_or((velda_core::UaFamily::None, velda_core::UaOs::Unknown, 0));
            (1u64 << (fam as u32), major)
        } else {
            (1u64, 0u16)
        };

        let mut hdr_bits = 0u64;
        for (idx, hk) in self.rules.header_keys.iter().enumerate() {
            if req.headers.get(&hk.name).is_some() {
                hdr_bits |= 1u64 << idx;
            }
        }

        let r = &self.rules;
        let count = r.scores.len();
        let ja4 = state.ja4;
        let mut total_score: u8 = 0;

        for i in 0..count {
            let mut matched = ((bool_bits & r.bool_masks[i]) == r.bool_vals[i])
                & ((group_bit & r.group_allows[i]) == group_bit)
                & ((ua_bit & r.ua_allows[i]) == ua_bit)
                & (ua_major >= r.ua_mins[i])
                & (ua_major <= r.ua_maxs[i])
                & ((hdr_bits & r.hdr_masks[i]) == r.hdr_vals[i]);

            if matched && r.has_ja4_matchers {
                let j_idx = r.ja4_indices[i] as usize;
                if let Some(m) = r.ja4_matchers.get(j_idx) {
                    matched = ja4_matcher_allows(m, ja4);
                }
            }

            if matched {
                total_score = total_score.saturating_add(r.scores[i]).min(100);
                if self.mode == BotDetectionMode::Enforce
                    && let Some(block) = self.block_score
                    && total_score >= block
                {
                    return total_score;
                }
            }
        }

        total_score
    }

    #[inline]
    fn compute_base_delay_ms(&self, score: u8) -> u64 {
        let Some(ds) = self.delay_score else {
            return 0;
        };
        if score < ds || self.min_delay_ms == 0 {
            return 0;
        }
        let top = self.block_score.unwrap_or(100);
        if top <= ds || self.max_delay_ms <= self.min_delay_ms {
            return self.min_delay_ms;
        }
        let span = (top - ds) as u64;
        let delta = ((score - ds) as u64).min(span);
        self.min_delay_ms + ((self.max_delay_ms - self.min_delay_ms) * delta) / span
    }

    /// Evaluates [`Action`] at an explicit coarse `100ms` tick (useful for deterministic unit tests).
    pub(crate) fn evaluate_at_tick(&self, req: &L7Request, now_tick_100ms: u64) -> Action {
        let score = self.evaluate_score(req);

        if self.mode == BotDetectionMode::Monitor {
            return Action::Continue;
        }

        if let Some(block) = self.block_score
            && score >= block
        {
            return Action::Reject(RejectReason::Forbidden);
        }

        let suspicious = self.delay_score.is_some_and(|ds| score >= ds);
        let base_delay = self.compute_base_delay_ms(score);

        if let Some(ref arena) = self.arena {
            let (strike, active) = arena.observe(
                req.state.client_ip,
                req.state.ja4,
                suspicious,
                now_tick_100ms,
            );
            if !suspicious && !active {
                return Action::Continue;
            }
            if let Some(sb) = self.strike_block_threshold
                && strike >= sb
            {
                return Action::Reject(RejectReason::Forbidden);
            }
            let effective_ms = if suspicious {
                base_delay
                    .checked_shl((strike.min(4)) as u32)
                    .unwrap_or(self.max_delay_ms)
                    .min(self.max_delay_ms)
            } else {
                // Previously flagged offender sending a seemingly clean request stays throttled via bitshift.
                self.min_delay_ms
                    .checked_shl(((strike.saturating_sub(1)).min(4)) as u32)
                    .unwrap_or(self.max_delay_ms)
                    .min(self.max_delay_ms)
            };
            if effective_ms > 0 {
                return Action::delay(Duration::from_millis(effective_ms), Action::Continue);
            }
        } else if base_delay > 0 {
            return Action::delay(Duration::from_millis(base_delay), Action::Continue);
        }

        Action::Continue
    }
}

#[inline]
fn ja4_matcher_allows(m: &Ja4SetMatcher, ja4: u64) -> bool {
    if !m.ja4_in.is_empty() && m.ja4_in.binary_search(&ja4).is_err() {
        return false;
    }
    if !m.ja4_not_in.is_empty() && m.ja4_not_in.binary_search(&ja4).is_ok() {
        return false;
    }
    true
}

impl PreUpstreamPlugin for BotDetectionPlugin {
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
        self.evaluate_at_tick(req, monotonic_tick_100ms())
    }

    #[inline(always)]
    fn capability(&self) -> PluginCapability {
        PluginCapability::new(AccessLevel::Read, AccessLevel::None)
    }

    #[inline(always)]
    fn supported_protocols(&self) -> SupportedProtocols {
        SupportedProtocols::ALL_HTTP
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{HeaderValue, Method};
    use serde_json::json;
    use velda_core::{Body, HeaderWorkspace};

    fn strict_rule_json(
        id: &str,
        score: u8,
        ua_family: Option<&str>,
        absent: Option<&[&str]>,
    ) -> serde_json::Value {
        json!({
            "id": id,
            "score": score,
            "when": {
                "is_tls": true,
                "http_version_in": ["h2"],
                "method_in": null,
                "tls_version_in": ["tls1.3"],
                "tls_alpn_in": null,
                "tls_alpn_not": null,
                "tls_has_sni": true,
                "tls_has_grease": false,
                "tls_has_cert_compress": null,
                "tls_has_alps": null,
                "tls_cipher_bucket_in": null,
                "tls_ext_bucket_in": null,
                "wire_pseudo_order_in": null,
                "wire_pseudo_order_not_in": null,
                "wire_odd_settings": null,
                "wire_no_rfc7540_priorities": null,
                "wire_init_window_in": null,
                "wire_conn_window_in": null,
                "ua_family_in": ua_family.map(|u| vec![u]),
                "ua_family_not_in": null,
                "ua_min_major": 110,
                "ua_max_major": null,
                "headers_present": null,
                "headers_absent": absent,
                "ja4_in": null,
                "ja4_not_in": null
            }
        })
    }

    fn make_real_chrome_req(ip: u128, ja4: u64) -> L7Request {
        let mut hdrs = HeaderWorkspace::empty(8);
        hdrs.insert(
            http::header::USER_AGENT,
            HeaderValue::from_static("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"),
        );
        hdrs.insert(
            http::header::HeaderName::from_static("sec-fetch-site"),
            HeaderValue::from_static("same-origin"),
        );
        hdrs.insert(
            http::header::HeaderName::from_static("sec-fetch-mode"),
            HeaderValue::from_static("cors"),
        );
        hdrs.insert(
            http::header::ACCEPT_LANGUAGE,
            HeaderValue::from_static("en-US,en;q=0.9"),
        );
        let mut req = L7Request::new(
            Method::POST,
            http::Uri::from_static("/login"),
            Version::HTTP_2,
            hdrs,
            Body::Empty,
        );
        req.state.client_ip = ip;
        req.state.is_tls = true;
        req.state.ja4 = ja4;
        req.state.tls_traits = TLS_VER_13
            | (TLS_ALPN_H2 << TLS_ALPN_SHIFT)
            | TLS_FLAG_HAS_SNI
            | TLS_FLAG_HAS_GREASE
            | TLS_FLAG_HAS_CERT_COMPRESS
            | (2 << TLS_CIPHER_BUCKET_SHIFT)
            | (3 << TLS_EXT_BUCKET_SHIFT);
        req.state
            .set_wire_traits((WIRE_PROTO_H2 << WIRE_PROTO_SHIFT) | (PSEUDO_ORDER_MASP as u64));
        req
    }

    fn make_spoofed_chrome_bot_req(ip: u128, ja4: u64) -> L7Request {
        let mut hdrs = HeaderWorkspace::empty(4);
        hdrs.insert(
            http::header::USER_AGENT,
            HeaderValue::from_static("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"),
        );
        let mut req = L7Request::new(
            Method::POST,
            http::Uri::from_static("/login"),
            Version::HTTP_2,
            hdrs,
            Body::Empty,
        );
        req.state.client_ip = ip;
        req.state.is_tls = true;
        req.state.ja4 = ja4;
        req.state.tls_traits = TLS_VER_13
            | (TLS_ALPN_H2 << TLS_ALPN_SHIFT)
            | TLS_FLAG_HAS_SNI
            | (2 << TLS_CIPHER_BUCKET_SHIFT)
            | (2 << TLS_EXT_BUCKET_SHIFT);
        req.state.set_wire_traits(
            (WIRE_PROTO_H2 << WIRE_PROTO_SHIFT)
                | (PSEUDO_ORDER_MSAP as u64)
                | WIRE_FLAG_ODD_SETTINGS,
        );
        req
    }

    #[test]
    fn test_strict_schema_rejects_missing_keys_in_when_and_thresholds() {
        let incomplete_when = json!({
            "mode": "enforce",
            "preset": "none",
            "thresholds": { "delay_score": 40, "min_delay_ms": 100, "max_delay_ms": 2000, "block_score": null },
            "escalation": null,
            "rules": [{
                "id": "r1",
                "score": 50,
                "when": { "is_tls": true }
            }]
        });
        assert!(
            BotDetectionPlugin::from_config(&PluginConfig {
                id: "bot_detection".into(),
                config: incomplete_when,
            })
            .is_err()
        );

        let missing_block_key = json!({
            "mode": "enforce",
            "preset": "balanced_browser",
            "thresholds": { "delay_score": 40, "min_delay_ms": 100, "max_delay_ms": 2000 },
            "escalation": null,
            "rules": []
        });
        assert!(
            BotDetectionPlugin::from_config(&PluginConfig {
                id: "bot_detection".into(),
                config: missing_block_key,
            })
            .is_err()
        );
    }

    #[test]
    fn test_progressive_delay_only_mode_and_l1_l2_escalation() {
        // Delay-only mode (`block_score: null`): slows repeat offender progressively 100ms -> 200ms -> 400ms
        // while keeping another IP sharing the exact same Chrome JA4 at zero overhead (`Action::Continue`).
        let cfg = PluginConfig {
            id: "bot_detection".into(),
            config: json!({
                "mode": "enforce",
                "preset": "none",
                "thresholds": {
                    "delay_score": 50,
                    "min_delay_ms": 100,
                    "max_delay_ms": 1500,
                    "block_score": null
                },
                "escalation": {
                    "window_secs": 30,
                    "decay_secs": 10,
                    "max_entries": 512,
                    "strike_block_threshold": 4
                },
                "rules": [
                    strict_rule_json("no-grease-chrome", 50, Some("chrome"), None)
                ]
            }),
        };
        let plugin = BotDetectionPlugin::from_config(&cfg).unwrap();
        assert_eq!(plugin.supported_protocols(), SupportedProtocols::ALL_HTTP);
        assert!(
            !plugin
                .supported_protocols()
                .contains(SupportedProtocols::GRPC)
        );
        let chrome_ja4 = 0x1234_5678_9abc_def0;
        let attacker_ip = 0x0a00_0002;
        let clean_ip = 0x0a00_0003;

        let bot_req = make_spoofed_chrome_bot_req(attacker_ip, chrome_ja4);
        let real_req = make_real_chrome_req(clean_ip, chrome_ja4);

        // Clean IP sharing identical Chrome JA4 passes immediately untouched.
        assert!(matches!(
            plugin.evaluate_at_tick(&real_req, 1000),
            Action::Continue
        ));

        // Attacker strike 0 -> 100ms
        assert!(matches!(
            plugin.evaluate_at_tick(&bot_req, 1001),
            Action::Delay { duration, .. } if duration == Duration::from_millis(100)
        ));
        // Strike 1 (200ms later = tick 1003) -> 200ms
        assert!(matches!(
            plugin.evaluate_at_tick(&bot_req, 1003),
            Action::Delay { duration, .. } if duration == Duration::from_millis(200)
        ));
        // Strike 2 -> 400ms
        assert!(matches!(
            plugin.evaluate_at_tick(&bot_req, 1005),
            Action::Delay { duration, .. } if duration == Duration::from_millis(400)
        ));
        // Attacker tries sending a clean-looking request right after -> still caught by L1+L2 -> 200ms!
        let sneaky_clean = make_real_chrome_req(attacker_ip, chrome_ja4);
        assert!(matches!(
            plugin.evaluate_at_tick(&sneaky_clean, 1006),
            Action::Delay { duration, .. } if duration == Duration::from_millis(200)
        ));
        // Clean user on different IP still untouched!
        assert!(matches!(
            plugin.evaluate_at_tick(&real_req, 1007),
            Action::Continue
        ));
        // Strike 3 -> 800ms, Strike 4 -> hits strike_block_threshold (4) -> Forbidden
        assert!(matches!(
            plugin.evaluate_at_tick(&bot_req, 1008),
            Action::Delay { duration, .. } if duration == Duration::from_millis(800)
        ));
        assert!(matches!(
            plugin.evaluate_at_tick(&bot_req, 1009),
            Action::Reject(RejectReason::Forbidden)
        ));
        // After window expires (tick 1400 >> +300 ticks = 30s), strikes decay away -> fresh strike 0 (100ms).
        assert!(matches!(
            plugin.evaluate_at_tick(&bot_req, 1400),
            Action::Delay { duration, .. } if duration == Duration::from_millis(100)
        ));
    }

    #[test]
    fn test_boot_fusion_merges_duplicate_predicates() {
        let r1 = strict_rule_json("dup-a", 25, Some("chrome"), None);
        let r2 = strict_rule_json("dup-b", 30, Some("chrome"), None);
        let plugin = BotDetectionPlugin::from_config(&PluginConfig {
            id: "bot_detection".into(),
            config: json!({
                "mode": "enforce",
                "preset": "none",
                "thresholds": {
                    "delay_score": 50,
                    "min_delay_ms": 120,
                    "max_delay_ms": 1200,
                    "block_score": 90
                },
                "escalation": null,
                "rules": [r1, r2]
            }),
        })
        .unwrap();
        let req = make_spoofed_chrome_bot_req(0x7f00_0001, 0x1111);
        assert_eq!(plugin.evaluate_score(&req), 55);
    }
}
