//! Strict Declarative Configuration Schema & Built-in Presets for `bot_detection`.
//!
//! Every JSON field (including nullable fields inside `thresholds`, `escalation`, and each rule's
//! `when` block) must be explicitly declared — omitted keys are rejected during validation/load.

use serde::{Deserialize, Serialize};

use crate::error::PluginError;

/// Execution mode for `bot_detection`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BotDetectionMode {
    /// Applies computed delay / optional hard-block actions when threshold scores match.
    Enforce,
    /// Evaluates risk scores without delaying or blocking traffic.
    Monitor,
}

/// Built-in cross-layer detection rule presets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BotDetectionPreset {
    /// Only rules explicitly supplied in `rules` are active.
    None,
    /// Core cross-layer browser consistency rules tuned against low false-positives.
    BalancedBrowser,
    /// Stricter browser rules including missing User-Agent and Chrome `NO_RFC7540_PRIORITIES`.
    StrictBrowser,
}

/// Delay-first graduated mitigation thresholds on the `0..=100` risk scale.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BotDetectionThresholds {
    /// Minimum score (`1..=100`) triggering progressive delay, or `null` to disable delay.
    pub delay_score: Option<u8>,
    /// Minimum tarpit delay in milliseconds (`> 0`) at `delay_score`, or `null`.
    pub min_delay_ms: Option<u64>,
    /// Maximum tarpit delay cap in milliseconds (`>= min_delay_ms`, `<= 10_000`), or `null`.
    pub max_delay_ms: Option<u64>,
    /// Optional high-confidence hard-block threshold (`1..=100`), or `null` for delay-only mode.
    pub block_score: Option<u8>,
}

/// Optional L1 (448-bit 64B Cache-Line Filter) + L2 (Exact `(client_ip, ja4)`) progressive delay escalation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BotEscalationConfig {
    /// Active strike tracking window in seconds (`1..=3600`).
    pub window_secs: u64,
    /// Elapsed interval in seconds (`1..=window_secs`) per 1-step strike decay.
    pub decay_secs: u64,
    /// Fixed bounded capacity for L2 exact-key slots allocated once at boot (`256..=1_048_576`).
    pub max_entries: usize,
    /// Optional consecutive strike count (`1..=15`) elevating mitigation to `403`, or `null` for delay-only.
    pub strike_block_threshold: Option<u8>,
}

/// Declarative weighted anomaly rule.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BotRuleConfig {
    /// Unique rule identifier (`rules` entry with same `id` overrides matching preset rule).
    pub id: String,
    /// Anomaly score added when `when` matches (`1..=100`).
    pub score: u8,
    /// Uniform fixed-schema predicate block (every key mandatory; unused keys = `null`).
    pub when: BotRuleWhen,
}

/// Strict uniform fixed-schema rule condition block (`26` fixed fields).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BotRuleWhen {
    pub is_tls: Option<bool>,
    pub http_version_in: Option<Vec<String>>,
    pub method_in: Option<Vec<String>>,
    pub tls_version_in: Option<Vec<String>>,
    pub tls_alpn_in: Option<Vec<String>>,
    pub tls_alpn_not: Option<String>,
    pub tls_has_sni: Option<bool>,
    pub tls_has_grease: Option<bool>,
    pub tls_has_cert_compress: Option<bool>,
    pub tls_has_alps: Option<bool>,
    pub tls_cipher_bucket_in: Option<Vec<u8>>,
    pub tls_ext_bucket_in: Option<Vec<u8>>,
    pub wire_pseudo_order_in: Option<Vec<String>>,
    pub wire_pseudo_order_not_in: Option<Vec<String>>,
    pub wire_odd_settings: Option<bool>,
    pub wire_no_rfc7540_priorities: Option<bool>,
    pub wire_init_window_in: Option<Vec<String>>,
    pub wire_conn_window_in: Option<Vec<String>>,
    pub ua_family_in: Option<Vec<String>>,
    pub ua_family_not_in: Option<Vec<String>>,
    pub ua_min_major: Option<u16>,
    pub ua_max_major: Option<u16>,
    pub headers_present: Option<Vec<String>>,
    pub headers_absent: Option<Vec<String>>,
    pub ja4_in: Option<Vec<String>>,
    pub ja4_not_in: Option<Vec<String>>,
}

impl BotRuleWhen {
    /// Unconstrained template (`null` in every field) for built-in preset construction.
    pub fn all_null() -> Self {
        Self {
            is_tls: None,
            http_version_in: None,
            method_in: None,
            tls_version_in: None,
            tls_alpn_in: None,
            tls_alpn_not: None,
            tls_has_sni: None,
            tls_has_grease: None,
            tls_has_cert_compress: None,
            tls_has_alps: None,
            tls_cipher_bucket_in: None,
            tls_ext_bucket_in: None,
            wire_pseudo_order_in: None,
            wire_pseudo_order_not_in: None,
            wire_odd_settings: None,
            wire_no_rfc7540_priorities: None,
            wire_init_window_in: None,
            wire_conn_window_in: None,
            ua_family_in: None,
            ua_family_not_in: None,
            ua_min_major: None,
            ua_max_major: None,
            headers_present: None,
            headers_absent: None,
            ja4_in: None,
            ja4_not_in: None,
        }
    }
}

/// Top-level configuration for `bot_detection`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BotDetectionConfig {
    pub mode: BotDetectionMode,
    pub preset: BotDetectionPreset,
    pub thresholds: BotDetectionThresholds,
    pub escalation: Option<BotEscalationConfig>,
    pub rules: Vec<BotRuleConfig>,
}

const REQUIRED_THRESHOLD_KEYS: [&str; 4] =
    ["delay_score", "min_delay_ms", "max_delay_ms", "block_score"];

const REQUIRED_ESCALATION_KEYS: [&str; 4] = [
    "window_secs",
    "decay_secs",
    "max_entries",
    "strike_block_threshold",
];

const REQUIRED_WHEN_KEYS: [&str; 26] = [
    "is_tls",
    "http_version_in",
    "method_in",
    "tls_version_in",
    "tls_alpn_in",
    "tls_alpn_not",
    "tls_has_sni",
    "tls_has_grease",
    "tls_has_cert_compress",
    "tls_has_alps",
    "tls_cipher_bucket_in",
    "tls_ext_bucket_in",
    "wire_pseudo_order_in",
    "wire_pseudo_order_not_in",
    "wire_odd_settings",
    "wire_no_rfc7540_priorities",
    "wire_init_window_in",
    "wire_conn_window_in",
    "ua_family_in",
    "ua_family_not_in",
    "ua_min_major",
    "ua_max_major",
    "headers_present",
    "headers_absent",
    "ja4_in",
    "ja4_not_in",
];

/// Parses JSON strictly requiring EVERY key in `thresholds`, optional `escalation`, and each
/// rule's `when` object to be explicitly present (even when `null`).
pub(crate) fn parse_strict_bot_detection_config(
    plugin_id: &str,
    raw: &serde_json::Value,
) -> Result<BotDetectionConfig, PluginError> {
    let obj = raw.as_object().ok_or_else(|| {
        PluginError::Config(format!(
            "Invalid configuration for plugin '{plugin_id}' (bot_detection): expected JSON object"
        ))
    })?;

    if let Some(th) = obj.get("thresholds").and_then(|v| v.as_object()) {
        for &k in &REQUIRED_THRESHOLD_KEYS {
            if !th.contains_key(k) {
                return Err(PluginError::Config(format!(
                    "Plugin '{plugin_id}' (bot_detection): missing mandatory field 'thresholds.{k}' (set to null if unused)"
                )));
            }
        }
    }

    if let Some(esc) = obj.get("escalation").and_then(|v| v.as_object()) {
        for &k in &REQUIRED_ESCALATION_KEYS {
            if !esc.contains_key(k) {
                return Err(PluginError::Config(format!(
                    "Plugin '{plugin_id}' (bot_detection): missing mandatory field 'escalation.{k}' (set to null if unused)"
                )));
            }
        }
    }

    if let Some(rules) = obj.get("rules").and_then(|v| v.as_array()) {
        for (idx, r) in rules.iter().enumerate() {
            if let Some(when_obj) = r.get("when").and_then(|w| w.as_object()) {
                let rid = r
                    .get("id")
                    .and_then(|v| v.as_str())
                    .filter(|s| !s.trim().is_empty())
                    .unwrap_or("<unnamed>");
                for &k in &REQUIRED_WHEN_KEYS {
                    if !when_obj.contains_key(k) {
                        return Err(PluginError::Config(format!(
                            "Plugin '{plugin_id}' (bot_detection) rule[{idx}] '{rid}': missing mandatory field 'when.{k}' (set to null if unused)"
                        )));
                    }
                }
            }
        }
    }

    serde_json::from_value(raw.clone()).map_err(|e| {
        PluginError::Config(format!(
            "Invalid configuration for plugin '{plugin_id}' (bot_detection): {e}"
        ))
    })
}

fn s_vec(items: &[&str]) -> Option<Vec<String>> {
    Some(items.iter().map(|s| (*s).to_string()).collect())
}

/// Expands a built-in preset into canonical rules.
pub(crate) fn preset_rules(preset: BotDetectionPreset) -> Vec<BotRuleConfig> {
    if preset == BotDetectionPreset::None {
        return Vec::new();
    }

    let mut rules = vec![
        BotRuleConfig {
            id: "modern_chrome_missing_grease".into(),
            score: 35,
            when: BotRuleWhen {
                is_tls: Some(true),
                ua_family_in: s_vec(&["chrome", "edge", "chrome-mobile"]),
                ua_min_major: Some(110),
                tls_has_grease: Some(false),
                ..BotRuleWhen::all_null()
            },
        },
        BotRuleConfig {
            id: "modern_chrome_missing_cert_compress".into(),
            score: 25,
            when: BotRuleWhen {
                is_tls: Some(true),
                ua_family_in: s_vec(&["chrome", "edge"]),
                ua_min_major: Some(110),
                tls_has_cert_compress: Some(false),
                ..BotRuleWhen::all_null()
            },
        },
        BotRuleConfig {
            id: "browser_ua_missing_sec_fetch".into(),
            score: 30,
            when: BotRuleWhen {
                is_tls: Some(true),
                ua_family_in: s_vec(&["chrome", "firefox", "edge", "brave"]),
                ua_min_major: Some(100),
                headers_absent: s_vec(&["sec-fetch-site", "sec-fetch-mode"]),
                ..BotRuleWhen::all_null()
            },
        },
        BotRuleConfig {
            id: "browser_ua_missing_accept_language".into(),
            score: 25,
            when: BotRuleWhen {
                ua_family_in: s_vec(&["chrome", "firefox", "safari", "edge", "brave"]),
                headers_absent: s_vec(&["accept-language"]),
                ..BotRuleWhen::all_null()
            },
        },
        BotRuleConfig {
            id: "h2_browser_ua_odd_settings".into(),
            score: 40,
            when: BotRuleWhen {
                http_version_in: s_vec(&["h2"]),
                ua_family_in: s_vec(&["chrome", "firefox", "safari", "edge"]),
                wire_odd_settings: Some(true),
                ..BotRuleWhen::all_null()
            },
        },
        BotRuleConfig {
            id: "h2_chrome_wrong_pseudo_order".into(),
            score: 35,
            when: BotRuleWhen {
                http_version_in: s_vec(&["h2", "h3"]),
                ua_family_in: s_vec(&["chrome", "edge", "brave"]),
                ua_min_major: Some(100),
                wire_pseudo_order_not_in: s_vec(&["masp"]),
                ..BotRuleWhen::all_null()
            },
        },
        BotRuleConfig {
            id: "h2_firefox_wrong_pseudo_order".into(),
            score: 35,
            when: BotRuleWhen {
                http_version_in: s_vec(&["h2", "h3"]),
                ua_family_in: s_vec(&["firefox"]),
                ua_min_major: Some(100),
                wire_pseudo_order_not_in: s_vec(&["mpas"]),
                ..BotRuleWhen::all_null()
            },
        },
        BotRuleConfig {
            id: "h2_missing_h2_alpn".into(),
            score: 45,
            when: BotRuleWhen {
                is_tls: Some(true),
                http_version_in: s_vec(&["h2"]),
                tls_alpn_not: Some("h2".into()),
                ..BotRuleWhen::all_null()
            },
        },
        BotRuleConfig {
            id: "tls_browser_tiny_client_hello".into(),
            score: 30,
            when: BotRuleWhen {
                is_tls: Some(true),
                ua_family_in: s_vec(&["chrome", "firefox", "safari", "edge"]),
                tls_cipher_bucket_in: Some(vec![0]),
                tls_ext_bucket_in: Some(vec![0]),
                ..BotRuleWhen::all_null()
            },
        },
        BotRuleConfig {
            id: "script_ua_identity".into(),
            score: 50,
            when: BotRuleWhen {
                ua_family_in: s_vec(&[
                    "curl",
                    "wget",
                    "python-requests",
                    "python-httpx",
                    "python-aiohttp",
                    "go-http-client",
                    "rust-reqwest",
                    "rust-hyper",
                ]),
                ..BotRuleWhen::all_null()
            },
        },
    ];

    if preset == BotDetectionPreset::StrictBrowser {
        rules.push(BotRuleConfig {
            id: "chrome_modern_missing_no_rfc7540".into(),
            score: 25,
            when: BotRuleWhen {
                http_version_in: s_vec(&["h2"]),
                ua_family_in: s_vec(&["chrome", "edge"]),
                ua_min_major: Some(125),
                wire_no_rfc7540_priorities: Some(false),
                ..BotRuleWhen::all_null()
            },
        });
        rules.push(BotRuleConfig {
            id: "missing_user_agent".into(),
            score: 40,
            when: BotRuleWhen {
                ua_family_in: s_vec(&["none"]),
                ..BotRuleWhen::all_null()
            },
        });
    }

    rules
}
