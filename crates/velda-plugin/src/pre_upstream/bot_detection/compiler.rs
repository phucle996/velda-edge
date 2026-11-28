//! Boot-Time SoA Rule Compiler & Fusion Engine for `bot_detection`.
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────────────────────┐
//! │                 BOT_DETECTION BOOT-TIME FUSION COMPILER                                 │
//! ├─────────────────────────────────────────────────────────────────────────────────────────┤
//! │  Preset + Custom Rules -> Contradiction Check -> Global Header Demand Registry (0..64)  │
//! │    -> Fuse identical bitmask predicates (sum scores) -> Sort descending by score        │
//! │    -> Contiguous SoA vectors for branchless single-pass request scoring                 │
//! └─────────────────────────────────────────────────────────────────────────────────────────┘
//! ```

use velda_core::bitfield::*;
use velda_core::buffer::header::HeaderKey;
use velda_core::lut::HttpMethod;
use velda_core::lut::user_agent::{UA_FAMILY_NAMES, UaFamily};

use crate::error::PluginError;
use crate::pre_upstream::bot_detection::config::{BotDetectionConfig, BotRuleConfig, preset_rules};
use crate::pre_upstream::ja4_filter::config::parse_ja4_entry;

pub(crate) const GRP_HTTP_VER_SHIFT: u32 = 0; // 4 bits
pub(crate) const GRP_TLS_VER_SHIFT: u32 = 4; // 4 bits
pub(crate) const GRP_TLS_ALPN_SHIFT: u32 = 8; // 5 bits
pub(crate) const GRP_CIPHER_BKT_SHIFT: u32 = 13; // 4 bits
pub(crate) const GRP_EXT_BKT_SHIFT: u32 = 17; // 4 bits
pub(crate) const GRP_PSEUDO_SHIFT: u32 = 21; // 6 bits
pub(crate) const GRP_INIT_WIN_SHIFT: u32 = 27; // 9 bits
pub(crate) const GRP_CONN_WIN_SHIFT: u32 = 36; // 6 bits
pub(crate) const GRP_METHOD_SHIFT: u32 = 42; // 10 bits

const ALL_HTTP_VER_BITS: u64 = 0xF << GRP_HTTP_VER_SHIFT;
const ALL_TLS_VER_BITS: u64 = 0xF << GRP_TLS_VER_SHIFT;
const ALL_TLS_ALPN_BITS: u64 = 0x1F << GRP_TLS_ALPN_SHIFT;
const ALL_CIPHER_BKT_BITS: u64 = 0xF << GRP_CIPHER_BKT_SHIFT;
const ALL_EXT_BKT_BITS: u64 = 0xF << GRP_EXT_BKT_SHIFT;
const ALL_PSEUDO_BITS: u64 = 0x3F << GRP_PSEUDO_SHIFT;
const ALL_INIT_WIN_BITS: u64 = 0x1FF << GRP_INIT_WIN_SHIFT;
const ALL_CONN_WIN_BITS: u64 = 0x3F << GRP_CONN_WIN_SHIFT;
const ALL_METHOD_BITS: u64 = 0x3FF << GRP_METHOD_SHIFT;

pub(crate) const ALL_GROUPS_MASK: u64 = ALL_HTTP_VER_BITS
    | ALL_TLS_VER_BITS
    | ALL_TLS_ALPN_BITS
    | ALL_CIPHER_BKT_BITS
    | ALL_EXT_BKT_BITS
    | ALL_PSEUDO_BITS
    | ALL_INIT_WIN_BITS
    | ALL_CONN_WIN_BITS
    | ALL_METHOD_BITS;

pub(crate) const ALL_UA_FAMILIES_MASK: u64 = (1u64 << UaFamily::COUNT) - 1;

pub(crate) const BOOL_IS_TLS: u64 = 1 << 0;
pub(crate) const BOOL_TLS_HAS_SNI: u64 = 1 << 1;
pub(crate) const BOOL_TLS_HAS_GREASE: u64 = 1 << 2;
pub(crate) const BOOL_TLS_HAS_CERT_COMPRESS: u64 = 1 << 3;
pub(crate) const BOOL_TLS_HAS_ALPS: u64 = 1 << 4;
pub(crate) const BOOL_WIRE_ODD_SETTINGS: u64 = 1 << 5;
pub(crate) const BOOL_WIRE_NO_RFC7540: u64 = 1 << 6;

/// Maps 5-bit `encode_pseudo_order` token into a 6-slot one-hot index (`0..=5`).
#[inline(always)]
pub(crate) fn pseudo_code_to_slot(code: u8) -> u32 {
    if code == PSEUDO_ORDER_MASP {
        0
    } else if code == PSEUDO_ORDER_MPAS {
        1
    } else if code == PSEUDO_ORDER_MSPA {
        2
    } else if code == PSEUDO_ORDER_MSAP {
        3
    } else if code == PSEUDO_ORDER_MAPS {
        4
    } else {
        5
    }
}

/// Optional per-rule JA4 set matchers stored in a side table when referenced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Ja4SetMatcher {
    pub(crate) ja4_in: Box<[u64]>,
    pub(crate) ja4_not_in: Box<[u64]>,
}

/// Structure-of-Arrays compiled & fused rule set for branchless hot-path evaluation.
#[derive(Debug, Clone)]
pub(crate) struct CompiledRuleSet {
    pub(crate) bool_masks: Box<[u64]>,
    pub(crate) bool_vals: Box<[u64]>,
    pub(crate) group_allows: Box<[u64]>,
    pub(crate) ua_allows: Box<[u64]>,
    pub(crate) ua_mins: Box<[u16]>,
    pub(crate) ua_maxs: Box<[u16]>,
    pub(crate) hdr_masks: Box<[u64]>,
    pub(crate) hdr_vals: Box<[u64]>,
    pub(crate) ja4_indices: Box<[u16]>,
    pub(crate) scores: Box<[u8]>,
    pub(crate) ja4_matchers: Box<[Ja4SetMatcher]>,
    pub(crate) header_keys: Box<[HeaderKey]>,
    pub(crate) needs_ua: bool,
    pub(crate) has_ja4_matchers: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct IntermediateRule {
    bool_mask: u64,
    bool_val: u64,
    group_allow: u64,
    ua_allow: u64,
    ua_min: u16,
    ua_max: u16,
    hdr_mask: u64,
    hdr_val: u64,
    ja4_in: Vec<u64>,
    ja4_not_in: Vec<u64>,
    score: u8,
}

/// Validates and fuses preset + custom rules into contiguous SoA bitmasks at boot/reload time.
pub(crate) fn compile_ruleset(
    plugin_id: &str,
    cfg: &BotDetectionConfig,
) -> Result<CompiledRuleSet, PluginError> {
    let mut merged = preset_rules(cfg.preset);
    let mut seen_custom = std::collections::HashSet::with_capacity(cfg.rules.len());
    for custom in &cfg.rules {
        let trimmed = custom.id.trim();
        if trimmed.is_empty() {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}' (bot_detection): rule 'id' must not be empty"
            )));
        }
        if !seen_custom.insert(trimmed) {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}' (bot_detection): duplicate custom rule id '{trimmed}'"
            )));
        }
        if let Some(pos) = merged.iter().position(|r| r.id == trimmed) {
            merged[pos] = custom.clone();
            merged[pos].id = trimmed.to_string();
        } else {
            let mut c = custom.clone();
            c.id = trimmed.to_string();
            merged.push(c);
        }
    }

    if merged.is_empty() {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}' (bot_detection): requires at least one rule or non-'none' preset"
        )));
    }

    let mut header_names: Vec<String> = Vec::new();
    let mut header_keys: Vec<HeaderKey> = Vec::new();
    let mut intermediate: Vec<IntermediateRule> = Vec::with_capacity(merged.len());

    for rule in &merged {
        if rule.score == 0 || rule.score > 100 {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}' (bot_detection) rule '{}': 'score' must be in 1..=100",
                rule.id
            )));
        }

        let compiled = compile_single_rule(plugin_id, rule, &mut header_names, &mut header_keys)?;

        // Fuse identical predicate tuples into one entry with summed score.
        if let Some(existing) = intermediate.iter_mut().find(|r| {
            r.bool_mask == compiled.bool_mask
                && r.bool_val == compiled.bool_val
                && r.group_allow == compiled.group_allow
                && r.ua_allow == compiled.ua_allow
                && r.ua_min == compiled.ua_min
                && r.ua_max == compiled.ua_max
                && r.hdr_mask == compiled.hdr_mask
                && r.hdr_val == compiled.hdr_val
                && r.ja4_in == compiled.ja4_in
                && r.ja4_not_in == compiled.ja4_not_in
        }) {
            existing.score = existing.score.saturating_add(compiled.score).min(100);
        } else {
            intermediate.push(compiled);
        }
    }

    intermediate.sort_unstable_by_key(|a| std::cmp::Reverse(a.score));

    let n = intermediate.len();
    let mut bool_masks = Vec::with_capacity(n);
    let mut bool_vals = Vec::with_capacity(n);
    let mut group_allows = Vec::with_capacity(n);
    let mut ua_allows = Vec::with_capacity(n);
    let mut ua_mins = Vec::with_capacity(n);
    let mut ua_maxs = Vec::with_capacity(n);
    let mut hdr_masks = Vec::with_capacity(n);
    let mut hdr_vals = Vec::with_capacity(n);
    let mut ja4_indices = Vec::with_capacity(n);
    let mut scores = Vec::with_capacity(n);
    let mut ja4_matchers = Vec::new();
    let mut needs_ua = false;

    for r in intermediate {
        if r.ua_allow != ALL_UA_FAMILIES_MASK || r.ua_min > 0 || r.ua_max < u16::MAX {
            needs_ua = true;
        }
        bool_masks.push(r.bool_mask);
        bool_vals.push(r.bool_val);
        group_allows.push(r.group_allow);
        ua_allows.push(r.ua_allow);
        ua_mins.push(r.ua_min);
        ua_maxs.push(r.ua_max);
        hdr_masks.push(r.hdr_mask);
        hdr_vals.push(r.hdr_val);
        scores.push(r.score);
        if r.ja4_in.is_empty() && r.ja4_not_in.is_empty() {
            ja4_indices.push(u16::MAX);
        } else {
            let idx = ja4_matchers.len();
            if idx >= u16::MAX as usize {
                return Err(PluginError::Config(format!(
                    "Plugin '{plugin_id}' (bot_detection): too many JA4 set matchers"
                )));
            }
            ja4_matchers.push(Ja4SetMatcher {
                ja4_in: r.ja4_in.into_boxed_slice(),
                ja4_not_in: r.ja4_not_in.into_boxed_slice(),
            });
            ja4_indices.push(idx as u16);
        }
    }

    let has_ja4_matchers = !ja4_matchers.is_empty();
    Ok(CompiledRuleSet {
        bool_masks: bool_masks.into_boxed_slice(),
        bool_vals: bool_vals.into_boxed_slice(),
        group_allows: group_allows.into_boxed_slice(),
        ua_allows: ua_allows.into_boxed_slice(),
        ua_mins: ua_mins.into_boxed_slice(),
        ua_maxs: ua_maxs.into_boxed_slice(),
        hdr_masks: hdr_masks.into_boxed_slice(),
        hdr_vals: hdr_vals.into_boxed_slice(),
        ja4_indices: ja4_indices.into_boxed_slice(),
        scores: scores.into_boxed_slice(),
        ja4_matchers: ja4_matchers.into_boxed_slice(),
        header_keys: header_keys.into_boxed_slice(),
        needs_ua,
        has_ja4_matchers,
    })
}

fn compile_single_rule(
    plugin_id: &str,
    rule: &BotRuleConfig,
    header_names: &mut Vec<String>,
    header_keys: &mut Vec<HeaderKey>,
) -> Result<IntermediateRule, PluginError> {
    let w = &rule.when;
    let rid = &rule.id;

    let mut bool_mask = 0u64;
    let mut bool_val = 0u64;
    let mut push_bool = |bit: u64, opt: Option<bool>| {
        if let Some(v) = opt {
            bool_mask |= bit;
            if v {
                bool_val |= bit;
            }
        }
    };
    push_bool(BOOL_IS_TLS, w.is_tls);
    push_bool(BOOL_TLS_HAS_SNI, w.tls_has_sni);
    push_bool(BOOL_TLS_HAS_GREASE, w.tls_has_grease);
    push_bool(BOOL_TLS_HAS_CERT_COMPRESS, w.tls_has_cert_compress);
    push_bool(BOOL_TLS_HAS_ALPS, w.tls_has_alps);
    push_bool(BOOL_WIRE_ODD_SETTINGS, w.wire_odd_settings);
    push_bool(BOOL_WIRE_NO_RFC7540, w.wire_no_rfc7540_priorities);

    let mut group_allow = ALL_GROUPS_MASK;

    if let Some(list) = &w.http_version_in {
        let mut allowed = 0u64;
        for s in list {
            let slot = match s.trim().to_ascii_lowercase().as_str() {
                "http/1.1" | "http1" | "h1" => 1,
                "http/2" | "http2" | "h2" => 2,
                "http/3" | "http3" | "h3" => 3,
                "other" => 0,
                other => {
                    return Err(PluginError::Config(format!(
                        "Plugin '{plugin_id}' rule '{rid}': invalid http_version '{other}'"
                    )));
                }
            };
            allowed |= 1u64 << (GRP_HTTP_VER_SHIFT + slot);
        }
        group_allow = (group_allow & !ALL_HTTP_VER_BITS) | (group_allow & allowed);
    }

    if let Some(list) = &w.method_in {
        let mut allowed = 0u64;
        for s in list {
            let m = HttpMethod::from_bytes(s.trim().as_bytes());
            if m == HttpMethod::Unknown && !s.trim().eq_ignore_ascii_case("unknown") {
                return Err(PluginError::Config(format!(
                    "Plugin '{plugin_id}' rule '{rid}': invalid HTTP method '{s}'"
                )));
            }
            allowed |= 1u64 << (GRP_METHOD_SHIFT + (m as u32));
        }
        group_allow = (group_allow & !ALL_METHOD_BITS) | (group_allow & allowed);
    }

    if let Some(list) = &w.tls_version_in {
        let mut allowed = 0u64;
        for s in list {
            let slot = match s.trim().to_ascii_lowercase().as_str() {
                "none" => TLS_VER_NONE,
                "legacy" | "tls1.0" | "tls1.1" => TLS_VER_LEGACY,
                "tls1.2" | "1.2" => TLS_VER_12,
                "tls1.3" | "1.3" => TLS_VER_13,
                other => {
                    return Err(PluginError::Config(format!(
                        "Plugin '{plugin_id}' rule '{rid}': invalid tls_version '{other}'"
                    )));
                }
            };
            allowed |= 1u64 << (GRP_TLS_VER_SHIFT + (slot as u32));
        }
        group_allow = (group_allow & !ALL_TLS_VER_BITS) | (group_allow & allowed);
    }

    if let Some(list) = &w.tls_alpn_in {
        let mut allowed = 0u64;
        for s in list {
            let slot = parse_alpn_slot(plugin_id, rid, s)?;
            allowed |= 1u64 << (GRP_TLS_ALPN_SHIFT + slot);
        }
        group_allow = (group_allow & !ALL_TLS_ALPN_BITS) | (group_allow & allowed);
    }
    if let Some(not_alpn) = &w.tls_alpn_not {
        let slot = parse_alpn_slot(plugin_id, rid, not_alpn)?;
        group_allow &= !(1u64 << (GRP_TLS_ALPN_SHIFT + slot));
    }

    if let Some(list) = &w.tls_cipher_bucket_in {
        let mut allowed = 0u64;
        for &b in list {
            if b > 3 {
                return Err(PluginError::Config(format!(
                    "Plugin '{plugin_id}' rule '{rid}': tls_cipher_bucket '{b}' must be in 0..=3"
                )));
            }
            allowed |= 1u64 << (GRP_CIPHER_BKT_SHIFT + (b as u32));
        }
        group_allow = (group_allow & !ALL_CIPHER_BKT_BITS) | (group_allow & allowed);
    }

    if let Some(list) = &w.tls_ext_bucket_in {
        let mut allowed = 0u64;
        for &b in list {
            if b > 3 {
                return Err(PluginError::Config(format!(
                    "Plugin '{plugin_id}' rule '{rid}': tls_ext_bucket '{b}' must be in 0..=3"
                )));
            }
            allowed |= 1u64 << (GRP_EXT_BKT_SHIFT + (b as u32));
        }
        group_allow = (group_allow & !ALL_EXT_BKT_BITS) | (group_allow & allowed);
    }

    if let Some(list) = &w.wire_pseudo_order_in {
        let mut allowed = 0u64;
        for s in list {
            let slot = parse_pseudo_slot(plugin_id, rid, s)?;
            allowed |= 1u64 << (GRP_PSEUDO_SHIFT + slot);
        }
        group_allow = (group_allow & !ALL_PSEUDO_BITS) | (group_allow & allowed);
    }
    if let Some(list) = &w.wire_pseudo_order_not_in {
        for s in list {
            let slot = parse_pseudo_slot(plugin_id, rid, s)?;
            group_allow &= !(1u64 << (GRP_PSEUDO_SHIFT + slot));
        }
    }

    if let Some(list) = &w.wire_init_window_in {
        let mut allowed = 0u64;
        for s in list {
            let slot = match s.trim().to_ascii_lowercase().as_str() {
                "absent" => 0,
                "65535" => 1,
                "131072" | "128k" => 2,
                "1048576" | "1m" => 3,
                "2097152" | "2m" => 4,
                "4194304" | "4m" => 5,
                "6291456" | "6m" => 6,
                "16777216" | "33554432" | "16m" => 7,
                "odd" => 8,
                other => {
                    return Err(PluginError::Config(format!(
                        "Plugin '{plugin_id}' rule '{rid}': invalid wire_init_window '{other}'"
                    )));
                }
            };
            allowed |= 1u64 << (GRP_INIT_WIN_SHIFT + slot);
        }
        group_allow = (group_allow & !ALL_INIT_WIN_BITS) | (group_allow & allowed);
    }

    if let Some(list) = &w.wire_conn_window_in {
        let mut allowed = 0u64;
        for s in list {
            let slot = match s.trim().to_ascii_lowercase().as_str() {
                "absent" => 0,
                "small" => 1,
                "firefox" => 2,
                "chrome" => 3,
                "large" => 4,
                "odd" => 5,
                other => {
                    return Err(PluginError::Config(format!(
                        "Plugin '{plugin_id}' rule '{rid}': invalid wire_conn_window '{other}'"
                    )));
                }
            };
            allowed |= 1u64 << (GRP_CONN_WIN_SHIFT + slot);
        }
        group_allow = (group_allow & !ALL_CONN_WIN_BITS) | (group_allow & allowed);
    }

    for (name, bits) in [
        ("http_version_in", ALL_HTTP_VER_BITS),
        ("method_in", ALL_METHOD_BITS),
        ("tls_version_in", ALL_TLS_VER_BITS),
        ("tls_alpn", ALL_TLS_ALPN_BITS),
        ("tls_cipher_bucket_in", ALL_CIPHER_BKT_BITS),
        ("tls_ext_bucket_in", ALL_EXT_BKT_BITS),
        ("wire_pseudo_order", ALL_PSEUDO_BITS),
        ("wire_init_window_in", ALL_INIT_WIN_BITS),
        ("wire_conn_window_in", ALL_CONN_WIN_BITS),
    ] {
        if (group_allow & bits) == 0 {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}' rule '{rid}': contradictory/empty condition on '{name}'"
            )));
        }
    }

    let mut ua_allow = ALL_UA_FAMILIES_MASK;
    if let Some(list) = &w.ua_family_in {
        let mut allowed = 0u64;
        for s in list {
            let idx = parse_ua_family_idx(plugin_id, rid, s)?;
            allowed |= 1u64 << idx;
        }
        ua_allow &= allowed;
    }
    if let Some(list) = &w.ua_family_not_in {
        for s in list {
            let idx = parse_ua_family_idx(plugin_id, rid, s)?;
            ua_allow &= !(1u64 << idx);
        }
    }
    if ua_allow == 0 {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}' rule '{rid}': contradictory/empty UA family condition"
        )));
    }

    let ua_min = w.ua_min_major.unwrap_or(0);
    let ua_max = w.ua_max_major.unwrap_or(u16::MAX);
    if ua_min > ua_max {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}' rule '{rid}': ua_min_major ({ua_min}) > ua_max_major ({ua_max})"
        )));
    }

    let mut hdr_mask = 0u64;
    let mut hdr_val = 0u64;
    if let Some(present) = &w.headers_present {
        for h in present {
            let bit = register_header_bit(plugin_id, rid, h, header_names, header_keys)?;
            hdr_mask |= bit;
            hdr_val |= bit;
        }
    }
    if let Some(absent) = &w.headers_absent {
        for h in absent {
            let bit = register_header_bit(plugin_id, rid, h, header_names, header_keys)?;
            if (hdr_mask & bit) != 0 && (hdr_val & bit) != 0 {
                return Err(PluginError::Config(format!(
                    "Plugin '{plugin_id}' rule '{rid}': header '{h}' cannot be both present and absent"
                )));
            }
            hdr_mask |= bit;
        }
    }

    let mut ja4_in = compile_ja4_list(plugin_id, rid, w.ja4_in.as_deref())?;
    let ja4_not_in = compile_ja4_list(plugin_id, rid, w.ja4_not_in.as_deref())?;
    ja4_in.retain(|v| ja4_not_in.binary_search(v).is_err());
    if w.ja4_in.is_some() && ja4_in.is_empty() {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}' rule '{rid}': contradictory/empty ja4_in list"
        )));
    }

    Ok(IntermediateRule {
        bool_mask,
        bool_val,
        group_allow,
        ua_allow,
        ua_min,
        ua_max,
        hdr_mask,
        hdr_val,
        ja4_in,
        ja4_not_in,
        score: rule.score,
    })
}

fn compile_ja4_list(
    plugin_id: &str,
    rid: &str,
    raw: Option<&[String]>,
) -> Result<Vec<u64>, PluginError> {
    let Some(list) = raw else {
        return Ok(Vec::new());
    };
    if list.is_empty() {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}' rule '{rid}': JA4 list cannot be empty (use null instead)"
        )));
    }
    let mut out = Vec::with_capacity(list.len());
    for entry in list {
        let id = parse_ja4_entry(entry)
            .map_err(|e| PluginError::Config(format!("Plugin '{plugin_id}' rule '{rid}': {e}")))?;
        out.push(id);
    }
    out.sort_unstable();
    out.dedup();
    Ok(out)
}

fn parse_alpn_slot(plugin_id: &str, rid: &str, s: &str) -> Result<u32, PluginError> {
    match s.trim().to_ascii_lowercase().as_str() {
        "none" => Ok(TLS_ALPN_NONE as u32),
        "http/1.1" | "h1" => Ok(TLS_ALPN_H1 as u32),
        "h2" | "http/2" => Ok(TLS_ALPN_H2 as u32),
        "h3" | "http/3" => Ok(TLS_ALPN_H3 as u32),
        "other" => Ok(TLS_ALPN_OTHER as u32),
        other => Err(PluginError::Config(format!(
            "Plugin '{plugin_id}' rule '{rid}': invalid tls_alpn '{other}'"
        ))),
    }
}

fn parse_pseudo_slot(plugin_id: &str, rid: &str, s: &str) -> Result<u32, PluginError> {
    match s.trim().to_ascii_lowercase().as_str() {
        "masp" => Ok(0),
        "mpas" => Ok(1),
        "mspa" => Ok(2),
        "msap" => Ok(3),
        "maps" => Ok(4),
        "other" | "none" => Ok(5),
        other => Err(PluginError::Config(format!(
            "Plugin '{plugin_id}' rule '{rid}': invalid wire_pseudo_order '{other}'"
        ))),
    }
}

fn parse_ua_family_idx(plugin_id: &str, rid: &str, raw: &str) -> Result<u32, PluginError> {
    let needle = raw.trim();
    for (idx, &canon) in UA_FAMILY_NAMES.iter().enumerate() {
        if canon.eq_ignore_ascii_case(needle) {
            return Ok(idx as u32);
        }
    }
    let norm: String = needle
        .chars()
        .filter(|c| *c != '-' && *c != '_')
        .map(|c| c.to_ascii_lowercase())
        .collect();
    for (idx, &canon) in UA_FAMILY_NAMES.iter().enumerate() {
        let canon_norm: String = canon.chars().filter(|c| *c != '-').collect();
        if canon_norm == norm {
            return Ok(idx as u32);
        }
    }
    Err(PluginError::Config(format!(
        "Plugin '{plugin_id}' rule '{rid}': unknown ua_family '{needle}'"
    )))
}

fn register_header_bit(
    plugin_id: &str,
    rid: &str,
    raw_name: &str,
    header_names: &mut Vec<String>,
    header_keys: &mut Vec<HeaderKey>,
) -> Result<u64, PluginError> {
    let lower = raw_name.trim().to_ascii_lowercase();
    if lower.is_empty() {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}' rule '{rid}': empty header name"
        )));
    }
    let hname = http::header::HeaderName::from_bytes(lower.as_bytes()).map_err(|_| {
        PluginError::Config(format!(
            "Plugin '{plugin_id}' rule '{rid}': invalid header name '{raw_name}'"
        ))
    })?;
    if let Some(pos) = header_names.iter().position(|n| *n == lower) {
        return Ok(1u64 << pos);
    }
    if header_names.len() >= 64 {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}' (bot_detection): ruleset references more than 64 unique headers"
        )));
    }
    let idx = header_names.len();
    header_names.push(lower);
    header_keys.push(HeaderKey::from_name(hname));
    Ok(1u64 << idx)
}
