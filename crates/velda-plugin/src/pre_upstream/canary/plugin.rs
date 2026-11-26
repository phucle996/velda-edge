//! Canary Traffic Splitting Pre-Upstream Plugin (`canary::plugin`).
//!
//! ```text
//! Core Upstreams Loaded ──► CanaryPlugin::from_config(cfg, upstream_ids) ──► canary_upstream_id: u16
//!
//! on_upstream_request(req, target_upstream)  [AccessLevel::Read, 0 Header Mutations, 0 Fallback]
//!   ├─► 1. Header Rules (`rules` against HeaderWorkspace)              -> force canary
//!   └─► 2. Multi-Header SWAR Hash (`hash_headers` all required)
//!         ├─ None (any configured hash_header missing/empty)           -> keep baseline
//!         └─ Some(bucket) -> ((hash64 as u128 * 1000) >> 64) < ratio
//!               ├── true  ─► *target_upstream = canary_upstream_id     -> Continue
//!               └── false ─► keep default baseline upstream            -> Continue
//! ```

use http::HeaderName;
use velda_core::{Action, L7Request};

use super::config::{CanaryConfig, HeaderRule, RuleOperator, schema_to_config};
use super::hash::evaluate_permille;
use super::validation::validate_canary_config;
use crate::capability::{AccessLevel, PluginCapability, SupportedProtocols};
use crate::config::PluginConfig;
use crate::error::PluginError;
use crate::pre_upstream::PreUpstreamPlugin;
use crate::pre_upstream::targets::target_set_primary;
#[cfg(test)]
use crate::pre_upstream::targets::{target_init, target_mirror, target_primary};

/// Lean, header-only, read-only Canary Traffic Splitting plugin.
#[derive(Debug)]
pub struct CanaryPlugin {
    pub id: String,
    pub canary_upstream_id: u16,
    /// Traffic share routed to canary in permille (`1..=999`).
    pub ratio: u32,
    pub rules: Box<[HeaderRule]>,
    pub hash_headers: Box<[HeaderName]>,
}

impl CanaryPlugin {
    /// Constructs a canary plugin with an already-resolved dense upstream index (`u16`).
    pub fn new(
        id: impl Into<String>,
        config: CanaryConfig,
        canary_upstream_id: u16,
    ) -> Result<Self, PluginError> {
        if config.canary_upstream.trim().is_empty() {
            return Err(PluginError::Config(
                "canary_upstream cannot be empty".to_string(),
            ));
        }
        if config.ratio == 0 || config.ratio >= 1000 {
            return Err(PluginError::Config(format!(
                "canary ratio ({}) must be in 1..=999",
                config.ratio
            )));
        }
        if config.hash_headers.is_empty() {
            return Err(PluginError::Config(
                "canary hash_headers cannot be empty".to_string(),
            ));
        }

        Ok(Self {
            id: id.into(),
            canary_upstream_id,
            ratio: config.ratio,
            rules: config.rules.into_boxed_slice(),
            hash_headers: config.hash_headers.into_boxed_slice(),
        })
    }

    /// Parses, validates, and resolves `"canary_upstream"` directly against pre-loaded `upstream_ids`.
    pub fn from_config(config: &PluginConfig, upstream_ids: &[&str]) -> Result<Self, PluginError> {
        let schema = validate_canary_config(&config.id, &config.config)?;
        let cfg = schema_to_config(schema)?;
        let pos = upstream_ids
            .iter()
            .position(|u| *u == cfg.canary_upstream)
            .ok_or_else(|| {
                PluginError::Config(format!(
                    "Plugin '{}' targets canary_upstream '{}', which does not exist in upstreams",
                    config.id, cfg.canary_upstream
                ))
            })?;
        let dense_id = u16::try_from(pos).map_err(|_| {
            PluginError::Config(format!(
                "Upstream index for '{}' exceeds u16::MAX",
                cfg.canary_upstream
            ))
        })?;
        Self::new(&config.id, cfg, dense_id)
    }

    /// Evaluates whether the request should route to the canary upstream.
    #[inline]
    pub fn should_route_to_canary(&self, req: &L7Request) -> bool {
        for rule in &self.rules {
            if rule_matches(req, rule) {
                return true;
            }
        }
        match evaluate_permille(req, &self.hash_headers) {
            Some(bucket) => bucket < self.ratio,
            None => false,
        }
    }
}

#[inline]
fn rule_matches(req: &L7Request, rule: &HeaderRule) -> bool {
    let needle = &rule.pattern;
    for val in req.headers.get_all(&rule.header) {
        let b = val.as_bytes();
        match rule.operator {
            RuleOperator::Exists => return true,
            RuleOperator::Equals => {
                if b.eq_ignore_ascii_case(needle) {
                    return true;
                }
            }
            RuleOperator::Prefix => {
                if let Some(head) = b.get(..needle.len())
                    && head.eq_ignore_ascii_case(needle)
                {
                    return true;
                }
            }
            RuleOperator::Suffix => {
                if let Some(start) = b.len().checked_sub(needle.len())
                    && let Some(tail) = b.get(start..)
                    && tail.eq_ignore_ascii_case(needle)
                {
                    return true;
                }
            }
            RuleOperator::Contains => {
                if !needle.is_empty()
                    && let Some(limit) = b.len().checked_sub(needle.len())
                {
                    for start in 0..=limit {
                        if let Some(win) = b.get(start..start + needle.len())
                            && win.eq_ignore_ascii_case(needle)
                        {
                            return true;
                        }
                    }
                }
            }
        }
    }
    false
}

impl PreUpstreamPlugin for CanaryPlugin {
    #[inline(always)]
    fn id(&self) -> &str {
        &self.id
    }

    #[inline(always)]
    fn pbww(&self) -> u32 {
        0
    }

    #[inline]
    fn on_upstream_request(&self, req: &mut L7Request, targets: &mut u32) -> Action {
        if self.should_route_to_canary(req) {
            target_set_primary(targets, self.canary_upstream_id);
        }
        Action::Continue
    }

    #[inline(always)]
    fn capability(&self) -> PluginCapability {
        PluginCapability::header_only(AccessLevel::Read)
    }

    #[inline(always)]
    fn supported_protocols(&self) -> SupportedProtocols {
        SupportedProtocols::ALL_L7
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{HeaderValue, Method, Uri, Version};
    use velda_core::{Body, HeaderWorkspace};

    #[test]
    fn test_canary_header_rules_and_missing_header_baseline() {
        let config = CanaryConfig {
            canary_upstream: "payments-v2".into(),
            ratio: 500,
            rules: vec![
                HeaderRule {
                    header: HeaderName::from_static("x-canary"),
                    operator: RuleOperator::Equals,
                    pattern: (*b"always").into(),
                },
                HeaderRule {
                    header: HeaderName::from_static("x-user-email"),
                    operator: RuleOperator::Suffix,
                    pattern: (*b"@velda.io").into(),
                },
                HeaderRule {
                    header: HeaderName::from_static("x-client-version"),
                    operator: RuleOperator::Prefix,
                    pattern: (*b"v2-").into(),
                },
                HeaderRule {
                    header: HeaderName::from_static("user-agent"),
                    operator: RuleOperator::Contains,
                    pattern: (*b"mobile").into(),
                },
                HeaderRule {
                    header: HeaderName::from_static("x-debug"),
                    operator: RuleOperator::Exists,
                    pattern: Box::new([]),
                },
            ],
            hash_headers: vec![
                HeaderName::from_static("x-tenant-id"),
                HeaderName::from_static("x-user-id"),
            ],
        };

        let plugin = CanaryPlugin::new("canary-test", config, 2).unwrap();
        assert_eq!(
            plugin.capability(),
            PluginCapability::header_only(AccessLevel::Read)
        );

        let make_req = |pairs: &[(&'static str, &'static str)]| {
            let mut h = HeaderWorkspace::empty(4);
            for &(k, v) in pairs {
                h.insert(k, HeaderValue::from_static(v));
            }
            L7Request::new(
                Method::GET,
                Uri::from_static("/v1/pay"),
                Version::HTTP_11,
                h,
                Body::Empty,
            )
        };

        // Rules force canary even when hash_headers are absent
        for (hdr, val) in [
            ("x-canary", "ALWAYS"),
            ("x-user-email", "dev@velda.io"),
            ("x-client-version", "v2-beta"),
            ("user-agent", "Mozilla-Mobile-Safari"),
            ("x-debug", "1"),
        ] {
            let mut req = make_req(&[(hdr, val)]);
            let mut target = target_init(0);
            plugin.on_upstream_request(&mut req, &mut target);
            assert_eq!(target_primary(target), Some(2), "failed rule {hdr}={val}");
            assert_eq!(target_mirror(target), None);
        }

        // Missing or partial hash_headers -> always stays safely on baseline (0), zero PRNG fallback
        for _ in 0..32 {
            let mut no_hdrs = make_req(&[]);
            let mut t1 = target_init(0);
            plugin.on_upstream_request(&mut no_hdrs, &mut t1);
            assert_eq!(target_primary(t1), Some(0));

            let mut partial = make_req(&[("x-tenant-id", "acme")]);
            let mut t2 = target_init(0);
            plugin.on_upstream_request(&mut partial, &mut t2);
            assert_eq!(target_primary(t2), Some(0));

            let mut blank_user = make_req(&[("x-tenant-id", "acme"), ("x-user-id", "   ")]);
            let mut t3 = target_init(0);
            plugin.on_upstream_request(&mut blank_user, &mut t3);
            assert_eq!(target_primary(t3), Some(0));
        }

        // Unmatched rule & missing headers preserves original headers verbatim
        let mut base = make_req(&[("x-canary", "never")]);
        let mut target_base = target_init(0);
        plugin.on_upstream_request(&mut base, &mut target_base);
        assert_eq!(target_primary(target_base), Some(0));
        assert_eq!(base.headers.get("x-canary").unwrap(), "never");
    }

    #[test]
    fn test_canary_multi_header_permille_and_monotonic_ratio() {
        let config_low = CanaryConfig {
            canary_upstream: "canary-v2".into(),
            ratio: 125, // 125/1000 = 12.5%
            rules: Vec::new(),
            hash_headers: vec![
                HeaderName::from_static("x-tenant-id"),
                HeaderName::from_static("x-user-id"),
            ],
        };
        let config_high = CanaryConfig {
            canary_upstream: "canary-v2".into(),
            ratio: 350, // Increasing threshold must preserve every canary user from ratio=125
            rules: Vec::new(),
            hash_headers: config_low.hash_headers.clone(),
        };

        let plugin_low = CanaryPlugin::new("canary-low", config_low, 1).unwrap();
        let plugin_high = CanaryPlugin::new("canary-high", config_high, 1).unwrap();

        let mut canary_hits = 0u64;
        let total = 20_000u64;
        let mut buf = itoa::Buffer::new();
        for uid in 1_000_000_000u64..(1_000_000_000u64 + total) {
            let mut h = HeaderWorkspace::empty(4);
            h.insert("x-tenant-id", HeaderValue::from_static("tenant-prod"));
            h.insert("x-user-id", HeaderValue::from_str(buf.format(uid)).unwrap());
            let mut req = L7Request::new(
                Method::GET,
                Uri::from_static("/"),
                Version::HTTP_11,
                h,
                Body::Empty,
            );
            let mut t_low = target_init(0);
            plugin_low.on_upstream_request(&mut req, &mut t_low);
            let mut t_high = target_init(0);
            plugin_high.on_upstream_request(&mut req, &mut t_high);

            if target_primary(t_low) == Some(1) {
                canary_hits += 1;
                assert_eq!(
                    target_primary(t_high),
                    Some(1),
                    "increasing ratio dropped previously canary user {uid}"
                );
            }
        }
        let observed_permille = (canary_hits as f64 / total as f64) * 1000.0;
        assert!(
            (observed_permille - 125.0).abs() < 15.0,
            "expected ~125/1000, got {observed_permille:.1}"
        );
    }

    #[test]
    fn test_canary_validation_rejects_boundaries_and_resolves_upstream_id() {
        for bad_ratio in [0u32, 1000u32, 1001u32] {
            assert!(
                validate_canary_config(
                    "c_bad_ratio",
                    &serde_json::json!({
                        "canary_upstream": "v2",
                        "ratio": bad_ratio,
                        "rules": [],
                        "hash_headers": ["x-user-id"]
                    })
                )
                .is_err(),
                "ratio={bad_ratio} must be rejected"
            );
        }

        // Empty hash_headers must be rejected by sync validation
        assert!(
            validate_canary_config(
                "c_empty_headers",
                &serde_json::json!({
                    "canary_upstream": "v2",
                    "ratio": 200,
                    "rules": [],
                    "hash_headers": []
                })
            )
            .is_err()
        );

        // Legacy extra fields like hash_strategy must be rejected strictly
        assert!(
            validate_canary_config(
                "c_legacy_field",
                &serde_json::json!({
                    "canary_upstream": "v2",
                    "ratio": 200,
                    "rules": [],
                    "hash_strategy": "priority",
                    "hash_headers": ["x-user-id"]
                })
            )
            .is_err()
        );

        // Whitespace rule pattern for Equals must be rejected
        assert!(
            validate_canary_config(
                "c_ws",
                &serde_json::json!({
                    "canary_upstream": "v2",
                    "ratio": 50,
                    "rules": [{ "name": "x-env", "operator": "equals", "value": "   " }],
                    "hash_headers": ["x-user-id"]
                })
            )
            .is_err()
        );

        let cfg = PluginConfig {
            id: "canary_main".into(),
            config: serde_json::json!({
                "canary_upstream": "orders-v2",
                "ratio": 999,
                "rules": [{ "name": "x-canary", "operator": "equals", "value": "true" }],
                "hash_headers": ["x-tenant-id", "cookie"]
            }),
        };

        // Missing target upstream is rejected directly at construction
        assert!(CanaryPlugin::from_config(&cfg, &["orders-v1"]).is_err());

        // Present target resolves directly to plain dense u16 index (2)
        let plugin = CanaryPlugin::from_config(&cfg, &["orders-v1", "auth", "orders-v2"]).unwrap();
        assert_eq!(plugin.canary_upstream_id, 2);

        let mut req = L7Request::new(
            Method::GET,
            Uri::from_static("/orders"),
            Version::HTTP_11,
            {
                let mut h = HeaderWorkspace::empty(2);
                h.insert("x-canary", HeaderValue::from_static("true"));
                h
            },
            Body::Empty,
        );
        let mut target = target_init(0);
        plugin.on_upstream_request(&mut req, &mut target);
        assert_eq!(target_primary(target), Some(2));
        assert_eq!(target_mirror(target), None);
    }
}
