//! Canary Traffic Splitting Pre-Upstream Plugin (`canary::plugin`).
//!
//! ```text
//! on_upstream_request(req, target_upstream)  [AccessLevel::Read, 0 Header Mutations, 0 Fallback]
//!   ├─► 1. Header Rules (`rules` against HeaderWorkspace)              -> force canary
//!   └─► 2. Multi-Header SWAR Hash (`hash_headers` all required)
//!         ├─ None (any configured hash_header missing/empty)           -> keep baseline
//!         └─ Some(bucket) -> ((hash64 as u128 * 1000) >> 64) < ratio
//!               ├── true  ─► *target_upstream = canary_upstream_id     -> Continue
//!               └── false ─► keep default baseline upstream            -> Continue
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicU16, Ordering};

use http::HeaderName;
use velda_core::{Action, L7Request, UNASSIGNED_UPSTREAM_ID};

use super::config::{CanaryConfig, HeaderRule, RuleOperator, schema_to_config};
use super::hash::evaluate_permille;
use super::validation::validate_canary_config;
use crate::capability::{AccessLevel, PluginCapability, SupportedProtocols};
use crate::config::PluginConfig;
use crate::error::PluginError;
use crate::pre_upstream::PreUpstreamPlugin;

/// Lean, header-only, read-only Canary Traffic Splitting plugin.
pub struct CanaryPlugin {
    pub id: String,
    pub canary_upstream: Arc<str>,
    pub canary_upstream_id: AtomicU16,
    /// Traffic share routed to canary in permille (`1..=999`).
    pub ratio: u32,
    pub rules: Box<[HeaderRule]>,
    pub hash_headers: Box<[HeaderName]>,
}

impl std::fmt::Debug for CanaryPlugin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CanaryPlugin")
            .field("id", &self.id)
            .field("canary_upstream", &self.canary_upstream)
            .field(
                "canary_upstream_id",
                &self.canary_upstream_id.load(Ordering::Relaxed),
            )
            .field("ratio", &self.ratio)
            .field("hash_headers", &self.hash_headers)
            .finish()
    }
}

impl CanaryPlugin {
    /// Constructs a new canary plugin from typed configuration.
    pub fn new(id: impl Into<String>, config: CanaryConfig) -> Result<Self, PluginError> {
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
            canary_upstream: Arc::from(config.canary_upstream.trim()),
            canary_upstream_id: AtomicU16::new(UNASSIGNED_UPSTREAM_ID),
            ratio: config.ratio,
            rules: config.rules.into_boxed_slice(),
            hash_headers: config.hash_headers.into_boxed_slice(),
        })
    }

    /// Parses, validates, and constructs a [`CanaryPlugin`] from control-plane JSON.
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let schema = validate_canary_config(&config.id, &config.config)?;
        let cfg = schema_to_config(schema)?;
        Self::new(&config.id, cfg)
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

    #[inline(always)]
    fn canary_upstream_target(&self) -> Option<&str> {
        Some(self.canary_upstream.as_ref())
    }

    #[inline(always)]
    fn attach_canary_upstream(&self, upstream_id: u16) {
        self.canary_upstream_id
            .store(upstream_id, Ordering::Relaxed);
    }

    #[inline]
    fn on_upstream_request(&self, req: &mut L7Request, target_upstream: &mut u16) -> Action {
        if self.should_route_to_canary(req) {
            let cid = self.canary_upstream_id.load(Ordering::Relaxed);
            if cid != UNASSIGNED_UPSTREAM_ID {
                *target_upstream = cid;
            }
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

        let plugin = CanaryPlugin::new("canary-test", config).unwrap();
        assert_eq!(
            plugin.capability(),
            PluginCapability::header_only(AccessLevel::Read)
        );
        plugin.attach_canary_upstream(2);

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
            let mut target = 0;
            plugin.on_upstream_request(&mut req, &mut target);
            assert_eq!(target, 2, "failed rule {hdr}={val}");
        }

        // Missing or partial hash_headers -> always stays safely on baseline (0), zero PRNG fallback
        for _ in 0..32 {
            let mut no_hdrs = make_req(&[]);
            let mut t1 = 0;
            plugin.on_upstream_request(&mut no_hdrs, &mut t1);
            assert_eq!(t1, 0);

            let mut partial = make_req(&[("x-tenant-id", "acme")]);
            let mut t2 = 0;
            plugin.on_upstream_request(&mut partial, &mut t2);
            assert_eq!(t2, 0);

            let mut blank_user = make_req(&[("x-tenant-id", "acme"), ("x-user-id", "   ")]);
            let mut t3 = 0;
            plugin.on_upstream_request(&mut blank_user, &mut t3);
            assert_eq!(t3, 0);
        }

        // Unmatched rule & missing headers preserves original headers verbatim
        let mut base = make_req(&[("x-canary", "never")]);
        let mut target_base = 0;
        plugin.on_upstream_request(&mut base, &mut target_base);
        assert_eq!(target_base, 0);
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

        let plugin_low = CanaryPlugin::new("canary-low", config_low).unwrap();
        let plugin_high = CanaryPlugin::new("canary-high", config_high).unwrap();
        plugin_low.attach_canary_upstream(1);
        plugin_high.attach_canary_upstream(1);

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
            let mut t_low = 0;
            plugin_low.on_upstream_request(&mut req, &mut t_low);
            let mut t_high = 0;
            plugin_high.on_upstream_request(&mut req, &mut t_high);

            if t_low == 1 {
                canary_hits += 1;
                assert_eq!(
                    t_high, 1,
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
    fn test_canary_validation_rejects_boundaries_and_wires_upstream() {
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

        let plugin = Arc::new(
            CanaryPlugin::from_config(&PluginConfig {
                id: "canary_main".into(),
                config: serde_json::json!({
                    "canary_upstream": "orders-v2",
                    "ratio": 999,
                    "rules": [{ "name": "x-canary", "operator": "equals", "value": "true" }],
                    "hash_headers": ["x-tenant-id", "cookie"]
                }),
            })
            .unwrap(),
        );

        let mut available = rustc_hash::FxHashMap::default();
        available.insert(
            "canary_main".to_string(),
            crate::LoadedPlugin::PreUpstream(Arc::clone(&plugin) as Arc<dyn PreUpstreamPlugin>),
        );
        crate::wire_plugin_upstreams(&available, &["orders-v1", "auth", "orders-v2"]).unwrap();

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
        let mut target = 0;
        plugin.on_upstream_request(&mut req, &mut target);
        assert_eq!(target, 2);
        assert!(crate::wire_plugin_upstreams(&available, &["orders-v1"]).is_err());
    }
}
