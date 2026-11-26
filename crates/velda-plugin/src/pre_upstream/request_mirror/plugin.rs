//! Request Mirroring (Traffic Shadowing) Pre-Upstream Plugin (`request_mirror::plugin`).
//!
//! ```text
//!   Core Upstreams Loaded ──► RequestMirrorPlugin::from_config(cfg, upstream_ids)
//!                               └──► resolves mirror_upstream -> mirror_upstream_id: u16
//!
//!   on_upstream_request(req, targets: &mut u32)  [AccessLevel::Read, 0 IO, 0 Header/Body clone]
//!     ├─► 1. ignore_methods bitmask check (1 bitwise `&`)        -> Action::Continue (no mirror)
//!     ├─► 2. threshold == 0 / u64::MAX fast paths
//!     └─► 3. entropy(req.state.request_id) < threshold_u64 (1 CPU `CMP` on SplitMix64 bits)
//!           ├── true  ──► target_set_mirror(targets, mirror_upstream_id) -> Action::Continue
//!           └── false ──► leave mirror lane 0xFFFF                     -> Action::Continue
//! ```
//!
//! # Architectural Scope Invariant
//! Strictly a pure traffic-decision plugin: it does NOT touch sockets, does NOT mutate
//! `HeaderWorkspace`, and does NOT clone payloads. `velda-edge` pipelines read the upper
//! 16 bits of `targets` at Phase 5 (once headers and buffered body are finalized) to clone and
//! dispatch the mirror copy asynchronously via `velda-shadow-{i}`.

use std::cell::Cell;

use velda_core::{Action, L7Request};

use super::config::{MethodMask, RequestMirrorConfig, compute_threshold_u64};
use super::validation::validate_request_mirror_config;
use crate::capability::{AccessLevel, PluginCapability, SupportedProtocols};
use crate::config::PluginConfig;
use crate::error::PluginError;
use crate::pre_upstream::PreUpstreamPlugin;
use crate::pre_upstream::targets::target_set_mirror;

thread_local! {
    /// Fallback SplitMix64 state used ONLY when `req.state.request_id` is 0 (e.g. unit tests).
    static TEST_ENTROPY_STATE: Cell<u64> = const { Cell::new(0x9E37_79B9_7F4A_7C15) };
}

#[cold]
#[inline(never)]
fn fallback_entropy_u64() -> u64 {
    TEST_ENTROPY_STATE.with(|c| {
        let mut x = c.get().wrapping_add(0x9E37_79B9_7F4A_7C15);
        c.set(x);
        x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        x ^ (x >> 31)
    })
}

/// Pure traffic-control plugin setting the upper 16 mirror bits of the `u32` upstream target word.
#[derive(Debug, Clone)]
pub struct RequestMirrorPlugin {
    pub id: String,
    pub config: RequestMirrorConfig,
    pub mirror_upstream_id: u16,
    threshold_u64: u64,
    ignore_mask: MethodMask,
}

impl RequestMirrorPlugin {
    /// Constructs a mirror plugin with an already-resolved dense upstream index (`u16`).
    pub fn new(
        id: impl Into<String>,
        config: RequestMirrorConfig,
        mirror_upstream_id: u16,
    ) -> Result<Self, PluginError> {
        if config.mirror_upstream.trim().is_empty() {
            return Err(PluginError::Config(
                "mirror_upstream cannot be empty".to_string(),
            ));
        }
        if mirror_upstream_id == crate::pre_upstream::targets::TARGET_UNASSIGNED {
            return Err(PluginError::Config(
                "mirror_upstream_id cannot be 0xFFFF (UNASSIGNED)".to_string(),
            ));
        }
        if !(0.0..=100.0).contains(&config.sample_percent) || config.sample_percent.is_nan() {
            return Err(PluginError::Config(format!(
                "sample_percent ({}) must be in 0.0..=100.0",
                config.sample_percent
            )));
        }

        let threshold_u64 = compute_threshold_u64(config.sample_percent);
        let ignore_mask = MethodMask::from_methods(&config.ignore_methods);

        Ok(Self {
            id: id.into(),
            config,
            mirror_upstream_id,
            threshold_u64,
            ignore_mask,
        })
    }

    /// Parses, validates, and resolves `"mirror_upstream"` directly against pre-loaded `upstream_ids`.
    pub fn from_config(config: &PluginConfig, upstream_ids: &[&str]) -> Result<Self, PluginError> {
        let cfg = validate_request_mirror_config(&config.id, &config.config).map_err(|e| {
            PluginError::Config(format!(
                "Invalid request_mirror configuration for '{}': {e}",
                config.id
            ))
        })?;
        let trimmed = cfg.mirror_upstream.trim();
        let pos = upstream_ids
            .iter()
            .position(|u| *u == trimmed)
            .ok_or_else(|| {
                PluginError::Config(format!(
                    "Plugin '{}' targets mirror_upstream '{}', which does not exist in upstreams",
                    config.id, cfg.mirror_upstream
                ))
            })?;
        let dense_id = u16::try_from(pos).map_err(|_| {
            PluginError::Config(format!(
                "Upstream index for '{}' exceeds u16::MAX",
                cfg.mirror_upstream
            ))
        })?;
        Self::new(&config.id, cfg, dense_id)
    }

    /// Evaluates whether the request should be mirrored using pre-generated SplitMix64 `request_id` entropy.
    #[inline(always)]
    pub fn should_mirror(&self, req: &L7Request) -> bool {
        if self.threshold_u64 == 0 || self.ignore_mask.matches(&req.method) {
            return false;
        }
        if self.threshold_u64 == u64::MAX {
            return true;
        }
        let entropy = velda_core::request_id::entropy(req.state.request_id);
        let sample_val = if entropy != 0 {
            entropy
        } else {
            fallback_entropy_u64()
        };
        sample_val < self.threshold_u64
    }
}

impl PreUpstreamPlugin for RequestMirrorPlugin {
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
        if self.should_mirror(req) {
            target_set_mirror(targets, self.mirror_upstream_id);
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
    use crate::pre_upstream::targets::{target_init, target_mirror, target_primary};
    use http::{HeaderValue, Method, Uri, Version};
    use velda_core::{Body, HeaderWorkspace};

    fn make_req(method: Method, request_id: u128) -> L7Request {
        let mut h = HeaderWorkspace::empty(2);
        h.insert("host", HeaderValue::from_static("api.prod.internal"));
        let mut req = L7Request::new(
            method,
            Uri::from_static("/api/v1/orders"),
            Version::HTTP_11,
            h,
            Body::Bytes(bytes::Bytes::from_static(br#"{"item":"cpu"}"#)),
        );
        req.state.request_id = request_id;
        req
    }

    #[test]
    fn test_mirror_sets_upper_u16_without_mutating_headers_or_primary() {
        let cfg = RequestMirrorConfig {
            mirror_upstream: "orders-staging".into(),
            sample_percent: 100.0,
            ignore_methods: vec![],
        };
        let plugin = RequestMirrorPlugin::new("mirror", cfg, 4).unwrap();
        assert_eq!(
            plugin.capability(),
            PluginCapability::header_only(AccessLevel::Read)
        );

        let mut req = make_req(Method::POST, 0x1111_2222_3333_4444);
        let mut targets = target_init(1);
        let action = plugin.on_upstream_request(&mut req, &mut targets);

        assert!(matches!(action, Action::Continue));
        assert_eq!(target_primary(targets), Some(1));
        assert_eq!(target_mirror(targets), Some(4));
        assert!(!req.headers.contains_key("x-velda-mirror"));
    }

    #[test]
    fn test_mirror_ignores_methods_via_bitmask() {
        let cfg = RequestMirrorConfig {
            mirror_upstream: "orders-staging".into(),
            sample_percent: 100.0,
            ignore_methods: vec!["DELETE".into(), "PUT".into()],
        };
        let plugin = RequestMirrorPlugin::new("mirror", cfg, 2).unwrap();

        let mut req_del = make_req(Method::DELETE, 99);
        let mut t_del = target_init(0);
        plugin.on_upstream_request(&mut req_del, &mut t_del);
        assert_eq!(target_primary(t_del), Some(0));
        assert_eq!(target_mirror(t_del), None);

        let mut req_post = make_req(Method::POST, 99);
        let mut t_post = target_init(0);
        plugin.on_upstream_request(&mut req_post, &mut t_post);
        assert_eq!(target_mirror(t_post), Some(2));
    }

    #[test]
    fn test_mirror_fractional_sampling_accuracy_on_request_id_entropy() {
        let cfg = RequestMirrorConfig {
            mirror_upstream: "orders-staging".into(),
            sample_percent: 5.0,
            ignore_methods: vec![],
        };
        let plugin = RequestMirrorPlugin::new("mirror", cfg, 3).unwrap();

        let mut mirrored = 0usize;
        let total = 20_000usize;
        // Deterministic SplitMix64 sequence matching core request_id entropy properties
        let mut state: u64 = 0x1234_5678_9ABC_DEF0;
        for i in 0..total {
            state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = (state ^ (state >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            let entropy = z ^ (z >> 31);
            let req_id = ((i as u128) << 64) | (entropy as u128).max(1);

            let mut req = make_req(Method::GET, req_id);
            let mut targets = target_init(0);
            plugin.on_upstream_request(&mut req, &mut targets);
            if target_mirror(targets) == Some(3) {
                mirrored += 1;
            }
        }

        // 5% of 20,000 = 1,000 (allow reasonable statistical margin [850..=1150])
        assert!(
            (850..=1150).contains(&mirrored),
            "Expected ~1000 mirrored out of {total}, got {mirrored}"
        );
    }

    #[test]
    fn test_mirror_from_config_resolves_dense_upstream_id() {
        let cfg = PluginConfig {
            id: "request_mirror".into(),
            config: serde_json::json!({
                "mirror_upstream": "orders-staging",
                "sample_percent": 12.5,
                "ignore_methods": ["HEAD"]
            }),
        };
        let plugin = RequestMirrorPlugin::from_config(&cfg, &["orders-prod", "orders-staging"])
            .expect("should resolve upstream index 1");
        assert_eq!(plugin.mirror_upstream_id, 1);

        let err = RequestMirrorPlugin::from_config(&cfg, &["orders-prod"]);
        assert!(err.is_err());
    }
}
