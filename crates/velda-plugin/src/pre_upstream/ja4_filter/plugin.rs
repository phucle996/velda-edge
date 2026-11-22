//! `ja4_filter` Pre-Upstream Plugin (`phase: "pre_upstream"`).
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────────────────────┐
//! │                     JA4 FILTER HOT-PATH EXECUTION                                       │
//! ├─────────────────────────────────────────────────────────────────────────────────────────┤
//! │  Routed L7Request                                                                       │
//! │    │                                                                                    │
//! │    ▼ Read req.state.ja4 (u64 inside 64B L1d RequestState cache line)                    │
//! │    ├── ja4 == 0 (plaintext / unavailable) ───────────────► Action::Continue             │
//! │    └── ja4 != 0 ──► Scan/BSearch denied_ids: Box<[u64]>                                 │
//! │                       ├── Match    ──────────────────────► Action::Reject(Forbidden)    │
//! │                       └── No Match ──────────────────────► Action::Continue             │
//! └─────────────────────────────────────────────────────────────────────────────────────────┘
//! ```

use velda_core::{Action, L7Request, RejectReason};

use crate::capability::{AccessLevel, PluginCapability, SupportedProtocols};
use crate::config::PluginConfig;
use crate::error::PluginError;
use crate::pre_upstream::PreUpstreamPlugin;
use crate::pre_upstream::ja4_filter::config::Ja4FilterConfig;

/// Threshold below which linear scan outperforms binary search due to branch predictability.
const LINEAR_SCAN_THRESHOLD: usize = 16;

/// Pre-upstream plugin that rejects requests matching configured JA4 TLS fingerprints.
#[derive(Debug, Clone)]
pub struct Ja4FilterPlugin {
    id: String,
    denied_ids: Box<[u64]>,
}

impl Ja4FilterPlugin {
    /// Creates a new `Ja4FilterPlugin` directly from compiled 64-bit JA4 IDs.
    pub fn new(id: impl Into<String>, mut denied_ids: Vec<u64>) -> Self {
        denied_ids.retain(|&v| v != 0);
        denied_ids.sort_unstable();
        denied_ids.dedup();
        Self {
            id: id.into(),
            denied_ids: denied_ids.into_boxed_slice(),
        }
    }

    /// Instantiates `Ja4FilterPlugin` from a declarative [`PluginConfig`].
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let parsed: Ja4FilterConfig =
            serde_json::from_value(config.config.clone()).map_err(|e| {
                PluginError::Config(format!(
                    "Invalid configuration for plugin '{}' (ja4_filter): {e}",
                    config.id
                ))
            })?;
        let denied_ids = parsed.compile_deny_ids()?;
        if denied_ids.is_empty() {
            return Err(PluginError::Config(format!(
                "Plugin '{}' (ja4_filter) requires at least one entry in 'deny'",
                config.id
            )));
        }
        Ok(Self {
            id: config.id.clone(),
            denied_ids,
        })
    }

    #[inline(always)]
    fn is_denied(&self, ja4: u64) -> bool {
        if self.denied_ids.len() <= LINEAR_SCAN_THRESHOLD {
            self.denied_ids.contains(&ja4)
        } else {
            self.denied_ids.binary_search(&ja4).is_ok()
        }
    }
}

impl PreUpstreamPlugin for Ja4FilterPlugin {
    #[inline(always)]
    fn id(&self) -> &str {
        &self.id
    }

    #[inline]
    fn on_upstream_request(&self, req: &mut L7Request, _target_upstream: &mut u16) -> Action {
        let ja4 = req.state.ja4;
        if ja4 != 0 && self.is_denied(ja4) {
            return Action::Reject(RejectReason::Forbidden);
        }
        Action::Continue
    }

    #[inline(always)]
    fn capability(&self) -> PluginCapability {
        PluginCapability::new(AccessLevel::None, AccessLevel::None)
    }

    #[inline(always)]
    fn supported_protocols(&self) -> SupportedProtocols {
        SupportedProtocols::ALL_HTTP.union(SupportedProtocols::GRPC)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{HeaderMap, Method, Uri, Version};
    use serde_json::json;
    use velda_core::{Body, ja4_fingerprint_id};

    fn make_req(ja4: u64) -> L7Request {
        let mut req = L7Request::new(
            Method::GET,
            Uri::from_static("/login"),
            Version::HTTP_11,
            HeaderMap::new(),
            Body::Empty,
        );
        req.state.ja4 = ja4;
        req
    }

    #[test]
    fn test_ja4_filter_denies_matching_string_and_hex() {
        let fp_str = "t13d1516h2_8daaf6152771_e5627efa2ab1";
        let fp_id = ja4_fingerprint_id(fp_str);
        let hex_id = 0xdead_beef_cafe_babe_u64;

        let cfg = PluginConfig {
            id: "ja4_filter".to_string(),
            phase: "pre_upstream".to_string(),
            config: json!({
                "deny": [fp_str, format!("0x{hex_id:x}")]
            }),
        };

        let plugin = Ja4FilterPlugin::from_config(&cfg).unwrap();
        let mut target = 0u16;

        // Matching canonical JA4 string -> Reject
        let mut req1 = make_req(fp_id);
        assert!(matches!(
            plugin.on_upstream_request(&mut req1, &mut target),
            Action::Reject(RejectReason::Forbidden)
        ));

        // Matching hex ID -> Reject
        let mut req2 = make_req(hex_id);
        assert!(matches!(
            plugin.on_upstream_request(&mut req2, &mut target),
            Action::Reject(RejectReason::Forbidden)
        ));

        // Non-matching JA4 -> Continue
        let mut req3 = make_req(ja4_fingerprint_id("t13d1516h2_000000000000_000000000000"));
        assert!(matches!(
            plugin.on_upstream_request(&mut req3, &mut target),
            Action::Continue
        ));

        // Sentinel 0 (plaintext / non-TLS) -> Continue
        let mut req0 = make_req(0);
        assert!(matches!(
            plugin.on_upstream_request(&mut req0, &mut target),
            Action::Continue
        ));
    }

    #[test]
    fn test_ja4_filter_large_list_binary_search() {
        let mut ids = Vec::new();
        for i in 1..=64u64 {
            ids.push(i * 1000 + 7);
        }
        let plugin = Ja4FilterPlugin::new("ja4-large", ids);
        let mut target = 0u16;

        let mut req_hit = make_req(32_007);
        assert!(matches!(
            plugin.on_upstream_request(&mut req_hit, &mut target),
            Action::Reject(RejectReason::Forbidden)
        ));

        let mut req_miss = make_req(32_008);
        assert!(matches!(
            plugin.on_upstream_request(&mut req_miss, &mut target),
            Action::Continue
        ));
    }
}
