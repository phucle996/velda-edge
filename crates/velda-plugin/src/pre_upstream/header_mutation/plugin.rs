//! Header Mutation Pre-Upstream Plugin (`header_mutation`).
//!
//! Operates strictly on `req.headers` ([`velda_core::HeaderWorkspace`]) to apply
//! configured static delete (`remove`), overwrite (`set`), and add (`append`)
//! rules verbatim, including entries tagged with `TAG_BYTE_BYPASS_BIT`.
//!
//! ```text
//!   req.headers: HeaderWorkspace
//!     │
//!     ├─► 0. op_mask == 0 ──► Action::Continue
//!     ├─► 1. OP_REMOVE: `contains_prehashed(&key)` ─► `remove(&key.name)`
//!     ├─► 2. OP_SET:    `insert_prehashed(&key, val)` (static HeaderValue)
//!     └─► 3. OP_APPEND: `append(name, val)` (static HeaderValue)
//! ```

use http::header::{HeaderName, HeaderValue};
use velda_core::{Action, HeaderKey, L7Request};

use crate::capability::{AccessLevel, PluginCapability, SupportedProtocols};
use crate::config::PluginConfig;
use crate::error::PluginError;
use crate::pre_upstream::PreUpstreamPlugin;

const OP_REMOVE: u8 = 1 << 0;
const OP_SET: u8 = 1 << 1;
const OP_APPEND: u8 = 1 << 2;

/// Explicit static mutation rules on `HeaderWorkspace`: remove, set (overwrite), and append (add).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HeaderMutationConfig {
    /// Header names to delete from `HeaderWorkspace` (including bypass-tagged entries).
    pub remove: Vec<HeaderName>,
    /// Static header entries to set or overwrite (`insert_prehashed`).
    pub set: Vec<(HeaderName, HeaderValue)>,
    /// Static header entries to append (`append`).
    pub append: Vec<(HeaderName, HeaderValue)>,
}

/// Pre-Upstream Header Mutation Plugin centered directly on `HeaderWorkspace`.
#[repr(C, align(64))]
#[derive(Debug, Clone)]
pub struct HeaderMutationPlugin {
    /// Bitmask of enabled operations (`0` = no-op fast path).
    op_mask: u8,
    remove_keys: Vec<HeaderKey>,
    sets: Vec<(HeaderKey, HeaderValue)>,
    appends: Vec<(HeaderName, HeaderValue)>,
    pub id: String,
    pub config: HeaderMutationConfig,
}

impl HeaderMutationPlugin {
    pub fn new(id: impl Into<String>, config: HeaderMutationConfig) -> Self {
        let remove_keys: Vec<HeaderKey> = config
            .remove
            .iter()
            .map(|name| HeaderKey::from_name(name.clone()))
            .collect();

        let sets: Vec<(HeaderKey, HeaderValue)> = config
            .set
            .iter()
            .map(|(name, val)| (HeaderKey::from_name(name.clone()), val.clone()))
            .collect();

        let appends = config.append.clone();

        let mut op_mask = 0u8;
        if !remove_keys.is_empty() {
            op_mask |= OP_REMOVE;
        }
        if !sets.is_empty() {
            op_mask |= OP_SET;
        }
        if !appends.is_empty() {
            op_mask |= OP_APPEND;
        }

        Self {
            op_mask,
            remove_keys,
            sets,
            appends,
            id: id.into(),
            config,
        }
    }

    /// Deserializes and constructs an instance directly from a [`PluginConfig`].
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let cfg = super::validation::validate_header_mutation_config(&config.id, &config.config)
            .map_err(|e| {
                PluginError::Config(format!(
                    "Invalid header_mutation configuration for '{}': {e}",
                    config.id
                ))
            })?;

        let mut remove = Vec::with_capacity(cfg.remove_headers.len());
        for s in &cfg.remove_headers {
            let name = HeaderName::from_bytes(s.as_bytes()).map_err(|e| {
                PluginError::Config(format!("Invalid remove header name '{s}': {e}"))
            })?;
            remove.push(name);
        }
        remove.sort_unstable_by(|a, b| a.as_str().cmp(b.as_str()));

        let mut set = Vec::with_capacity(cfg.set_headers.len());
        for (k, v) in &cfg.set_headers {
            let name = HeaderName::from_bytes(k.as_bytes())
                .map_err(|e| PluginError::Config(format!("Invalid set header name '{k}': {e}")))?;
            let val = HeaderValue::from_str(v).map_err(|e| {
                PluginError::Config(format!("Invalid set header value for '{k}': {e}"))
            })?;
            set.push((name, val));
        }
        set.sort_unstable_by(|a, b| a.0.as_str().cmp(b.0.as_str()));

        let mut append = Vec::with_capacity(cfg.append_headers.len());
        for (k, v) in &cfg.append_headers {
            let name = HeaderName::from_bytes(k.as_bytes()).map_err(|e| {
                PluginError::Config(format!("Invalid append header name '{k}': {e}"))
            })?;
            let val = HeaderValue::from_str(v).map_err(|e| {
                PluginError::Config(format!("Invalid append header value for '{k}': {e}"))
            })?;
            append.push((name, val));
        }
        append.sort_unstable_by(|a, b| a.0.as_str().cmp(b.0.as_str()));

        Ok(Self::new(
            &config.id,
            HeaderMutationConfig {
                remove,
                set,
                append,
            },
        ))
    }
}

impl PreUpstreamPlugin for HeaderMutationPlugin {
    #[inline(always)]
    fn id(&self) -> &str {
        &self.id
    }

    #[inline(always)]
    fn pbww(&self) -> u32 {
        0
    }

    fn on_upstream_request(&self, req: &mut L7Request, _targets: &mut u32) -> Action {
        let mask = self.op_mask;
        if mask == 0 {
            return Action::Continue;
        }

        // Step 1: Delete configured headers (including bypass-tagged entries)
        if (mask & OP_REMOVE) != 0 {
            for key in &self.remove_keys {
                if req.headers.contains_prehashed(key) {
                    req.headers.remove(&key.name);
                }
            }
        }

        // Step 2: Set / overwrite static header values
        if (mask & OP_SET) != 0 {
            for (key, val) in &self.sets {
                req.headers.insert_prehashed(key, val.clone());
            }
        }

        // Step 3: Append static header values
        if (mask & OP_APPEND) != 0 {
            for (name, val) in &self.appends {
                req.headers.append(name.clone(), val.clone());
            }
        }

        Action::Continue
    }

    #[inline(always)]
    fn capability(&self) -> PluginCapability {
        PluginCapability::header_only(AccessLevel::Write)
    }

    #[inline(always)]
    fn supported_protocols(&self) -> SupportedProtocols {
        SupportedProtocols::ALL_HTTP | SupportedProtocols::GRPC
    }
}

// ── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use http::{Method, Uri, Version};
    use velda_core::{Body, HeaderWorkspace};

    #[test]
    fn test_does_not_touch_unconfigured_headers() {
        let plugin = HeaderMutationPlugin::new(
            "hm-explicit-only",
            HeaderMutationConfig {
                remove: vec![HeaderName::from_static("x-only-remove-this")],
                set: Vec::new(),
                append: Vec::new(),
            },
        );

        let mut headers = HeaderWorkspace::new(8);
        headers.insert(
            http::header::CONNECTION,
            HeaderValue::from_static("keep-alive, x-custom-hop"),
        );
        headers.insert(http::header::TE, HeaderValue::from_static("trailers"));
        headers.insert(http::header::UPGRADE, HeaderValue::from_static("websocket"));
        headers.insert(
            HeaderName::from_static("x-only-remove-this"),
            HeaderValue::from_static("bye"),
        );

        let mut req = L7Request::new(
            Method::GET,
            Uri::from_static("/"),
            Version::HTTP_11,
            headers,
            Body::Empty,
        );
        let mut target = 1;
        let action = plugin.on_upstream_request(&mut req, &mut target);

        assert!(matches!(action, Action::Continue));
        assert!(!req.headers.contains_key("x-only-remove-this"));
        assert_eq!(
            req.headers.get(http::header::CONNECTION).unwrap(),
            "keep-alive, x-custom-hop"
        );
        assert_eq!(req.headers.get(http::header::TE).unwrap(), "trailers");
        assert_eq!(req.headers.get(http::header::UPGRADE).unwrap(), "websocket");
    }

    #[test]
    fn test_can_remove_and_overwrite_bypass_tagged_headers() {
        let plugin = HeaderMutationPlugin::new(
            "hm-bypass",
            HeaderMutationConfig {
                remove: vec![
                    HeaderName::from_static("x-request-id"),
                    HeaderName::from_static("x-forwarded-for"),
                ],
                set: vec![(
                    HeaderName::from_static("host"),
                    HeaderValue::from_static("overwritten.internal"),
                )],
                append: vec![(
                    HeaderName::from_static("x-trace"),
                    HeaderValue::from_static("hop-1"),
                )],
            },
        );

        let mut headers = HeaderWorkspace::new(8);
        assert!(headers.contains_key("x-request-id"));
        headers.insert_bypass(
            HeaderName::from_static("x-forwarded-for"),
            HeaderValue::from_static("10.0.0.1"),
        );
        headers.insert_bypass(
            HeaderName::from_static("host"),
            HeaderValue::from_static("original.example.com"),
        );
        headers.insert_bypass(
            HeaderName::from_static("x-keep-bypass"),
            HeaderValue::from_static("keep-me"),
        );

        let mut req = L7Request::new(
            Method::GET,
            Uri::from_static("/v1/items"),
            Version::HTTP_11,
            headers,
            Body::Empty,
        );
        let mut target = 1;
        plugin.on_upstream_request(&mut req, &mut target);

        assert!(!req.headers.contains_key("x-request-id"));
        assert!(!req.headers.contains_key("x-forwarded-for"));
        assert_eq!(
            req.headers.get(http::header::HOST).unwrap(),
            "overwritten.internal"
        );
        assert_eq!(req.headers.get("x-trace").unwrap(), "hop-1");
        assert_eq!(req.headers.get("x-keep-bypass").unwrap(), "keep-me");
    }

    #[test]
    fn test_set_overwrites_multi_value_and_append_adds_slot() {
        let plugin = HeaderMutationPlugin::new(
            "hm-ops",
            HeaderMutationConfig {
                remove: vec![HeaderName::from_static("x-sensitive")],
                set: vec![(
                    HeaderName::from_static("x-custom-gateway"),
                    HeaderValue::from_static("velda-v2"),
                )],
                append: vec![(
                    HeaderName::from_static("x-hop"),
                    HeaderValue::from_static("edge-2"),
                )],
            },
        );

        let mut headers = HeaderWorkspace::new(8);
        headers.insert(
            HeaderName::from_static("x-sensitive"),
            HeaderValue::from_static("secret"),
        );
        headers.append(
            HeaderName::from_static("x-custom-gateway"),
            HeaderValue::from_static("old-1"),
        );
        headers.append(
            HeaderName::from_static("x-custom-gateway"),
            HeaderValue::from_static("old-2"),
        );
        headers.append(
            HeaderName::from_static("x-hop"),
            HeaderValue::from_static("edge-1"),
        );

        let mut req = L7Request::new(
            Method::GET,
            Uri::from_static("/test"),
            Version::HTTP_11,
            headers,
            Body::Empty,
        );

        let mut target = 1;
        let action = plugin.on_upstream_request(&mut req, &mut target);
        assert!(matches!(action, Action::Continue));
        assert!(!req.headers.contains_key("x-sensitive"));

        let gw: Vec<_> = req.headers.get_all("x-custom-gateway").iter().collect();
        assert_eq!(gw.len(), 1);
        assert_eq!(gw[0], "velda-v2");

        let hops: Vec<_> = req.headers.get_all("x-hop").iter().collect();
        assert_eq!(hops.len(), 2);
        assert_eq!(hops[0], "edge-1");
        assert_eq!(hops[1], "edge-2");
    }

    #[test]
    fn test_from_config_complete() {
        let json_cfg = serde_json::json!({
            "remove_headers": ["x-internal-secret"],
            "set_headers": {
                "x-forwarded-proto": "https"
            },
            "append_headers": {
                "x-proxy-tag": "velda-edge"
            }
        });
        let config = PluginConfig {
            id: "header_mutation".into(),
            config: json_cfg,
        };
        let plugin = HeaderMutationPlugin::from_config(&config).expect("valid config");
        assert_eq!(plugin.id, "header_mutation");
        assert_eq!(plugin.config.remove.len(), 1);
        assert_eq!(plugin.config.set.len(), 1);
        assert_eq!(plugin.config.append.len(), 1);

        let bad_config = PluginConfig {
            id: "header_mutation".into(),
            config: serde_json::json!({}),
        };
        assert!(HeaderMutationPlugin::from_config(&bad_config).is_err());
    }
}
