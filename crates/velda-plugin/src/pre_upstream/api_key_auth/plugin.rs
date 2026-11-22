//! `api_key_auth` Pre-Upstream Plugin (`phase: "pre_upstream"`).
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────────────────────┐
//! │                     API KEY AUTH HOT-PATH EXECUTION (HTTP & gRPC)                       │
//! ├─────────────────────────────────────────────────────────────────────────────────────────┤
//! │  L7Request.headers (HeaderWorkspace)                                                    │
//! │    │                                                                                    │
//! │    ├── Missing or len ∉ [min_key_len, max_key_len] (<= 64B) ──► Reject(Unauthorized)    │
//! │    │                                                                                    │
//! │    ▼ encode_key_words(bytes) -> (words: [u64; 8], tag: u64) + coarse_sec()              │
//! │    ├── CoreCacheArena::probe(lane, tag, now_sec) [Positive-Only, Spray-Immune]          │
//! │    │     ├── Positive Hit -> Verify ct_eq_words & expires_at  ► Inject & Continue       │
//! │    │     └── Miss -> ContiguousKeyStore::find_record()                                  │
//! │    │           ├── Not Found / Expired (No cache write!) ─────► Reject(Unauthorized)    │
//! │    │           └── Valid -> insert_positive() ────────────────► Inject & Continue       │
//! │                                                                                         │
//! │  On Match: optional remove(header_name), overwrite inject_headers via insert()          │
//! └─────────────────────────────────────────────────────────────────────────────────────────┘
//! ```

use std::str::FromStr;

use http::header::{HeaderName, HeaderValue};
use velda_core::clock::coarse_sec;
use velda_core::{Action, L7Request, RejectReason};

use crate::capability::{AccessLevel, PluginCapability, SupportedProtocols};
use crate::config::PluginConfig;
use crate::error::PluginError;
use crate::pre_upstream::PreUpstreamPlugin;
use crate::pre_upstream::api_key_auth::arena::{
    CompiledKeyInput, ContiguousKeyStore, CoreCacheArena, ct_eq_words, encode_key_words,
};
use crate::pre_upstream::api_key_auth::config::MAX_API_KEY_LEN;
use crate::pre_upstream::api_key_auth::validation::validate_api_key_auth_config;

/// Pre-upstream API key authentication plugin backed by contiguous `k * 4096`-byte arenas.
pub struct ApiKeyAuthPlugin {
    id: String,
    header_name: HeaderName,
    hide_credentials: bool,
    cache_ttl_secs: u64,
    store: ContiguousKeyStore,
    cache: CoreCacheArena,
}

impl ApiKeyAuthPlugin {
    /// Instantiates `ApiKeyAuthPlugin` from a declarative [`PluginConfig`].
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let cfg = validate_api_key_auth_config(&config.id, &config.config)?;

        let header_name = HeaderName::from_str(&cfg.header_name).map_err(|e| {
            PluginError::Config(format!(
                "Invalid header_name '{}' for plugin '{}': {e}",
                cfg.header_name, config.id
            ))
        })?;

        let mut compiled_entries = Vec::with_capacity(cfg.keys.len());
        for k in cfg.keys {
            let mut hdrs = Vec::with_capacity(k.inject_headers.len());
            for (hname_str, hval_str) in k.inject_headers {
                let hname = HeaderName::from_str(&hname_str).map_err(|e| {
                    PluginError::Config(format!(
                        "Invalid inject_headers name '{hname_str}' for plugin '{}': {e}",
                        config.id
                    ))
                })?;
                let hval = HeaderValue::from_str(&hval_str).map_err(|e| {
                    PluginError::Config(format!(
                        "Invalid inject_headers value for '{hname_str}' in plugin '{}': {e}",
                        config.id
                    ))
                })?;
                hdrs.push((hname, hval));
            }
            compiled_entries.push(CompiledKeyInput {
                key_bytes: k.key.into_bytes(),
                expires_at: k.expires_at,
                inject_headers: hdrs,
            });
        }

        let store = ContiguousKeyStore::compile(compiled_entries);
        let cache = CoreCacheArena::new(cfg.cache_slots_per_core);

        Ok(Self {
            id: config.id.clone(),
            header_name,
            hide_credentials: cfg.hide_credentials,
            cache_ttl_secs: cfg.cache_ttl_secs,
            store,
            cache,
        })
    }

    #[cold]
    #[inline(never)]
    fn reject_unauthorized(&self, req: &mut L7Request) -> Action {
        req.headers.insert_bypass(
            http::header::WWW_AUTHENTICATE,
            HeaderValue::from_static("ApiKey realm=\"velda-edge\""),
        );
        Action::Reject(RejectReason::Unauthorized)
    }

    #[inline(always)]
    fn apply_authorized(&self, req: &mut L7Request, record_idx: u32) -> Action {
        if self.hide_credentials {
            req.headers.remove(&self.header_name);
        }
        let rec = self.store.record(record_idx);
        for (name, val) in self.store.headers_for(rec) {
            req.headers.insert(name.clone(), val.clone());
        }
        Action::Continue
    }
}

impl PreUpstreamPlugin for ApiKeyAuthPlugin {
    #[inline(always)]
    fn id(&self) -> &str {
        &self.id
    }

    #[inline(always)]
    fn pbww(&self) -> u32 {
        0
    }

    #[inline]
    fn on_upstream_request(&self, req: &mut L7Request, _target_upstream: &mut u16) -> Action {
        let Some(raw_val) = req.headers.get(&self.header_name) else {
            return self.reject_unauthorized(req);
        };

        let raw_bytes = raw_val.as_bytes().trim_ascii();
        let key_len = raw_bytes.len();
        if key_len == 0
            || key_len > MAX_API_KEY_LEN
            || key_len < self.store.min_key_len
            || key_len > self.store.max_key_len
        {
            return self.reject_unauthorized(req);
        }

        let (words, tag) = encode_key_words(raw_bytes);
        let now = coarse_sec();
        let lane = self.cache.current_lane();

        if let Some(record_idx) = self.cache.probe(lane, tag, now) {
            let rec = self.store.record(record_idx);
            if ct_eq_words(&words, key_len as u8, &rec.words, rec.key_len)
                && (rec.expires_at == 0 || now < rec.expires_at)
            {
                return self.apply_authorized(req, record_idx);
            }
        }

        let Some(record_idx) = self.store.find_record(tag, &words, key_len as u8) else {
            return self.reject_unauthorized(req);
        };

        let rec = self.store.record(record_idx);
        if rec.expires_at != 0 && now >= rec.expires_at {
            return self.reject_unauthorized(req);
        }

        let mut expire_sec = now.saturating_add(self.cache_ttl_secs);
        if rec.expires_at != 0 && rec.expires_at < expire_sec {
            expire_sec = rec.expires_at;
        }
        self.cache
            .insert_positive(lane, tag, record_idx, expire_sec);

        self.apply_authorized(req, record_idx)
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
    use http::{HeaderMap, Method, Uri, Version};
    use serde_json::json;
    use velda_core::Body;

    fn make_req(header_val: Option<&'static str>) -> L7Request {
        let mut headers = HeaderMap::new();
        if let Some(v) = header_val {
            headers.insert(
                HeaderName::from_static("x-api-key"),
                HeaderValue::from_static(v),
            );
        }
        headers.insert(
            HeaderName::from_static("x-user-id"),
            HeaderValue::from_static("spoofed-attacker"),
        );
        L7Request::new(
            Method::GET,
            Uri::from_static("/v1/orders"),
            Version::HTTP_11,
            headers,
            Body::Empty,
        )
    }

    #[test]
    fn test_valid_key_overwrites_headers_and_caches_per_core() {
        velda_core::clock::init_clock();
        let cfg = PluginConfig {
            id: "api_key_auth".to_string(),
            phase: "pre_upstream".to_string(),
            config: json!({
                "header_name": "x-api-key",
                "hide_credentials": true,
                "cache_ttl_secs": 300,
                "keys": [
                    {
                        "key": "sk-live-secret-key-001",
                        "expires_at": 0,
                        "inject_headers": {
                            "x-user-id": "usr_001",
                            "x-tier": "enterprise"
                        }
                    }
                ]
            }),
        };

        let plugin = ApiKeyAuthPlugin::from_config(&cfg).unwrap();
        assert_eq!(plugin.cache.byte_size() % 4096, 0);

        let mut target = 0u16;
        // First request -> cache miss -> store hit -> cache populate
        let mut req1 = make_req(Some("sk-live-secret-key-001"));
        assert!(matches!(
            plugin.on_upstream_request(&mut req1, &mut target),
            Action::Continue
        ));
        assert!(req1.headers.get("x-api-key").is_none());
        assert_eq!(req1.headers.get("x-user-id").unwrap(), "usr_001");
        assert_eq!(req1.headers.get("x-tier").unwrap(), "enterprise");

        // Second request -> positive cache hit
        let mut req2 = make_req(Some("sk-live-secret-key-001"));
        assert!(matches!(
            plugin.on_upstream_request(&mut req2, &mut target),
            Action::Continue
        ));
        assert_eq!(req2.headers.get("x-user-id").unwrap(), "usr_001");
    }

    #[test]
    fn test_invalid_expired_and_oversized_keys_rejected() {
        velda_core::clock::init_clock();
        let cfg = PluginConfig {
            id: "api_key_auth".to_string(),
            phase: "pre_upstream".to_string(),
            config: json!({
                "header_name": "x-api-key",
                "hide_credentials": false,
                "keys": [
                    { "key": "sk-valid-key-12345", "expires_at": 0 },
                    { "key": "sk-expired-key-1", "expires_at": 1000 }
                ]
            }),
        };
        let plugin = ApiKeyAuthPlugin::from_config(&cfg).unwrap();
        let mut target = 0u16;

        // Missing header
        let mut req_missing = make_req(None);
        assert!(matches!(
            plugin.on_upstream_request(&mut req_missing, &mut target),
            Action::Reject(RejectReason::Unauthorized)
        ));

        // Wrong key (rejected without polluting CoreCacheArena)
        let mut req_bad = make_req(Some("sk-wrong-key-12345"));
        assert!(matches!(
            plugin.on_upstream_request(&mut req_bad, &mut target),
            Action::Reject(RejectReason::Unauthorized)
        ));

        // Expired key (expires_at = 1000 < coarse_sec())
        let mut req_exp = make_req(Some("sk-expired-key-1"));
        assert!(matches!(
            plugin.on_upstream_request(&mut req_exp, &mut target),
            Action::Reject(RejectReason::Unauthorized)
        ));
    }
}
