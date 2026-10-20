//! Pre-Upstream API Key Authentication Plugin (`api_key_auth`).
//!
//! Enforces fast L7 API key verification against an in-memory hash store backed by
//! a lock-striped Dual Cache (`Positive Cache` + `Negative Cache`) to eliminate brute-force
//! attacks and achieve sub-20ns authentication on the request hot path.

use std::str::FromStr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use http::StatusCode;
use http::header::{HeaderName, HeaderValue};
use rustc_hash::FxHashMap;
use velda_core::{Action, L7Request, L7Response};
use velda_sync::post_sync::plugin::PluginConfig;

use super::cache::ApiKeyCache;
use super::source::{ApiKeyEntry, KeySource, extract_token};
use crate::capability::{AccessLevel, PluginCapability, SupportedProtocols};
use crate::error::PluginError;
use crate::pre_upstream::PreUpstreamPlugin;

/// Pre-Upstream API Key Authentication Plugin.
pub struct ApiKeyAuthPlugin {
    pub id: String,
    pub sources: Vec<KeySource>,
    pub hide_credentials: bool,
    /// Fast in-memory hash store: key_hash -> ApiKeyEntry
    pub keys: FxHashMap<u64, ApiKeyEntry>,
    /// Thread-safe lock-striped Dual Cache
    pub cache: ApiKeyCache,
    /// Pre-computed 401 response for missing API key
    unauthorized_missing: L7Response,
    /// Pre-computed 401 response for invalid API key
    unauthorized_invalid: L7Response,
}

impl ApiKeyAuthPlugin {
    /// Constructs an `ApiKeyAuthPlugin` from raw parameters.
    pub fn new(
        id: impl Into<String>,
        sources: Vec<KeySource>,
        hide_credentials: bool,
        keys: FxHashMap<String, (String, Vec<(HeaderName, HeaderValue)>)>,
        cache: ApiKeyCache,
    ) -> Self {
        let mut key_map = FxHashMap::default();
        for (raw_key, (consumer_id, injected_headers)) in keys {
            let hash = ApiKeyCache::hash_key(raw_key.trim());
            key_map.insert(
                hash,
                ApiKeyEntry {
                    consumer_id,
                    injected_headers,
                },
            );
        }

        let unauthorized_missing =
            L7Response::from_bytes(StatusCode::UNAUTHORIZED, "{\"error\":\"Missing API key\"}")
                .with_header(
                    http::header::WWW_AUTHENTICATE,
                    HeaderValue::from_static("ApiKey realm=\"velda-edge\""),
                )
                .with_header(
                    http::header::CONTENT_TYPE,
                    HeaderValue::from_static("application/json"),
                );

        let unauthorized_invalid =
            L7Response::from_bytes(StatusCode::UNAUTHORIZED, "{\"error\":\"Invalid API key\"}")
                .with_header(
                    http::header::WWW_AUTHENTICATE,
                    HeaderValue::from_static("ApiKey error=\"invalid_key\""),
                )
                .with_header(
                    http::header::CONTENT_TYPE,
                    HeaderValue::from_static("application/json"),
                );

        Self {
            id: id.into(),
            sources,
            hide_credentials,
            keys: key_map,
            cache,
            unauthorized_missing,
            unauthorized_invalid,
        }
    }

    /// Deserializes and constructs an instance directly from a [`PluginConfig`].
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let raw = config.config.as_object().ok_or_else(|| {
            PluginError::Config(format!("Plugin '{}' config must be an object", config.id))
        })?;

        // 1. Parse extraction sources (defaults to X-API-Key and Authorization: ApiKey)
        let mut sources = Vec::new();
        if let Some(sources_arr) = raw.get("sources").and_then(|v| v.as_array()) {
            for item in sources_arr {
                if let Some(src_obj) = item.as_object() {
                    let stype = src_obj
                        .get("type")
                        .and_then(|v| v.as_str())
                        .unwrap_or("header");
                    match stype {
                        "header" => {
                            let name_str = src_obj
                                .get("name")
                                .and_then(|v| v.as_str())
                                .unwrap_or("x-api-key");
                            let prefix = src_obj
                                .get("prefix")
                                .and_then(|v| v.as_str())
                                .unwrap_or("")
                                .to_string();
                            let name = HeaderName::from_str(name_str).map_err(|e| {
                                PluginError::Config(format!(
                                    "Invalid header name '{name_str}' for plugin '{}': {e}",
                                    config.id
                                ))
                            })?;
                            sources.push(KeySource::Header { name, prefix });
                        }
                        "query" => {
                            let param = src_obj
                                .get("param")
                                .and_then(|v| v.as_str())
                                .unwrap_or("api_key")
                                .to_string();
                            sources.push(KeySource::Query { param });
                        }
                        other => {
                            return Err(PluginError::Config(format!(
                                "Unknown key source type '{other}' for plugin '{}'",
                                config.id
                            )));
                        }
                    }
                }
            }
        }

        if sources.is_empty() {
            sources.push(KeySource::Header {
                name: HeaderName::from_static("x-api-key"),
                prefix: String::new(),
            });
            sources.push(KeySource::Header {
                name: http::header::AUTHORIZATION,
                prefix: "ApiKey ".to_string(),
            });
        }

        let hide_credentials = raw
            .get("hide_credentials")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        // 2. Parse configured keys
        let mut keys_map = FxHashMap::default();
        if let Some(keys_arr) = raw.get("keys").and_then(|v| v.as_array()) {
            for (idx, item) in keys_arr.iter().enumerate() {
                let key_obj = item.as_object().ok_or_else(|| {
                    PluginError::Config(format!(
                        "Key entry at index {idx} in plugin '{}' must be an object",
                        config.id
                    ))
                })?;

                let key_str = key_obj
                    .get("key")
                    .and_then(|v| v.as_str())
                    .filter(|s| !s.trim().is_empty())
                    .ok_or_else(|| {
                        PluginError::Config(format!(
                            "Missing non-empty 'key' at index {idx} in plugin '{}'",
                            config.id
                        ))
                    })?;

                let consumer_id = key_obj
                    .get("consumer_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or(key_str)
                    .to_string();

                let mut injected_headers = Vec::new();
                if let Some(inject_map) = key_obj.get("inject_headers").and_then(|v| v.as_object())
                {
                    for (hname_str, hval_val) in inject_map {
                        if let Some(hval_str) = hval_val.as_str() {
                            let hname = HeaderName::from_str(hname_str).map_err(|e| {
                                PluginError::Config(format!(
                                    "Invalid injected header name '{hname_str}' for plugin '{}': {e}",
                                    config.id
                                ))
                            })?;
                            let hval = HeaderValue::from_str(hval_str).map_err(|e| {
                                PluginError::Config(format!(
                                    "Invalid injected header value '{hval_str}' for plugin '{}': {e}",
                                    config.id
                                ))
                            })?;
                            injected_headers.push((hname, hval));
                        }
                    }
                }

                keys_map.insert(key_str.to_string(), (consumer_id, injected_headers));
            }
        }

        // 3. Cache configuration
        let cache_ttl_secs = raw
            .get("cache_ttl_secs")
            .and_then(|v| v.as_u64())
            .unwrap_or(300);
        let negative_cache_ttl_secs = raw
            .get("negative_cache_ttl_secs")
            .and_then(|v| v.as_u64())
            .unwrap_or(10);
        let cache_shards = raw
            .get("cache_shards")
            .and_then(|v| v.as_u64())
            .unwrap_or(64) as usize;
        let cache_shard_capacity = raw
            .get("cache_shard_capacity")
            .and_then(|v| v.as_u64())
            .unwrap_or(2048) as usize;

        let cache = ApiKeyCache::new(
            cache_shards,
            cache_shard_capacity,
            cache_ttl_secs,
            negative_cache_ttl_secs,
        );

        Ok(Self::new(
            &config.id,
            sources,
            hide_credentials,
            keys_map,
            cache,
        ))
    }
}

impl PreUpstreamPlugin for ApiKeyAuthPlugin {
    #[inline(always)]
    fn id(&self) -> &str {
        &self.id
    }

    fn on_upstream_request(&self, req: &mut L7Request) -> Action {
        let (token_str, matched_header) = match extract_token(&self.sources, req) {
            Some(res) => res,
            None => {
                return Action::Respond(self.unauthorized_missing.clone());
            }
        };

        let key_hash = ApiKeyCache::hash_key(token_str);
        let now = current_epoch_secs();

        // 1. Negative Cache check: fail fast on known invalid/brute-forced keys (~10ns)
        if self.cache.is_negative(key_hash, now) {
            return Action::Respond(self.unauthorized_invalid.clone());
        }

        // 2. Positive Cache check: ultra-fast (~15ns) instant injection
        if let Some((_consumer_id, injected_headers)) = self.cache.get_valid(key_hash, now) {
            if self.hide_credentials
                && let Some(ref hdr) = matched_header
            {
                req.headers.remove(hdr);
            }
            for (name, val) in injected_headers {
                req.headers.insert(name, val);
            }
            return Action::Continue;
        }

        // 3. Cache Miss: lookup in pre-compiled root keys
        match self.keys.get(&key_hash) {
            Some(entry) => {
                self.cache.put_valid(
                    key_hash,
                    entry.consumer_id.clone(),
                    entry.injected_headers.clone(),
                    now,
                );

                if self.hide_credentials
                    && let Some(ref hdr) = matched_header
                {
                    req.headers.remove(hdr);
                }

                for (name, val) in &entry.injected_headers {
                    req.headers.insert(name.clone(), val.clone());
                }

                Action::Continue
            }
            None => {
                // Unknown/invalid key -> record in Negative Cache to mitigate DoS
                self.cache.put_invalid(key_hash, now);
                Action::Respond(self.unauthorized_invalid.clone())
            }
        }
    }

    #[inline(always)]
    fn capability(&self) -> PluginCapability {
        PluginCapability::header_only(AccessLevel::Write)
    }

    #[inline(always)]
    fn supported_protocols(&self) -> SupportedProtocols {
        SupportedProtocols::ALL_HTTP
    }
}

/// Fast wall-clock epoch time in seconds.
#[inline]
fn current_epoch_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{HeaderMap, Method, Uri, Version};
    use serde_json::json;
    use velda_core::Body;

    fn build_test_request(headers: HeaderMap, query: Option<&str>) -> L7Request {
        let path = match query {
            Some(q) => format!("/api/v1/resource?{q}"),
            None => "/api/v1/resource".to_string(),
        };
        L7Request::new(
            Method::GET,
            path.parse::<Uri>().unwrap(),
            Version::HTTP_11,
            headers,
            Body::Empty,
        )
    }

    #[test]
    fn test_valid_api_key_header_injection() {
        let mut keys = FxHashMap::default();
        keys.insert(
            "sk-secret-12345".to_string(),
            (
                "org-alpha".to_string(),
                vec![
                    (
                        HeaderName::from_static("x-consumer-id"),
                        HeaderValue::from_static("org-alpha"),
                    ),
                    (
                        HeaderName::from_static("x-consumer-tier"),
                        HeaderValue::from_static("pro"),
                    ),
                ],
            ),
        );

        let plugin = ApiKeyAuthPlugin::new(
            "api_key_auth",
            vec![KeySource::Header {
                name: HeaderName::from_static("x-api-key"),
                prefix: String::new(),
            }],
            true, // hide_credentials = true
            keys,
            ApiKeyCache::new(4, 16, 60, 10),
        );

        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("x-api-key"),
            HeaderValue::from_static("sk-secret-12345"),
        );
        let mut req = build_test_request(headers, None);

        let action = plugin.on_upstream_request(&mut req);
        assert!(matches!(action, Action::Continue));

        // Credentials should be stripped when hide_credentials = true
        assert!(!req.headers.contains_key("x-api-key"));

        // Injected headers should be present
        assert_eq!(req.headers.get("x-consumer-id").unwrap(), "org-alpha");
        assert_eq!(req.headers.get("x-consumer-tier").unwrap(), "pro");

        // Subsequent call hits Positive Cache
        let mut req2 = build_test_request(
            {
                let mut h = HeaderMap::new();
                h.insert("x-api-key", "sk-secret-12345".parse().unwrap());
                h
            },
            None,
        );
        let action2 = plugin.on_upstream_request(&mut req2);
        assert!(matches!(action2, Action::Continue));
        assert_eq!(req2.headers.get("x-consumer-id").unwrap(), "org-alpha");
    }

    #[test]
    fn test_missing_api_key_returns_401() {
        let plugin = ApiKeyAuthPlugin::new(
            "api_key_auth",
            vec![KeySource::Header {
                name: HeaderName::from_static("x-api-key"),
                prefix: String::new(),
            }],
            false,
            FxHashMap::default(),
            ApiKeyCache::new(4, 16, 60, 10),
        );

        let mut req = build_test_request(HeaderMap::new(), None);
        let action = plugin.on_upstream_request(&mut req);
        match action {
            Action::Respond(resp) => {
                assert_eq!(resp.status, StatusCode::UNAUTHORIZED);
                assert_eq!(
                    resp.headers.get("www-authenticate").unwrap(),
                    "ApiKey realm=\"velda-edge\""
                );
            }
            _ => panic!("Expected Action::Respond(401)"),
        }
    }

    #[test]
    fn test_invalid_api_key_triggers_negative_cache() {
        let plugin = ApiKeyAuthPlugin::new(
            "api_key_auth",
            vec![KeySource::Header {
                name: HeaderName::from_static("x-api-key"),
                prefix: String::new(),
            }],
            false,
            FxHashMap::default(),
            ApiKeyCache::new(4, 16, 60, 10),
        );

        let bad_key = "invalid-stolen-key";
        let mut headers = HeaderMap::new();
        headers.insert("x-api-key", bad_key.parse().unwrap());
        let mut req = build_test_request(headers, None);

        // First attempt -> Miss root keys, added to negative cache
        let action1 = plugin.on_upstream_request(&mut req);
        match action1 {
            Action::Respond(resp) => assert_eq!(resp.status, StatusCode::UNAUTHORIZED),
            _ => panic!("Expected 401 response"),
        }

        // Second attempt -> Hits Negative Cache directly
        let mut headers2 = HeaderMap::new();
        headers2.insert("x-api-key", bad_key.parse().unwrap());
        let mut req2 = build_test_request(headers2, None);
        let action2 = plugin.on_upstream_request(&mut req2);
        match action2 {
            Action::Respond(resp) => {
                assert_eq!(resp.status, StatusCode::UNAUTHORIZED);
                assert_eq!(
                    resp.headers.get("www-authenticate").unwrap(),
                    "ApiKey error=\"invalid_key\""
                );
            }
            _ => panic!("Expected 401 response"),
        }
    }

    #[test]
    fn test_authorization_header_with_prefix_and_query_source() {
        let mut keys = FxHashMap::default();
        keys.insert(
            "secret-query-token".to_string(),
            (
                "partner-beta".to_string(),
                vec![(
                    HeaderName::from_static("x-partner"),
                    HeaderValue::from_static("beta"),
                )],
            ),
        );

        let plugin = ApiKeyAuthPlugin::new(
            "api_key_auth",
            vec![
                KeySource::Header {
                    name: http::header::AUTHORIZATION,
                    prefix: "ApiKey ".to_string(),
                },
                KeySource::Query {
                    param: "key".to_string(),
                },
            ],
            false,
            keys,
            ApiKeyCache::new(4, 16, 60, 10),
        );

        // 1. Authorization header: ApiKey secret-query-token
        let mut headers = HeaderMap::new();
        headers.insert(
            http::header::AUTHORIZATION,
            "ApiKey secret-query-token".parse().unwrap(),
        );
        let mut req_hdr = build_test_request(headers, None);
        assert!(matches!(
            plugin.on_upstream_request(&mut req_hdr),
            Action::Continue
        ));
        assert_eq!(req_hdr.headers.get("x-partner").unwrap(), "beta");

        // 2. Query parameter: ?key=secret-query-token
        let mut req_query =
            build_test_request(HeaderMap::new(), Some("key=secret-query-token&mode=raw"));
        assert!(matches!(
            plugin.on_upstream_request(&mut req_query),
            Action::Continue
        ));
        assert_eq!(req_query.headers.get("x-partner").unwrap(), "beta");
    }

    #[test]
    fn test_from_config_complete_deserialization() {
        let config = PluginConfig {
            id: "api_key_auth".into(),
            phase: String::new(),
            config: json!({
                "sources": [
                    { "type": "header", "name": "x-api-key" },
                    { "type": "query", "param": "token" }
                ],
                "hide_credentials": true,
                "cache_ttl_secs": 600,
                "negative_cache_ttl_secs": 15,
                "keys": [
                    {
                        "key": "sk-live-alpha-1234",
                        "consumer_id": "stripe-integration",
                        "inject_headers": {
                            "x-consumer-id": "stripe-integration",
                            "x-tier": "enterprise"
                        }
                    }
                ]
            }),
        };

        let plugin = ApiKeyAuthPlugin::from_config(&config).expect("valid config");
        assert_eq!(plugin.id, "api_key_auth");
        assert_eq!(plugin.sources.len(), 2);
        assert!(plugin.hide_credentials);
        assert_eq!(plugin.keys.len(), 1);

        let mut headers = HeaderMap::new();
        headers.insert("x-api-key", "sk-live-alpha-1234".parse().unwrap());
        let mut req = build_test_request(headers, None);
        assert!(matches!(
            plugin.on_upstream_request(&mut req),
            Action::Continue
        ));
        assert_eq!(
            req.headers.get("x-consumer-id").unwrap(),
            "stripe-integration"
        );
        assert_eq!(req.headers.get("x-tier").unwrap(), "enterprise");
        assert!(!req.headers.contains_key("x-api-key"));
    }
}
