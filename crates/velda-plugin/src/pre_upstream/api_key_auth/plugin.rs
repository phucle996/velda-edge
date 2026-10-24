//! Pre-Upstream API Key Authentication Plugin (`api_key_auth`).
//!
//! Enforces high-performance L7 API key verification against an in-memory hash store backed by
//! constant-time cryptographic verification, length bounds checking, managed header sanitization,
//! and complete credential leakage prevention on the hot path.

use std::str::FromStr;
use std::sync::Arc;

use http::header::{HeaderName, HeaderValue};
use rustc_hash::FxHashMap;
use velda_core::{Action, L7Request, RejectReason};
use velda_sync::post_sync::plugin::PluginConfig;

use super::cache::ApiKeyCache;
use super::source::{ApiKeyEntry, KeySource, MatchedSource, extract_token_ref};
use crate::capability::{AccessLevel, PluginCapability, SupportedProtocols};
use crate::error::PluginError;
use crate::pre_upstream::PreUpstreamPlugin;

/// Pre-Upstream API Key Authentication Plugin.
pub struct ApiKeyAuthPlugin {
    pub id: String,
    pub sources: Vec<KeySource>,
    pub hide_credentials: bool,
    /// Fast in-memory hash store: key_hash -> Vec<ApiKeyEntry> (collision-safe)
    pub keys: FxHashMap<u64, Vec<ApiKeyEntry>>,
    /// Thread-safe lock-striped Dual Cache (kept for API compatibility)
    pub cache: ApiKeyCache,
    /// Minimum valid key length among configured keys
    pub min_key_len: usize,
    /// Maximum valid key length among configured keys
    pub max_key_len: usize,
    /// Pre-compiled list of all injected header names across keys to sanitize prior to injection
    pub sanitized_headers: Vec<HeaderName>,
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
        let mut key_map: FxHashMap<u64, Vec<ApiKeyEntry>> = FxHashMap::default();
        let mut min_len = usize::MAX;
        let mut max_len = 0usize;
        let mut sanitized = Vec::new();

        for (raw_key, (consumer_id, injected_headers)) in keys {
            let trimmed_key = raw_key.trim();
            let k_len = trimmed_key.len();
            min_len = min_len.min(k_len);
            max_len = max_len.max(k_len);

            for (hname, _) in &injected_headers {
                if !sanitized.contains(hname) {
                    sanitized.push(hname.clone());
                }
            }

            let hash = ApiKeyCache::hash_key(trimmed_key);
            key_map.entry(hash).or_default().push(ApiKeyEntry {
                key: trimmed_key.to_string(),
                consumer_id,
                injected_headers,
            });
        }

        if min_len == usize::MAX {
            min_len = 0;
        }

        Self {
            id: id.into(),
            sources,
            hide_credentials,
            keys: key_map,
            cache,
            min_key_len: min_len,
            max_key_len: max_len,
            sanitized_headers: sanitized,
        }
    }

    /// Deserializes and constructs an instance directly from a [`PluginConfig`].
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let cfg =
            velda_sync::validate_api_key_auth_config(&config.id, &config.config).map_err(|e| {
                PluginError::Config(format!(
                    "Invalid api_key_auth configuration for '{}': {e}",
                    config.id
                ))
            })?;

        let mut sources = Vec::with_capacity(cfg.sources.len());
        for src in &cfg.sources {
            match src {
                velda_sync::ApiKeySourceConfig::Header { name, prefix } => {
                    let hname = HeaderName::from_str(name).map_err(|e| {
                        PluginError::Config(format!(
                            "Invalid header name '{name}' for plugin '{}': {e}",
                            config.id
                        ))
                    })?;
                    sources.push(KeySource::Header {
                        name: hname,
                        prefix: prefix.clone().unwrap_or_default(),
                    });
                }
                velda_sync::ApiKeySourceConfig::Query { param } => {
                    sources.push(KeySource::Query {
                        param: param.clone(),
                    });
                }
            }
        }

        let mut keys_map = FxHashMap::default();
        for k in &cfg.keys {
            let consumer_id = k.consumer_id.clone().unwrap_or_else(|| k.key.clone());
            let mut injected_headers = Vec::with_capacity(k.inject_headers.len());
            for (hname_str, hval_str) in &k.inject_headers {
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
            keys_map.insert(k.key.clone(), (consumer_id, injected_headers));
        }

        let cache = ApiKeyCache::new(64, 2048, cfg.cache_ttl_secs, 10);

        Ok(Self::new(
            &config.id,
            sources,
            cfg.hide_credentials,
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

    fn on_upstream_request(&self, req: &mut L7Request, _target_upstream: &mut Arc<str>) -> Action {
        let (entry, matched_source) = {
            let (token_str, location) = match extract_token_ref(&self.sources, req) {
                Some(res) => res,
                None => {
                    req.headers.insert_bypass(
                        http::header::WWW_AUTHENTICATE,
                        HeaderValue::from_static("ApiKey realm=\"velda-edge\""),
                    );
                    return Action::Reject(RejectReason::Unauthorized);
                }
            };

            // 1. Fast-Reject Length Guard (< 1ns): reject tokens outside valid configured bounds
            let token_len = token_str.len();
            if token_len < self.min_key_len || token_len > self.max_key_len {
                req.headers.insert_bypass(
                    http::header::WWW_AUTHENTICATE,
                    HeaderValue::from_static("ApiKey error=\"invalid_key\""),
                );
                return Action::Reject(RejectReason::Unauthorized);
            }

            // 2. Fast O(1) hash indexing
            let key_hash = ApiKeyCache::hash_key(token_str);

            // 3. Constant-time key comparison to prevent timing attacks and hash collisions
            let matched_entry = self.keys.get(&key_hash).and_then(|entries| {
                entries
                    .iter()
                    .find(|entry| constant_time_eq(token_str.as_bytes(), entry.key.as_bytes()))
            });

            let entry = match matched_entry {
                Some(e) => e,
                None => {
                    req.headers.insert_bypass(
                        http::header::WWW_AUTHENTICATE,
                        HeaderValue::from_static("ApiKey error=\"invalid_key\""),
                    );
                    return Action::Reject(RejectReason::Unauthorized);
                }
            };

            (entry, location)
        };

        // 4. Strip credentials if hide_credentials is true (both headers and query params)
        if self.hide_credentials {
            match matched_source {
                MatchedSource::Header(hdr) => {
                    req.headers.remove(hdr);
                }
                MatchedSource::Query(param) => {
                    if let Some(new_uri) = strip_query_param(&req.uri, param) {
                        req.uri = new_uri;
                    }
                }
            }
        }

        // 5. Prevent privilege escalation: sanitize all managed identity headers
        for hname in &self.sanitized_headers {
            req.headers.remove(hname);
        }

        // 6. Inject authoritative consumer metadata
        for (name, val) in &entry.injected_headers {
            req.headers.insert(name.clone(), val.clone());
        }

        Action::Continue
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

/// Constant-time slice equality comparison preventing timing attacks (CWE-208).
#[inline]
pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for i in 0..a.len() {
        diff |= a[i] ^ b[i];
    }
    diff == 0
}

/// Strips a specific query parameter from a URI, returning a new URI without the secret token.
fn strip_query_param(uri: &http::Uri, target_param: &str) -> Option<http::Uri> {
    let query = uri.query()?;
    let mut new_query = String::with_capacity(query.len());
    let mut found = false;

    for part in query.split('&') {
        let key = match part.split_once('=') {
            Some((k, _)) => k,
            None => part,
        };
        if key == target_param {
            found = true;
            continue;
        }
        if !new_query.is_empty() {
            new_query.push('&');
        }
        new_query.push_str(part);
    }

    if !found {
        return None;
    }

    let path = uri.path();
    let new_pq_str = if new_query.is_empty() {
        path.to_string()
    } else {
        format!("{path}?{new_query}")
    };

    let mut parts = uri.clone().into_parts();
    parts.path_and_query =
        Some(http::uri::PathAndQuery::from_maybe_shared(bytes::Bytes::from(new_pq_str)).ok()?);
    http::Uri::from_parts(parts).ok()
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

        let mut target = Arc::from("default-upstream");
        let action = plugin.on_upstream_request(&mut req, &mut target);
        assert!(matches!(action, Action::Continue));

        // Credentials should be stripped when hide_credentials = true
        assert!(!req.headers.contains_key("x-api-key"));

        // Injected headers should be present
        assert_eq!(req.headers.get("x-consumer-id").unwrap(), "org-alpha");
        assert_eq!(req.headers.get("x-consumer-tier").unwrap(), "pro");
    }

    #[test]
    fn test_missing_api_key_returns_401_with_no_store() {
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
        let mut target = Arc::from("default-upstream");
        let action = plugin.on_upstream_request(&mut req, &mut target);
        assert!(matches!(action, Action::Reject(RejectReason::Unauthorized)));
        assert_eq!(
            req.headers.get("www-authenticate").unwrap(),
            "ApiKey realm=\"velda-edge\""
        );
    }

    #[test]
    fn test_invalid_api_key_rejected_with_no_store() {
        let mut keys = FxHashMap::default();
        keys.insert("valid-key-123".to_string(), ("user-1".to_string(), vec![]));

        let plugin = ApiKeyAuthPlugin::new(
            "api_key_auth",
            vec![KeySource::Header {
                name: HeaderName::from_static("x-api-key"),
                prefix: String::new(),
            }],
            false,
            keys,
            ApiKeyCache::new(4, 16, 60, 10),
        );

        let bad_key = "invalid-stolen-key";
        let mut headers = HeaderMap::new();
        headers.insert("x-api-key", bad_key.parse().unwrap());
        let mut req = build_test_request(headers, None);

        let mut target = Arc::from("default-upstream");
        let action = plugin.on_upstream_request(&mut req, &mut target);
        assert!(matches!(action, Action::Reject(RejectReason::Unauthorized)));
        assert_eq!(
            req.headers.get("www-authenticate").unwrap(),
            "ApiKey error=\"invalid_key\""
        );
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
        let mut target_hdr = Arc::from("default-upstream");
        assert!(matches!(
            plugin.on_upstream_request(&mut req_hdr, &mut target_hdr),
            Action::Continue
        ));
        assert_eq!(req_hdr.headers.get("x-partner").unwrap(), "beta");

        // 2. Query parameter: ?key=secret-query-token
        let mut req_query =
            build_test_request(HeaderMap::new(), Some("key=secret-query-token&mode=raw"));
        let mut target_query = Arc::from("default-upstream");
        assert!(matches!(
            plugin.on_upstream_request(&mut req_query, &mut target_query),
            Action::Continue
        ));
        assert_eq!(req_query.headers.get("x-partner").unwrap(), "beta");
    }

    #[test]
    fn test_privilege_escalation_sanitizes_spoofed_headers() {
        let mut keys = FxHashMap::default();
        // Low privilege key only injects x-consumer-id
        keys.insert(
            "sk-low-priv-123".to_string(),
            (
                "partner-low".to_string(),
                vec![(
                    HeaderName::from_static("x-consumer-id"),
                    HeaderValue::from_static("partner-low"),
                )],
            ),
        );
        // High privilege key injects x-consumer-id and x-consumer-role
        keys.insert(
            "sk-admin-root-456".to_string(),
            (
                "admin-user".to_string(),
                vec![
                    (
                        HeaderName::from_static("x-consumer-id"),
                        HeaderValue::from_static("admin-user"),
                    ),
                    (
                        HeaderName::from_static("x-consumer-role"),
                        HeaderValue::from_static("superuser"),
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
            true,
            keys,
            ApiKeyCache::new(4, 16, 60, 10),
        );

        // Low priv client sends a spoofed x-consumer-role header
        let mut headers = HeaderMap::new();
        headers.insert("x-api-key", "sk-low-priv-123".parse().unwrap());
        headers.insert("x-consumer-role", "superuser".parse().unwrap());
        let mut req = build_test_request(headers, None);

        let mut target = Arc::from("default-upstream");
        assert!(matches!(
            plugin.on_upstream_request(&mut req, &mut target),
            Action::Continue
        ));

        // Assert: x-consumer-id is partner-low, and spoofed x-consumer-role is completely stripped!
        assert_eq!(req.headers.get("x-consumer-id").unwrap(), "partner-low");
        assert!(!req.headers.contains_key("x-consumer-role"));
    }

    #[test]
    fn test_hide_credentials_strips_query_param() {
        let mut keys = FxHashMap::default();
        keys.insert(
            "secret-token-xyz".to_string(),
            ("user-1".to_string(), vec![]),
        );

        let plugin = ApiKeyAuthPlugin::new(
            "api_key_auth",
            vec![KeySource::Query {
                param: "api_key".to_string(),
            }],
            true, // hide_credentials = true
            keys,
            ApiKeyCache::new(4, 16, 60, 10),
        );

        let mut req = build_test_request(
            HeaderMap::new(),
            Some("page=1&api_key=secret-token-xyz&sort=desc"),
        );
        let mut target = Arc::from("default-upstream");
        assert!(matches!(
            plugin.on_upstream_request(&mut req, &mut target),
            Action::Continue
        ));

        // Query param api_key should be completely removed, keeping page=1&sort=desc
        assert_eq!(req.uri.query(), Some("page=1&sort=desc"));
    }

    #[test]
    fn test_length_guard_fast_reject() {
        let mut keys = FxHashMap::default();
        keys.insert(
            "valid-16byte-key".to_string(),
            ("user-1".to_string(), vec![]),
        );

        let plugin = ApiKeyAuthPlugin::new(
            "api_key_auth",
            vec![KeySource::Header {
                name: HeaderName::from_static("x-api-key"),
                prefix: String::new(),
            }],
            false,
            keys,
            ApiKeyCache::new(4, 16, 60, 10),
        );

        // 1-character token (< min_key_len) -> rejected
        let mut headers = HeaderMap::new();
        headers.insert("x-api-key", "a".parse().unwrap());
        let mut req = build_test_request(headers, None);
        let mut target = Arc::from("default-upstream");
        assert!(matches!(
            plugin.on_upstream_request(&mut req, &mut target),
            Action::Reject(RejectReason::Unauthorized)
        ));

        // 100-character token (> max_key_len) -> rejected
        let mut headers2 = HeaderMap::new();
        headers2.insert("x-api-key", "x".repeat(100).parse().unwrap());
        let mut req2 = build_test_request(headers2, None);
        let mut target2 = Arc::from("default-upstream");
        assert!(matches!(
            plugin.on_upstream_request(&mut req2, &mut target2),
            Action::Reject(RejectReason::Unauthorized)
        ));
    }

    #[test]
    fn test_constant_time_eq() {
        assert!(constant_time_eq(b"hello", b"hello"));
        assert!(!constant_time_eq(b"hello", b"hellp"));
        assert!(!constant_time_eq(b"hello", b"hell"));
        assert!(!constant_time_eq(b"hello", b"hello world"));
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
        let mut target = Arc::from("default-upstream");
        assert!(matches!(
            plugin.on_upstream_request(&mut req, &mut target),
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
