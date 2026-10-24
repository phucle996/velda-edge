//! JWT Authentication Pre-Upstream Plugin (`phase: "pre_upstream"`).
//!
//! # Execution Invariants
//! - **Route-Scoped Authentication**: Public routes omit this plugin, while protected routes attach it.
//! - **Fail-Fast Defense**: Rejects invalid, missing, or expired tokens with `401 Unauthorized` before
//!   leasing backend connections or streaming request bodies.
//! - **Dual Caching**: Hits positive cache in ~20ns; caches invalid tokens in negative cache to defeat
//!   cryptographic exhaustion attacks.
//! - **Flexible Payloads & Claims Mapping**: Injects extracted claims into downstream request headers for
//!   consumption by subsequent plugins (e.g., user-scoped `rate_limit`) and backend upstreams.

use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use http::header::{HeaderName, HeaderValue};
use velda_core::{Action, L7Request, RejectReason};
use velda_sync::post_sync::plugin::PluginConfig;

use super::cache::JwtCache;
use super::crypto::{JwtAlgorithm, JwtValidationPolicy, VerifierKey, verify_jwt};
use crate::capability::{AccessLevel, Direction, PluginCapability, SupportedProtocols};
use crate::error::PluginError;
use crate::pre_upstream::PreUpstreamPlugin;

/// Source location from which to extract the JWT token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TokenSource {
    /// Extracts token from an HTTP header with an optional prefix (e.g. `Authorization: Bearer <token>`).
    Header { name: HeaderName, prefix: String },
    /// Extracts token from an HTTP Cookie (e.g. `Cookie: access_token=<token>`).
    Cookie { name: String },
    /// Extracts token from a URL query parameter (e.g. `?token=<token>`).
    Query { param: String },
}

/// Production JWT authentication and claim injection plugin.
pub struct JwtAuthPlugin {
    pub id: String,
    pub algorithm: JwtAlgorithm,
    pub key: VerifierKey,
    pub token_sources: Vec<TokenSource>,
    pub validate_exp: bool,
    pub clock_skew_secs: u64,
    pub issuer: Option<String>,
    pub audience: Option<String>,
    pub claims_to_headers: Vec<(String, HeaderName)>,
    pub assert_claims: Vec<(String, String)>,
    pub cache: JwtCache,
}

impl JwtAuthPlugin {
    /// Deserializes and constructs an instance directly from a [`PluginConfig`].
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let cfg =
            velda_sync::validate_jwt_auth_config(&config.id, &config.config).map_err(|e| {
                PluginError::Config(format!(
                    "Invalid jwt_auth configuration for '{}': {e}",
                    config.id
                ))
            })?;

        let algorithm = JwtAlgorithm::from_str(&cfg.algorithm)?;
        let key = VerifierKey::new(
            algorithm,
            cfg.secret_key.as_deref(),
            cfg.public_key_pem.as_deref(),
        )?;

        let mut token_sources = Vec::with_capacity(cfg.token_sources.len());
        for src in &cfg.token_sources {
            match src {
                velda_sync::JwtTokenSourceConfig::Header { name, prefix } => {
                    let hname = HeaderName::from_str(name).map_err(|e| {
                        PluginError::Config(format!(
                            "Invalid header name '{name}' for plugin '{}': {e}",
                            config.id
                        ))
                    })?;
                    token_sources.push(TokenSource::Header {
                        name: hname,
                        prefix: prefix.clone().unwrap_or_default(),
                    });
                }
                velda_sync::JwtTokenSourceConfig::Cookie { name } => {
                    token_sources.push(TokenSource::Cookie { name: name.clone() });
                }
                velda_sync::JwtTokenSourceConfig::Query { param } => {
                    token_sources.push(TokenSource::Query {
                        param: param.clone(),
                    });
                }
            }
        }

        let mut claims_to_headers = Vec::with_capacity(cfg.claims_to_headers.len());
        for (claim_path, hdr_str) in &cfg.claims_to_headers {
            let hdr_name = HeaderName::from_str(hdr_str).map_err(|e| {
                PluginError::Config(format!(
                    "Invalid target header name '{hdr_str}' for plugin '{}': {e}",
                    config.id
                ))
            })?;
            claims_to_headers.push((claim_path.clone(), hdr_name));
        }

        let mut assert_claims = Vec::with_capacity(cfg.assert_claims.len());
        for (claim_path, val_str) in &cfg.assert_claims {
            assert_claims.push((claim_path.clone(), val_str.clone()));
        }

        let cache = JwtCache::new(64, 2048, cfg.cache_ttl_secs, 10);

        Ok(Self {
            id: config.id.clone(),
            algorithm,
            key,
            token_sources,
            validate_exp: cfg.validate_exp,
            clock_skew_secs: cfg.clock_skew_secs,
            issuer: cfg.issuer,
            audience: cfg.audience,
            claims_to_headers,
            assert_claims,
            cache,
        })
    }

    /// Extracts the raw token string from the request using configured token sources.
    fn extract_token<'a>(&self, req: &'a L7Request) -> Option<&'a str> {
        for source in &self.token_sources {
            match source {
                TokenSource::Header { name, prefix } => {
                    if let Some(val) = req.headers.get(name)
                        && let Ok(s) = val.to_str()
                    {
                        let trimmed = s.trim();
                        if prefix.is_empty() {
                            if !trimmed.is_empty() {
                                return Some(trimmed);
                            }
                        } else if trimmed.len() > prefix.len()
                            && trimmed.is_char_boundary(prefix.len())
                            && trimmed[..prefix.len()].eq_ignore_ascii_case(prefix)
                        {
                            let token = trimmed[prefix.len()..].trim();
                            if !token.is_empty() {
                                return Some(token);
                            }
                        }
                    }
                }
                TokenSource::Cookie { name } => {
                    if let Some(val) = req.headers.get(http::header::COOKIE)
                        && let Ok(cookie_str) = val.to_str()
                    {
                        for pair in cookie_str.split(';') {
                            if let Some((k, v)) = pair.trim().split_once('=')
                                && k.trim() == name
                            {
                                let token = v.trim();
                                if !token.is_empty() {
                                    return Some(token);
                                }
                            }
                        }
                    }
                }
                TokenSource::Query { param } => {
                    if let Some(query) = req.uri.query() {
                        for pair in query.split('&') {
                            if let Some((k, v)) = pair.split_once('=')
                                && k == param
                            {
                                let token = v.trim();
                                if !token.is_empty() {
                                    return Some(token);
                                }
                            }
                        }
                    }
                }
            }
        }
        None
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

impl PreUpstreamPlugin for JwtAuthPlugin {
    #[inline]
    fn id(&self) -> &str {
        &self.id
    }

    fn on_upstream_request(&self, req: &mut L7Request, _target_upstream: &mut Arc<str>) -> Action {
        let token_str = match self.extract_token(req) {
            Some(t) => t,
            None => {
                req.headers.insert_bypass(
                    http::header::WWW_AUTHENTICATE,
                    HeaderValue::from_static("Bearer realm=\"velda-edge\""),
                );
                return Action::Reject(RejectReason::Unauthorized);
            }
        };

        let token_hash = JwtCache::hash_token(token_str);
        let now = current_epoch_secs();

        // 1. Negative Cache check: fail-fast on known bad tokens
        if self.cache.is_negative(token_hash, now) {
            req.headers.insert_bypass(
                http::header::WWW_AUTHENTICATE,
                HeaderValue::from_static("Bearer error=\"invalid_token\""),
            );
            return Action::Reject(RejectReason::Unauthorized);
        }

        // 2. Positive Cache check: ultra-fast (~20ns) zero-allocation claims injection
        if let Some(cached_headers) = self.cache.get_valid(token_hash, now) {
            for (name, val) in cached_headers {
                req.headers.insert(name, val);
            }
            return Action::Continue;
        }

        // 3. Cache Miss: verify signature and business policies
        let policy = JwtValidationPolicy {
            expected_alg: self.algorithm,
            key: &self.key,
            validate_exp: self.validate_exp,
            clock_skew_secs: self.clock_skew_secs,
            expected_issuer: self.issuer.as_deref(),
            expected_audience: self.audience.as_deref(),
            claims_to_headers: &self.claims_to_headers,
            assert_claims: &self.assert_claims,
        };

        match verify_jwt(token_str, &policy, now) {
            Ok(verified) => {
                for (name, val) in &verified.injected_headers {
                    req.headers.insert(name.clone(), val.clone());
                }
                self.cache.insert_valid(
                    token_hash,
                    verified.exp_epoch_secs,
                    verified.injected_headers,
                    now,
                );
                Action::Continue
            }
            Err("Claim assertion failed") => {
                // Claim mismatch results in 403 Forbidden
                Action::Reject(RejectReason::Forbidden)
            }
            Err(_err_msg) => {
                self.cache.insert_invalid(token_hash, now);
                req.headers.insert_bypass(
                    http::header::WWW_AUTHENTICATE,
                    HeaderValue::from_static("Bearer error=\"invalid_token\""),
                );
                Action::Reject(RejectReason::Unauthorized)
            }
        }
    }

    #[inline]
    fn capability(&self) -> PluginCapability {
        PluginCapability::header_only(AccessLevel::Write)
    }

    #[inline]
    fn supported_protocols(&self) -> SupportedProtocols {
        SupportedProtocols::ALL_L7
    }

    #[inline(always)]
    fn direction(&self) -> Direction {
        Direction::Request
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::prelude::*;
    use http::header::AUTHORIZATION;
    use http::{HeaderMap, Method, Uri, Version};
    use ring::hmac;
    use velda_core::Body;

    fn create_hs256_token(secret: &str, payload_json: &str) -> String {
        let header = r#"{"alg":"HS256","typ":"JWT"}"#;
        let h_b64 = BASE64_URL_SAFE_NO_PAD.encode(header.as_bytes());
        let p_b64 = BASE64_URL_SAFE_NO_PAD.encode(payload_json.as_bytes());
        let signing_input = format!("{h_b64}.{p_b64}");

        let key = hmac::Key::new(hmac::HMAC_SHA256, secret.as_bytes());
        let tag = hmac::sign(&key, signing_input.as_bytes());
        let s_b64 = BASE64_URL_SAFE_NO_PAD.encode(tag.as_ref());

        format!("{signing_input}.{s_b64}")
    }

    fn default_jwt_json(secret: &str, claims_to_headers: serde_json::Value) -> serde_json::Value {
        serde_json::json!({
            "algorithm": "HS256",
            "secret_key": secret,
            "public_key_pem": null,
            "token_sources": [
                {
                    "type": "header",
                    "name": "authorization",
                    "prefix": "Bearer "
                }
            ],
            "validate_exp": true,
            "clock_skew_secs": 60,
            "issuer": null,
            "audience": null,
            "claims_to_headers": claims_to_headers,
            "assert_claims": {},
            "cache_shards": 16,
            "cache_shard_capacity": 512,
            "cache_ttl_secs": 300,
            "negative_cache_ttl_secs": 10
        })
    }

    #[test]
    fn test_on_upstream_request_missing_token() {
        let config = PluginConfig {
            id: "jwt_auth".into(),
            phase: "pre_upstream".into(),
            config: default_jwt_json("my-secret-key", serde_json::json!({})),
        };

        let plugin = JwtAuthPlugin::from_config(&config).unwrap();
        let mut req = L7Request::new(
            Method::GET,
            Uri::from_static("/api/resource"),
            Version::HTTP_11,
            HeaderMap::new(),
            Body::Empty,
        );

        let mut target = Arc::from("default-upstream");
        let action = plugin.on_upstream_request(&mut req, &mut target);
        assert!(matches!(action, Action::Reject(RejectReason::Unauthorized)));
        assert_eq!(
            req.headers.get("www-authenticate").unwrap(),
            "Bearer realm=\"velda-edge\""
        );
    }

    #[test]
    fn test_on_upstream_request_valid_token_with_cache() {
        let secret = "secret-12345678901234567890";
        let config = PluginConfig {
            id: "jwt_auth".into(),
            phase: "pre_upstream".into(),
            config: default_jwt_json(
                secret,
                serde_json::json!({
                    "sub": "x-user-id",
                    "roles": "x-user-roles"
                }),
            ),
        };

        let plugin = JwtAuthPlugin::from_config(&config).unwrap();
        let token = create_hs256_token(
            secret,
            r#"{"sub":"user_123","roles":["admin","billing"],"exp":2500000000}"#,
        );

        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
        );

        let mut req = L7Request::new(
            Method::GET,
            Uri::from_static("/api/resource"),
            Version::HTTP_11,
            headers,
            Body::Empty,
        );

        // First invocation: Cache Miss -> Cryptographic verification -> Cache Populate
        let mut target1 = Arc::from("default-upstream");
        let action1 = plugin.on_upstream_request(&mut req, &mut target1);
        assert!(matches!(action1, Action::Continue));
        assert_eq!(req.headers.get("x-user-id").unwrap(), "user_123");
        assert_eq!(req.headers.get("x-user-roles").unwrap(), "admin, billing");

        // Second invocation with fresh request: Positive Cache Hit
        let mut req2 = L7Request::new(
            Method::GET,
            Uri::from_static("/api/resource"),
            Version::HTTP_11,
            HeaderMap::from_iter([(
                AUTHORIZATION,
                HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
            )]),
            Body::Empty,
        );

        let mut target2 = Arc::from("default-upstream");
        let action2 = plugin.on_upstream_request(&mut req2, &mut target2);
        assert!(matches!(action2, Action::Continue));
        assert_eq!(req2.headers.get("x-user-id").unwrap(), "user_123");
        assert_eq!(req2.headers.get("x-user-roles").unwrap(), "admin, billing");
    }

    #[test]
    fn test_on_upstream_request_negative_cache() {
        let secret = "secret-12345678901234567890";
        let config = PluginConfig {
            id: "jwt_auth".into(),
            phase: "pre_upstream".into(),
            config: default_jwt_json(secret, serde_json::json!({})),
        };

        let plugin = JwtAuthPlugin::from_config(&config).unwrap();
        let bad_token = "invalid.token.signature";

        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {bad_token}")).unwrap(),
        );

        let mut req = L7Request::new(
            Method::GET,
            Uri::from_static("/api/resource"),
            Version::HTTP_11,
            headers,
            Body::Empty,
        );

        // First attempt: validation fails and gets added to negative cache
        let mut target1 = Arc::from("default-upstream");
        let action1 = plugin.on_upstream_request(&mut req, &mut target1);
        assert!(matches!(
            action1,
            Action::Reject(RejectReason::Unauthorized)
        ));
        assert_eq!(
            req.headers.get("www-authenticate").unwrap(),
            "Bearer error=\"invalid_token\""
        );

        // Second attempt: hits negative cache immediately
        let mut target2 = Arc::from("default-upstream");
        let action2 = plugin.on_upstream_request(&mut req, &mut target2);
        assert!(matches!(
            action2,
            Action::Reject(RejectReason::Unauthorized)
        ));
        assert_eq!(
            req.headers.get("www-authenticate").unwrap(),
            "Bearer error=\"invalid_token\""
        );
    }

    #[test]
    fn test_token_from_cookie_and_query() {
        let secret = "cookie-secret";
        let config = PluginConfig {
            id: "jwt_auth".into(),
            phase: "pre_upstream".into(),
            config: serde_json::json!({
                "algorithm": "HS256",
                "secret_key": secret,
                "public_key_pem": null,
                "token_sources": [
                    { "type": "cookie", "name": "auth_cookie" },
                    { "type": "query", "param": "access_token" }
                ],
                "validate_exp": true,
                "clock_skew_secs": 60,
                "issuer": null,
                "audience": null,
                "claims_to_headers": { "sub": "x-user-id" },
                "assert_claims": {},
                "cache_ttl_secs": 300
            }),
        };

        let plugin = JwtAuthPlugin::from_config(&config).unwrap();
        let token = create_hs256_token(secret, r#"{"sub":"cookie_user","exp":2500000000}"#);

        // Test cookie extraction
        let mut headers = HeaderMap::new();
        headers.insert(
            http::header::COOKIE,
            HeaderValue::from_str(&format!("theme=dark; auth_cookie={token}; lang=en")).unwrap(),
        );
        let mut req = L7Request::new(
            Method::GET,
            Uri::from_static("/api/profile"),
            Version::HTTP_11,
            headers,
            Body::Empty,
        );
        let mut target = Arc::from("default-upstream");
        let action = plugin.on_upstream_request(&mut req, &mut target);
        assert!(matches!(action, Action::Continue));
        assert_eq!(req.headers.get("x-user-id").unwrap(), "cookie_user");

        // Test query param extraction
        let query_uri = format!("/api/profile?access_token={token}&format=json")
            .parse::<Uri>()
            .unwrap();
        let mut req_query = L7Request::new(
            Method::GET,
            query_uri,
            Version::HTTP_11,
            HeaderMap::new(),
            Body::Empty,
        );
        let mut target_q = Arc::from("default-upstream");
        let action_query = plugin.on_upstream_request(&mut req_query, &mut target_q);
        assert!(matches!(action_query, Action::Continue));
        assert_eq!(req_query.headers.get("x-user-id").unwrap(), "cookie_user");
    }

    #[test]
    fn test_malformed_multibyte_utf8_auth_header_does_not_panic() {
        let config = PluginConfig {
            id: "jwt_auth".into(),
            phase: "pre_upstream".into(),
            config: serde_json::json!({
                "algorithm": "HS256",
                "secret_key": "my-secret-key",
                "public_key_pem": null,
                "token_sources": [
                    { "type": "header", "name": "authorization", "prefix": "Bearer " }
                ],
                "validate_exp": true,
                "clock_skew_secs": 60,
                "issuer": null,
                "audience": null,
                "claims_to_headers": {},
                "assert_claims": {},
                "cache_ttl_secs": 300
            }),
        };

        let plugin = JwtAuthPlugin::from_config(&config).unwrap();

        // 3-byte UTF-8 character (e.g. 'Tiếng Việt' or '日') at the boundary
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str("Tiếng_Việt_Bearer_Token").unwrap(),
        );

        let mut req = L7Request::new(
            Method::GET,
            Uri::from_static("/api/resource"),
            Version::HTTP_11,
            headers,
            Body::Empty,
        );

        // Must reject cleanly with 401 without panicking!
        let mut target = Arc::from("default-upstream");
        let action = plugin.on_upstream_request(&mut req, &mut target);
        assert!(matches!(action, Action::Reject(RejectReason::Unauthorized)));
        assert_eq!(
            req.headers.get("www-authenticate").unwrap(),
            "Bearer realm=\"velda-edge\""
        );
    }
}
