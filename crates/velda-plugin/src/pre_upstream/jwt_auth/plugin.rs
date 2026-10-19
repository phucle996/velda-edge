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
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use http::StatusCode;
use http::header::{AUTHORIZATION, HeaderName, HeaderValue};
use velda_core::{Action, L7Request, L7Response};
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
        let raw = &config.config;

        let alg_str = raw
            .get("algorithm")
            .and_then(|v| v.as_str())
            .unwrap_or("HS256");
        let algorithm = JwtAlgorithm::from_str(alg_str)?;

        let secret_key = raw.get("secret_key").and_then(|v| v.as_str());
        let public_key_pem = raw.get("public_key_pem").and_then(|v| v.as_str());
        let key = VerifierKey::new(algorithm, secret_key, public_key_pem)?;

        let mut token_sources = Vec::new();
        if let Some(sources_arr) = raw.get("token_sources").and_then(|v| v.as_array()) {
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
                                .unwrap_or("authorization");
                            let prefix = src_obj
                                .get("prefix")
                                .and_then(|v| v.as_str())
                                .unwrap_or("")
                                .to_string();
                            let name = HeaderName::from_str(name_str).map_err(|e| {
                                PluginError::Config(format!(
                                    "Invalid header name '{name_str}': {e}"
                                ))
                            })?;
                            token_sources.push(TokenSource::Header { name, prefix });
                        }
                        "cookie" => {
                            let name = src_obj
                                .get("name")
                                .and_then(|v| v.as_str())
                                .unwrap_or("access_token")
                                .to_string();
                            token_sources.push(TokenSource::Cookie { name });
                        }
                        "query" => {
                            let param = src_obj
                                .get("name")
                                .or_else(|| src_obj.get("param"))
                                .and_then(|v| v.as_str())
                                .unwrap_or("token")
                                .to_string();
                            token_sources.push(TokenSource::Query { param });
                        }
                        other => {
                            return Err(PluginError::Config(format!(
                                "Unknown token source type '{other}'"
                            )));
                        }
                    }
                }
            }
        }

        if token_sources.is_empty() {
            token_sources.push(TokenSource::Header {
                name: AUTHORIZATION,
                prefix: "Bearer ".to_string(),
            });
        }

        let validate_exp = raw
            .get("validate_exp")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);
        let clock_skew_secs = raw
            .get("clock_skew_secs")
            .and_then(|v| v.as_u64())
            .unwrap_or(60);

        let issuer = raw
            .get("issuer")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty() && *s != "none")
            .map(|s| s.to_string());

        let audience = raw
            .get("audience")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty() && *s != "none")
            .map(|s| s.to_string());

        let mut claims_to_headers = Vec::new();
        if let Some(map) = raw.get("claims_to_headers").and_then(|v| v.as_object()) {
            for (claim_path, hdr_val) in map {
                if let Some(hdr_str) = hdr_val.as_str() {
                    let hdr_name = HeaderName::from_str(hdr_str).map_err(|e| {
                        PluginError::Config(format!("Invalid target header name '{hdr_str}': {e}"))
                    })?;
                    claims_to_headers.push((claim_path.clone(), hdr_name));
                }
            }
        }

        let mut assert_claims = Vec::new();
        if let Some(map) = raw.get("assert_claims").and_then(|v| v.as_object()) {
            for (claim_path, val) in map {
                if let Some(val_str) = val.as_str() {
                    assert_claims.push((claim_path.clone(), val_str.to_string()));
                }
            }
        }

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

        let cache = JwtCache::new(
            cache_shards,
            cache_shard_capacity,
            cache_ttl_secs,
            negative_cache_ttl_secs,
        );

        Ok(Self {
            id: config.id.clone(),
            algorithm,
            key,
            token_sources,
            validate_exp,
            clock_skew_secs,
            issuer,
            audience,
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

    fn on_upstream_request(&self, req: &mut L7Request) -> Action {
        let token_str = match self.extract_token(req) {
            Some(t) => t,
            None => {
                return Action::Respond(
                    L7Response::from_bytes(
                        StatusCode::UNAUTHORIZED,
                        "Missing authentication token",
                    )
                    .with_header(
                        http::header::WWW_AUTHENTICATE,
                        HeaderValue::from_static("Bearer realm=\"velda-edge\""),
                    ),
                );
            }
        };

        let token_hash = JwtCache::hash_token(token_str);
        let now = current_epoch_secs();

        // 1. Negative Cache check: fail-fast on known bad tokens
        if self.cache.is_negative(token_hash, now) {
            return Action::Respond(
                L7Response::from_bytes(StatusCode::UNAUTHORIZED, "Invalid authentication token")
                    .with_header(
                        http::header::WWW_AUTHENTICATE,
                        HeaderValue::from_static("Bearer error=\"invalid_token\""),
                    ),
            );
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
                Action::Respond(L7Response::from_bytes(
                    StatusCode::FORBIDDEN,
                    "Access denied: claim assertion failed",
                ))
            }
            Err(err_msg) => {
                self.cache.insert_invalid(token_hash, now);
                Action::Respond(
                    L7Response::from_bytes(StatusCode::UNAUTHORIZED, err_msg).with_header(
                        http::header::WWW_AUTHENTICATE,
                        HeaderValue::from_static("Bearer error=\"invalid_token\""),
                    ),
                )
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

    #[test]
    fn test_on_upstream_request_missing_token() {
        let config = PluginConfig {
            id: "jwt_auth".into(),
            phase: "pre_upstream".into(),
            config: serde_json::json!({
                "secret_key": "my-secret-key"
            }),
        };

        let plugin = JwtAuthPlugin::from_config(&config).unwrap();
        let mut req = L7Request::new(
            Method::GET,
            Uri::from_static("/api/resource"),
            Version::HTTP_11,
            HeaderMap::new(),
            Body::Empty,
        );

        let action = plugin.on_upstream_request(&mut req);
        match action {
            Action::Respond(resp) => {
                assert_eq!(resp.status, StatusCode::UNAUTHORIZED);
                assert_eq!(
                    resp.headers.get("www-authenticate").unwrap(),
                    "Bearer realm=\"velda-edge\""
                );
            }
            _ => panic!("Expected Action::Respond(401)"),
        }
    }

    #[test]
    fn test_on_upstream_request_valid_token_with_cache() {
        let secret = "secret-12345678901234567890";
        let config = PluginConfig {
            id: "jwt_auth".into(),
            phase: "pre_upstream".into(),
            config: serde_json::json!({
                "secret_key": secret,
                "claims_to_headers": {
                    "sub": "x-user-id",
                    "roles": "x-user-roles"
                }
            }),
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
        let action1 = plugin.on_upstream_request(&mut req);
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

        let action2 = plugin.on_upstream_request(&mut req2);
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
            config: serde_json::json!({
                "secret_key": secret
            }),
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
        let action1 = plugin.on_upstream_request(&mut req);
        assert!(matches!(action1, Action::Respond(_)));

        // Second attempt: hits negative cache immediately
        let action2 = plugin.on_upstream_request(&mut req);
        match action2 {
            Action::Respond(resp) => {
                assert_eq!(resp.status, StatusCode::UNAUTHORIZED);
            }
            _ => panic!("Expected Action::Respond(401)"),
        }
    }

    #[test]
    fn test_token_from_cookie_and_query() {
        let secret = "cookie-secret";
        let config = PluginConfig {
            id: "jwt_auth".into(),
            phase: "pre_upstream".into(),
            config: serde_json::json!({
                "secret_key": secret,
                "token_sources": [
                    { "type": "cookie", "name": "auth_cookie" },
                    { "type": "query", "param": "access_token" }
                ],
                "claims_to_headers": { "sub": "x-user-id" }
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
        let action = plugin.on_upstream_request(&mut req);
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
        let action_query = plugin.on_upstream_request(&mut req_query);
        assert!(matches!(action_query, Action::Continue));
        assert_eq!(req_query.headers.get("x-user-id").unwrap(), "cookie_user");
    }

    #[test]
    fn test_malformed_multibyte_utf8_auth_header_does_not_panic() {
        let config = PluginConfig {
            id: "jwt_auth".into(),
            phase: "pre_upstream".into(),
            config: serde_json::json!({
                "secret_key": "my-secret-key"
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
        let action = plugin.on_upstream_request(&mut req);
        match action {
            Action::Respond(resp) => {
                assert_eq!(resp.status, StatusCode::UNAUTHORIZED);
            }
            _ => panic!("Expected Action::Respond(401)"),
        }
    }
}
