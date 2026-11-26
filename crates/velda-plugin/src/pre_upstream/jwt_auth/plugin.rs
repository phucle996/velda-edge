//! JWT Authentication Pre-Upstream Plugin (`jwt_auth::plugin`).
//!
//! ```text
//!                          JWT AUTH PRE-UPSTREAM WORKFLOW
//!
//!                      Inbound Request (`L7Request`, L2 Warm)
//!                                       │
//!                                       ▼
//!                   1. Prefetch L1D Control Line (`_MM_HINT_T0`)
//!                   2. Extract Token via Pre-Hashed `HeaderKey` / Cookie / Query
//!                         ├── (Missing)      ──► 401 + `Bearer realm="velda-edge"`
//!                         └── (Bad 2-dot)    ──► 401 + `Bearer error="invalid_token"`
//!                                       │
//!                                       ▼
//!                   3. Convert JWT -> `fp: u128` (`SHA-256(boot_salt || jwt)`)
//!                   4. Bit-Shift -> `_MM_HINT_T2` L3 Bucket -> 64B SIMD Scan:
//!                         ├── [Positive Hit] ──► `insert_prehashed` claims & Continue
//!                         ├── [Reject 401]   ──► 401 + `Bearer error="invalid_token"`
//!                         ├── [Reject 403]   ──► 403 Forbidden
//!                         └── [Cache Miss]
//!                                       │
//!                                       ▼
//!                   5. Load `SharedJwksStore` (`ArcSwap` + `epoch`)
//!                   6. Constant-Time Verify (`HMAC` / `RSA` / `ECDSA` / `EdDSA`)
//!                         ├── (Valid)        ──► Commit L3 Positive + Inject Claims
//!                         ├── (Assert Fail)  ──► Commit L3 Forbidden + 403 Forbidden
//!                         ├── (NoActiveKeys) ──► 401 (Skip Negative Cache during boot)
//!                         └── (Invalid)      ──► Commit L3 Unauthorized + 401
//! ```

use std::str::FromStr;
use std::sync::Arc;

use http::header::{HeaderName, HeaderValue};
use velda_core::{Action, HeaderKey, L7Request, RejectReason};

use super::cache::{
    CacheLookup, DEFAULT_GROUPS_PER_SHARD, DEFAULT_SHARD_COUNT, JwtCache, is_plausible_jwt_shape,
};
use super::crypto::{
    ClaimPath, JwtAlgorithm, JwtValidationPolicy, JwtVerifyError, VerifierKey, verify_jwt,
};
use super::table::{JwksKeyTable, SharedJwksStore};
use crate::capability::{AccessLevel, Direction, PluginCapability, SupportedProtocols};
use crate::config::PluginConfig;
use crate::error::PluginError;
use crate::pre_upstream::PreUpstreamPlugin;

const KEY_COOKIE: HeaderKey = HeaderKey::from_static("cookie");
const WWW_AUTH_MISSING: HeaderValue = HeaderValue::from_static("Bearer realm=\"velda-edge\"");
const WWW_AUTH_INVALID: HeaderValue = HeaderValue::from_static("Bearer error=\"invalid_token\"");

/// Pre-compiled token extraction source using pre-hashed [`HeaderKey`] descriptors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TokenSource {
    /// Extracts token from HTTP header(s) with an optional case-insensitive prefix (e.g. `"Bearer "`).
    Header { key: HeaderKey, prefix: Box<str> },
    /// Extracts token from HTTP `Cookie` header(s) (both `;`-joined HTTP/1.1 and split HTTP/2/3 fields).
    Cookie { name: Box<str> },
    /// Extracts token from a URL query parameter (e.g. `?token=<token>`).
    Query { param: Box<str> },
}

/// Production JWT authentication and claim projection plugin.
///
/// Cache Line 0 (`0x00..0x40`) embeds the 64-byte [`JwtCache`] L1D control block directly inline.
#[repr(C, align(64))]
pub struct JwtAuthPlugin {
    pub cache: JwtCache,
    pub algorithm: JwtAlgorithm,
    pub validate_exp: bool,
    has_static_key: bool,
    pub clock_skew_secs: u64,
    pub key_store: Arc<SharedJwksStore>,
    pub token_sources: Box<[TokenSource]>,
    pub issuer: Option<Box<str>>,
    pub audience: Option<Box<str>>,
    pub claims_to_headers: Box<[(ClaimPath, HeaderKey)]>,
    pub assert_claims: Box<[(ClaimPath, Box<str>)]>,
    pub id: String,
}

impl JwtAuthPlugin {
    /// Deserializes and compiles an instance directly from a [`PluginConfig`].
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let cfg = super::validation::validate_jwt_auth_config(&config.id, &config.config).map_err(
            |e| {
                PluginError::Config(format!(
                    "Invalid jwt_auth configuration for '{}': {e}",
                    config.id
                ))
            },
        )?;

        let algorithm = JwtAlgorithm::from_str(&cfg.algorithm)?;
        let (key_table, has_static_key) =
            if algorithm.is_symmetric() || cfg.public_key_pem.is_some() {
                let key = VerifierKey::new(
                    algorithm,
                    cfg.secret_key.as_deref(),
                    cfg.public_key_pem.as_deref(),
                )?;
                (JwksKeyTable::with_static_key(key, algorithm), true)
            } else {
                // Companion `jwks_rotator` mode: initialized empty until rotator populates keys
                (JwksKeyTable::empty(), false)
            };
        let key_store = Arc::new(SharedJwksStore::new(key_table));

        let mut token_sources = Vec::with_capacity(cfg.token_sources.len());
        for src in &cfg.token_sources {
            match src {
                super::validation::JwtTokenSourceConfig::Header { name, prefix } => {
                    let hname = HeaderName::from_str(name).map_err(|e| {
                        PluginError::Config(format!(
                            "Invalid header name '{name}' for plugin '{}': {e}",
                            config.id
                        ))
                    })?;
                    token_sources.push(TokenSource::Header {
                        key: HeaderKey::from_name(hname),
                        prefix: prefix.clone().unwrap_or_default().into_boxed_str(),
                    });
                }
                super::validation::JwtTokenSourceConfig::Cookie { name } => {
                    token_sources.push(TokenSource::Cookie {
                        name: name.clone().into_boxed_str(),
                    });
                }
                super::validation::JwtTokenSourceConfig::Query { param } => {
                    token_sources.push(TokenSource::Query {
                        param: param.clone().into_boxed_str(),
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
            claims_to_headers.push((
                ClaimPath::compile(claim_path),
                HeaderKey::from_name(hdr_name),
            ));
        }

        let mut assert_claims = Vec::with_capacity(cfg.assert_claims.len());
        for (claim_path, val_str) in &cfg.assert_claims {
            assert_claims.push((
                ClaimPath::compile(claim_path),
                val_str.clone().into_boxed_str(),
            ));
        }

        let negative_ttl = if cfg.cache_ttl_secs == 0 {
            0
        } else {
            10.min(cfg.cache_ttl_secs)
        };
        let cache = JwtCache::new(
            DEFAULT_SHARD_COUNT,
            DEFAULT_GROUPS_PER_SHARD,
            cfg.cache_ttl_secs,
            negative_ttl,
        );

        Ok(Self {
            cache,
            algorithm,
            validate_exp: cfg.validate_exp,
            has_static_key,
            clock_skew_secs: cfg.clock_skew_secs,
            key_store,
            token_sources: token_sources.into_boxed_slice(),
            issuer: cfg.issuer.map(String::into_boxed_str),
            audience: cfg.audience.map(String::into_boxed_str),
            claims_to_headers: claims_to_headers.into_boxed_slice(),
            assert_claims: assert_claims.into_boxed_slice(),
            id: config.id.clone(),
        })
    }

    /// Returns the shared JWKS key store for this plugin instance.
    #[inline(always)]
    pub fn key_store(&self) -> &Arc<SharedJwksStore> {
        &self.key_store
    }

    /// Replaces the key store with an externally shared store.
    pub fn with_shared_key_store(mut self, store: Arc<SharedJwksStore>) -> Self {
        self.key_store = store;
        self
    }

    #[cold]
    #[inline(never)]
    fn reject_missing_token(req: &mut L7Request) -> Action {
        req.headers
            .insert_bypass(http::header::WWW_AUTHENTICATE, WWW_AUTH_MISSING);
        Action::Reject(RejectReason::Unauthorized)
    }

    #[cold]
    #[inline(never)]
    fn reject_invalid_token(req: &mut L7Request) -> Action {
        req.headers
            .insert_bypass(http::header::WWW_AUTHENTICATE, WWW_AUTH_INVALID);
        Action::Reject(RejectReason::Unauthorized)
    }

    /// Extracts the raw token slice from `L7Request` using pre-hashed [`HeaderKey`] lookups.
    #[inline]
    fn extract_token<'a>(&self, req: &'a L7Request) -> Option<&'a str> {
        for source in &self.token_sources {
            match source {
                TokenSource::Header { key, prefix } => {
                    for val in req.headers.get_all_prehashed(key) {
                        let Ok(s) = val.to_str() else {
                            continue;
                        };
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
                    for val in req.headers.get_all_prehashed(&KEY_COOKIE) {
                        let Ok(cookie_str) = val.to_str() else {
                            continue;
                        };
                        for pair in cookie_str.split(';') {
                            if let Some((k, v)) = pair.trim().split_once('=')
                                && k.trim() == name.as_ref()
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
                                && k == param.as_ref()
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

impl PreUpstreamPlugin for JwtAuthPlugin {
    #[inline]
    fn id(&self) -> &str {
        &self.id
    }

    #[inline(always)]
    fn pbww(&self) -> u32 {
        0
    }

    fn on_upstream_request(&self, req: &mut L7Request, _target_upstream: &mut u16) -> Action {
        // 1. Pull 64B JwtCache control block (`boot_salt` + bit-shift masks) into L1D (`_MM_HINT_T0`)
        //    while scanning L2-resident `HeaderWorkspace` for the inbound token.
        self.cache.prefetch_control_l1();

        let Some(token_str) = self.extract_token(req) else {
            return Self::reject_missing_token(req);
        };

        // 2. Fast structural 2-dot guard: drop malformed garbage before hashing or touching L3 cache
        if !is_plausible_jwt_shape(token_str) {
            return Self::reject_invalid_token(req);
        }

        let now = req.ingress_time_sec();
        let key_epoch = self.key_store.epoch();
        let cache_enabled = self.cache.is_enabled();

        // 3. Convert JWT -> `fp: u128` and query bit-shifted L3 SIMD bucket
        let fp = if cache_enabled {
            let fp = self.cache.token_to_u128(token_str);
            match self.cache.lookup(fp, now, key_epoch) {
                CacheLookup::Positive(cached_headers) => {
                    if let Some(headers) = cached_headers.as_deref() {
                        for (key, val) in headers {
                            req.headers.insert_prehashed(key, val.clone());
                        }
                    }
                    return Action::Continue;
                }
                CacheLookup::Unauthorized => return Self::reject_invalid_token(req),
                CacheLookup::Forbidden => return Action::Reject(RejectReason::Forbidden),
                CacheLookup::Miss => fp,
            }
        } else {
            0
        };

        // 4. Cache Miss: verify cryptographic signature and claim policies
        let table_guard = self.key_store.load();
        let policy = JwtValidationPolicy {
            expected_alg: self.algorithm,
            key_table: &table_guard,
            validate_exp: self.validate_exp,
            clock_skew_secs: self.clock_skew_secs,
            expected_issuer: self.issuer.as_deref(),
            expected_audience: self.audience.as_deref(),
            claims_to_headers: &self.claims_to_headers,
            assert_claims: &self.assert_claims,
        };

        match verify_jwt(token_str, &policy, now) {
            Ok(verified) => {
                if let Some(headers) = verified.injected_headers.as_deref() {
                    for (key, val) in headers {
                        req.headers.insert_prehashed(key, val.clone());
                    }
                }
                if cache_enabled {
                    self.cache.insert_valid(
                        fp,
                        verified.exp_epoch_secs,
                        verified.injected_headers,
                        now,
                        key_epoch,
                    );
                }
                Action::Continue
            }
            Err(err @ JwtVerifyError::ClaimAssertionFailed) => {
                tracing::debug!(plugin_id = %self.id, error = ?err, "JWT rejected by claim assertion");
                if cache_enabled {
                    self.cache.insert_reject(fp, true, now, key_epoch);
                }
                Action::Reject(RejectReason::Forbidden)
            }
            Err(JwtVerifyError::NoActiveKeys) => {
                tracing::debug!(plugin_id = %self.id, "JWT rejected: JWKS key table empty");
                // Initial background JWKS fetch has not populated keys yet; do not poison negative cache
                Self::reject_invalid_token(req)
            }
            Err(err) => {
                tracing::debug!(plugin_id = %self.id, error = ?err, now = now, "JWT verification failed");
                if cache_enabled {
                    self.cache.insert_reject(fp, false, now, key_epoch);
                }
                Self::reject_invalid_token(req)
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

    #[inline(always)]
    fn shared_jwks_store(&self) -> Option<Arc<SharedJwksStore>> {
        Some(Arc::clone(&self.key_store))
    }

    #[inline(always)]
    fn requires_external_jwks(&self) -> bool {
        !self.has_static_key
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::prelude::*;
    use http::header::AUTHORIZATION;
    use http::{Method, Uri, Version};
    use ring::hmac;
    use velda_core::{Body, HeaderWorkspace};

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
            "cache_ttl_secs": 300
        })
    }

    #[test]
    fn test_on_upstream_request_missing_token() {
        let config = PluginConfig {
            id: "jwt_auth".into(),
            config: default_jwt_json("my-secret-key", serde_json::json!({})),
        };

        let plugin = JwtAuthPlugin::from_config(&config).unwrap();
        let mut req = L7Request::new(
            Method::GET,
            Uri::from_static("/api/resource"),
            Version::HTTP_11,
            HeaderWorkspace::empty(4),
            Body::Empty,
        );

        let mut target = 1;
        let action = plugin.on_upstream_request(&mut req, &mut target);
        assert!(matches!(action, Action::Reject(RejectReason::Unauthorized)));
        assert_eq!(
            req.headers.get("www-authenticate").unwrap(),
            "Bearer realm=\"velda-edge\""
        );
    }

    #[test]
    fn test_valid_token_cache_hit_and_missing_optional_claim_preserves_client_headers() {
        let secret = "secret-12345678901234567890";
        let config = PluginConfig {
            id: "jwt_auth".into(),
            config: default_jwt_json(
                secret,
                serde_json::json!({
                    "sub": "x-user-id",
                    "roles": "x-user-roles",
                    "preferred_lang": "accept-language"
                }),
            ),
        };

        let plugin = JwtAuthPlugin::from_config(&config).unwrap();
        // Token includes `sub` and `roles`, omits `preferred_lang`
        let token = create_hs256_token(
            secret,
            r#"{"sub":"user_123","roles":["admin","billing"],"exp":2500000000}"#,
        );

        let mut headers = HeaderWorkspace::empty(6);
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
        );
        headers.insert(
            HeaderName::from_static("x-user-id"),
            HeaderValue::from_static("old_val_overwritten"),
        );
        headers.insert(
            http::header::ACCEPT_LANGUAGE,
            HeaderValue::from_static("vi-VN"),
        );

        let mut req = L7Request::new(
            Method::GET,
            Uri::from_static("/api/resource"),
            Version::HTTP_11,
            headers,
            Body::Empty,
        );

        // 1. Cold verify -> overwrites present claim header (`x-user-id`), adds `x-user-roles`,
        //    and leaves absent optional claim header (`accept-language`) untouched!
        let mut target1 = 1;
        let action1 = plugin.on_upstream_request(&mut req, &mut target1);
        assert!(matches!(action1, Action::Continue));
        assert_eq!(req.headers.get("x-user-id").unwrap(), "user_123");
        assert_eq!(req.headers.get("x-user-roles").unwrap(), "admin, billing");
        assert_eq!(req.headers.get("accept-language").unwrap(), "vi-VN");

        // 2. Second request -> L3 SIMD Positive Cache Hit
        let mut req2 = L7Request::new(
            Method::GET,
            Uri::from_static("/api/resource"),
            Version::HTTP_11,
            HeaderWorkspace::from_iter([(
                AUTHORIZATION,
                HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
            )]),
            Body::Empty,
        );

        let mut target2 = 1;
        let action2 = plugin.on_upstream_request(&mut req2, &mut target2);
        assert!(matches!(action2, Action::Continue));
        assert_eq!(req2.headers.get("x-user-id").unwrap(), "user_123");
        assert_eq!(req2.headers.get("x-user-roles").unwrap(), "admin, billing");
    }

    #[test]
    fn test_negative_and_forbidden_caching_plus_key_rotation_invalidation() {
        let secret1 = "secret-11111111111111111111";
        let secret2 = "secret-22222222222222222222";
        let config = PluginConfig {
            id: "jwt_auth".into(),
            config: serde_json::json!({
                "algorithm": "HS256",
                "secret_key": secret1,
                "token_sources": [{ "type": "header", "name": "authorization", "prefix": "Bearer " }],
                "validate_exp": true,
                "clock_skew_secs": 60,
                "claims_to_headers": {},
                "assert_claims": { "role": "admin" },
                "cache_ttl_secs": 300
            }),
        };

        let plugin = JwtAuthPlugin::from_config(&config).unwrap();
        let tok_admin =
            create_hs256_token(secret1, r#"{"sub":"u1","role":"admin","exp":2500000000}"#);
        let tok_viewer =
            create_hs256_token(secret1, r#"{"sub":"u2","role":"viewer","exp":2500000000}"#);

        let make_req = |tok: &str| {
            L7Request::new(
                Method::GET,
                Uri::from_static("/admin"),
                Version::HTTP_11,
                HeaderWorkspace::from_iter([(
                    AUTHORIZATION,
                    HeaderValue::from_str(&format!("Bearer {tok}")).unwrap(),
                )]),
                Body::Empty,
            )
        };

        let mut target = 1;
        // Valid admin passes and populates Positive Cache
        assert!(matches!(
            plugin.on_upstream_request(&mut make_req(&tok_admin), &mut target),
            Action::Continue
        ));
        // Viewer fails assert_claims -> 403 Forbidden (cached in L3 reject bit)
        assert!(matches!(
            plugin.on_upstream_request(&mut make_req(&tok_viewer), &mut target),
            Action::Reject(RejectReason::Forbidden)
        ));
        assert!(matches!(
            plugin.on_upstream_request(&mut make_req(&tok_viewer), &mut target),
            Action::Reject(RejectReason::Forbidden)
        ));

        // Rotate SharedJwksStore to secret2 -> epoch increments, revoking cached tok_admin immediately!
        let new_key = VerifierKey::new(JwtAlgorithm::HS256, Some(secret2), None).unwrap();
        plugin
            .key_store()
            .update(JwksKeyTable::with_static_key(new_key, JwtAlgorithm::HS256));

        assert!(matches!(
            plugin.on_upstream_request(&mut make_req(&tok_admin), &mut target),
            Action::Reject(RejectReason::Unauthorized)
        ));
    }

    #[test]
    fn test_http2_split_cookie_headers_and_query_extraction() {
        let secret = "cookie-secret";
        let config = PluginConfig {
            id: "jwt_auth".into(),
            config: serde_json::json!({
                "algorithm": "HS256",
                "secret_key": secret,
                "token_sources": [
                    { "type": "cookie", "name": "auth_cookie" },
                    { "type": "query", "param": "access_token" }
                ],
                "validate_exp": true,
                "clock_skew_secs": 60,
                "claims_to_headers": { "sub": "x-user-id" },
                "assert_claims": {},
                "cache_ttl_secs": 300
            }),
        };

        let plugin = JwtAuthPlugin::from_config(&config).unwrap();
        let token = create_hs256_token(secret, r#"{"sub":"cookie_user","exp":2500000000}"#);

        // RFC 9113 §8.2.3 split `cookie` header fields: token sits in the SECOND `cookie` header!
        let mut headers = HeaderWorkspace::empty(4);
        headers.append(http::header::COOKIE, HeaderValue::from_static("theme=dark"));
        headers.append(
            http::header::COOKIE,
            HeaderValue::from_str(&format!("auth_cookie={token}")).unwrap(),
        );
        let mut req = L7Request::new(
            Method::GET,
            Uri::from_static("/api/profile"),
            Version::HTTP_2,
            headers,
            Body::Empty,
        );
        let mut target = 1;
        assert!(matches!(
            plugin.on_upstream_request(&mut req, &mut target),
            Action::Continue
        ));
        assert_eq!(req.headers.get("x-user-id").unwrap(), "cookie_user");
    }

    #[test]
    fn test_malformed_multibyte_utf8_auth_header_does_not_panic() {
        let config = PluginConfig {
            id: "jwt_auth".into(),
            config: default_jwt_json("my-secret-key", serde_json::json!({})),
        };
        let plugin = JwtAuthPlugin::from_config(&config).unwrap();

        let mut headers = HeaderWorkspace::empty(4);
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

        let mut target = 1;
        let action = plugin.on_upstream_request(&mut req, &mut target);
        assert!(matches!(action, Action::Reject(RejectReason::Unauthorized)));
        assert_eq!(
            req.headers.get("www-authenticate").unwrap(),
            "Bearer realm=\"velda-edge\""
        );
    }
}
