//! Pre-Upstream OAuth 2.0 Token Introspection Plugin (`oauth2_introspection`).
//!
//! Enforces RFC 7662 token introspection on incoming HTTP requests with:
//! - Multi-source token extraction with zero-clone header borrowing.
//! - 128-bit Chunk-based Bit-Mixing fingerprint (Full-avalanche, 100% IdP safe).
//! - 2-Tier Caching: Tier-1 Core-Local L1 (Zero Lock, Zero Atomic) + Tier-2 Hardware-Sharded L2.
//! - Single-flight asynchronous background revalidation via dedicated low-priority Shadow Workers.
//! - Pre-materialized claims & zero-overhead bulk claim-to-header injection with anti-spoofing header cleaning.

use std::str::FromStr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use http::header::{HeaderName, HeaderValue};
use velda_core::{Action, L7Request, RejectReason};
use velda_sync::Oauth2IntrospectionConfig;
use velda_sync::post_sync::plugin::PluginConfig;

use super::cache::{CacheLookup, CachedClaims, IntrospectionCache, STALE_REFRESH_THRESHOLD_SECS};
use super::client::{IdpKeepAliveClient, IntrospectionResponse};
use super::scope::check_scopes;
use crate::capability::{AccessLevel, PluginCapability, SupportedProtocols};
use crate::error::PluginError;
use crate::pre_upstream::PreUpstreamPlugin;

#[inline(always)]
fn current_epoch_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Helper function to build pre-materialized cached claims with precomputed headers and scopes.
/// Executed during cold-miss introspection or asynchronously inside the Shadow Worker.
#[inline]
fn build_cached_claims(
    claims_to_headers: &[(String, HeaderName)],
    required_scopes: &[String],
    cache_ttl_seconds: u64,
    resp: &IntrospectionResponse,
    now: u64,
) -> CachedClaims {
    let has_required_scopes = check_scopes(resp.scope.as_deref(), required_scopes);

    // Precompute all upstream headers once: extract string claims and convert to HeaderValue
    let string_claims = resp.string_claims();
    let mut headers = Vec::with_capacity(claims_to_headers.len());
    for (claim_name, header_name) in claims_to_headers {
        if let Some(val) = string_claims.get(claim_name)
            && let Ok(hval) = HeaderValue::from_str(val)
        {
            headers.push((header_name.clone(), hval));
        }
    }

    let mut expires_at = now.saturating_add(cache_ttl_seconds);
    if let Some(exp_val) = resp.exp {
        expires_at = expires_at.min(exp_val);
    }

    CachedClaims {
        exp: resp.exp,
        sub: resp.sub.clone(),
        client_id: resp.client_id.clone(),
        scope: resp.scope.clone(),
        has_required_scopes,
        precomputed_headers: Arc::from(headers.into_boxed_slice()),
        expires_at,
    }
}

/// OAuth 2.0 Token Introspection Plugin instance.
pub struct Oauth2IntrospectionPlugin {
    pub id: String,
    pub config: Oauth2IntrospectionConfig,
    cache: Arc<IntrospectionCache>,
    client: Arc<IdpKeepAliveClient>,
    token_headers_preparsed: Vec<HeaderName>,
    claims_to_headers_preparsed: Arc<[(String, HeaderName)]>,
    required_scopes_preparsed: Arc<[String]>,
}

impl Oauth2IntrospectionPlugin {
    /// Constructs a new `Oauth2IntrospectionPlugin` instance with full validation.
    pub fn new(
        id: impl Into<String>,
        config: Oauth2IntrospectionConfig,
    ) -> Result<Self, PluginError> {
        let id_str = id.into();

        let mut token_headers_preparsed = Vec::with_capacity(config.token_headers.len());
        for h in &config.token_headers {
            let hname = HeaderName::from_str(h).map_err(|e| {
                PluginError::Config(format!(
                    "Invalid token_header '{h}' for plugin '{id_str}': {e}"
                ))
            })?;
            token_headers_preparsed.push(hname);
        }

        let mut claims_to_headers = Vec::with_capacity(config.claims_to_headers.len());
        for mapping in &config.claims_to_headers {
            let hname = HeaderName::from_str(&mapping.header).map_err(|e| {
                PluginError::Config(format!(
                    "Invalid target header '{}' in claims_to_headers for plugin '{id_str}': {e}",
                    mapping.header
                ))
            })?;
            claims_to_headers.push((mapping.claim.clone(), hname));
        }

        let claims_to_headers_preparsed: Arc<[(String, HeaderName)]> =
            Arc::from(claims_to_headers.into_boxed_slice());
        let required_scopes_preparsed: Arc<[String]> =
            Arc::from(config.required_scopes.clone().into_boxed_slice());

        let client = Arc::new(IdpKeepAliveClient::new(&config)?);
        let cache = Arc::new(IntrospectionCache::new(config.max_cache_entries));

        Ok(Self {
            id: id_str,
            config,
            cache,
            client,
            token_headers_preparsed,
            claims_to_headers_preparsed,
            required_scopes_preparsed,
        })
    }

    /// Deserializes and constructs an instance directly from a [`PluginConfig`].
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let cfg = velda_sync::validate_oauth2_introspection_config(&config.id, &config.config)
            .map_err(|e| PluginError::Config(e.to_string()))?;
        Self::new(&config.id, cfg)
    }

    /// Extracts the token from the request headers, cookies, or query parameters.
    /// Returns borrowed slices without any heap clone on the hot path.
    fn extract_token<'r, 's>(
        &'s self,
        req: &'r L7Request,
    ) -> Option<(&'r str, Option<&'s HeaderName>)> {
        // 1. Scan exact token headers in configured order (borrowed &'s HeaderName, zero clone!)
        for hname in &self.token_headers_preparsed {
            if let Some(val) = req.headers.get(hname)
                && let Ok(val_str) = val.to_str()
            {
                let trimmed = val_str.trim();
                if !trimmed.is_empty() {
                    // Automatically strip "Bearer " prefix if present (RFC 6750 standard)
                    if trimmed.len() > 7 && trimmed[..7].eq_ignore_ascii_case("bearer ") {
                        let token = trimmed[7..].trim();
                        if !token.is_empty() {
                            return Some((token, Some(hname)));
                        }
                    } else {
                        return Some((trimmed, Some(hname)));
                    }
                }
            }
        }

        // 2. Scan cookies in configured order
        if !self.config.token_cookies.is_empty()
            && let Some(val) = req.headers.get(http::header::COOKIE)
            && let Ok(cookie_header) = val.to_str()
        {
            for target_cookie in &self.config.token_cookies {
                for pair in cookie_header.split(';') {
                    if let Some((k, v)) = pair.split_once('=')
                        && k.trim() == target_cookie.as_str()
                    {
                        let token = v.trim();
                        if !token.is_empty() {
                            return Some((token, None));
                        }
                    }
                }
            }
        }

        // 3. Scan query parameters in configured order
        if !self.config.token_query_params.is_empty()
            && let Some(query_str) = req.uri.query()
        {
            for target_param in &self.config.token_query_params {
                for pair in query_str.split('&') {
                    if let Some((k, v)) = pair.split_once('=')
                        && k == target_param.as_str()
                    {
                        let token = v.trim();
                        if !token.is_empty() {
                            return Some((token, None));
                        }
                    }
                }
            }
        }

        None
    }

    /// Dispatches background revalidation to dedicated low-priority Shadow Workers.
    /// Strictly non-blocking: drops cleanly if shadow queue is full to protect Traffic Worker runtime.
    fn dispatch_shadow_revalidation(&self, token: &str, fp: u128) {
        let client = Arc::clone(&self.client);
        let cache = Arc::clone(&self.cache);
        let claims_to_headers = Arc::clone(&self.claims_to_headers_preparsed);
        let required_scopes = Arc::clone(&self.required_scopes_preparsed);
        let token_owned = token.to_string();
        let ttl = self.config.cache_ttl_seconds;
        let neg_ttl = self.config.negative_cache_ttl_seconds;

        let revalidation_task = Box::pin(async move {
            match client.introspect(&token_owned) {
                Ok(resp) => {
                    let cur = current_epoch_secs();
                    if resp.active {
                        let new_entry = build_cached_claims(
                            &claims_to_headers,
                            &required_scopes,
                            ttl,
                            &resp,
                            cur,
                        );
                        cache.store_positive(fp, Arc::new(new_entry), cur);
                    } else {
                        cache.store_negative(fp, neg_ttl, cur);
                    }
                }
                Err(_) => {
                    cache.finish_revalidation(fp);
                }
            }
        });

        if crate::hook::try_dispatch_shadow(revalidation_task).is_err() {
            // Shadow queue is saturated; clear revalidation lease so future requests can retry.
            // Under zero circumstances do we spawn on the hot Traffic Worker runtime!
            self.cache.finish_revalidation(fp);
        }
    }
}

impl PreUpstreamPlugin for Oauth2IntrospectionPlugin {
    #[inline(always)]
    fn id(&self) -> &str {
        &self.id
    }

    fn on_upstream_request(&self, req: &mut L7Request, _target_upstream: &mut Arc<str>) -> Action {
        let (token, matched_header) = match self.extract_token(req) {
            Some(res) => res,
            None => {
                req.headers.insert_bypass(
                    http::header::WWW_AUTHENTICATE,
                    HeaderValue::from_static("Bearer realm=\"velda-edge\""),
                );
                return Action::Reject(RejectReason::Unauthorized);
            }
        };

        let now = current_epoch_secs();
        let fp = super::cache::token_fingerprint(token);

        // Tier 1: Check Core-Local L1 Cache (Zero Lock, Zero Atomic, sub-2ns hot path)
        let claims_result = if let Some(claims) = super::cache::lookup_l1(fp, now) {
            claims
        } else {
            // Tier 2: Check Hardware-Sharded L2 Cache
            match self.cache.lookup(fp, now) {
                CacheLookup::NegativeHit => {
                    req.headers.insert_bypass(
                        http::header::WWW_AUTHENTICATE,
                        HeaderValue::from_static("Bearer error=\"invalid_token\""),
                    );
                    return Action::Reject(RejectReason::Unauthorized);
                }
                CacheLookup::Hit(claims) => {
                    let l1_exp = claims
                        .expires_at
                        .saturating_sub(STALE_REFRESH_THRESHOLD_SECS);
                    if l1_exp > now {
                        super::cache::insert_l1(fp, Arc::clone(&claims), l1_exp);
                    }
                    claims
                }
                CacheLookup::Stale(claims) => {
                    // Stale window: dispatch background refresh if we win single-flight lease
                    if self.cache.mark_revalidating(fp, now) {
                        self.dispatch_shadow_revalidation(token, fp);
                    }
                    claims
                }
                CacheLookup::Miss => {
                    // Cold miss: introspect over Keep-Alive connection
                    match self.client.introspect(token) {
                        Ok(resp) => {
                            if !resp.active {
                                super::cache::invalidate_l1(fp);
                                self.cache.store_negative(
                                    fp,
                                    self.config.negative_cache_ttl_seconds,
                                    now,
                                );
                                req.headers.insert_bypass(
                                    http::header::WWW_AUTHENTICATE,
                                    HeaderValue::from_static("Bearer error=\"invalid_token\""),
                                );
                                return Action::Reject(RejectReason::Unauthorized);
                            }

                            if let Some(exp) = resp.exp
                                && now >= exp
                            {
                                req.headers.insert_bypass(
                                    http::header::WWW_AUTHENTICATE,
                                    HeaderValue::from_static("Bearer error=\"invalid_token\""),
                                );
                                return Action::Reject(RejectReason::Unauthorized);
                            }

                            let cached_claims = build_cached_claims(
                                &self.claims_to_headers_preparsed,
                                &self.required_scopes_preparsed,
                                self.config.cache_ttl_seconds,
                                &resp,
                                now,
                            );

                            let arc_claims = Arc::new(cached_claims);
                            self.cache.store_positive(fp, Arc::clone(&arc_claims), now);

                            let l1_exp = arc_claims
                                .expires_at
                                .saturating_sub(STALE_REFRESH_THRESHOLD_SECS);
                            if l1_exp > now {
                                super::cache::insert_l1(fp, Arc::clone(&arc_claims), l1_exp);
                            }

                            arc_claims
                        }
                        Err(_) => {
                            if self.config.fallback_behavior == "allow" {
                                return Action::Continue;
                            }
                            return Action::Reject(RejectReason::BadGateway);
                        }
                    }
                }
            }
        };

        // 1. Fast Scope Check (Single boolean check pre-evaluated by build_cached_claims)
        if !claims_result.has_required_scopes {
            req.headers.insert_bypass(
                http::header::WWW_AUTHENTICATE,
                HeaderValue::from_static("Bearer error=\"insufficient_scope\""),
            );
            return Action::Reject(RejectReason::Forbidden);
        }

        // 2. Zero-clone remove credentials if configured (using borrowed &HeaderName)
        if self.config.hide_credentials
            && let Some(hname) = matched_header
        {
            req.headers.remove(hname);
        }

        // 3. Security: Strip client-supplied headers matching configured claims_to_headers
        // to prevent header spoofing when optional claims are absent from the IdP.
        for (_, target_header) in self.claims_to_headers_preparsed.iter() {
            req.headers.remove(target_header);
        }

        // 4. Bulk Inject Pre-materialized Headers into Upstream Request
        req.headers.reserve(claims_result.precomputed_headers.len());
        for (hname, hval) in claims_result.precomputed_headers.iter() {
            req.headers.insert(hname.clone(), hval.clone());
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
