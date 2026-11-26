//! Pre-Upstream OAuth 2.0 Token Introspection Plugin (`oauth2_introspection::plugin`).
//!
//! ```text
//!                     OAUTH2 INTROSPECTION HYBRID HOT / COLD PIPELINE
//!
//!   Inbound Request (`L7Request`, L2 Warm)
//!        │
//!        ▼
//!   [Fast Sync Path: `try_on_upstream_request`] (99.9%+ requests, 0 .await, 0 Future alloc)
//!        ├──► Prefetch 64B `IntrospectionCache` L1D Control (`_MM_HINT_T0`)
//!        ├──► Extract token via pre-hashed `HeaderKey` / split HTTP/2 `Cookie` / URI Query
//!        ├──► SHA-256(boot_salt || token) -> `fp: u128`
//!        └──► Bit-Shift -> `_MM_HINT_T2` L3 Bucket -> 64B SIMD Scan:
//!               ├── [Hit]          ──► `insert_prehashed` present claims -> Continue
//!               ├── [Stale <=10%]  ──► Non-blocking `try_dispatch_shadow` (async HTTPS) + Continue
//!               ├── [Forbidden]    ──► 403 (`insufficient_scope`)
//!               ├── [Unauthorized] ──► 401 (`invalid_token`)
//!               └── [Cold Miss]    ──► Return `None`
//!                                        │
//!                                        ▼
//!   [Cold Async Path: `on_upstream_request_async` (.await yields `velda-worker-{id}`)]
//!        └──► Sharded Single-Flight Join / Leader (`broadcast` per `fp: u128`, <= 4096 inflight)
//!               └──► `IdpKeepAliveClient::introspect(token).await` (`velda-discovery` + TLS/HTTP)
//!                      ├── `active: true` & valid `exp`/`nbf` -> Commit L3 SIMD + Inject Claims
//!                      ├── `active: false` / expired          -> Commit L3 Negative + 401
//!                      └── IdP error                          -> `allow` (Continue) | 502 BadGateway
//! ```

use std::future::Future;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use http::header::{HeaderName, HeaderValue};
use rustc_hash::FxHashMap;
use tokio::sync::broadcast;
use velda_core::{Action, HeaderKey, L7Request, RejectReason};
use velda_discovery::HostsFileSource;

use super::cache::{CacheLookup, ColdOutcome, IntrospectionCache};
use super::client::{IdpKeepAliveClient, IntrospectionResponse};
use super::scope::check_scopes;
use super::validation::Oauth2IntrospectionConfig;
use crate::capability::{AccessLevel, Direction, PluginCapability, SupportedProtocols};
use crate::config::PluginConfig;
use crate::error::PluginError;
use crate::pre_upstream::PreUpstreamPlugin;

const KEY_COOKIE: HeaderKey = HeaderKey::from_static("cookie");
const WWW_AUTH_MISSING: HeaderValue = HeaderValue::from_static("Bearer realm=\"velda-edge\"");
const WWW_AUTH_INVALID: HeaderValue = HeaderValue::from_static("Bearer error=\"invalid_token\"");
const WWW_AUTH_SCOPE: HeaderValue = HeaderValue::from_static("Bearer error=\"insufficient_scope\"");

const INFLIGHT_SHARDS: usize = 64;
const MAX_INFLIGHT_PER_SHARD: usize = 64; // 64 * 64 = 4,096 concurrent distinct tokens max

enum InFlightSlot {
    Leader(broadcast::Sender<ColdOutcome>, usize),
    Follower(broadcast::Receiver<ColdOutcome>),
    Saturated,
}

#[repr(align(64))]
struct InFlightShard {
    entries: Mutex<FxHashMap<u128, broadcast::Sender<ColdOutcome>>>,
}

/// Bounded sharded single-flight deduplicator preventing IdP stampedes on cold token misses.
struct InFlightCoordinator {
    shards: Box<[InFlightShard]>,
}

impl InFlightCoordinator {
    fn new() -> Self {
        let mut shards = Vec::with_capacity(INFLIGHT_SHARDS);
        for _ in 0..INFLIGHT_SHARDS {
            shards.push(InFlightShard {
                entries: Mutex::new(FxHashMap::default()),
            });
        }
        Self {
            shards: shards.into_boxed_slice(),
        }
    }

    fn enter(&self, fp: u128) -> InFlightSlot {
        let shard_idx = (fp as usize) & (INFLIGHT_SHARDS - 1);
        let mut guard = match self.shards[shard_idx].entries.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        if let Some(tx) = guard.get(&fp) {
            return InFlightSlot::Follower(tx.subscribe());
        }
        if guard.len() >= MAX_INFLIGHT_PER_SHARD {
            return InFlightSlot::Saturated;
        }
        let (tx, _) = broadcast::channel(1);
        guard.insert(fp, tx.clone());
        InFlightSlot::Leader(tx, shard_idx)
    }

    fn remove(&self, fp: u128, shard_idx: usize) {
        if let Ok(mut guard) = self.shards[shard_idx].entries.lock() {
            guard.remove(&fp);
        }
    }
}

struct InFlightLeaderGuard<'a> {
    coord: &'a InFlightCoordinator,
    fp: u128,
    shard_idx: usize,
    tx: Option<broadcast::Sender<ColdOutcome>>,
    finished: bool,
}

impl<'a> InFlightLeaderGuard<'a> {
    fn complete(mut self, outcome: ColdOutcome) -> ColdOutcome {
        self.coord.remove(self.fp, self.shard_idx);
        if let Some(tx) = self.tx.take() {
            let _ = tx.send(outcome.clone());
        }
        self.finished = true;
        outcome
    }
}

impl Drop for InFlightLeaderGuard<'_> {
    fn drop(&mut self) {
        if !self.finished {
            self.coord.remove(self.fp, self.shard_idx);
            if let Some(tx) = self.tx.take() {
                let _ = tx.send(ColdOutcome::Unavailable);
            }
        }
    }
}

fn evaluate_introspection_response(
    claims_to_headers: &[(Box<str>, HeaderKey)],
    required_scopes: &[String],
    resp: &IntrospectionResponse,
    now: u64,
) -> (Option<u64>, ColdOutcome) {
    if !resp.active {
        return (None, ColdOutcome::Unauthorized);
    }

    let Ok(exp_opt) = resp.parsed_exp_epoch() else {
        return (None, ColdOutcome::Unauthorized);
    };
    if let Some(exp) = exp_opt
        && now >= exp
    {
        return (None, ColdOutcome::Unauthorized);
    }

    let Ok(nbf_opt) = resp.parsed_nbf_epoch() else {
        return (None, ColdOutcome::Unauthorized);
    };
    if let Some(nbf) = nbf_opt
        && now < nbf
    {
        return (None, ColdOutcome::Unauthorized);
    }

    if !check_scopes(resp.scope.as_deref(), required_scopes) {
        return (exp_opt, ColdOutcome::Forbidden);
    }

    let headers = if claims_to_headers.is_empty() {
        None
    } else {
        let mut pairs = Vec::with_capacity(claims_to_headers.len());
        for (claim_path, header_key) in claims_to_headers {
            if let Some(val_str) = resp.resolve_claim_str(claim_path)
                && let Ok(hval) = HeaderValue::try_from(val_str)
            {
                pairs.push((header_key.clone(), hval));
            }
        }
        if pairs.is_empty() {
            None
        } else {
            Some(Arc::from(pairs))
        }
    };

    (exp_opt, ColdOutcome::Allow(headers))
}

/// OAuth 2.0 RFC 7662 Token Introspection Pre-Upstream Plugin.
#[repr(C, align(64))]
pub struct Oauth2IntrospectionPlugin {
    cache: Arc<IntrospectionCache>,
    client: Arc<IdpKeepAliveClient>,
    in_flight: InFlightCoordinator,
    fail_open: bool,
    hide_credentials: bool,
    token_headers: Box<[HeaderKey]>,
    token_cookies: Box<[Box<str>]>,
    token_query_params: Box<[Box<str>]>,
    claims_to_headers: Arc<[(Box<str>, HeaderKey)]>,
    required_scopes: Arc<[String]>,
    pub id: String,
    pub config: Oauth2IntrospectionConfig,
}

impl Oauth2IntrospectionPlugin {
    /// Constructs a new validated [`Oauth2IntrospectionPlugin`] using system `/etc/hosts` & DNS.
    pub fn new(
        id: impl Into<String>,
        config: Oauth2IntrospectionConfig,
    ) -> Result<Self, PluginError> {
        Self::new_with_hosts(id, config, HostsFileSource::load_system())
    }

    /// Constructs a new [`Oauth2IntrospectionPlugin`] with an explicit [`HostsFileSource`].
    pub fn new_with_hosts(
        id: impl Into<String>,
        config: Oauth2IntrospectionConfig,
        hosts: HostsFileSource,
    ) -> Result<Self, PluginError> {
        let id_str = id.into();

        let mut token_headers = Vec::with_capacity(config.token_headers.len());
        for h in &config.token_headers {
            let hname = HeaderName::from_str(h).map_err(|e| {
                PluginError::Config(format!(
                    "Invalid token_header '{h}' for plugin '{id_str}': {e}"
                ))
            })?;
            token_headers.push(HeaderKey::from_name(hname));
        }

        let mut claims_to_headers = Vec::with_capacity(config.claims_to_headers.len());
        for mapping in &config.claims_to_headers {
            let hname = HeaderName::from_str(&mapping.header).map_err(|e| {
                PluginError::Config(format!(
                    "Invalid target header '{}' in claims_to_headers for plugin '{id_str}': {e}",
                    mapping.header
                ))
            })?;
            claims_to_headers.push((
                mapping.claim.clone().into_boxed_str(),
                HeaderKey::from_name(hname),
            ));
        }

        let client = Arc::new(IdpKeepAliveClient::new_with_hosts(&config, hosts)?);
        let cache = Arc::new(IntrospectionCache::new(
            config.cache_ttl_seconds,
            config.negative_cache_ttl_seconds,
        ));

        Ok(Self {
            cache,
            client,
            in_flight: InFlightCoordinator::new(),
            fail_open: config.fallback_behavior == "allow",
            hide_credentials: config.hide_credentials,
            token_headers: token_headers.into_boxed_slice(),
            token_cookies: config
                .token_cookies
                .iter()
                .map(|s| s.clone().into_boxed_str())
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            token_query_params: config
                .token_query_params
                .iter()
                .map(|s| s.clone().into_boxed_str())
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            claims_to_headers: Arc::from(claims_to_headers.into_boxed_slice()),
            required_scopes: Arc::from(config.required_scopes.clone().into_boxed_slice()),
            id: id_str,
            config,
        })
    }

    /// Deserializes, validates, and constructs an instance directly from a [`PluginConfig`].
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let cfg =
            super::validation::validate_oauth2_introspection_config(&config.id, &config.config)?;
        Self::new(&config.id, cfg)
    }

    /// Returns a reference to the shared 64-byte L1D/L3 SIMD cache.
    #[inline]
    pub fn cache(&self) -> &Arc<IntrospectionCache> {
        &self.cache
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

    #[cold]
    #[inline(never)]
    fn reject_insufficient_scope(req: &mut L7Request) -> Action {
        req.headers
            .insert_bypass(http::header::WWW_AUTHENTICATE, WWW_AUTH_SCOPE);
        Action::Reject(RejectReason::Forbidden)
    }

    /// Extracts token slice from pre-hashed headers, split HTTP/2 cookies, or URI query params.
    #[inline]
    fn extract_token<'r, 's>(
        &'s self,
        req: &'r L7Request,
    ) -> Option<(&'r str, Option<&'s HeaderKey>)> {
        for key in &self.token_headers {
            for val in req.headers.get_all_prehashed(key) {
                let Ok(s) = val.to_str() else {
                    continue;
                };
                let trimmed = s.trim();
                if trimmed.is_empty() {
                    continue;
                }
                if trimmed.len() > 7
                    && trimmed.is_char_boundary(7)
                    && trimmed[..7].eq_ignore_ascii_case("bearer ")
                {
                    let token = trimmed[7..].trim();
                    if !token.is_empty() {
                        return Some((token, Some(key)));
                    }
                } else {
                    return Some((trimmed, Some(key)));
                }
            }
        }

        if !self.token_cookies.is_empty() {
            for val in req.headers.get_all_prehashed(&KEY_COOKIE) {
                let Ok(cookie_str) = val.to_str() else {
                    continue;
                };
                for target_cookie in &self.token_cookies {
                    for pair in cookie_str.split(';') {
                        if let Some((k, v)) = pair.trim().split_once('=')
                            && k.trim() == target_cookie.as_ref()
                        {
                            let token = v.trim();
                            if !token.is_empty() {
                                return Some((token, None));
                            }
                        }
                    }
                }
            }
        }

        if !self.token_query_params.is_empty()
            && let Some(query_str) = req.uri.query()
        {
            for target_param in &self.token_query_params {
                for pair in query_str.split('&') {
                    if let Some((k, v)) = pair.split_once('=')
                        && k == target_param.as_ref()
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

    #[inline]
    fn apply_allow(
        &self,
        req: &mut L7Request,
        matched_header: Option<&HeaderKey>,
        headers: Option<&Arc<[(HeaderKey, HeaderValue)]>>,
    ) -> Action {
        if self.hide_credentials
            && let Some(key) = matched_header
        {
            req.headers.remove(key.name.as_str());
        }
        if let Some(pairs) = headers {
            req.headers.reserve(pairs.len());
            for (key, val) in pairs.iter() {
                req.headers.insert_prehashed(key, val.clone());
            }
        }
        Action::Continue
    }

    fn dispatch_shadow_revalidation(&self, token: &str, fp: u128) {
        let client = Arc::clone(&self.client);
        let cache = Arc::clone(&self.cache);
        let claims_to_headers = Arc::clone(&self.claims_to_headers);
        let required_scopes = Arc::clone(&self.required_scopes);
        let token_owned = token.to_string();

        let task = Box::pin(async move {
            match client.introspect(&token_owned).await {
                Ok(resp) => {
                    let cur = velda_core::clock::coarse_sec();
                    let (exp_opt, outcome) = evaluate_introspection_response(
                        &claims_to_headers,
                        &required_scopes,
                        &resp,
                        cur,
                    );
                    match outcome {
                        ColdOutcome::Allow(hdrs) => {
                            cache.store_positive(fp, true, hdrs, exp_opt, cur);
                        }
                        ColdOutcome::Forbidden => {
                            cache.store_positive(fp, false, None, exp_opt, cur);
                        }
                        ColdOutcome::Unauthorized => {
                            cache.store_negative(fp, cur);
                        }
                        ColdOutcome::Unavailable => {
                            cache.finish_revalidation(fp);
                        }
                    }
                }
                Err(_) => {
                    cache.finish_revalidation(fp);
                }
            }
        });

        if crate::hook::try_dispatch_shadow(task).is_err() {
            self.cache.finish_revalidation(fp);
        }
    }

    async fn introspect_cold(&self, token: &str, fp: u128) -> ColdOutcome {
        let _use_single_flight = self.cache.is_enabled();
        match self.in_flight.enter(fp) {
            InFlightSlot::Follower(mut rx) => match rx.recv().await {
                Ok(outcome) => outcome,
                Err(_) => self.query_idp_and_commit(token, fp).await,
            },
            InFlightSlot::Saturated => self.query_idp_and_commit(token, fp).await,
            InFlightSlot::Leader(tx, shard_idx) => {
                let guard = InFlightLeaderGuard {
                    coord: &self.in_flight,
                    fp,
                    shard_idx,
                    tx: Some(tx),
                    finished: false,
                };
                let outcome = self.query_idp_and_commit(token, fp).await;
                guard.complete(outcome)
            }
        }
    }

    async fn query_idp_and_commit(&self, token: &str, fp: u128) -> ColdOutcome {
        match self.client.introspect(token).await {
            Ok(resp) => {
                let cur = velda_core::clock::coarse_sec();
                let (exp_opt, outcome) = evaluate_introspection_response(
                    &self.claims_to_headers,
                    &self.required_scopes,
                    &resp,
                    cur,
                );
                match &outcome {
                    ColdOutcome::Allow(hdrs) => {
                        self.cache
                            .store_positive(fp, true, hdrs.clone(), exp_opt, cur);
                    }
                    ColdOutcome::Forbidden => {
                        self.cache.store_positive(fp, false, None, exp_opt, cur);
                    }
                    ColdOutcome::Unauthorized => {
                        self.cache.store_negative(fp, cur);
                    }
                    ColdOutcome::Unavailable => {}
                }
                outcome
            }
            Err(_) => ColdOutcome::Unavailable,
        }
    }
}

impl PreUpstreamPlugin for Oauth2IntrospectionPlugin {
    #[inline(always)]
    fn id(&self) -> &str {
        &self.id
    }

    #[inline(always)]
    fn pbww(&self) -> u32 {
        0
    }

    fn on_upstream_request(&self, req: &mut L7Request, target_upstream: &mut u16) -> Action {
        if let Some(action) = self.try_on_upstream_request(req, target_upstream) {
            return action;
        }
        // Blocking network I/O is forbidden on sync calls; respect configured fallback semantics.
        if self.fail_open {
            Action::Continue
        } else {
            Action::Reject(RejectReason::BadGateway)
        }
    }

    #[inline]
    fn try_on_upstream_request(
        &self,
        req: &mut L7Request,
        _target_upstream: &mut u16,
    ) -> Option<Action> {
        self.cache.prefetch_control_l1();

        let Some((token, matched_header)) = self.extract_token(req) else {
            return Some(Self::reject_missing_token(req));
        };

        if !self.cache.is_enabled() {
            return None;
        }

        let now = req.ingress_time_sec();
        let fp = self.cache.token_to_u128(token);

        match self.cache.lookup(fp, now) {
            CacheLookup::Hit(headers) => {
                Some(self.apply_allow(req, matched_header, headers.as_ref()))
            }
            CacheLookup::Stale(headers) => {
                if self.cache.mark_revalidating(fp, now) {
                    self.dispatch_shadow_revalidation(token, fp);
                }
                Some(self.apply_allow(req, matched_header, headers.as_ref()))
            }
            CacheLookup::Forbidden => Some(Self::reject_insufficient_scope(req)),
            CacheLookup::Unauthorized => Some(Self::reject_invalid_token(req)),
            CacheLookup::Miss => None,
        }
    }

    fn on_upstream_request_async<'a>(
        &'a self,
        req: &'a mut L7Request,
        _target_upstream: &'a mut u16,
    ) -> Pin<Box<dyn Future<Output = Action> + Send + 'a>> {
        Box::pin(async move {
            self.cache.prefetch_control_l1();

            let Some((token, matched_header)) = self.extract_token(req) else {
                return Self::reject_missing_token(req);
            };
            let matched_header_cloned = matched_header.cloned();
            let token_owned = token.to_string();

            let fp = self.cache.token_to_u128(&token_owned);
            let now = req.ingress_time_sec();

            if self.cache.is_enabled() {
                match self.cache.lookup(fp, now) {
                    CacheLookup::Hit(headers) => {
                        return self.apply_allow(
                            req,
                            matched_header_cloned.as_ref(),
                            headers.as_ref(),
                        );
                    }
                    CacheLookup::Stale(headers) => {
                        if self.cache.mark_revalidating(fp, now) {
                            self.dispatch_shadow_revalidation(&token_owned, fp);
                        }
                        return self.apply_allow(
                            req,
                            matched_header_cloned.as_ref(),
                            headers.as_ref(),
                        );
                    }
                    CacheLookup::Forbidden => return Self::reject_insufficient_scope(req),
                    CacheLookup::Unauthorized => return Self::reject_invalid_token(req),
                    CacheLookup::Miss => {}
                }
            }

            match self.introspect_cold(&token_owned, fp).await {
                ColdOutcome::Allow(headers) => {
                    self.apply_allow(req, matched_header_cloned.as_ref(), headers.as_ref())
                }
                ColdOutcome::Forbidden => Self::reject_insufficient_scope(req),
                ColdOutcome::Unauthorized => Self::reject_invalid_token(req),
                ColdOutcome::Unavailable => {
                    if self.fail_open {
                        Action::Continue
                    } else {
                        Action::Reject(RejectReason::BadGateway)
                    }
                }
            }
        })
    }

    #[inline(always)]
    fn capability(&self) -> PluginCapability {
        PluginCapability::header_only(AccessLevel::Write)
    }

    #[inline(always)]
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
    use crate::pre_upstream::run_pre_upstream_plugins_async;
    use base64::prelude::*;
    use http::{Method, Uri, Version};
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use velda_core::{Body, HeaderWorkspace};

    async fn spawn_mock_idp(
        calls: Arc<AtomicUsize>,
        body_fn: impl Fn(usize, &str) -> String + Send + Sync + 'static,
        chunked: bool,
    ) -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let body_fn = Arc::new(body_fn);

        tokio::spawn(async move {
            while let Ok((mut sock, _)) = listener.accept().await {
                let calls = Arc::clone(&calls);
                let body_fn = Arc::clone(&body_fn);
                tokio::spawn(async move {
                    let mut buf = [0u8; 4096];
                    while let Ok(n) = sock.read(&mut buf).await {
                        if n == 0 {
                            break;
                        }
                        let req_str = String::from_utf8_lossy(&buf[..n]);
                        let seq = calls.fetch_add(1, AtomicOrdering::SeqCst) + 1;
                        let body = body_fn(seq, &req_str);
                        let wire = if chunked {
                            let hex_len = format!("{:x}", body.len());
                            format!(
                                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\nConnection: keep-alive\r\n\r\n{hex_len}\r\n{body}\r\n0\r\n\r\n"
                            )
                        } else {
                            format!(
                                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n{body}",
                                body.len()
                            )
                        };
                        if sock.write_all(wire.as_bytes()).await.is_err() {
                            break;
                        }
                    }
                });
            }
        });

        addr
    }

    fn base_config(url: &str, cache_ttl_seconds: u64) -> Oauth2IntrospectionConfig {
        serde_json::from_value(serde_json::json!({
            "introspection_url": url,
            "client_id": "velda-gw",
            "client_secret": "gw-secret-12345",
            "auth_method": "client_secret_basic",
            "token_headers": ["x-access-token", "authorization"],
            "token_cookies": ["access_token"],
            "token_query_params": ["token"],
            "token_type_hint": "access_token",
            "cache_ttl_seconds": cache_ttl_seconds,
            "negative_cache_ttl_seconds": 10,
            "keepalive_timeout_secs": 60,
            "pool_max_idle_connections": 8,
            "request_timeout_ms": 2000,
            "required_scopes": ["read"],
            "claims_to_headers": [
                { "claim": "sub", "header": "x-user-id" },
                { "claim": "optional_note", "header": "x-client-note" }
            ],
            "hide_credentials": true,
            "fallback_behavior": "reject"
        }))
        .unwrap()
    }

    #[tokio::test]
    async fn test_discovery_hosts_chunked_cold_miss_warm_hit_and_header_semantics() {
        let calls = Arc::new(AtomicUsize::new(0));
        let addr = spawn_mock_idp(
            Arc::clone(&calls),
            |_seq, _req| {
                // Omits optional `optional_note` claim intentionally
                r#"{"active":true,"sub":"usr_777","scope":"read orders","exp":2500000000}"#.into()
            },
            true,
        )
        .await;

        let hosts = HostsFileSource::from_content(&format!("{} idp.internal.local\n", addr.ip()));
        let cfg = base_config(
            &format!("http://idp.internal.local:{}/oauth/introspect", addr.port()),
            300,
        );
        let plugin =
            Arc::new(Oauth2IntrospectionPlugin::new_with_hosts("oauth2", cfg, hosts).unwrap());
        let chain: Vec<Arc<dyn PreUpstreamPlugin>> = vec![plugin];

        // 1. Cold miss -> yields via `.await`, resolves `idp.internal.local` via HostsFileSource, decodes chunked body
        let mut h1 = HeaderWorkspace::empty(6);
        h1.insert(
            http::header::AUTHORIZATION,
            HeaderValue::from_static("Bearer tok_chunked_1"),
        );
        h1.insert(
            HeaderName::from_static("x-client-note"),
            HeaderValue::from_static("preserve-if-claim-absent"),
        );
        let mut req1 = L7Request::new(
            Method::GET,
            Uri::from_static("/orders"),
            Version::HTTP_11,
            h1,
            Body::Empty,
        );
        let mut up1 = 1;
        let act1 = run_pre_upstream_plugins_async(&chain, &mut req1, &mut up1).await;
        assert!(matches!(act1, Action::Continue));
        assert_eq!(calls.load(AtomicOrdering::SeqCst), 1);
        assert_eq!(req1.headers.get("x-user-id").unwrap(), "usr_777");
        assert_eq!(
            req1.headers.get("x-client-note").unwrap(),
            "preserve-if-claim-absent"
        );
        assert!(req1.headers.get("authorization").is_none());

        // 2. Warm hit -> 0 extra IdP calls, served strictly synchronously via try_on_upstream_request!
        let mut h2 = HeaderWorkspace::empty(4);
        h2.insert(
            http::header::AUTHORIZATION,
            HeaderValue::from_static("Bearer tok_chunked_1"),
        );
        let mut req2 = L7Request::new(
            Method::GET,
            Uri::from_static("/orders"),
            Version::HTTP_11,
            h2,
            Body::Empty,
        );
        let mut up2 = 1;
        let sync_hit = chain[0].try_on_upstream_request(&mut req2, &mut up2);
        assert!(matches!(sync_hit, Some(Action::Continue)));
        assert_eq!(calls.load(AtomicOrdering::SeqCst), 1);
        assert_eq!(req2.headers.get("x-user-id").unwrap(), "usr_777");
    }

    #[tokio::test]
    async fn test_https_tls_introspection_endpoint() {
        let cert = rcgen::generate_simple_self_signed(vec!["idp.tls.test".into()]).unwrap();
        let cert_pem = cert.cert.pem();
        let key_pem = cert.signing_key.serialize_pem();
        let cert_der = rustls::pki_types::CertificateDer::from(cert.cert.der().to_vec());
        let key_der =
            rustls::pki_types::PrivateKeyDer::try_from(cert.signing_key.serialize_der()).unwrap();

        let server_cfg = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![cert_der], key_der)
            .unwrap();
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server_cfg));

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            if let Ok((tcp, _)) = listener.accept().await
                && let Ok(mut tls) = acceptor.accept(tcp).await
            {
                let mut buf = [0u8; 2048];
                let _ = tls.read(&mut buf).await;
                let body = r#"{"active":true,"sub":"tls_user","scope":"read","exp":2500000000}"#;
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
                    body.len()
                );
                let _ = tls.write_all(resp.as_bytes()).await;
            }
        });

        let hosts = HostsFileSource::from_content("127.0.0.1 idp.tls.test\n");
        let mut cfg = base_config(
            &format!("https://idp.tls.test:{}/introspect", addr.port()),
            60,
        );
        cfg.tls.ca_pem = Some(cert_pem);
        let _ = key_pem;

        let plugin = Oauth2IntrospectionPlugin::new_with_hosts("oauth2", cfg, hosts).unwrap();
        let mut h = HeaderWorkspace::empty(4);
        h.insert(
            http::header::AUTHORIZATION,
            HeaderValue::from_static("Bearer tok_https"),
        );
        let mut req = L7Request::new(
            Method::GET,
            Uri::from_static("/v1"),
            Version::HTTP_11,
            h,
            Body::Empty,
        );
        let mut up = 1;
        let act = plugin.on_upstream_request_async(&mut req, &mut up).await;
        assert!(matches!(act, Action::Continue));
        assert_eq!(req.headers.get("x-user-id").unwrap(), "tls_user");
    }

    #[tokio::test]
    async fn test_single_flight_deduplicates_concurrent_cold_misses() {
        let calls = Arc::new(AtomicUsize::new(0));
        let addr = spawn_mock_idp(
            Arc::clone(&calls),
            |_seq, _req| {
                r#"{"active":true,"sub":"sf_user","scope":"read","exp":2500000000}"#.into()
            },
            false,
        )
        .await;

        let cfg = base_config(&format!("http://{addr}/introspect"), 60);
        let plugin = Arc::new(Oauth2IntrospectionPlugin::new("oauth2", cfg).unwrap());

        let mut futs = Vec::new();
        for _ in 0..16 {
            let p = Arc::clone(&plugin);
            futs.push(tokio::spawn(async move {
                let mut h = HeaderWorkspace::empty(4);
                h.insert(
                    http::header::AUTHORIZATION,
                    HeaderValue::from_static("Bearer shared_cold_token"),
                );
                let mut req = L7Request::new(
                    Method::GET,
                    Uri::from_static("/items"),
                    Version::HTTP_11,
                    h,
                    Body::Empty,
                );
                let mut up = 1;
                let act = p.on_upstream_request_async(&mut req, &mut up).await;
                assert!(matches!(act, Action::Continue));
                assert_eq!(req.headers.get("x-user-id").unwrap(), "sf_user");
            }));
        }
        for f in futs {
            f.await.unwrap();
        }

        // All 16 concurrent misses coalesce cleanly (well below 16; typically 1)
        let n_calls = calls.load(AtomicOrdering::SeqCst);
        assert!(
            n_calls <= 2,
            "expected single-flight coalescing, got {n_calls} IdP calls"
        );
    }

    #[tokio::test]
    async fn test_zero_ttl_disables_positive_cache_for_instant_logout_revocation() {
        let calls = Arc::new(AtomicUsize::new(0));
        let addr = spawn_mock_idp(
            Arc::clone(&calls),
            |seq, _req| {
                if seq == 1 {
                    r#"{"active":true,"sub":"sensitive_user","scope":"read","exp":2500000000}"#
                        .into()
                } else {
                    r#"{"active":false}"#.into()
                }
            },
            false,
        )
        .await;

        let cfg = base_config(&format!("http://{addr}/introspect"), 0);
        let plugin = Arc::new(Oauth2IntrospectionPlugin::new("oauth2", cfg).unwrap());

        let make_req = || {
            let mut h = HeaderWorkspace::empty(4);
            h.insert(
                http::header::AUTHORIZATION,
                HeaderValue::from_static("Bearer revoke_me"),
            );
            L7Request::new(
                Method::GET,
                Uri::from_static("/transfer"),
                Version::HTTP_11,
                h,
                Body::Empty,
            )
        };

        let mut up = 1;
        assert!(matches!(
            plugin
                .on_upstream_request_async(&mut make_req(), &mut up)
                .await,
            Action::Continue
        ));
        // Second call hits IdP immediately (no positive cache) and observes revocation!
        assert!(matches!(
            plugin
                .on_upstream_request_async(&mut make_req(), &mut up)
                .await,
            Action::Reject(RejectReason::Unauthorized)
        ));
        // Third call hits negative cache (0 extra IdP calls)
        assert!(matches!(
            plugin.try_on_upstream_request(&mut make_req(), &mut up),
            Some(Action::Reject(RejectReason::Unauthorized))
        ));
        assert_eq!(calls.load(AtomicOrdering::SeqCst), 2);
        let _ = BASE64_STANDARD.encode(b"ok");
    }

    #[tokio::test]
    async fn test_insufficient_scope_and_idp_unavailable_fallback() {
        let calls = Arc::new(AtomicUsize::new(0));
        let addr = spawn_mock_idp(
            Arc::clone(&calls),
            |_seq, _req| {
                r#"{"active":true,"sub":"viewer","scope":"profile","exp":2500000000}"#.into()
            },
            false,
        )
        .await;

        let cfg = base_config(&format!("http://{addr}/introspect"), 60);
        let plugin = Oauth2IntrospectionPlugin::new("oauth2", cfg).unwrap();

        let make_req = || {
            let mut h = HeaderWorkspace::empty(4);
            h.insert(
                http::header::AUTHORIZATION,
                HeaderValue::from_static("Bearer low_scope_tok"),
            );
            L7Request::new(
                Method::GET,
                Uri::from_static("/orders"),
                Version::HTTP_11,
                h,
                Body::Empty,
            )
        };

        let mut up = 1;
        let mut req1 = make_req();
        assert!(matches!(
            plugin.on_upstream_request_async(&mut req1, &mut up).await,
            Action::Reject(RejectReason::Forbidden)
        ));
        assert_eq!(
            req1.headers.get("www-authenticate").unwrap(),
            "Bearer error=\"insufficient_scope\""
        );
        // Subsequent call hits L3 Forbidden cache directly with zero IdP query
        assert!(matches!(
            plugin.try_on_upstream_request(&mut make_req(), &mut up),
            Some(Action::Reject(RejectReason::Forbidden))
        ));
        assert_eq!(calls.load(AtomicOrdering::SeqCst), 1);

        // Closed port -> 502 BadGateway (`reject`) vs Continue (`allow`)
        let dead_cfg = base_config("http://127.0.0.1:1/introspect", 60);
        let fail_closed =
            Oauth2IntrospectionPlugin::new("oauth2-closed", dead_cfg.clone()).unwrap();
        assert!(matches!(
            fail_closed
                .on_upstream_request_async(&mut make_req(), &mut up)
                .await,
            Action::Reject(RejectReason::BadGateway)
        ));

        let mut open_cfg = dead_cfg;
        open_cfg.fallback_behavior = "allow".into();
        let fail_open = Oauth2IntrospectionPlugin::new("oauth2-open", open_cfg).unwrap();
        assert!(matches!(
            fail_open
                .on_upstream_request_async(&mut make_req(), &mut up)
                .await,
            Action::Continue
        ));
    }
}
