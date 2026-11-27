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
//!        ├──► Reject malformed / oversized tokens (`> 16 KiB`) prior to SHA-256
//!        ├──► SHA-256(boot_salt || token) -> `fp: u128`
//!        └──► Core-Local 2-Tier Lookup (Hot L3 Cap + Cold DRAM Promotion):
//!               ├── [Hit]          ──► `insert_prehashed` present claims -> Continue
//!               ├── [Stale <=10%]  ──► Non-blocking `try_dispatch_shadow` + Continue
//!               ├── [Forbidden]    ──► 403 (`insufficient_scope`)
//!               ├── [Unauthorized] ──► 401 (`invalid_token`)
//!               └── [Cold Miss]    ──► Return `None`
//!                                        │
//!                                        ▼
//!   [Cold Async Path: `on_upstream_request_async` (.await yields `velda-worker-{id}`)]
//!        └──► `InFlightCoordinator::introspect_coalesced()` (64x64 Single-Flight Shards)
//!               └──► `providers::<etp_kind>::introspect(EtpInstance, token).await`
//!                      ├── `active: true` & valid `exp`/`nbf` -> Commit core lane + Inject Claims
//!                      ├── `active: false` / expired          -> Commit Negative + 401
//!                      └── IdP / ETP unavailable              -> `allow` (Continue) | 502 BadGateway
//! ```

use std::future::Future;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use http::header::{HeaderName, HeaderValue};
use velda_core::{Action, HeaderKey, L7Request, RejectReason};
use velda_etp::{EtpBackend, EtpInstance};

use super::cache::{CacheLookupKind, ColdOutcome, IntrospectionCache, MAX_OAUTH2_TOKEN_BYTES};
use super::inflight::{InFlightCoordinator, revalidate_on_shadow};
use super::providers::{AuthMethod, IntrospectionCallParams, validate_attached_etp};
use super::validation::Oauth2IntrospectionConfig;
use crate::capability::{AccessLevel, Direction, PluginCapability, SupportedProtocols};
use crate::config::PluginConfig;
use crate::error::PluginError;
use crate::pre_upstream::PreUpstreamPlugin;

const KEY_COOKIE: HeaderKey = HeaderKey::from_static("cookie");
const WWW_AUTH_MISSING: HeaderValue = HeaderValue::from_static("Bearer realm=\"velda-edge\"");
const WWW_AUTH_INVALID: HeaderValue = HeaderValue::from_static("Bearer error=\"invalid_token\"");
const WWW_AUTH_SCOPE: HeaderValue = HeaderValue::from_static("Bearer error=\"insufficient_scope\"");

/// Pre-Upstream OAuth 2.0 / OIDC / Vault Token Introspection Plugin backed by `velda-etp`.
#[repr(C, align(64))]
pub struct Oauth2IntrospectionPlugin {
    cache: Arc<IntrospectionCache>,
    in_flight: Arc<InFlightCoordinator>,
    attached_etp: OnceLock<EtpBackend>,
    call_params: Arc<IntrospectionCallParams>,
    fail_open: bool,
    hide_credentials: bool,
    token_headers: Box<[HeaderKey]>,
    token_cookies: Box<[Box<str>]>,
    token_query_params: Box<[Box<str>]>,
    claims_to_headers: Arc<[(Box<str>, HeaderKey)]>,
    required_scopes: Arc<[String]>,
    etp_id: Box<str>,
    pub id: String,
    pub config: Oauth2IntrospectionConfig,
}

impl Oauth2IntrospectionPlugin {
    /// Constructs a new [`Oauth2IntrospectionPlugin`] awaiting `wire_plugin_etps` / `attach_etp`.
    pub fn new(
        id: impl Into<String>,
        config: Oauth2IntrospectionConfig,
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

        let call_params = Arc::new(IntrospectionCallParams {
            path_override: config
                .path
                .as_deref()
                .map(|s| s.to_string().into_boxed_str()),
            client_id: config
                .client_id
                .as_deref()
                .map(|s| s.to_string().into_boxed_str()),
            client_secret: config
                .client_secret
                .as_deref()
                .map(|s| s.to_string().into_boxed_str()),
            auth_method: AuthMethod::from_validated(&config.auth_method),
            token_type_hint: if config.token_type_hint == "none" {
                None
            } else {
                Some(config.token_type_hint.clone().into_boxed_str())
            },
            request_timeout: Duration::from_millis(config.request_timeout_ms),
            prebaked_auth: OnceLock::new(),
        });

        let cache = Arc::new(IntrospectionCache::new(
            config.cache_ttl_seconds,
            config.negative_cache_ttl_seconds,
        ));

        Ok(Self {
            cache,
            in_flight: Arc::new(InFlightCoordinator::new()),
            attached_etp: OnceLock::new(),
            call_params,
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
            etp_id: config.etp_id.clone().into_boxed_str(),
            id: id_str,
            config,
        })
    }

    /// Builder convenience for tests attaching an [`EtpInstance`] directly.
    pub fn with_etp(self, instance: &EtpInstance) -> Result<Self, PluginError> {
        self.attach_etp(instance)?;
        Ok(self)
    }

    /// Deserializes, validates, and constructs an instance directly from a [`PluginConfig`].
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let cfg =
            super::validation::validate_oauth2_introspection_config(&config.id, &config.config)?;
        Self::new(&config.id, cfg)
    }

    /// Returns a reference to the Per-Core Two-Tier L1D/L3 SIMD + Cold DRAM cache.
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
                let trimmed = s.trim_ascii();
                if trimmed.is_empty() {
                    continue;
                }
                if trimmed
                    .as_bytes()
                    .get(..7)
                    .is_some_and(|p| p.eq_ignore_ascii_case(b"bearer "))
                    && let Some(rest) = trimmed.get(7..)
                {
                    let token = rest.trim_ascii();
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
                        if let Some((k, v)) = pair.trim_ascii().split_once('=')
                            && k.trim_ascii() == target_cookie.as_ref()
                        {
                            let token = v.trim_ascii();
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
                        let token = v.trim_ascii();
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
            req.headers.remove(&key.name);
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
        let Some(etp) = self.attached_etp.get().cloned() else {
            self.cache.finish_revalidation(fp);
            return;
        };
        let origin_lane = self.cache.current_lane_idx();
        let params = Arc::clone(&self.call_params);
        let cache = Arc::clone(&self.cache);
        let claims_to_headers = Arc::clone(&self.claims_to_headers);
        let required_scopes = Arc::clone(&self.required_scopes);
        let token_owned = token.to_string();

        let task = Box::pin(async move {
            revalidate_on_shadow(
                &etp,
                &params,
                &cache,
                &claims_to_headers,
                &required_scopes,
                &token_owned,
                fp,
                origin_lane,
            )
            .await;
        });

        if crate::hook::try_dispatch_shadow(task).is_err() {
            self.cache.finish_revalidation_on_lane(origin_lane, fp);
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

    #[inline(always)]
    fn etp_id(&self) -> Option<&str> {
        Some(&self.etp_id)
    }

    fn attach_etp(&self, instance: &EtpInstance) -> Result<(), PluginError> {
        validate_attached_etp(&self.id, instance, &self.call_params)?;
        let _ = self.attached_etp.set(instance.backend.clone());
        Ok(())
    }

    fn on_upstream_request(&self, req: &mut L7Request, targets: &mut u32) -> Action {
        if let Some(action) = self.try_on_upstream_request(req, targets) {
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
    fn try_on_upstream_request(&self, req: &mut L7Request, _targets: &mut u32) -> Option<Action> {
        self.cache.prefetch_control_l1();

        let (fp, matched_header) = {
            let Some((token, matched_header)) = self.extract_token(req) else {
                return Some(Self::reject_missing_token(req));
            };
            if token.len() > MAX_OAUTH2_TOKEN_BYTES {
                return Some(Self::reject_invalid_token(req));
            }
            if !self.cache.is_enabled() {
                return None;
            }
            (self.cache.token_to_u128(token), matched_header)
        };

        let now = req.ingress_time_sec();
        let status = self.cache.lookup_and_apply(fp, now, |pairs| {
            req.headers.reserve(pairs.len());
            for (key, val) in pairs {
                req.headers.insert_prehashed(key, val.clone());
            }
        });

        match status {
            CacheLookupKind::Hit => {
                if self.hide_credentials
                    && let Some(key) = matched_header
                {
                    req.headers.remove(&key.name);
                }
                Some(Action::Continue)
            }
            CacheLookupKind::Stale => {
                if self.cache.mark_revalidating(fp, now)
                    && let Some((token, _)) = self.extract_token(req)
                {
                    self.dispatch_shadow_revalidation(token, fp);
                }
                if self.hide_credentials
                    && let Some(key) = matched_header
                {
                    req.headers.remove(&key.name);
                }
                Some(Action::Continue)
            }
            CacheLookupKind::Forbidden => Some(Self::reject_insufficient_scope(req)),
            CacheLookupKind::Unauthorized => Some(Self::reject_invalid_token(req)),
            CacheLookupKind::Miss => None,
        }
    }

    fn on_upstream_request_async<'a>(
        &'a self,
        req: &'a mut L7Request,
        targets: &'a mut u32,
    ) -> Pin<Box<dyn Future<Output = Action> + Send + 'a>> {
        Box::pin(async move {
            if let Some(action) = self.try_on_upstream_request(req, targets) {
                return action;
            }

            // Defer owned `String` allocation strictly to uncached/miss requests requiring async IdP I/O.
            let Some((token, matched_header)) = self.extract_token(req) else {
                return Self::reject_missing_token(req);
            };
            if token.len() > MAX_OAUTH2_TOKEN_BYTES {
                return Self::reject_invalid_token(req);
            }
            let fp = self.cache.token_to_u128(token);
            let token_owned = token.to_string();

            let Some(etp) = self.attached_etp.get() else {
                return if self.fail_open {
                    Action::Continue
                } else {
                    Action::Reject(RejectReason::BadGateway)
                };
            };

            let outcome = self
                .in_flight
                .introspect_coalesced(
                    etp,
                    &self.call_params,
                    &self.cache,
                    &self.claims_to_headers,
                    &self.required_scopes,
                    &token_owned,
                    fp,
                )
                .await;

            match outcome {
                ColdOutcome::Allow(headers) => {
                    self.apply_allow(req, matched_header, headers.as_ref())
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
    use crate::hook::set_current_worker_lane;
    use crate::pre_upstream::run_pre_upstream_plugins_async;
    use http::{Method, Uri, Version};
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use velda_core::{Body, HeaderWorkspace};
    use velda_etp::registry::build_instance;
    use velda_etp::{EtpEntryConfig, EtpKind, EtpPoolSettings, EtpTlsConfig, EtpTransportSecurity};

    async fn spawn_mock_idp(
        calls: Arc<AtomicUsize>,
        resp_fn: impl Fn(usize, &str) -> (u16, String) + Send + Sync + 'static,
        chunked: bool,
    ) -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let resp_fn = Arc::new(resp_fn);

        tokio::spawn(async move {
            while let Ok((mut sock, _)) = listener.accept().await {
                let calls = Arc::clone(&calls);
                let resp_fn = Arc::clone(&resp_fn);
                tokio::spawn(async move {
                    let mut buf = [0u8; 4096];
                    while let Ok(n) = sock.read(&mut buf).await {
                        if n == 0 {
                            break;
                        }
                        let req_str = String::from_utf8_lossy(&buf[..n]);
                        let seq = calls.fetch_add(1, AtomicOrdering::SeqCst) + 1;
                        let (status, body) = resp_fn(seq, &req_str);
                        let reason = match status {
                            200 => "OK",
                            400 => "Bad Request",
                            401 => "Unauthorized",
                            403 => "Forbidden",
                            _ => "Error",
                        };
                        let wire = if chunked {
                            let hex_len = format!("{:x}", body.len());
                            format!(
                                "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\nConnection: keep-alive\r\n\r\n{hex_len}\r\n{body}\r\n0\r\n\r\n"
                            )
                        } else {
                            format!(
                                "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n{body}",
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

    fn make_etp(
        id: &str,
        kind: EtpKind,
        security: EtpTransportSecurity,
        settings: serde_json::Value,
        tls: Option<EtpTlsConfig>,
    ) -> EtpInstance {
        let entry = EtpEntryConfig {
            id: id.into(),
            kind,
            security,
            pool: EtpPoolSettings {
                pool_size: 2,
                connect_timeout_ms: 2000,
                idle_timeout_ms: 30_000,
            },
            tls,
            settings,
        };
        build_instance(&entry).unwrap()
    }

    fn base_config(
        etp_id: &str,
        path: Option<&str>,
        auth_method: &str,
        cache_ttl_seconds: u64,
    ) -> Oauth2IntrospectionConfig {
        serde_json::from_value(serde_json::json!({
            "etp_id": etp_id,
            "path": path,
            "client_id": if auth_method == "none" { serde_json::Value::Null } else { serde_json::json!("velda-gw") },
            "client_secret": if auth_method == "none" { serde_json::Value::Null } else { serde_json::json!("gw-secret-12345") },
            "auth_method": auth_method,
            "token_headers": ["x-access-token", "authorization"],
            "token_cookies": ["access_token"],
            "token_query_params": ["token"],
            "token_type_hint": "access_token",
            "cache_ttl_seconds": cache_ttl_seconds,
            "negative_cache_ttl_seconds": 10,
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
    async fn test_keycloak_chunked_cold_miss_warm_hit_and_header_semantics() {
        set_current_worker_lane(0);
        let calls = Arc::new(AtomicUsize::new(0));
        let addr = spawn_mock_idp(
            Arc::clone(&calls),
            |_seq, req| {
                assert!(req.contains("POST /realms/app/protocol/openid-connect/token/introspect"));
                assert!(req.contains("Authorization: Basic "));
                (
                    200,
                    r#"{"active":true,"sub":"usr_777","scope":"read orders","exp":2500000000}"#
                        .into(),
                )
            },
            true,
        )
        .await;

        let etp = make_etp(
            "kc-main",
            EtpKind::Keycloak,
            EtpTransportSecurity::Plain,
            serde_json::json!({ "host": addr.ip().to_string(), "port": addr.port() }),
            None,
        );
        let cfg = base_config(
            "kc-main",
            Some("/realms/app/protocol/openid-connect/token/introspect"),
            "client_secret_basic",
            300,
        );
        let plugin = Arc::new(
            Oauth2IntrospectionPlugin::new("oauth2", cfg)
                .unwrap()
                .with_etp(&etp)
                .unwrap(),
        );
        let chain: Vec<Arc<dyn PreUpstreamPlugin>> = vec![plugin];

        // 1. Cold miss -> yields via `.await`, decodes chunked body
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
    async fn test_https_tls_introspection_via_etp() {
        set_current_worker_lane(0);
        let cert = rcgen::generate_simple_self_signed(vec!["127.0.0.1".into()]).unwrap();
        let cert_pem = cert.cert.pem();
        let cert_der = rustls::pki_types::CertificateDer::from(cert.cert.der().to_vec());
        let key_der =
            rustls::pki_types::PrivateKeyDer::try_from(cert.signing_key.serialize_der()).unwrap();

        let server_cfg = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::aws_lc_rs::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
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

        let tls_cfg = EtpTlsConfig {
            ca_cert_pem: Some(cert_pem),
            server_name: Some("127.0.0.1".into()),
            ..Default::default()
        };
        let etp = make_etp(
            "kc-tls",
            EtpKind::Keycloak,
            EtpTransportSecurity::Tls,
            serde_json::json!({ "host": addr.ip().to_string(), "port": addr.port() }),
            Some(tls_cfg),
        );
        let cfg = base_config("kc-tls", Some("/introspect"), "client_secret_basic", 60);
        let plugin = Oauth2IntrospectionPlugin::new("oauth2", cfg)
            .unwrap()
            .with_etp(&etp)
            .unwrap();

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
        set_current_worker_lane(0);
        let calls = Arc::new(AtomicUsize::new(0));
        let addr = spawn_mock_idp(
            Arc::clone(&calls),
            |_seq, _req| {
                (
                    200,
                    r#"{"active":true,"sub":"sf_user","scope":"read","exp":2500000000}"#.into(),
                )
            },
            false,
        )
        .await;

        let etp = make_etp(
            "hydra-sf",
            EtpKind::Hydra,
            EtpTransportSecurity::Plain,
            serde_json::json!({ "host": addr.ip().to_string(), "port": addr.port() }),
            None,
        );
        let cfg = base_config("hydra-sf", None, "none", 60);
        let plugin = Arc::new(
            Oauth2IntrospectionPlugin::new("oauth2", cfg)
                .unwrap()
                .with_etp(&etp)
                .unwrap(),
        );

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

        let n_calls = calls.load(AtomicOrdering::SeqCst);
        assert!(
            n_calls <= 2,
            "expected single-flight coalescing, got {n_calls} IdP calls"
        );
    }

    #[tokio::test]
    async fn test_zero_ttl_disables_positive_cache_for_instant_logout_revocation() {
        set_current_worker_lane(0);
        let calls = Arc::new(AtomicUsize::new(0));
        let addr = spawn_mock_idp(
            Arc::clone(&calls),
            |seq, _req| {
                if seq == 1 {
                    (
                        200,
                        r#"{"active":true,"sub":"sensitive_user","scope":"read","exp":2500000000}"#
                            .into(),
                    )
                } else {
                    (200, r#"{"active":false}"#.into())
                }
            },
            false,
        )
        .await;

        let etp = make_etp(
            "okta-rev",
            EtpKind::Okta,
            EtpTransportSecurity::Plain,
            serde_json::json!({ "host": addr.ip().to_string(), "port": addr.port() }),
            None,
        );
        let cfg = base_config("okta-rev", None, "client_secret_post", 0);
        let plugin = Arc::new(
            Oauth2IntrospectionPlugin::new("oauth2", cfg)
                .unwrap()
                .with_etp(&etp)
                .unwrap(),
        );

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
        assert!(matches!(
            plugin
                .on_upstream_request_async(&mut make_req(), &mut up)
                .await,
            Action::Reject(RejectReason::Unauthorized)
        ));
        assert!(matches!(
            plugin.try_on_upstream_request(&mut make_req(), &mut up),
            Some(Action::Reject(RejectReason::Unauthorized))
        ));
        assert_eq!(calls.load(AtomicOrdering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_insufficient_scope_and_idp_unavailable_fallback() {
        set_current_worker_lane(0);
        let calls = Arc::new(AtomicUsize::new(0));
        let addr = spawn_mock_idp(
            Arc::clone(&calls),
            |_seq, _req| {
                (
                    200,
                    r#"{"active":true,"sub":"viewer","scope":"profile","exp":2500000000}"#.into(),
                )
            },
            false,
        )
        .await;

        let etp = make_etp(
            "oidc-scope",
            EtpKind::Oidc,
            EtpTransportSecurity::Plain,
            serde_json::json!({ "host": addr.ip().to_string(), "port": addr.port() }),
            None,
        );
        let cfg = base_config("oidc-scope", None, "client_secret_basic", 60);
        let plugin = Oauth2IntrospectionPlugin::new("oauth2", cfg)
            .unwrap()
            .with_etp(&etp)
            .unwrap();

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
        assert!(matches!(
            plugin.try_on_upstream_request(&mut make_req(), &mut up),
            Some(Action::Reject(RejectReason::Forbidden))
        ));
        assert_eq!(calls.load(AtomicOrdering::SeqCst), 1);

        let dead_etp = make_etp(
            "dead-idp",
            EtpKind::Authelia,
            EtpTransportSecurity::Plain,
            serde_json::json!({ "host": "127.0.0.1", "port": 1 }),
            None,
        );
        let dead_cfg = base_config("dead-idp", None, "client_secret_basic", 60);
        let fail_closed = Oauth2IntrospectionPlugin::new("oauth2-closed", dead_cfg.clone())
            .unwrap()
            .with_etp(&dead_etp)
            .unwrap();
        assert!(matches!(
            fail_closed
                .on_upstream_request_async(&mut make_req(), &mut up)
                .await,
            Action::Reject(RejectReason::BadGateway)
        ));

        let mut open_cfg = dead_cfg;
        open_cfg.fallback_behavior = "allow".into();
        let fail_open = Oauth2IntrospectionPlugin::new("oauth2-open", open_cfg)
            .unwrap()
            .with_etp(&dead_etp)
            .unwrap();
        assert!(matches!(
            fail_open
                .on_upstream_request_async(&mut make_req(), &mut up)
                .await,
            Action::Continue
        ));
    }

    #[tokio::test]
    async fn test_auth0_userinfo_and_vault_token_lookup_providers() {
        set_current_worker_lane(0);
        let auth0_calls = Arc::new(AtomicUsize::new(0));
        let auth0_addr = spawn_mock_idp(
            Arc::clone(&auth0_calls),
            |_seq, req| {
                assert!(req.starts_with("GET /userinfo "));
                if req.contains("Bearer tok_valid_a0") {
                    (200, r#"{"sub":"auth0|123","scope":"read"}"#.into())
                } else {
                    (401, r#"{"error":"invalid_token"}"#.into())
                }
            },
            false,
        )
        .await;

        let auth0_etp = make_etp(
            "auth0-prod",
            EtpKind::Auth0,
            EtpTransportSecurity::Plain,
            serde_json::json!({ "host": auth0_addr.ip().to_string(), "port": auth0_addr.port() }),
            None,
        );
        let auth0_plugin = Oauth2IntrospectionPlugin::new(
            "oauth2-auth0",
            base_config("auth0-prod", None, "none", 60),
        )
        .unwrap()
        .with_etp(&auth0_etp)
        .unwrap();

        let mut up = 1;
        let mut h_ok = HeaderWorkspace::empty(4);
        h_ok.insert(
            http::header::AUTHORIZATION,
            HeaderValue::from_static("Bearer tok_valid_a0"),
        );
        let mut req_ok = L7Request::new(
            Method::GET,
            Uri::from_static("/api"),
            Version::HTTP_11,
            h_ok,
            Body::Empty,
        );
        assert!(matches!(
            auth0_plugin
                .on_upstream_request_async(&mut req_ok, &mut up)
                .await,
            Action::Continue
        ));
        assert_eq!(req_ok.headers.get("x-user-id").unwrap(), "auth0|123");

        let mut h_bad = HeaderWorkspace::empty(4);
        h_bad.insert(
            http::header::AUTHORIZATION,
            HeaderValue::from_static("Bearer tok_expired_a0"),
        );
        let mut req_bad = L7Request::new(
            Method::GET,
            Uri::from_static("/api"),
            Version::HTTP_11,
            h_bad,
            Body::Empty,
        );
        assert!(matches!(
            auth0_plugin
                .on_upstream_request_async(&mut req_bad, &mut up)
                .await,
            Action::Reject(RejectReason::Unauthorized)
        ));

        // Vault lookup using ETP token
        let vault_calls = Arc::new(AtomicUsize::new(0));
        let vault_addr = spawn_mock_idp(
            Arc::clone(&vault_calls),
            |_seq, req| {
                assert!(req.contains("POST /v1/auth/token/lookup"));
                assert!(req.contains("X-Vault-Token: svc-root"));
                if req.contains("vlt_good_tok") {
                    (
                        200,
                        r#"{"data":{"entity_id":"ent-9","policies":["read","write"],"ttl":120}}"#
                            .into(),
                    )
                } else {
                    (400, r#"{"errors":["bad token"]}"#.into())
                }
            },
            false,
        )
        .await;
        let vault_etp = make_etp(
            "vault-prod",
            EtpKind::Vault,
            EtpTransportSecurity::Plain,
            serde_json::json!({
                "host": vault_addr.ip().to_string(),
                "port": vault_addr.port(),
                "token": "svc-root"
            }),
            None,
        );
        let vault_plugin = Oauth2IntrospectionPlugin::new(
            "oauth2-vault",
            base_config("vault-prod", None, "none", 60),
        )
        .unwrap()
        .with_etp(&vault_etp)
        .unwrap();

        let mut h_v = HeaderWorkspace::empty(4);
        h_v.insert(
            http::header::AUTHORIZATION,
            HeaderValue::from_static("Bearer vlt_good_tok"),
        );
        let mut req_v = L7Request::new(
            Method::GET,
            Uri::from_static("/secrets"),
            Version::HTTP_11,
            h_v,
            Body::Empty,
        );
        assert!(matches!(
            vault_plugin
                .on_upstream_request_async(&mut req_v, &mut up)
                .await,
            Action::Continue
        ));
        assert_eq!(req_v.headers.get("x-user-id").unwrap(), "ent-9");
    }
}
