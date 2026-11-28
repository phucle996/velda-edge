//! Unified JWT Authentication Pre-Upstream Plugin (`jwt_auth::plugin`).
//!
//! ```text
//!                      UNIFIED JWT AUTH ARCHITECTURE (1 PLUGIN)
//!
//!   Config (`mode`: internal[0] | external[1] bit on L1D)
//!     ├─► MODE_INTERNAL (0): Static key pinned at boot (`pbww() == 0`, 0 network I/O)
//!     └─► MODE_EXTERNAL (1): Borrows ETP Lease in Shadow (`pbww() > 0`, `coarse_sec()` loop)
//!                           └─► providers/<etp_kind> ──► JwksStore (ArcSwap<JwksKeyTable>)
//!
//!   Hot Path (`on_upstream_request` on `velda-worker`):
//!     1. Prefetch L1D JwtCache control block + extract token from Header / Cookie / Query
//!     2. Convert JWT -> `fp: u128`, check L3 `JwtCache` against `JwksStore` key_tags
//!          ├─ [Positive Hit] ──► `insert_prehashed` headers & Continue
//!          ├─ [Reject]       ──► 401 / 403
//!          └─ [Cache Miss]   ──► Decode header alg (`u4`) & kid (`u64` w/ boot_salt)
//!                                  └─► Scan L1D `tags & valid_mask` -> verify byte arena
//!                                        ├─ Valid -> commit `key_tag` + Continue
//!                                        └─ Fail  -> commit bounded reject (skip NoActiveKeys)
//! ```

use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use http::header::{HeaderName, HeaderValue};
use tokio::sync::{Notify, watch};
use velda_core::{Action, HeaderKey, InternalStatus, L7Request};
use velda_etp::{EtpBackend, EtpInstance};

use super::cache::{CacheHitKind, JwtCache, is_plausible_jwt_shape};
use super::claims::ClaimPath;
use super::crypto::{JwtAlgorithm, JwtValidationPolicy, JwtVerifyError, VerifierKey, verify_jwt};
use super::providers::fetch_etp_key_table;
use super::store::{JwksKeyTable, JwksStore, KeyEntrySpec};
use super::validation::{MODE_EXTERNAL, MODE_INTERNAL};
use crate::BackgroundTaskFuture;
use crate::capability::{AccessLevel, Direction, PluginCapability, SupportedProtocols};
use crate::config::PluginConfig;
use crate::error::PluginError;
use crate::pre_upstream::PreUpstreamPlugin;

const KEY_COOKIE: HeaderKey = HeaderKey::from_static("cookie");
const WWW_AUTH_MISSING: HeaderValue = HeaderValue::from_static("Bearer realm=\"velda-edge\"");
const WWW_AUTH_INVALID: HeaderValue = HeaderValue::from_static("Bearer error=\"invalid_token\"");
const UNKNOWN_KID_RESYNC_COOLDOWN_SECS: u64 = 2;

/// Pre-compiled token extraction source using pre-hashed [`HeaderKey`] descriptors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TokenSource {
    Header { key: HeaderKey, prefix: Box<str> },
    Cookie { name: Box<str> },
    Query { param: Box<str> },
}

#[inline]
fn is_supported_jwt_etp(backend: &EtpBackend) -> bool {
    matches!(
        backend,
        EtpBackend::Vault(_)
            | EtpBackend::AwsSecretsManager(_)
            | EtpBackend::S3(_)
            | EtpBackend::Keycloak(_)
            | EtpBackend::Hydra(_)
            | EtpBackend::Authelia(_)
            | EtpBackend::Okta(_)
            | EtpBackend::Auth0(_)
            | EtpBackend::EntraId(_)
            | EtpBackend::Cognito(_)
            | EtpBackend::Oidc(_)
    )
}

/// Self-contained JWT authentication plugin supporting static keys (`internal`)
/// and ETP-backed background rotation (`external`) with isolated `JwksStore`.
#[repr(C, align(64))]
pub struct JwtAuthPlugin {
    pub cache: JwtCache,
    pub mode_bit: u8,
    pub preserve_static_key: bool,
    pub validate_exp: bool,
    pub default_alg: Option<JwtAlgorithm>,
    pub pbww_weight: u32,
    pub clock_skew_secs: u64,
    pub refresh_interval_secs: u64,
    pub request_timeout_secs: u64,
    pub key_store: Arc<JwksStore>,
    pub static_fallback: Option<KeyEntrySpec>,
    pub etp_id: Option<Box<str>>,
    pub etp_path: Option<Box<str>>,
    attached_etp: OnceLock<EtpInstance>,
    pub token_sources: Box<[TokenSource]>,
    pub issuer: Option<Box<str>>,
    pub audience: Option<Box<str>>,
    pub claims_to_headers: Box<[(ClaimPath, HeaderKey)]>,
    pub assert_claims: Box<[(ClaimPath, Box<str>)]>,
    pub id: String,
    unknown_kid_notify: Notify,
    last_demand_sync_sec: AtomicU64,
}

impl JwtAuthPlugin {
    /// Deserializes and constructs a [`JwtAuthPlugin`] from a [`PluginConfig`].
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let cfg = super::validation::validate_jwt_auth_config(&config.id, &config.config).map_err(
            |e| {
                PluginError::Config(format!(
                    "Invalid jwt_auth configuration for '{}': {e}",
                    config.id
                ))
            },
        )?;

        let negative_ttl = if cfg.cache_ttl_secs == 0 {
            0
        } else {
            10.min(cfg.cache_ttl_secs)
        };
        let core_lanes = velda_core::worker::get_allowed_cores().len().max(1);
        let cache = JwtCache::hardware_sized(core_lanes, cfg.cache_ttl_secs, negative_ttl);

        let mode_bit = cfg.mode_bit();
        let default_alg = match cfg.algorithm.as_deref() {
            Some(a) => Some(JwtAlgorithm::from_str(a)?),
            None => None,
        };

        let static_spec = if cfg.secret_key.is_some() || cfg.public_key_pem.is_some() {
            let alg = default_alg
                .ok_or_else(|| PluginError::Config("Static key requires 'algorithm'".into()))?;
            let key = VerifierKey::new(
                alg,
                cfg.secret_key.as_deref(),
                cfg.public_key_pem.as_deref(),
            )?;
            Some(KeyEntrySpec {
                raw_kid: None,
                alg,
                key,
            })
        } else {
            None
        };

        let initial_table = if let Some(spec) = static_spec.as_ref() {
            JwksKeyTable::build_from_entries(
                std::slice::from_ref(spec),
                0,
                true,
                mode_bit == MODE_EXTERNAL,
                cache.boot_salt(),
            )?
        } else {
            JwksKeyTable::build_from_entries(
                &[],
                0,
                false,
                mode_bit == MODE_EXTERNAL,
                cache.boot_salt(),
            )?
        };
        let static_fallback = if cfg.preserve_static_key {
            static_spec
        } else {
            None
        };
        let key_store = Arc::new(JwksStore::new(initial_table));

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

        Ok(Self {
            cache,
            mode_bit,
            preserve_static_key: cfg.preserve_static_key,
            validate_exp: cfg.validate_exp,
            default_alg,
            pbww_weight: if mode_bit == MODE_EXTERNAL {
                cfg.pbww
            } else {
                0
            },
            clock_skew_secs: cfg.clock_skew_secs,
            refresh_interval_secs: cfg.refresh_interval_secs,
            request_timeout_secs: cfg.request_timeout_secs,
            key_store,
            static_fallback,
            etp_id: cfg.etp_id.map(String::into_boxed_str),
            etp_path: cfg.path.map(String::into_boxed_str),
            attached_etp: OnceLock::new(),
            token_sources: token_sources.into_boxed_slice(),
            issuer: cfg.issuer.map(String::into_boxed_str),
            audience: cfg.audience.map(String::into_boxed_str),
            claims_to_headers: claims_to_headers.into_boxed_slice(),
            assert_claims: assert_claims.into_boxed_slice(),
            id: config.id.clone(),
            unknown_kid_notify: Notify::new(),
            last_demand_sync_sec: AtomicU64::new(0),
        })
    }

    /// Triggers at most one Shadow JWKS sync every `UNKNOWN_KID_RESYNC_COOLDOWN_SECS` when an
    /// unrecognized `kid` arrives in `external` mode, using 0 blocking I/O on the Frontline thread.
    #[inline]
    fn maybe_trigger_unknown_kid_sync(&self, now_sec: u64) {
        if self.mode_bit != MODE_EXTERNAL || self.pbww_weight == 0 {
            return;
        }
        let prev = self.last_demand_sync_sec.load(Ordering::Relaxed);
        if prev != 0 && now_sec.saturating_sub(prev) < UNKNOWN_KID_RESYNC_COOLDOWN_SECS {
            return;
        }
        let next_stamp = now_sec.max(1);
        if self
            .last_demand_sync_sec
            .compare_exchange(prev, next_stamp, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
        {
            self.unknown_kid_notify.notify_one();
        }
    }

    /// Executes one key-fetch and atomic [`JwksStore`] swap against the attached ETP instance.
    pub async fn sync_keys_once(&self) -> Result<bool, PluginError> {
        let etp = self.attached_etp.get().ok_or_else(|| {
            PluginError::Config(format!(
                "Plugin '{}' (jwt_auth) has no wired ETP instance for etp_id '{}'",
                self.id,
                self.etp_id.as_deref().unwrap_or("")
            ))
        })?;

        let table = fetch_etp_key_table(
            etp,
            self.etp_path.as_deref(),
            self.static_fallback.clone(),
            self.default_alg,
            self.cache.boot_salt(),
            Duration::from_secs(self.request_timeout_secs.max(1)),
        )
        .await?;

        let primary_alg = JwtAlgorithm::from_u4(table.alg_u4(table.dir.primary_slot as usize))
            .map(|a| a.as_str())
            .unwrap_or("unknown");
        let slot_count = table.dir.valid_mask.count_ones();
        let changed = self.key_store.update(table);
        if changed {
            tracing::info!(
                plugin_id = %self.id,
                etp_id = %etp.id,
                etp_kind = %etp.kind,
                active_slots = slot_count,
                primary_alg = %primary_alg,
                "Updated JwksStore keys from ETP provider"
            );
        }
        Ok(changed)
    }

    /// Scans L2-warm `HeaderWorkspace` and URI using pre-compiled `TokenSource` order.
    #[inline]
    fn extract_token<'a>(&self, req: &'a L7Request) -> Option<&'a str> {
        for src in &self.token_sources {
            match src {
                TokenSource::Header { key, prefix } => {
                    if let Some(hdr_val) = req.headers.get_prehashed(key)
                        && let Ok(raw_str) = hdr_val.to_str()
                    {
                        let trimmed = raw_str.trim();
                        let token = if prefix.is_empty() {
                            trimmed
                        } else if let Some(rest) = trimmed.strip_prefix(prefix.as_ref()) {
                            rest.trim()
                        } else {
                            continue;
                        };
                        if !token.is_empty() {
                            return Some(token);
                        }
                    }
                }
                TokenSource::Cookie { name } => {
                    for hdr_val in req.headers.get_all_prehashed(&KEY_COOKIE) {
                        if let Ok(cookie_header) = hdr_val.to_str() {
                            for pair in cookie_header.split(';') {
                                let pair = pair.trim();
                                if let Some((k, v)) = pair.split_once('=')
                                    && k.trim() == name.as_ref()
                                {
                                    let val = v.trim();
                                    if !val.is_empty() {
                                        return Some(val);
                                    }
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
                                && !v.is_empty()
                            {
                                return Some(v);
                            }
                        }
                    }
                }
            }
        }
        None
    }

    #[inline(always)]
    fn reject_missing_token(req: &mut L7Request) -> Action {
        req.headers
            .insert_bypass(http::header::WWW_AUTHENTICATE, WWW_AUTH_MISSING);
        Action::Status(InternalStatus::Unauthorized)
    }

    #[inline(always)]
    fn reject_invalid_token(req: &mut L7Request) -> Action {
        req.headers
            .insert_bypass(http::header::WWW_AUTHENTICATE, WWW_AUTH_INVALID);
        Action::Status(InternalStatus::Unauthorized)
    }
}

impl PreUpstreamPlugin for JwtAuthPlugin {
    #[inline]
    fn id(&self) -> &str {
        &self.id
    }

    #[inline(always)]
    fn pbww(&self) -> u32 {
        if self.mode_bit == MODE_EXTERNAL {
            self.pbww_weight
        } else {
            0
        }
    }

    #[inline(always)]
    fn etp_id(&self) -> Option<&str> {
        if self.mode_bit == MODE_EXTERNAL {
            self.etp_id.as_deref()
        } else {
            None
        }
    }

    fn attach_etp(&self, instance: &EtpInstance) -> Result<(), PluginError> {
        if self.mode_bit == MODE_INTERNAL {
            return Ok(());
        }
        if !is_supported_jwt_etp(&instance.backend) {
            return Err(PluginError::Config(format!(
                "Plugin '{}' (jwt_auth) does not support ETP kind '{}' for etp_id '{}'",
                self.id, instance.kind, instance.id
            )));
        }
        let cloned = EtpInstance {
            id: instance.id.clone(),
            kind: instance.kind,
            fingerprint: instance.fingerprint,
            config: instance.config.clone(),
            backend: instance.backend.clone(),
        };
        let _ = self.attached_etp.set(cloned);
        Ok(())
    }

    fn run_background(
        self: Arc<Self>,
        mut shutdown: watch::Receiver<bool>,
    ) -> Option<BackgroundTaskFuture> {
        if self.mode_bit != MODE_EXTERNAL || self.pbww_weight == 0 {
            return None;
        }
        Some(Box::pin(async move {
            tracing::info!(
                plugin_id = %self.id,
                etp_id = %self.etp_id.as_deref().unwrap_or(""),
                refresh_interval_secs = self.refresh_interval_secs,
                "Starting unified jwt_auth Shadow key rotation daemon (coarse_sec)"
            );

            if let Err(err) = self.sync_keys_once().await {
                tracing::warn!(
                    plugin_id = %self.id,
                    error = %err,
                    "Initial jwt_auth ETP key fetch failed; retaining fallback/LKG"
                );
            }

            let mut next_sync_sec =
                velda_core::clock::coarse_sec().saturating_add(self.refresh_interval_secs);

            loop {
                tokio::select! {
                    biased;
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() {
                            tracing::info!(plugin_id = %self.id, "Shutting down jwt_auth key rotation loop");
                            return Ok(());
                        }
                    }
                    _ = self.unknown_kid_notify.notified() => {
                        tracing::info!(
                            plugin_id = %self.id,
                            "Waking jwt_auth Shadow key rotation daemon on UnknownKid signal"
                        );
                        if let Err(err) = self.sync_keys_once().await {
                            tracing::warn!(
                                plugin_id = %self.id,
                                error = %err,
                                "On-demand jwt_auth ETP key sync failed; retaining LKG JwksStore"
                            );
                        }
                        next_sync_sec = velda_core::clock::coarse_sec()
                            .saturating_add(self.refresh_interval_secs);
                    }
                    _ = tokio::time::sleep(Duration::from_millis(250)) => {
                        let now_sec = velda_core::clock::coarse_sec();
                        if now_sec >= next_sync_sec {
                            if let Err(err) = self.sync_keys_once().await {
                                tracing::warn!(
                                    plugin_id = %self.id,
                                    error = %err,
                                    "Periodic jwt_auth ETP key rotation failed; retaining LKG JwksStore"
                                );
                            }
                            next_sync_sec = velda_core::clock::coarse_sec()
                                .saturating_add(self.refresh_interval_secs);
                        }
                    }
                }
            }
        }))
    }

    fn on_upstream_request(&self, req: &mut L7Request, _targets: &mut u32) -> Action {
        // 1. Pull 64B JwtCache control block into L1D (`_MM_HINT_T0`)
        self.cache.prefetch_control_l1();

        let now = req.ingress_time_sec();
        let table_guard = self.key_store.load();
        let cache_enabled = self.cache.is_enabled();

        // 2. Check L3 JwtCache via zero-atomic `lookup_and_apply` against JwksStore key_tags
        let fp = if cache_enabled {
            let fp = {
                let Some(token_str) = self.extract_token(req) else {
                    return Self::reject_missing_token(req);
                };
                if !is_plausible_jwt_shape(token_str) {
                    return Self::reject_invalid_token(req);
                }
                self.cache.token_to_u128(token_str)
            };

            match self
                .cache
                .lookup_and_apply(fp, now, &table_guard, |headers| {
                    for (key, val) in headers {
                        req.headers.insert_prehashed(key, val.clone());
                    }
                }) {
                CacheHitKind::Positive => return Action::Continue,
                CacheHitKind::Unauthorized => return Self::reject_invalid_token(req),
                CacheHitKind::Forbidden => return Action::Status(InternalStatus::Forbidden),
                CacheHitKind::Miss => {}
            }
            fp
        } else {
            0
        };

        let Some(token_str) = self.extract_token(req) else {
            return Self::reject_missing_token(req);
        };
        if !cache_enabled && !is_plausible_jwt_shape(token_str) {
            return Self::reject_invalid_token(req);
        }

        // 3. Cache Miss (or uncached `cache_ttl_secs: 0` hot path): verify signature via L1D/L3 JwksStore + per-slot u4 alg.
        // Kept non-#[cold] so uncached traffic avoids `.text.unlikely` / `-Os` degradation.
        let policy = JwtValidationPolicy {
            boot_salt: self.cache.boot_salt(),
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
                        verified.key_tag,
                        &table_guard,
                    );
                }
                Action::Continue
            }
            Err(JwtVerifyError::ClaimAssertionFailed) => {
                tracing::debug!(plugin_id = %self.id, "JWT rejected by claim assertion");
                if cache_enabled {
                    self.cache.insert_reject(
                        fp,
                        true,
                        now,
                        table_guard.dir.table_version_tag,
                        &table_guard,
                    );
                }
                Action::Status(InternalStatus::Forbidden)
            }
            Err(JwtVerifyError::NoActiveKeys) => {
                tracing::debug!(plugin_id = %self.id, "JWT rejected: JwksStore empty");
                self.maybe_trigger_unknown_kid_sync(now);
                Self::reject_invalid_token(req)
            }
            Err(err) => {
                tracing::debug!(plugin_id = %self.id, error = ?err, now = now, "JWT verification failed");
                if err == JwtVerifyError::UnknownKid {
                    self.maybe_trigger_unknown_kid_sync(now);
                }
                if cache_enabled {
                    self.cache.insert_reject(
                        fp,
                        false,
                        now,
                        table_guard.dir.table_version_tag,
                        &table_guard,
                    );
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::prelude::*;
    use http::header::AUTHORIZATION;
    use http::{Method, Uri, Version};
    use ring::hmac;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use velda_core::{Body, HeaderWorkspace};
    use velda_etp::registry::build_registry;
    use velda_etp::{EtpEntryConfig, EtpKind, EtpPoolSettings, EtpTransportSecurity};

    fn create_hs_token(
        alg: JwtAlgorithm,
        kid: Option<&str>,
        secret: &str,
        payload: &str,
    ) -> String {
        let header = match kid {
            Some(k) => format!(r#"{{"alg":"{}","typ":"JWT","kid":"{k}"}}"#, alg.as_str()),
            None => format!(r#"{{"alg":"{}","typ":"JWT"}}"#, alg.as_str()),
        };
        let h_b64 = BASE64_URL_SAFE_NO_PAD.encode(header.as_bytes());
        let p_b64 = BASE64_URL_SAFE_NO_PAD.encode(payload.as_bytes());
        let signing_input = format!("{h_b64}.{p_b64}");

        let ring_alg = match alg {
            JwtAlgorithm::HS256 => hmac::HMAC_SHA256,
            JwtAlgorithm::HS384 => hmac::HMAC_SHA384,
            JwtAlgorithm::HS512 => hmac::HMAC_SHA512,
            _ => unreachable!(),
        };
        let key = hmac::Key::new(ring_alg, secret.as_bytes());
        let tag = hmac::sign(&key, signing_input.as_bytes());
        format!(
            "{signing_input}.{}",
            BASE64_URL_SAFE_NO_PAD.encode(tag.as_ref())
        )
    }

    fn internal_jwt_json(secret: &str, claims_to_headers: serde_json::Value) -> serde_json::Value {
        serde_json::json!({
            "mode": "internal",
            "algorithm": "HS256",
            "secret_key": secret,
            "public_key_pem": null,
            "token_sources": [{ "type": "header", "name": "authorization", "prefix": "Bearer " }],
            "validate_exp": true,
            "clock_skew_secs": 60,
            "claims_to_headers": claims_to_headers,
            "assert_claims": {},
            "cache_ttl_secs": 300
        })
    }

    #[test]
    fn test_internal_mode_missing_and_valid_tokens() {
        let config = PluginConfig {
            id: "jwt_auth".into(),
            config: internal_jwt_json("my-secret-key", serde_json::json!({ "sub": "x-sub" })),
        };
        let plugin = JwtAuthPlugin::from_config(&config).unwrap();
        assert_eq!(plugin.mode_bit, MODE_INTERNAL);
        assert_eq!(plugin.pbww(), 0);
        assert!(plugin.etp_id().is_none());

        let mut req_missing = L7Request::new(
            Method::GET,
            Uri::from_static("http://example.com/api"),
            Version::HTTP_11,
            HeaderWorkspace::new(16),
            Body::Empty,
        );
        req_missing.ingress_time_nanos = 1_700_000_000 * 1_000_000_000;
        let mut up = 0;
        assert!(matches!(
            plugin.on_upstream_request(&mut req_missing, &mut up),
            Action::Status(InternalStatus::Unauthorized)
        ));

        let token = create_hs_token(
            JwtAlgorithm::HS256,
            None,
            "my-secret-key",
            r#"{"sub":"u100","exp":2000000000}"#,
        );
        let mut hdrs = HeaderWorkspace::new(16);
        hdrs.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
        );
        let mut req_ok = L7Request::new(
            Method::GET,
            Uri::from_static("http://example.com/api"),
            Version::HTTP_11,
            hdrs,
            Body::Empty,
        );
        req_ok.ingress_time_nanos = 1_700_000_000 * 1_000_000_000;
        assert!(matches!(
            plugin.on_upstream_request(&mut req_ok, &mut up),
            Action::Continue
        ));
        assert_eq!(
            req_ok
                .headers
                .get(HeaderName::from_static("x-sub"))
                .unwrap()
                .to_str()
                .unwrap(),
            "u100"
        );
    }

    #[tokio::test]
    async fn test_external_mode_vault_etp_fetches_key_with_alg_and_prioritizes_etp() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let srv = tokio::spawn(async move {
            while let Ok((mut s, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut buf = [0u8; 2048];
                    let _ = s.read(&mut buf).await;
                    let body = serde_json::json!({
                        "data": {
                            "data": {
                                "kid": "vault-hs512-v2",
                                "alg": "HS512",
                                "secret_key": "vault-managed-hs512-secret-value"
                            }
                        }
                    })
                    .to_string();
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = s.write_all(resp.as_bytes()).await;
                    let _ = s.flush().await;
                });
            }
        });

        let etp_reg = build_registry(&[EtpEntryConfig {
            id: "vault-jwt".into(),
            kind: EtpKind::Vault,
            security: EtpTransportSecurity::Plain,
            pool: EtpPoolSettings {
                pool_size: 1,
                connect_timeout_ms: 2000,
                idle_timeout_ms: 30_000,
            },
            tls: None,
            settings: serde_json::json!({ "host": "127.0.0.1", "port": port, "token": "root" }),
        }])
        .unwrap();

        let cfg = PluginConfig {
            id: "jwt_auth".into(),
            config: serde_json::json!({
                "mode": "external",
                "etp_id": "vault-jwt",
                "path": "/v1/secret/data/jwt",
                "algorithm": "HS256",
                "secret_key": "static-fallback-hs256-secret",
                "preserve_static_key": true,
                "token_sources": [{ "type": "header", "name": "authorization", "prefix": "Bearer " }],
                "validate_exp": true,
                "clock_skew_secs": 60,
                "claims_to_headers": { "sub": "x-user" },
                "assert_claims": {},
                "cache_ttl_secs": 300
            }),
        };
        let plugin = JwtAuthPlugin::from_config(&cfg).unwrap();
        assert_eq!(plugin.mode_bit, MODE_EXTERNAL);
        assert!(plugin.pbww() > 0);
        assert_eq!(plugin.etp_id(), Some("vault-jwt"));

        plugin
            .attach_etp(etp_reg.entries.get("vault-jwt").unwrap())
            .unwrap();
        assert!(plugin.sync_keys_once().await.unwrap());

        // Token signed with Vault-supplied HS512 key (different alg from static HS256!) verifies 200
        let vault_tok = create_hs_token(
            JwtAlgorithm::HS512,
            Some("vault-hs512-v2"),
            "vault-managed-hs512-secret-value",
            r#"{"sub":"alice","exp":2000000000}"#,
        );
        let mut h = HeaderWorkspace::new(16);
        h.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {vault_tok}")).unwrap(),
        );
        let mut req = L7Request::new(
            Method::GET,
            Uri::from_static("http://example.com/v1"),
            Version::HTTP_11,
            h,
            Body::Empty,
        );
        req.ingress_time_nanos = 1_700_000_000 * 1_000_000_000;
        let mut up = 0;
        assert!(matches!(
            plugin.on_upstream_request(&mut req, &mut up),
            Action::Continue
        ));
        assert_eq!(
            req.headers
                .get(HeaderName::from_static("x-user"))
                .unwrap()
                .to_str()
                .unwrap(),
            "alice"
        );

        srv.abort();
    }

    #[tokio::test]
    async fn test_unknown_kid_triggers_rate_limited_shadow_resync_and_recovers() {
        use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let fetch_count = Arc::new(AtomicUsize::new(0));
        let fetch_count_srv = Arc::clone(&fetch_count);

        let srv = tokio::spawn(async move {
            loop {
                let Ok((mut s, _)) = listener.accept().await else {
                    break;
                };
                let cnt = Arc::clone(&fetch_count_srv);
                tokio::spawn(async move {
                    let mut buf = [0u8; 2048];
                    let Ok(bytes) = s.read(&mut buf).await else {
                        return;
                    };
                    if bytes == 0 {
                        return;
                    }
                    let n = cnt.fetch_add(1, AtomicOrdering::SeqCst) + 1;
                    let body = if n == 1 {
                        serde_json::json!({
                            "data": { "data": { "kid": "v1", "alg": "HS256", "secret_key": "sec-v1" } }
                        })
                    } else {
                        serde_json::json!({
                            "data": { "data": { "kid": "v2", "alg": "HS256", "secret_key": "sec-v2" } }
                        })
                    }
                    .to_string();
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = s.write_all(resp.as_bytes()).await;
                    let _ = s.flush().await;
                });
            }
        });

        let etp_reg = build_registry(&[EtpEntryConfig {
            id: "vault-rot".into(),
            kind: EtpKind::Vault,
            security: EtpTransportSecurity::Plain,
            pool: EtpPoolSettings {
                pool_size: 1,
                connect_timeout_ms: 2000,
                idle_timeout_ms: 30_000,
            },
            tls: None,
            settings: serde_json::json!({ "host": "127.0.0.1", "port": port, "token": "root" }),
        }])
        .unwrap();

        let cfg = PluginConfig {
            id: "jwt_auth".into(),
            config: serde_json::json!({
                "mode": "external",
                "etp_id": "vault-rot",
                "path": "/v1/secret/data/jwt",
                "refresh_interval_secs": 300,
                "token_sources": [{ "type": "header", "name": "authorization", "prefix": "Bearer " }],
                "validate_exp": true,
                "clock_skew_secs": 60,
                "claims_to_headers": { "sub": "x-user" },
                "assert_claims": {},
                "cache_ttl_secs": 300
            }),
        };
        let plugin = Arc::new(JwtAuthPlugin::from_config(&cfg).unwrap());
        plugin
            .attach_etp(etp_reg.entries.get("vault-rot").unwrap())
            .unwrap();

        let (tx, rx) = watch::channel(false);
        let bg = plugin.clone().run_background(rx).unwrap();
        let bg_handle = tokio::spawn(bg);

        // Wait for initial sync (fetch #1 -> kid v1)
        for _ in 0..40 {
            if fetch_count.load(AtomicOrdering::SeqCst) >= 1 && plugin.key_store.load().has_keys() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert_eq!(fetch_count.load(AtomicOrdering::SeqCst), 1);

        let tok_v2 = create_hs_token(
            JwtAlgorithm::HS256,
            Some("v2"),
            "sec-v2",
            r#"{"sub":"bob","exp":2000000000}"#,
        );
        let mk_req = |t: &str, ts_sec: u64| {
            let mut h = HeaderWorkspace::new(16);
            h.insert(
                AUTHORIZATION,
                HeaderValue::from_str(&format!("Bearer {t}")).unwrap(),
            );
            let mut req = L7Request::new(
                Method::GET,
                Uri::from_static("http://example.com/v1"),
                Version::HTTP_11,
                h,
                Body::Empty,
            );
            req.ingress_time_nanos = ts_sec * 1_000_000_000;
            req
        };

        let mut up = 0;
        // Flood UnknownKid inside cooldown -> triggers only ONE immediate background sync
        let mut req1 = mk_req(&tok_v2, 1_700_000_000);
        assert!(matches!(
            plugin.on_upstream_request(&mut req1, &mut up),
            Action::Status(InternalStatus::Unauthorized)
        ));
        let mut req2 = mk_req(&tok_v2, 1_700_000_000);
        assert!(matches!(
            plugin.on_upstream_request(&mut req2, &mut up),
            Action::Status(InternalStatus::Unauthorized)
        ));

        // Shadow wakes immediately without waiting 300s and loads v2 (fetch #2)
        for _ in 0..40 {
            if fetch_count.load(AtomicOrdering::SeqCst) >= 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert_eq!(fetch_count.load(AtomicOrdering::SeqCst), 2);

        // Give store a moment to finish swapping after fetch #2
        for _ in 0..40 {
            let mut req3 = mk_req(&tok_v2, 1_700_000_001);
            if matches!(
                plugin.on_upstream_request(&mut req3, &mut up),
                Action::Continue
            ) {
                let _ = tx.send(true);
                let _ = bg_handle.await;
                srv.abort();
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("Expected UnknownKid demand sync to recover v2 within cooldown window");
    }
}
