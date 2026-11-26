//! Pure ETP Provider Dispatcher (`oauth2_introspection::providers`).
//!
//! ```text
//!   inflight / shadow
//!        │
//!        ├─► execute(&IntrospectionCall)    ──► providers::<etp_kind>::execute()    (fetch -> parse -> cache)
//!        └─► revalidate(&IntrospectionCall) ──► providers::<etp_kind>::revalidate() (fetch -> parse -> cache)
//!
//!   Each provider subfolder (`providers/<name>/`) is isolated into:
//!     • `mod.rs`      -> Pure root export
//!     • `client.rs`   -> ETP StreamPool request/keep-alive HTTP execution
//!     • `response.rs` -> Provider JSON decode, claim/scope eval, and direct cache commit
//! ```

use std::time::Duration;

use velda_core::HeaderKey;
use velda_etp::{EtpBackend, EtpInstance};

use super::cache::{ColdOutcome, IntrospectionCache};
use crate::error::PluginError;

pub mod auth0;
pub mod authelia;
pub mod cognito;
pub mod entra_id;
pub mod hydra;
pub mod keycloak;
pub mod oidc;
pub mod okta;
pub mod vault;

/// Client authentication mode when contacting OAuth 2.0 endpoints.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthMethod {
    Basic,
    Post,
    None,
}

impl AuthMethod {
    /// Parses validated normalized authentication method string.
    #[inline]
    pub fn from_validated(s: &str) -> Self {
        match s {
            "client_secret_post" => Self::Post,
            "none" => Self::None,
            _ => Self::Basic,
        }
    }
}

/// Immutable runtime parameters supplied by `Oauth2IntrospectionPlugin`.
#[derive(Debug, Clone)]
pub struct IntrospectionCallParams {
    pub path_override: Option<Box<str>>,
    pub client_id: Option<Box<str>>,
    pub client_secret: Option<Box<str>>,
    pub auth_method: AuthMethod,
    pub token_type_hint: Option<Box<str>>,
    pub request_timeout: Duration,
}

/// Self-contained provider invocation context allowing each provider adapter to fetch,
/// parse, evaluate policies, and commit directly into `IntrospectionCache`.
pub struct IntrospectionCall<'a> {
    pub backend: &'a EtpBackend,
    pub params: &'a IntrospectionCallParams,
    pub cache: &'a IntrospectionCache,
    pub claims_to_headers: &'a [(Box<str>, HeaderKey)],
    pub required_scopes: &'a [String],
    pub token: &'a str,
    pub fp: u128,
    pub target_lane: usize,
}

/// Returns `true` when `backend` belongs to one of the supported OAuth2/Introspection ETP kinds.
#[inline]
pub fn is_supported_oauth2_etp(backend: &EtpBackend) -> bool {
    matches!(
        backend,
        EtpBackend::Keycloak(_)
            | EtpBackend::Hydra(_)
            | EtpBackend::Authelia(_)
            | EtpBackend::Okta(_)
            | EtpBackend::Oidc(_)
            | EtpBackend::Auth0(_)
            | EtpBackend::EntraId(_)
            | EtpBackend::Cognito(_)
            | EtpBackend::Vault(_)
    )
}

/// Validates ETP backend compatibility and credential completeness at wiring time.
pub fn validate_attached_etp(
    plugin_id: &str,
    etp: &EtpInstance,
    params: &IntrospectionCallParams,
) -> Result<(), PluginError> {
    if !is_supported_oauth2_etp(&etp.backend) {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}' cannot attach unsupported ETP '{}' of kind '{}'",
            etp.id, etp.kind
        )));
    }

    if params.auth_method == AuthMethod::None {
        return Ok(());
    }

    let (etp_cid, etp_csec) = match &etp.backend {
        EtpBackend::Keycloak(p) => (
            p.config.client_id.as_deref(),
            p.config.client_secret.as_deref(),
        ),
        EtpBackend::Hydra(p) => (
            p.config.client_id.as_deref(),
            p.config.client_secret.as_deref(),
        ),
        EtpBackend::Authelia(p) => (
            p.config.client_id.as_deref(),
            p.config.client_secret.as_deref(),
        ),
        EtpBackend::Okta(p) => (
            p.config.client_id.as_deref(),
            p.config.client_secret.as_deref(),
        ),
        EtpBackend::Oidc(p) => (
            p.config.client_id.as_deref(),
            p.config.client_secret.as_deref(),
        ),
        EtpBackend::Auth0(p) => {
            if params.path_override.is_none() {
                return Ok(());
            }
            (
                p.config.client_id.as_deref(),
                p.config.client_secret.as_deref(),
            )
        }
        EtpBackend::EntraId(p) => {
            if params.path_override.is_none() {
                return Ok(());
            }
            (
                p.config.client_id.as_deref(),
                p.config.client_secret.as_deref(),
            )
        }
        EtpBackend::Cognito(p) => {
            if params.path_override.is_none() {
                return Ok(());
            }
            (
                p.config.client_id.as_deref(),
                p.config.client_secret.as_deref(),
            )
        }
        EtpBackend::Vault(_) => return Ok(()),
        _ => unreachable!(),
    };

    let eff_cid = params
        .client_id
        .as_deref()
        .or(etp_cid)
        .filter(|s| !s.trim().is_empty());
    let eff_csec = params
        .client_secret
        .as_deref()
        .or(etp_csec)
        .filter(|s| !s.trim().is_empty());

    if eff_cid.is_none() || eff_csec.is_none() {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}' requires client_id and client_secret (in plugin config or ETP '{}') when auth_method != 'none'",
            etp.id
        )));
    }
    Ok(())
}

/// Dispatches a cold-miss introspection call to the target provider (fetches, parses, and caches).
pub async fn execute(call: &IntrospectionCall<'_>) -> (Option<u64>, ColdOutcome) {
    match call.backend {
        EtpBackend::Keycloak(pool) => keycloak::execute(pool, call).await,
        EtpBackend::Hydra(pool) => hydra::execute(pool, call).await,
        EtpBackend::Authelia(pool) => authelia::execute(pool, call).await,
        EtpBackend::Okta(pool) => okta::execute(pool, call).await,
        EtpBackend::Oidc(pool) => oidc::execute(pool, call).await,
        EtpBackend::Auth0(pool) => auth0::execute(pool, call).await,
        EtpBackend::EntraId(pool) => entra_id::execute(pool, call).await,
        EtpBackend::Cognito(pool) => cognito::execute(pool, call).await,
        EtpBackend::Vault(pool) => vault::execute(pool, call).await,
        _ => (None, ColdOutcome::Unavailable),
    }
}

/// Dispatches a shadow stale-while-revalidate refresh to the target provider.
pub async fn revalidate(call: &IntrospectionCall<'_>) {
    match call.backend {
        EtpBackend::Keycloak(pool) => keycloak::revalidate(pool, call).await,
        EtpBackend::Hydra(pool) => hydra::revalidate(pool, call).await,
        EtpBackend::Authelia(pool) => authelia::revalidate(pool, call).await,
        EtpBackend::Okta(pool) => okta::revalidate(pool, call).await,
        EtpBackend::Oidc(pool) => oidc::revalidate(pool, call).await,
        EtpBackend::Auth0(pool) => auth0::revalidate(pool, call).await,
        EtpBackend::EntraId(pool) => entra_id::revalidate(pool, call).await,
        EtpBackend::Cognito(pool) => cognito::revalidate(pool, call).await,
        EtpBackend::Vault(pool) => vault::revalidate(pool, call).await,
        _ => call
            .cache
            .finish_revalidation_on_lane(call.target_lane, call.fp),
    }
}
