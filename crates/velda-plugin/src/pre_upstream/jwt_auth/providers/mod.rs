//! Isolated ETP Provider Adapters Dispatcher (`jwt_auth::providers`).
//!
//! ```text
//!   Shadow Loop ──► fetch_etp_key_table(EtpInstance)
//!                        └──► match EtpBackend -> self-contained provider subfolder:
//!                               vault | aws_secrets_manager | s3
//!                               keycloak | hydra | authelia | okta | auth0 | entra_id | cognito | oidc
//! ```

use std::time::Duration;

use velda_etp::{EtpBackend, EtpInstance};

use super::crypto::JwtAlgorithm;
use super::store::{JwksKeyTable, KeyEntrySpec, MAX_JWKS_SLOTS};
use crate::error::PluginError;

pub mod auth0;
pub mod authelia;
pub mod aws_secrets_manager;
pub mod cognito;
pub mod entra_id;
pub mod hydra;
pub mod keycloak;
pub mod oidc;
pub mod okta;
pub mod s3;
pub mod vault;

/// Dispatches key retrieval to the selected provider subfolder and builds [`JwksKeyTable`].
pub async fn fetch_etp_key_table(
    instance: &EtpInstance,
    path_override: Option<&str>,
    static_fallback: Option<KeyEntrySpec>,
    default_alg: Option<JwtAlgorithm>,
    boot_salt: &[u8; 32],
    timeout: Duration,
) -> Result<JwksKeyTable, PluginError> {
    let mut entries = match &instance.backend {
        EtpBackend::Vault(pool) => {
            vault::fetch_keys(pool, path_override, default_alg, timeout).await?
        }
        EtpBackend::AwsSecretsManager(pool) => {
            aws_secrets_manager::fetch_keys(pool, path_override, default_alg, timeout).await?
        }
        EtpBackend::S3(pool) => s3::fetch_keys(pool, path_override, default_alg, timeout).await?,
        EtpBackend::Keycloak(pool) => {
            keycloak::fetch_keys(pool, path_override, default_alg, timeout).await?
        }
        EtpBackend::Hydra(pool) => {
            hydra::fetch_keys(pool, path_override, default_alg, timeout).await?
        }
        EtpBackend::Authelia(pool) => {
            authelia::fetch_keys(pool, path_override, default_alg, timeout).await?
        }
        EtpBackend::Okta(pool) => {
            okta::fetch_keys(pool, path_override, default_alg, timeout).await?
        }
        EtpBackend::Auth0(pool) => {
            auth0::fetch_keys(pool, path_override, default_alg, timeout).await?
        }
        EtpBackend::EntraId(pool) => {
            entra_id::fetch_keys(pool, path_override, default_alg, timeout).await?
        }
        EtpBackend::Cognito(pool) => {
            cognito::fetch_keys(pool, path_override, default_alg, timeout).await?
        }
        EtpBackend::Oidc(pool) => {
            oidc::fetch_keys(pool, path_override, default_alg, timeout).await?
        }
        other => {
            return Err(PluginError::Config(format!(
                "ETP '{}' kind '{:?}' is not a supported JWT key provider",
                instance.id,
                std::mem::discriminant(other)
            )));
        }
    };

    if entries.is_empty() && static_fallback.is_none() {
        return Err(PluginError::Config(format!(
            "ETP '{}' returned zero usable JWT keys",
            instance.id
        )));
    }

    if let Some(fallback) = static_fallback
        && entries.len() < MAX_JWKS_SLOTS
    {
        entries.push(fallback);
    }

    JwksKeyTable::build_from_entries(&entries, 0, false, true, boot_salt)
}
