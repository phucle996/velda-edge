//! Lock-free Vault/OpenBao ETP connection pool (`VaultEtp`) backed by [`StreamPool`].
//!
//! ```text
//!   VaultEtp::acquire()        ──► StreamLease (&mut EtpStream)
//!   VaultEtp::resolve_token()  ──► StaticToken | cached AppRole token (coarse_sec())
//!                                    └─► miss/expired -> POST /v1/auth/{mount}/login
//! ```

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;

use crate::config::EtpEntryConfig;
use crate::error::EtpError;
use crate::stream_pool::{StreamLease, StreamPool};
use crate::vault::config::{VaultAuthConfig, VaultEtpConfig};

const VAULT_TOKEN_RENEW_MARGIN_SECS: u64 = 10;
const MAX_VAULT_AUTH_RESPONSE_BYTES: usize = 256 * 1024;

#[derive(Debug, Clone)]
struct CachedVaultToken {
    token: String,
    expires_coarse_sec: u64,
}

/// Bounded connection pool & AppRole token manager for Vault / OpenBao.
pub struct VaultEtp {
    pub config: VaultEtpConfig,
    pub pool: Arc<StreamPool>,
    session: Mutex<Option<CachedVaultToken>>,
}

impl fmt::Debug for VaultEtp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VaultEtp")
            .field("id", &self.config.id)
            .field("endpoint", &self.config.endpoint)
            .field("namespace", &self.config.namespace)
            .field("baseline_slots", &self.pool.arena.baseline_slots)
            .finish()
    }
}

impl VaultEtp {
    pub fn from_entry(entry: &EtpEntryConfig) -> Result<Self, EtpError> {
        let cfg = VaultEtpConfig::from_entry(entry)?;
        let pool = Arc::new(StreamPool::new(
            &cfg.id,
            &cfg.host,
            cfg.port,
            cfg.tls.as_ref(),
            &cfg.pool,
        )?);
        Ok(Self {
            config: cfg,
            pool,
            session: Mutex::new(None),
        })
    }

    pub async fn warm(&self) -> Result<(), EtpError> {
        self.pool.warm().await
    }

    #[inline]
    pub async fn acquire(&self) -> Result<StreamLease, EtpError> {
        self.pool.acquire().await
    }

    /// Resolves active `X-Vault-Token`, performing AppRole login when absent or nearing expiry.
    pub async fn resolve_token(&self) -> Result<Option<String>, EtpError> {
        match &self.config.auth {
            VaultAuthConfig::None => Ok(None),
            VaultAuthConfig::Token { token } => Ok(Some(token.clone())),
            VaultAuthConfig::AppRole {
                role_id,
                secret_id,
                auth_mount,
            } => {
                let mut guard = self.session.lock().await;
                let now = velda_core::clock::coarse_sec();
                if let Some(cached) = guard.as_ref()
                    && cached.expires_coarse_sec > now.saturating_add(VAULT_TOKEN_RENEW_MARGIN_SECS)
                {
                    return Ok(Some(cached.token.clone()));
                }

                let (token, lease_secs) =
                    self.login_approle(auth_mount, role_id, secret_id).await?;
                let expires_coarse_sec = if lease_secs == 0 {
                    u64::MAX
                } else {
                    now.saturating_add(lease_secs)
                };
                *guard = Some(CachedVaultToken {
                    token: token.clone(),
                    expires_coarse_sec,
                });
                Ok(Some(token))
            }
        }
    }

    /// Invalidates cached AppRole token so the next `resolve_token()` re-authenticates.
    pub async fn invalidate_token(&self) {
        let mut guard = self.session.lock().await;
        *guard = None;
    }

    async fn login_approle(
        &self,
        auth_mount: &str,
        role_id: &str,
        secret_id: &str,
    ) -> Result<(String, u64), EtpError> {
        let mut lease = self.pool.acquire().await?;
        let mount = auth_mount.trim_matches('/');
        let body = serde_json::json!({ "role_id": role_id, "secret_id": secret_id }).to_string();
        let ns_header = self
            .config
            .namespace
            .as_deref()
            .map(|ns| format!("X-Vault-Namespace: {ns}\r\n"))
            .unwrap_or_default();

        let req = format!(
            "POST /v1/auth/{mount}/login HTTP/1.1\r\nHost: {}\r\n{ns_header}Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            self.config.host_header,
            body.len(),
            body
        );

        let timeout = Duration::from_millis(self.config.pool.connect_timeout_ms.max(1000));
        let raw_resp = tokio::time::timeout(timeout, async {
            lease.write_all(req.as_bytes()).await?;
            lease.flush().await?;
            let mut buf = Vec::with_capacity(2048);
            let mut chunk = [0u8; 4096];
            loop {
                let n = lease.read(&mut chunk).await?;
                if n == 0 {
                    break;
                }
                if buf.len() + n > MAX_VAULT_AUTH_RESPONSE_BYTES {
                    return Err(EtpError::Config(
                        "Vault AppRole login response exceeded 256 KiB limit".into(),
                    ));
                }
                buf.extend_from_slice(&chunk[..n]);
            }
            Ok::<Vec<u8>, EtpError>(buf)
        })
        .await
        .map_err(|_| EtpError::Timeout {
            id: self.config.id.clone(),
            timeout_ms: self.config.pool.connect_timeout_ms,
        })?
        .inspect_err(|_| lease.recreate_in_background())?;

        lease.recreate_in_background();

        let resp_str = std::str::from_utf8(&raw_resp)
            .map_err(|_| EtpError::Config("Vault AppRole login returned non-UTF8 HTTP".into()))?;
        let (head, raw_body) = resp_str.split_once("\r\n\r\n").ok_or_else(|| {
            EtpError::Config("Vault AppRole login returned malformed HTTP response".into())
        })?;
        let status_code: u16 = head
            .lines()
            .next()
            .and_then(|l| l.split_whitespace().nth(1))
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        if !(200..300).contains(&status_code) {
            return Err(EtpError::Config(format!(
                "Vault ETP '{}' AppRole login failed with HTTP {status_code}",
                self.config.id
            )));
        }

        let parsed: serde_json::Value = serde_json::from_str(raw_body.trim()).map_err(|e| {
            EtpError::Config(format!(
                "Vault ETP '{}' AppRole JSON decode error: {e}",
                self.config.id
            ))
        })?;
        let client_token = parsed
            .get("auth")
            .and_then(|a| a.get("client_token"))
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                EtpError::Config(format!(
                    "Vault ETP '{}' AppRole response missing auth.client_token",
                    self.config.id
                ))
            })?;
        let lease_duration = parsed
            .get("auth")
            .and_then(|a| a.get("lease_duration"))
            .and_then(|v| v.as_u64())
            .unwrap_or(0);

        Ok((client_token.to_string(), lease_duration))
    }
}
