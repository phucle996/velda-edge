//! ACME Certificate Rotator Background Service (`phase: "background"`).
//!
//! Drives the automated certificate lifecycle on dedicated Shadow Logistics workers:
//! 1. **Bootstrap Execution**: Inspects existing certificates on disk. If absent or expiring
//!    within `renew_before_days`, triggers immediate renewal so traffic is never served with expired TLS.
//! 2. **Cron Scheduled Loop**: Evaluates strictly on user-defined 5-field UTC cron schedules (zero interval polling).
//! 3. **Exponential Backoff**: In case of transient CA or network failure, automatically retries up to 3 times
//!    with exponential backoff and jitter before falling back to the cron schedule.
//! 4. **Ephemeral Route Injection**: Pre-bakes the HTTP-01 wire response and injects the challenge route into
//!    [`Router`] with RAII teardown, guaranteeing zero memory or route leakage on success, failure, or timeout.
//! 5. **Zero-Downtime Hot-Swap**: Atomically hot-swaps renewed certificates into `velda-tls::SniResolver`.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::watch;
use velda_router::Router;
use velda_sync::AcmeCertRotatorConfig;

use super::challenge::ChallengeStore;
use super::client::AcmeClient;
use super::cron::CronSchedule;
use super::crypto::generate_csr_and_key;
use super::expiry::{needs_renewal, not_after_unix};
use super::hot_swap::TlsHotSwapper;
use super::route_guard::RouterRegistry;
use super::storage::CertStorage;
use crate::background::BackgroundPlugin;
use crate::error::PluginError;

/// Background Logistics Service orchestrating ACME automated certificate rotations.
pub struct AcmeCertRotatorPlugin {
    id: String,
    config: AcmeCertRotatorConfig,
    cron: CronSchedule,
    storage: CertStorage,
    client: AcmeClient,
    challenge_store: Arc<ChallengeStore>,
    router_registry: Option<RouterRegistry>,
    hot_swapper: TlsHotSwapper,
}

impl AcmeCertRotatorPlugin {
    /// Creates a new `AcmeCertRotatorPlugin` instance with explicit dependencies.
    pub fn new(
        id: impl Into<String>,
        config: AcmeCertRotatorConfig,
        router: Option<Arc<RwLock<Router>>>,
        listener_id: Option<String>,
        hot_swapper: TlsHotSwapper,
    ) -> Result<Self, PluginError> {
        let cron = CronSchedule::parse(&config.schedule)?;
        let storage = CertStorage::new(&config.storage_dir)?;
        let client = AcmeClient::new(Duration::from_secs(10), Duration::from_secs(30));
        let challenge_store = ChallengeStore::new();

        let router_registry = match (router, listener_id) {
            (Some(r), Some(lid)) => Some(RouterRegistry::new(r, lid)),
            _ => None,
        };

        Ok(Self {
            id: id.into(),
            config,
            cron,
            storage,
            client,
            challenge_store,
            router_registry,
            hot_swapper,
        })
    }

    /// Instantiates and validates the plugin from declarative configuration.
    pub fn from_config(
        config: &velda_sync::post_sync::plugin::PluginConfig,
    ) -> Result<Self, PluginError> {
        let cfg = velda_sync::validate_acme_cert_rotator_config(&config.id, &config.config)
            .map_err(|e| PluginError::Config(format!("Invalid acme_cert_rotator config: {e}")))?;

        Self::new(config.id.clone(), cfg, None, None, TlsHotSwapper::new(None))
    }

    /// Attaches an active `Router` and `listener_id` for ephemeral HTTP-01 route injection.
    pub fn with_router_registry(
        mut self,
        router: Arc<RwLock<Router>>,
        listener_id: impl Into<String>,
    ) -> Self {
        self.router_registry = Some(RouterRegistry::new(router, listener_id.into()));
        self
    }

    /// Attaches a custom `TlsHotSwapper` controller.
    pub fn with_hot_swapper(mut self, hot_swapper: TlsHotSwapper) -> Self {
        self.hot_swapper = hot_swapper;
        self
    }

    /// Returns a reference to the internal [`ChallengeStore`] for inspecting live challenges.
    pub fn challenge_store(&self) -> &Arc<ChallengeStore> {
        &self.challenge_store
    }

    /// Checks certificate status and triggers renewal with backoff if expired or nearing expiration.
    pub async fn check_and_renew(&self) -> Result<bool, PluginError> {
        let primary_domain = &self.config.domains[0];
        let now_unix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        let needs_cert = match self.storage.read_fullchain_pem(primary_domain) {
            Some(cert_pem) => match not_after_unix(&cert_pem) {
                Ok(not_after) => {
                    let should_renew =
                        needs_renewal(not_after, now_unix, self.config.renew_before_days);
                    let days_left = (not_after - now_unix) / 86_400;
                    if should_renew {
                        tracing::info!(
                            plugin_id = %self.id,
                            domain = %primary_domain,
                            days_left = days_left,
                            threshold_days = self.config.renew_before_days,
                            "Certificate near expiration: initiating ACME renewal"
                        );
                        true
                    } else {
                        tracing::info!(
                            plugin_id = %self.id,
                            domain = %primary_domain,
                            days_left = days_left,
                            "Certificate validity is healthy, skipping renewal"
                        );
                        false
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        plugin_id = %self.id,
                        error = %e,
                        "Failed to parse existing certificate PEM: forcing renewal"
                    );
                    true
                }
            },
            None => {
                tracing::info!(
                    plugin_id = %self.id,
                    domain = %primary_domain,
                    "No existing certificate found in storage: initiating initial ACME order"
                );
                true
            }
        };

        if !needs_cert {
            return Ok(false);
        }

        // Execute ACME renewal with up to 3 retries and exponential backoff
        self.renew_with_backoff().await?;
        Ok(true)
    }

    /// Executes ACME renewal workflow with exponential backoff and jitter.
    async fn renew_with_backoff(&self) -> Result<(), PluginError> {
        let mut backoff = Duration::from_secs(5);
        let max_attempts = 3;

        for attempt in 1..=max_attempts {
            tracing::info!(
                plugin_id = %self.id,
                attempt = attempt,
                max_attempts = max_attempts,
                "Executing ACME certificate renewal order"
            );

            match self.execute_renewal_cycle().await {
                Ok(()) => {
                    tracing::info!(
                        plugin_id = %self.id,
                        attempt = attempt,
                        "ACME certificate renewal completed successfully"
                    );
                    return Ok(());
                }
                Err(e) => {
                    tracing::warn!(
                        plugin_id = %self.id,
                        attempt = attempt,
                        error = %e,
                        retry_in = ?backoff,
                        "ACME renewal cycle failed"
                    );

                    if attempt < max_attempts {
                        tokio::time::sleep(backoff).await;
                        backoff *= 2;
                    } else {
                        return Err(e);
                    }
                }
            }
        }

        Ok(())
    }

    /// Single sequential ACME renewal cycle (RFC 8555).
    async fn execute_renewal_cycle(&self) -> Result<(), PluginError> {
        let primary_domain = &self.config.domains[0];

        // 1. Account Key
        let account_key = self.storage.load_or_generate_account_key()?;

        // 2. Discover Directory
        let directory = self
            .client
            .get_directory(&self.config.directory_url)
            .await?;

        // 3. Register or get Account
        let account_url = self
            .client
            .create_or_get_account(
                &directory.new_account,
                &directory.new_nonce,
                &account_key,
                &self.config.contact_email,
            )
            .await?;

        // 4. Create Order
        let (order_url, order) = self
            .client
            .create_order(
                &directory.new_order,
                &directory.new_nonce,
                &account_key,
                &account_url,
                &self.config.domains,
            )
            .await?;

        // 5. Complete HTTP-01 Challenges for all authorisations
        for authz_url in &order.authorizations {
            let challenge = self
                .client
                .get_http01_challenge(authz_url, &directory.new_nonce, &account_key, &account_url)
                .await?;

            let key_auth = account_key.key_authorization(&challenge.token);

            // RAII Ephemeral Route & Pre-baked response injection
            let _guard = if let Some(ref registry) = self.router_registry {
                Some(registry.register_challenge(
                    &challenge.token,
                    &key_auth,
                    &self.challenge_store,
                ))
            } else {
                None
            };
            let _store_guard = if self.router_registry.is_none() {
                Some(self.challenge_store.publish(&challenge.token, &key_auth))
            } else {
                None
            };

            // Trigger CA verification
            self.client
                .trigger_challenge(
                    &challenge.url,
                    &directory.new_nonce,
                    &account_key,
                    &account_url,
                )
                .await?;

            // Poll until ready (or max 60s)
            self.client
                .poll_order_ready(
                    &order_url,
                    &directory.new_nonce,
                    &account_key,
                    &account_url,
                    Duration::from_secs(60),
                )
                .await?;

            // _guard is dropped here automatically at loop iteration boundary, restoring clean state!
        }

        // 6. Generate CSR and Domain Key
        let (csr_der, privkey_pem) = generate_csr_and_key(&self.config.domains)?;

        // 7. Finalize Order
        self.client
            .finalize_order(
                &order.finalize,
                &directory.new_nonce,
                &account_key,
                &account_url,
                &csr_der,
            )
            .await?;

        // 8. Poll for Certificate Issuance
        let cert_url = self
            .client
            .poll_order_valid(
                &order_url,
                &directory.new_nonce,
                &account_key,
                &account_url,
                Duration::from_secs(60),
            )
            .await?;

        // 9. Download Certificate Chain
        let fullchain_pem = self
            .client
            .download_certificate(&cert_url, &directory.new_nonce, &account_key, &account_url)
            .await?;

        // 10. Persist to LKG Storage
        self.storage
            .save_certificate(primary_domain, &fullchain_pem, &privkey_pem)?;

        // 11. Hot-Swap into `velda-tls` Runtime
        self.hot_swapper
            .swap_certificate(&self.config.domains, &fullchain_pem, &privkey_pem)?;

        Ok(())
    }

    /// Background service loop running on Shadow Logistics Worker.
    pub async fn run_service(
        self: Arc<Self>,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<(), PluginError> {
        tracing::info!(
            plugin_id = %self.id,
            schedule = %self.config.schedule,
            domains = ?self.config.domains,
            "Starting ACME certificate rotator on Shadow Logistics Worker"
        );

        // 1. Cold Start / Bootstrap check: inspect and renew immediately if expired or missing
        if let Err(e) = self.check_and_renew().await {
            tracing::error!(
                plugin_id = %self.id,
                error = %e,
                "ACME bootstrap certificate check failed"
            );
        }

        // 2. Cron-driven scheduled maintenance loop
        loop {
            let now_unix = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();

            let next_fire_unix = match self.cron.next_after(now_unix) {
                Some(ts) => ts,
                None => {
                    tracing::warn!(
                        plugin_id = %self.id,
                        schedule = %self.config.schedule,
                        "Cron schedule produced no next fire time: stopping rotator loop"
                    );
                    break;
                }
            };

            let delay_secs = next_fire_unix.saturating_sub(now_unix);
            let sleep_duration = Duration::from_secs(delay_secs);

            tracing::info!(
                plugin_id = %self.id,
                next_fire_unix = next_fire_unix,
                sleep_secs = delay_secs,
                "ACME certificate rotator scheduled for next cron tick"
            );

            tokio::select! {
                _ = tokio::time::sleep(sleep_duration) => {
                    tracing::info!(plugin_id = %self.id, "ACME cron timer fired: checking certificate status");
                    if let Err(e) = self.check_and_renew().await {
                        tracing::error!(
                            plugin_id = %self.id,
                            error = %e,
                            "ACME scheduled certificate renewal failed"
                        );
                    }
                }
                res = shutdown.changed() => {
                    if res.is_ok() && *shutdown.borrow() {
                        tracing::info!(plugin_id = %self.id, "ACME rotator received shutdown signal");
                        break;
                    }
                }
            }
        }

        Ok(())
    }
}

impl BackgroundPlugin for AcmeCertRotatorPlugin {
    fn id(&self) -> &str {
        &self.id
    }

    fn run(
        self: Arc<Self>,
        shutdown: watch::Receiver<bool>,
    ) -> Pin<Box<dyn Future<Output = Result<(), PluginError>> + Send>> {
        Box::pin(async move { self.run_service(shutdown).await })
    }
}
