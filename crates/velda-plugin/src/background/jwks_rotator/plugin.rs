//! JWKS Background Key Rotator Plugin (`phase: "background"`).
//!
//! ```text
//!                      JWKS BACKGROUND ROTATION RUNTIME
//!
//!          [ velda-shadow / Background Worker (SCHED_IDLE) ]
//!                                  |
//!                                  v
//!                     +--------------------------+
//!                     | Initial Boot Fetch       |
//!                     | (Populate RAM Table)     |
//!                     +--------------------------+
//!                                  |
//!                                  v
//!                   +------------------------------+
//!                   | Integer Coarse Clock Loop    |
//!                   | - coarse_now_sec() check     |
//!                   | - tokio::select! shutdown_rx |
//!                   +------------------------------+
//!                                  | (Interval elapsed)
//!                                  v
//!                     +--------------------------+
//!                     | JwksFetcher::fetch()     |
//!                     +--------------------------+
//!                                  |
//!                                  v
//!                     +--------------------------+
//!                     | parse_jwks_into_table()  |
//!                     | (Up to 8 active keys)    |
//!                     +--------------------------+
//!                                  |
//!                                  v
//!                     +--------------------------+
//!                     | SharedJwksStore::update()|
//!                     | (Lock-free ArcSwap Store)|
//!                     +--------------------------+
//!                                  |
//!                                  v
//!                     +--------------------------+
//!                     | Frontline Workers        |
//!                     | Instant Core-Local Read  |
//!                     +--------------------------+
//! ```
//!
//! # Architectural Invariants
//! - **Strict Task Isolation**: Executes purely on Shadow Logistics companion threads.
//! - **Zero Syscall Clock Checks**: Interval progression uses [`velda_core::clock::coarse_sec`].
//! - **Lock-Free Hotswap**: Installs updated keys using atomic pointer replacement (`ArcSwap`),
//!   never taking mutexes or halting traffic workers.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use crate::config::PluginConfig;
use arc_swap::ArcSwapOption;
use tokio::sync::watch;

use super::config::JwksRotatorConfig;
use super::fetcher::JwksFetcher;
use super::parser::parse_jwks_into_table;
use crate::background::BackgroundPlugin;
use crate::error::PluginError;
use crate::pre_upstream::jwt_auth::table::SharedJwksStore;

/// Background service plugin responsible for continuously rotating JWKS keys into RAM.
pub struct JwksRotatorPlugin {
    id: String,
    config: JwksRotatorConfig,
    key_store: ArcSwapOption<SharedJwksStore>,
    fetcher: JwksFetcher,
}

impl JwksRotatorPlugin {
    /// Deserializes and constructs an instance directly from a [`PluginConfig`].
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let raw_config: JwksRotatorConfig =
            serde_json::from_value(config.config.clone()).map_err(|e| {
                PluginError::Config(format!(
                    "jwks_rotator: failed to parse config for '{}': {e}",
                    config.id
                ))
            })?;
        let parsed_config = raw_config.validate_and_sanitize()?;

        let fetcher = JwksFetcher::new(parsed_config.request_timeout_secs, &parsed_config.tls)?;

        Ok(Self {
            id: config.id.clone(),
            config: parsed_config,
            key_store: ArcSwapOption::empty(),
            fetcher,
        })
    }

    /// Injects the target [`SharedJwksStore`] associated with the target `jwt_auth` plugin.
    pub fn with_shared_key_store(self, store: Arc<SharedJwksStore>) -> Self {
        self.key_store.store(Some(store));
        self
    }

    /// Performs a single fetch, parse, and atomic update cycle into the shared key store.
    pub async fn execute_rotation_step(&self) -> Result<(), PluginError> {
        let store_guard = self.key_store.load();
        let store = store_guard.as_ref().ok_or_else(|| {
            PluginError::Config(format!(
                "jwks_rotator '{}': shared key store has not been wired for target '{}'",
                self.id, self.config.target_jwt_auth_id
            ))
        })?;

        let json_bytes = self.fetcher.fetch_jwks_json(&self.config.jwks_uri).await?;

        // If configured to preserve the static key at Slot 0, extract it from the current table
        let static_slot = if self.config.preserve_static_key {
            let current = store.load();
            if current.active_count > 0 && current.slots[0].is_active != 0 {
                Some(current.slots[0].clone())
            } else {
                None
            }
        } else {
            None
        };

        let new_table = parse_jwks_into_table(&json_bytes, static_slot)?;
        let active_count = new_table.active_count;

        store.update(new_table);
        tracing::info!(
            plugin_id = %self.id,
            target_id = %self.config.target_jwt_auth_id,
            active_keys = active_count,
            "Successfully rotated JWKS keys into shared table"
        );

        Ok(())
    }
}

impl BackgroundPlugin for JwksRotatorPlugin {
    fn id(&self) -> &str {
        &self.id
    }

    #[inline(always)]
    fn weight(&self) -> u32 {
        5
    }

    #[inline(always)]
    fn target_jwt_auth_id(&self) -> Option<&str> {
        Some(&self.config.target_jwt_auth_id)
    }

    #[inline(always)]
    fn attach_shared_jwks_store(&self, store: Arc<SharedJwksStore>) -> Result<(), PluginError> {
        self.key_store.store(Some(store));
        Ok(())
    }

    fn run(
        self: Arc<Self>,
        mut shutdown: watch::Receiver<bool>,
    ) -> Pin<Box<dyn Future<Output = Result<(), PluginError>> + Send>> {
        Box::pin(async move {
            tracing::info!(
                plugin_id = %self.id,
                target_id = %self.config.target_jwt_auth_id,
                interval_secs = self.config.refresh_interval_secs,
                "JWKS Rotator background service initiated"
            );

            // 1. Initial boot fetch: attempt to populate active keys immediately before serving
            if let Err(e) = self.execute_rotation_step().await {
                tracing::warn!(
                    plugin_id = %self.id,
                    error = %e,
                    "Initial JWKS fetch failed; continuing with existing in-memory keys"
                );
            }

            // 2. Coarse Clock interval tracking
            let mut next_refresh_sec =
                velda_core::clock::coarse_sec().saturating_add(self.config.refresh_interval_secs);

            // 3. Periodic rotation loop
            loop {
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {
                        let now_sec = velda_core::clock::coarse_sec();
                        if now_sec >= next_refresh_sec {
                            if let Err(e) = self.execute_rotation_step().await {
                                tracing::error!(
                                    plugin_id = %self.id,
                                    error = %e,
                                    "Periodic JWKS rotation step failed; keeping current keys"
                                );
                            }
                            next_refresh_sec = now_sec.saturating_add(self.config.refresh_interval_secs);
                        }
                    }
                    changed = shutdown.changed() => {
                        if changed.is_ok() && *shutdown.borrow() {
                            tracing::info!(
                                plugin_id = %self.id,
                                "JWKS Rotator shutdown signal received, terminating service loop"
                            );
                            break;
                        }
                    }
                }
            }

            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pre_upstream::jwt_auth::table::JwksKeyTable;
    use base64::prelude::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn test_from_config_defaults_and_validation() {
        let json = serde_json::json!({
            "target_jwt_auth_id": "jwt_auth",
            "jwks_uri": "https://example.com/jwks.json",
        });
        let config = PluginConfig {
            id: "jwks_rotator".into(),
            phase: "background".into(),
            config: json,
        };

        let plugin = JwksRotatorPlugin::from_config(&config).unwrap();
        assert_eq!(plugin.id, "jwks_rotator");
        assert_eq!(plugin.config.target_jwt_auth_id, "jwt_auth");
        assert_eq!(plugin.config.refresh_interval_secs, 300);
        assert_eq!(plugin.config.request_timeout_secs, 10);
        assert!(plugin.config.preserve_static_key);
    }

    #[tokio::test]
    async fn test_execute_rotation_step_updates_shared_store() {
        // 1. Bind local mock HTTP server
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        // 2. Build mock JWKS JSON
        let k_bytes = vec![0x42u8; 32];
        let k_b64 = BASE64_URL_SAFE_NO_PAD.encode(&k_bytes);
        let jwks_body = format!(
            r#"{{"keys":[{{"kty":"oct","kid":"rotated-key-1","alg":"HS256","k":"{k_b64}"}}]}}"#
        );

        // 3. Spawn background mock server task
        tokio::spawn(async move {
            if let Ok((mut socket, _)) = listener.accept().await {
                let mut buf = [0u8; 1024];
                let _ = socket.read(&mut buf).await;
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    jwks_body.len(),
                    jwks_body
                );
                let _ = socket.write_all(resp.as_bytes()).await;
                let _ = socket.flush().await;
            }
        });

        // 4. Create empty shared store
        let initial_table = JwksKeyTable::empty();
        let shared_store = Arc::new(SharedJwksStore::new(initial_table));

        // 5. Build plugin targeting mock server
        let json = serde_json::json!({
            "target_jwt_auth_id": "jwt_auth",
            "jwks_uri": format!("http://127.0.0.1:{port}/.well-known/jwks.json"),
            "refresh_interval_secs": 15,
            "request_timeout_secs": 5,
            "preserve_static_key": false,
        });
        let config = PluginConfig {
            id: "jwks_rotator".into(),
            phase: "background".into(),
            config: json,
        };

        let plugin = JwksRotatorPlugin::from_config(&config)
            .unwrap()
            .with_shared_key_store(Arc::clone(&shared_store));

        // 6. Execute rotation step
        plugin.execute_rotation_step().await.unwrap();

        // 7. Verify atomic update in shared store
        let current = shared_store.load();
        assert_eq!(current.active_count, 1);
        let found = current.find_by_raw_kid("rotated-key-1");
        assert!(found.is_some());
    }
}
