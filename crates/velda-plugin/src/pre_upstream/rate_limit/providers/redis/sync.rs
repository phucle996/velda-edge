//! Redis Shadow Worker synchronization loop (`sync.rs`).
//!
//! ```text
//!   Shadow Tick / Shutdown Signal
//!         │
//!         ├─► 1. Drain up to 2048 DirtyKeyDelta entries from RateLimitArena
//!         ├─► 2. Push & read cluster totals via providers::redis::client
//!         │     ├─ Ok  ──► set SOT_HEALTH_OK, reconcile_cluster_totals()
//!         │     └─ Err ──► requeue_failed_deltas(), update health per FallbackBehavior
//!         └─► Repeat until shutdown watch channel flips true
//! ```

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use velda_etp::redis::RedisEtp;

use crate::pre_upstream::rate_limit::arena::{
    RateLimitArena, SOT_HEALTH_FAIL_CLOSED, SOT_HEALTH_FAIL_OPEN, SOT_HEALTH_OK,
};
use crate::pre_upstream::rate_limit::providers::redis::client::push_and_read_totals;
use crate::pre_upstream::rate_limit::validation::{FallbackBehavior, RateLimitConfig};

/// Runs the periodic Shadow SoT sync loop for Redis until shutdown.
pub async fn run_sot_sync_loop(
    etp: Arc<RedisEtp>,
    arena: Arc<RateLimitArena>,
    config: RateLimitConfig,
    mut shutdown: watch::Receiver<bool>,
) {
    let prefix = etp
        .config
        .key_prefix
        .as_deref()
        .unwrap_or(&config.key_prefix)
        .to_string();
    let interval = Duration::from_millis(config.sync_interval_ms.max(10));
    let io_timeout = Duration::from_millis(etp.config.pool.connect_timeout_ms.clamp(250, 5000));
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = ticker.tick() => {}
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    break;
                }
            }
        }
        if *shutdown.borrow() {
            break;
        }

        let deltas = arena.collect_dirty_deltas(2048);
        if deltas.is_empty() {
            continue;
        }

        match push_and_read_totals(&etp, &prefix, &deltas, io_timeout).await {
            Ok(totals) => {
                arena.set_sot_health(SOT_HEALTH_OK);
                arena.reconcile_cluster_totals(&totals);
            }
            Err(err) => {
                tracing::warn!(
                    etp_id = %etp.config.id,
                    error = %err,
                    "Redis rate_limit SoT sync failed"
                );
                arena.requeue_failed_deltas(&deltas);
                let health = match config.fallback_behavior {
                    FallbackBehavior::Memory => SOT_HEALTH_OK,
                    FallbackBehavior::Allow => SOT_HEALTH_FAIL_OPEN,
                    FallbackBehavior::Deny => SOT_HEALTH_FAIL_CLOSED,
                };
                arena.set_sot_health(health);
            }
        }
    }
}
