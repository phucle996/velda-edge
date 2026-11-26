//! MySQL Shadow Worker synchronization loop (`sync.rs`).
//!
//! ```text
//!   Shadow Init: ensure_table() (best-effort CREATE TABLE IF NOT EXISTS)
//!         │
//!         ▼ Tick / Shutdown Signal:
//!     1. Drain up to 64 DirtyKeyDelta entries
//!     2. COM_QUERY UPSERT + SELECT via providers::mysql::client
//!     3. Reconcile cluster totals and maintain health flag
//! ```

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use velda_etp::mysql::MysqlEtp;

use crate::pre_upstream::rate_limit::arena::{
    RateLimitArena, SOT_HEALTH_FAIL_CLOSED, SOT_HEALTH_FAIL_OPEN, SOT_HEALTH_OK,
};
use crate::pre_upstream::rate_limit::providers::mysql::{
    client::push_and_read_totals, schema::ensure_table,
};
use crate::pre_upstream::rate_limit::validation::{FallbackBehavior, RateLimitConfig};

/// Runs the periodic Shadow SoT sync loop for MySQL until shutdown.
pub async fn run_sot_sync_loop(
    etp: Arc<MysqlEtp>,
    arena: Arc<RateLimitArena>,
    config: RateLimitConfig,
    mut shutdown: watch::Receiver<bool>,
) {
    let io_timeout = Duration::from_millis(etp.config.pool.connect_timeout_ms.clamp(250, 10_000));
    let mut ddl_ready = ensure_table(&etp, io_timeout).await.is_ok();
    let interval = Duration::from_millis(config.sync_interval_ms.max(10));
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

        if !ddl_ready {
            ddl_ready = ensure_table(&etp, io_timeout).await.is_ok();
        }

        let deltas = arena.collect_dirty_deltas(64);
        if deltas.is_empty() {
            continue;
        }

        match push_and_read_totals(&etp, &config.key_prefix, &deltas, io_timeout).await {
            Ok(totals) => {
                arena.set_sot_health(SOT_HEALTH_OK);
                arena.reconcile_cluster_totals(&totals);
            }
            Err(err) => {
                tracing::warn!(
                    etp_id = %etp.config.id,
                    error = %err,
                    "MySQL rate_limit SoT sync failed"
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
