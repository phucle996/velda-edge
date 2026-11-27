//! AWS DynamoDB Shadow Worker synchronization loop (`sync.rs`).
//!
//! ```text
//!   Shadow Tick / Shutdown Signal
//!         │
//!         ├─► 1. Drain up to 64 DirtyKeyDelta entries from RateLimitArena
//!         ├─► 2. Push & read cluster totals via providers::dynamodb::client
//!         └─► 3. Update SOT_HEALTH_* flag and reconcile cluster totals
//! ```

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use velda_etp::dynamodb::DynamoDbEtp;

use crate::pre_upstream::rate_limit::arena::{
    RateLimitArena, SOT_HEALTH_FAIL_CLOSED, SOT_HEALTH_FAIL_OPEN, SOT_HEALTH_OK,
};
use crate::pre_upstream::rate_limit::providers::dynamodb::{
    client::push_and_read_totals, schema::ensure_table,
};
use crate::pre_upstream::rate_limit::validation::{FallbackBehavior, RateLimitConfig};

/// Runs the periodic Shadow SoT sync loop for DynamoDB until shutdown.
pub async fn run_sot_sync_loop(
    etp: Arc<DynamoDbEtp>,
    arena: Arc<RateLimitArena>,
    config: RateLimitConfig,
    mut shutdown: watch::Receiver<bool>,
) {
    let prefix = config.key_prefix.clone();
    let interval = Duration::from_millis(config.sync_interval_ms.max(10));
    let io_timeout = Duration::from_millis(etp.config.pool.connect_timeout_ms.clamp(250, 10_000));
    let mut ddl_ready = ensure_table(&etp, io_timeout).await.is_ok();
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

        let deltas = arena.collect_dirty_deltas(128);
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
                    "DynamoDB rate_limit SoT sync failed"
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
