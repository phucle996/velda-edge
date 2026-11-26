//! PostgreSQL Shadow Worker synchronization loop for `api_metering` (`sync.rs`).
//!
//! ```text
//!   Shadow Init: ensure_table() -> CREATE SCHEMA velda / TABLE velda.velda_api_metering
//!         │
//!         ▼ Tick / Shutdown:
//!     state.collect_deltas() -> flush_deltas() -> reconcile_balances() | revert
//! ```

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use velda_etp::postgres::PostgresEtp;

use crate::error::PluginError;
use crate::pre_upstream::api_metering::algorithm::ShardedMeteringState;
use crate::pre_upstream::api_metering::providers::postgres::client::flush_deltas;
use crate::pre_upstream::api_metering::providers::postgres::schema::ensure_table;

/// Asynchronous Shadow Logistics loop reconciling local credit deltas with PostgreSQL.
pub async fn run_shadow_sync(
    etp: Arc<PostgresEtp>,
    state: Arc<ShardedMeteringState>,
    sync_interval_ms: u64,
    key_prefix: String,
    initial_balance: i64,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), PluginError> {
    let io_timeout = Duration::from_millis(etp.config.pool.connect_timeout_ms.clamp(250, 10_000));
    let mut ddl_ready = ensure_table(&etp, io_timeout).await.is_ok();
    let mut interval = tokio::time::interval(Duration::from_millis(sync_interval_ms.max(10)));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = interval.tick() => {}
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

        let deltas = state.collect_deltas();
        if deltas.is_empty() {
            continue;
        }

        match flush_deltas(&etp, &key_prefix, &deltas, io_timeout).await {
            Ok(results) => {
                let mut updates = Vec::with_capacity(results.len());
                for (idx, &res) in results.iter().enumerate() {
                    let (acc_hash, _) = deltas[idx];
                    updates.push((acc_hash, initial_balance.saturating_sub(res)));
                }
                state.reconcile_balances(&updates);
            }
            Err(e) => {
                state.revert_in_flight_deltas(&deltas);
                tracing::warn!(
                    etp_id = %etp.config.id,
                    error = %e,
                    "PostgreSQL api_metering delta flush failed"
                );
            }
        }
    }

    Ok(())
}
