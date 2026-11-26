//! Valkey Shadow Worker synchronization loop for `api_metering` (`sync.rs`).
//!
//! ```text
//!   Shadow Tick / Shutdown Signal
//!         ├─► state.collect_deltas() ──► push pipelined HINCRBY via client.rs
//!         ├─► Ok  ──► reconcile_balances(initial_balance - consumed_total)
//!         └─► Err ──► revert_in_flight_deltas(&deltas)
//! ```

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use velda_etp::valkey::ValkeyEtp;

use crate::error::PluginError;
use crate::pre_upstream::api_metering::algorithm::ShardedMeteringState;
use crate::pre_upstream::api_metering::providers::valkey::client::flush_deltas;

/// Asynchronous Shadow Logistics loop reconciling local credit deltas with [`ValkeyEtp`].
pub async fn run_shadow_sync(
    etp: Arc<ValkeyEtp>,
    state: Arc<ShardedMeteringState>,
    sync_interval_ms: u64,
    key_prefix: String,
    initial_balance: i64,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), PluginError> {
    let mut interval = tokio::time::interval(Duration::from_millis(sync_interval_ms.max(10)));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let effective_prefix = etp.config.key_prefix.as_deref().unwrap_or(&key_prefix);
    let key = format!("{effective_prefix}:metering:consumed");
    let mut cmd_buf = Vec::with_capacity(8192);
    let mut read_buf = Vec::with_capacity(8192);

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

        let deltas = state.collect_deltas();
        if deltas.is_empty() {
            continue;
        }

        match flush_deltas(&etp, &key, &deltas, &mut cmd_buf, &mut read_buf).await {
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
                    "Valkey api_metering delta flush failed"
                );
            }
        }
    }

    Ok(())
}
