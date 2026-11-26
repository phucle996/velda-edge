//! MySQL Shadow Worker micro-quota synchronization loop (`sync.rs`).
//!
//! ```text
//!   Shadow Init: ensure_table() -> CREATE SCHEMA velda / TABLE velda.velda_openai_token_quota
//!         │
//!         ▼ Notify / Interval / Shutdown:
//!     scan SharedUsageArena slots -> client::flush_batch()
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::sync::{Notify, watch};
use velda_etp::mysql::MysqlEtp;

use crate::error::PluginError;
use crate::post_response::openai_token_quota::local::{
    FLAG_NEEDS_REFILL, MAX_CONSUMER_KEY_LEN, SharedUsageArena,
};
use crate::post_response::openai_token_quota::providers::mysql::{
    client::flush_batch, schema::ensure_table,
};

/// Runs the cooperative MySQL micro-quota sync loop inside the Shadow Worker until shutdown.
#[allow(clippy::too_many_arguments)]
pub async fn run_quota_sync(
    plugin_id: String,
    etp: Arc<MysqlEtp>,
    arena: Arc<SharedUsageArena>,
    notify: Arc<Notify>,
    connected: Arc<AtomicBool>,
    micro_quota_batch: u64,
    batch_users: usize,
    sync_interval_ms: u64,
    pbww: u32,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), PluginError> {
    let io_timeout = Duration::from_millis(etp.config.pool.connect_timeout_ms.clamp(250, 10_000));
    let mut ddl_ready = ensure_table(&etp, io_timeout).await.is_ok();
    let batch_i64 = i64::try_from(micro_quota_batch.max(1)).unwrap_or(i64::MAX / 2);
    let interval_dur = Duration::from_millis(sync_interval_ms.max(10));
    let mut key_buf = [0u8; MAX_CONSUMER_KEY_LEN];
    let mut pending: Vec<(usize, String)> = Vec::with_capacity(batch_users);

    loop {
        tokio::select! {
            _ = notify.notified() => {}
            _ = tokio::time::sleep(interval_dur) => {}
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

        pending.clear();
        let slots = arena.slots();
        let mut aborted = false;

        for (idx, slot) in slots.iter().enumerate() {
            if slot.consumer_hash.load(Ordering::Relaxed) == 0 {
                continue;
            }
            let rem = slot.local_remaining.load(Ordering::Relaxed);
            let flags = slot.flags.load(Ordering::Relaxed);
            if (flags & FLAG_NEEDS_REFILL) == 0 && rem > arena.refill_watermark {
                continue;
            }

            let consumer_key = SharedUsageArena::read_slot_key(slot, &mut key_buf);
            pending.push((idx, format!("velda:plugin:{plugin_id}:{consumer_key}")));

            if pending.len() >= batch_users {
                if !flush_batch(&etp, slots, &pending, batch_i64, io_timeout, &connected).await {
                    aborted = true;
                    break;
                }
                pending.clear();
            }
        }

        if !aborted && !pending.is_empty() {
            let _ = flush_batch(&etp, slots, &pending, batch_i64, io_timeout, &connected).await;
        }

        if pbww < 50 {
            tokio::task::yield_now().await;
        }
    }

    Ok(())
}
