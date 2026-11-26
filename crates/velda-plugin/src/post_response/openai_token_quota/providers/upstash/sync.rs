//! Upstash HTTPS REST Shadow Worker micro-quota synchronization loop (`sync.rs`).
//!
//! ```text
//!   Notify / Interval / Shutdown
//!         ├─► scan SharedUsageArena slots for (FLAG_NEEDS_REFILL | rem <= watermark)
//!         └─► batch up to batch_users -> client::flush_batch()
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::sync::{Notify, watch};
use velda_etp::upstash::UpstashEtp;

use crate::error::PluginError;
use crate::post_response::openai_token_quota::local::{
    FLAG_NEEDS_REFILL, MAX_CONSUMER_KEY_LEN, SharedUsageArena,
};
use crate::post_response::openai_token_quota::providers::upstash::client::flush_batch;

/// Runs the cooperative Upstash REST micro-quota sync loop inside the Shadow Worker.
#[allow(clippy::too_many_arguments)]
pub async fn run_quota_sync(
    plugin_id: String,
    etp: Arc<UpstashEtp>,
    arena: Arc<SharedUsageArena>,
    notify: Arc<Notify>,
    connected: Arc<AtomicBool>,
    micro_quota_batch: u64,
    batch_users: usize,
    sync_interval_ms: u64,
    pbww: u32,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), PluginError> {
    let batch_i64 = i64::try_from(micro_quota_batch.max(1)).unwrap_or(i64::MAX / 2);
    let interval_dur = Duration::from_millis(sync_interval_ms.max(10));
    let key_prefix = etp.config.key_prefix.as_deref().unwrap_or("");
    let mut resp_buf = Vec::with_capacity(2048);
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
            let formatted = if key_prefix.is_empty() {
                format!("velda:plugin:{plugin_id}:{consumer_key}")
            } else {
                format!("{key_prefix}:plugin:{plugin_id}:{consumer_key}")
            };
            pending.push((idx, formatted));

            if pending.len() >= batch_users {
                if !flush_batch(&etp, slots, &pending, batch_i64, &mut resp_buf, &connected).await {
                    aborted = true;
                    break;
                }
                pending.clear();
            }
        }

        if !aborted && !pending.is_empty() {
            let _ = flush_batch(&etp, slots, &pending, batch_i64, &mut resp_buf, &connected).await;
        }

        if pbww < 50 {
            tokio::task::yield_now().await;
        }
    }

    Ok(())
}
