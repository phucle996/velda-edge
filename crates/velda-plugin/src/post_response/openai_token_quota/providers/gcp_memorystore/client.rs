//! GCP Memorystore Lua-Free RESP2 Pipelined Micro-Quota Lease (`DECRBY` + compensating `INCRBY`)
//! coordinator supporting `standalone` and `cluster` [`GcpMemorystoreEtp`] (`client.rs`).
//!
//! ```text
//!   flush_batch(&etp, slots, batch, batch_i64)
//!     ├─► !is_cluster() ──► standalone::flush_standalone()
//!     └─►  is_cluster() ──► cluster::flush_cluster()
//!            └──► update UsageSlot atomics (local_remaining, pending_delta, flags)
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use velda_etp::gcp_memorystore::GcpMemorystoreEtp;

use super::cluster::flush_cluster;
use super::codec::ShardItemOutcome;
use super::standalone::flush_standalone;
use crate::post_response::openai_token_quota::local::{
    FLAG_EXHAUSTED, FLAG_NEEDS_REFILL, UsageSlot,
};

#[allow(clippy::too_many_arguments)]
pub(crate) async fn flush_batch(
    etp: &Arc<GcpMemorystoreEtp>,
    slots: &[UsageSlot],
    batch: &[(usize, String)],
    batch_i64: i64,
    batch_str: &str,
    cmd_buf: &mut Vec<u8>,
    resp_buf: &mut Vec<u8>,
    connected: &AtomicBool,
) -> bool {
    if batch.is_empty() {
        return true;
    }

    let outcomes = if !etp.is_cluster() {
        let Some(res) = flush_standalone(
            etp, batch, batch_i64, batch_str, cmd_buf, resp_buf, connected,
        )
        .await
        else {
            return false;
        };
        res
    } else {
        let Some(res) = flush_cluster(
            etp, batch, batch_i64, batch_str, cmd_buf, resp_buf, connected,
        )
        .await
        else {
            return false;
        };
        res
    };

    connected.store(true, Ordering::Relaxed);
    for item in outcomes {
        if let ShardItemOutcome::Leased { slot_idx, leased } = item
            && let Some(slot) = slots.get(slot_idx)
        {
            if leased > 0 {
                slot.local_remaining.fetch_add(leased, Ordering::Relaxed);
                slot.pending_delta.store(0, Ordering::Relaxed);
                slot.flags
                    .fetch_and(!(FLAG_NEEDS_REFILL | FLAG_EXHAUSTED), Ordering::Relaxed);
            } else {
                slot.flags.fetch_or(FLAG_EXHAUSTED, Ordering::Relaxed);
                slot.flags.fetch_and(!FLAG_NEEDS_REFILL, Ordering::Relaxed);
            }
        }
    }
    true
}
