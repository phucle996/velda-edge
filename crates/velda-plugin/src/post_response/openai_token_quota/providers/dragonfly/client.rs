//! DragonflyDB Lua-Free RESP2 Pipelined Micro-Quota Lease (`DECRBY` + compensating `INCRBY`)
//! over a single shared-nothing [`DragonflyEtp`] endpoint (`client.rs`).
//!
//! ```text
//!   flush_batch(&etp, slots, batch, batch_i64)
//!     ├── pipelined DECRBY batch (1 RTT) -> compute (leased, refund)
//!     └── if any refund > 0 -> pipelined INCRBY refund -> update UsageSlot atomics
//! ```

use std::io::ErrorKind;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use velda_etp::EtpError;
use velda_etp::dragonfly::DragonflyEtp;

use super::codec::{
    LeaseLineReply, compute_lease_and_refund, encode_decrby_batch, encode_incrby_refunds,
    read_expected_replies,
};
use crate::post_response::openai_token_quota::local::{
    FLAG_EXHAUSTED, FLAG_NEEDS_REFILL, UsageSlot,
};

#[allow(clippy::too_many_arguments)]
pub(crate) async fn flush_batch(
    etp: &Arc<DragonflyEtp>,
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

    let Ok(mut lease) = etp.acquire().await else {
        connected.store(false, Ordering::Relaxed);
        return false;
    };

    encode_decrby_batch(batch, batch_str, cmd_buf);
    let mut replies = None;
    for attempt in 0..2 {
        match read_expected_replies(&mut lease, cmd_buf, batch.len(), resp_buf).await {
            Ok(r) => {
                replies = Some(r);
                break;
            }
            Err(EtpError::Io(io_err))
                if attempt == 0 && io_err.kind() != ErrorKind::InvalidData =>
            {
                if lease.reconnect().await.is_err() {
                    lease.recreate_in_background();
                    connected.store(false, Ordering::Relaxed);
                    return false;
                }
            }
            Err(err) => {
                lease.recreate_in_background();
                tracing::warn!(id = %etp.config.id, error = %err, "Dragonfly quota DECRBY batch failed");
                connected.store(false, Ordering::Relaxed);
                return false;
            }
        }
    }
    let Some(replies) = replies else {
        connected.store(false, Ordering::Relaxed);
        return false;
    };

    let mut refunds: Vec<(String, i64)> = Vec::new();
    let mut updates: Vec<(usize, i64)> = Vec::with_capacity(batch.len());

    for ((slot_idx, key), reply) in batch.iter().zip(replies) {
        match reply {
            LeaseLineReply::Int(after) => {
                let (leased, refund) = compute_lease_and_refund(after, batch_i64);
                if refund > 0 {
                    refunds.push((key.clone(), refund));
                }
                updates.push((*slot_idx, leased));
            }
            LeaseLineReply::Err(err_msg) => {
                tracing::warn!(key = %key, error = %err_msg, "Dragonfly DECRBY error");
                updates.push((*slot_idx, 0));
            }
        }
    }

    if !refunds.is_empty() {
        encode_incrby_refunds(&refunds, cmd_buf);
        if let Err(err) = read_expected_replies(&mut lease, cmd_buf, refunds.len(), resp_buf).await
        {
            lease.recreate_in_background();
            tracing::warn!(id = %etp.config.id, error = %err, "Failed compensating INCRBY refunds");
        }
    }

    connected.store(true, Ordering::Relaxed);
    for (slot_idx, leased) in updates {
        if let Some(slot) = slots.get(slot_idx) {
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
