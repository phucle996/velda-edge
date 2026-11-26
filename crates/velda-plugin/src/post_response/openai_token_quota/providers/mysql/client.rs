//! MySQL Transaction + Prepared Statement non-negative micro-quota lease (`client.rs`).
//!
//! ```text
//!   flush_batch(&etp, slots, batch, batch_i64)
//!     PREPARE SELECT remaining ... WHERE k = ? FOR UPDATE
//!     PREPARE UPDATE ... SET remaining = remaining - ?, updated_at = ? WHERE k = ?
//!     For each key: START TRANSACTION -> EXECUTE select -> if rem > 0 EXECUTE update + COMMIT else ROLLBACK
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use velda_etp::EtpError;
use velda_etp::mysql::MysqlEtp;

use super::wire::{MysqlParam, execute_com_query, stmt_close, stmt_execute, stmt_prepare};
use crate::post_response::openai_token_quota::local::{
    FLAG_EXHAUSTED, FLAG_NEEDS_REFILL, UsageSlot,
};

const MY_SELECT_FOR_UPDATE_SQL: &str = "\
SELECT CAST(remaining AS SIGNED) \
FROM (SELECT remaining FROM velda.velda_openai_token_quota WHERE k = ? FOR UPDATE) AS locked";

const MY_UPDATE_REMAINING_SQL: &str = "\
UPDATE velda.velda_openai_token_quota \
SET remaining = remaining - ?, updated_at = ? \
WHERE k = ?";

pub(crate) async fn flush_batch(
    etp: &Arc<MysqlEtp>,
    slots: &[UsageSlot],
    batch: &[(usize, String)],
    batch_i64: i64,
    io_timeout: Duration,
    connected: &AtomicBool,
) -> bool {
    if batch.is_empty() {
        return true;
    }

    let now_i64 = velda_core::clock::coarse_sec().min(i64::MAX as u64) as i64;
    let Ok(mut lease) = etp.acquire().await else {
        connected.store(false, Ordering::Relaxed);
        return false;
    };

    let mut updates = None;
    for attempt in 0..2 {
        let res: Result<Result<Vec<(usize, i64)>, EtpError>, _> =
            tokio::time::timeout(io_timeout, async {
                let sel_stmt = stmt_prepare(&mut lease, MY_SELECT_FOR_UPDATE_SQL).await?;
                let upd_stmt = stmt_prepare(&mut lease, MY_UPDATE_REMAINING_SQL).await?;
                let mut out = Vec::with_capacity(batch.len());

                for (slot_idx, key) in batch {
                    execute_com_query(&mut lease, "START TRANSACTION").await?;
                    let rem = stmt_execute(&mut lease, &sel_stmt, &[MysqlParam::Str(key)])
                        .await?
                        .unwrap_or(0);
                    let leased = if rem > 0 { rem.min(batch_i64) } else { 0 };
                    if leased > 0 {
                        stmt_execute(
                            &mut lease,
                            &upd_stmt,
                            &[
                                MysqlParam::I64(leased),
                                MysqlParam::I64(now_i64),
                                MysqlParam::Str(key),
                            ],
                        )
                        .await?;
                        execute_com_query(&mut lease, "COMMIT").await?;
                    } else {
                        execute_com_query(&mut lease, "ROLLBACK").await?;
                    }
                    out.push((*slot_idx, leased));
                }

                stmt_close(&mut lease, sel_stmt.stmt_id).await?;
                stmt_close(&mut lease, upd_stmt.stmt_id).await?;
                Ok(out)
            })
            .await;

        match res {
            Ok(Ok(vals)) => {
                updates = Some(vals);
                break;
            }
            Ok(Err(EtpError::Io(_))) if attempt == 0 && lease.reconnect().await.is_ok() => {}
            Ok(Err(err)) => {
                lease.recreate_in_background();
                tracing::warn!(id = %etp.config.id, error = %err, "MySQL quota lease batch failed");
                connected.store(false, Ordering::Relaxed);
                return false;
            }
            Err(_) => {
                lease.recreate_in_background();
                connected.store(false, Ordering::Relaxed);
                return false;
            }
        }
    }

    let Some(updates) = updates else {
        connected.store(false, Ordering::Relaxed);
        return false;
    };

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
