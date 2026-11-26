//! MySQL `api_metering` CTE consumed-credit accumulator workflow (`client.rs`).
//!
//! ```text
//!   flush_deltas(&etp, key_prefix, deltas)
//!     ├─► PREPARE upsert CTE -> EXECUTE [(k, d, now)]* -> CLOSE
//!     └─► PREPARE SELECT consumed -> EXECUTE [k]* -> CLOSE
//! ```

use std::fmt::Write as _;
use std::sync::Arc;
use std::time::Duration;

use velda_etp::EtpError;
use velda_etp::mysql::MysqlEtp;

use crate::pre_upstream::api_metering::providers::mysql::wire::{
    MysqlParam, stmt_close, stmt_execute, stmt_prepare,
};

const MY_UPSERT_CTE_SQL: &str = "\
INSERT INTO velda.velda_api_metering (k, consumed, updated_at) \
SELECT k, d, ts FROM (SELECT ? AS k, ? AS d, ? AS ts) AS delta_in \
ON DUPLICATE KEY UPDATE \
  consumed = consumed + VALUES(consumed), \
  updated_at = VALUES(updated_at)";

const MY_SELECT_SQL: &str =
    "SELECT CAST(consumed AS SIGNED) FROM velda.velda_api_metering WHERE k = ?";

/// Pushes account credit consumption deltas into MySQL via parameterized CTE statements.
pub async fn flush_deltas(
    etp: &Arc<MysqlEtp>,
    key_prefix: &str,
    deltas: &[(u64, i64)],
    io_timeout: Duration,
) -> Result<Vec<i64>, EtpError> {
    if deltas.is_empty() {
        return Ok(Vec::new());
    }

    let now_i64 = velda_core::clock::coarse_sec().min(i64::MAX as u64) as i64;
    let mut lease = etp.acquire().await?;

    let mut last_err = EtpError::Timeout {
        id: etp.config.id.clone(),
        timeout_ms: u64::try_from(io_timeout.as_millis()).unwrap_or(u64::MAX),
    };

    for attempt in 0..2 {
        let res: Result<Result<Vec<i64>, EtpError>, _> = tokio::time::timeout(io_timeout, async {
            let mut keys = Vec::with_capacity(deltas.len());
            for &(acc_hash, _) in deltas {
                let mut k = String::with_capacity(64);
                let _ = write!(k, "{key_prefix}:metering:{acc_hash}");
                keys.push(k);
            }

            let upsert_stmt = stmt_prepare(&mut lease, MY_UPSERT_CTE_SQL).await?;
            for (k, &(_, delta)) in keys.iter().zip(deltas.iter()) {
                stmt_execute(
                    &mut lease,
                    &upsert_stmt,
                    &[
                        MysqlParam::Str(k),
                        MysqlParam::I64(delta),
                        MysqlParam::I64(now_i64),
                    ],
                )
                .await?;
            }
            stmt_close(&mut lease, upsert_stmt.stmt_id).await?;

            let select_stmt = stmt_prepare(&mut lease, MY_SELECT_SQL).await?;
            let mut out = Vec::with_capacity(deltas.len());
            for (k, &(_, delta)) in keys.iter().zip(deltas.iter()) {
                let total = stmt_execute(&mut lease, &select_stmt, &[MysqlParam::Str(k)])
                    .await?
                    .unwrap_or(delta);
                out.push(total);
            }
            stmt_close(&mut lease, select_stmt.stmt_id).await?;
            Ok(out)
        })
        .await;

        match res {
            Ok(Ok(vals)) => return Ok(vals),
            Ok(Err(err)) => {
                last_err = err;
                if attempt == 0 && lease.reconnect().await.is_ok() {
                    continue;
                }
                lease.recreate_in_background();
                return Err(last_err);
            }
            Err(_) => {
                lease.recreate_in_background();
                return Err(last_err);
            }
        }
    }

    Err(last_err)
}
