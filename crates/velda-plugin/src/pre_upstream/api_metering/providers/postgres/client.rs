//! PostgreSQL `api_metering` CTE credit accumulator client (`client.rs`).
//!
//! ```text
//!   flush_deltas(&etp, key_prefix, deltas)
//!     ├─► write_all(&PG_UPSERT_PARSE_SYNC_WIRE)   (.rodata const Parse+Sync frame)
//!     └─► per delta: BakedWire key + stack decimal params ─► Bind/Execute/Sync
//! ```

use std::sync::Arc;
use std::time::Duration;

use velda_core::lut::decimal::fast_write_u64_slice;
use velda_etp::EtpError;
use velda_etp::postgres::PostgresEtp;

use crate::pre_upstream::api_metering::providers::postgres::wire::{
    append_bind_execute_sync_wire, read_until_ready,
};
use crate::wire_baked::{BakedWire, build_pg_parse_sync, pg_parse_sync_len};

const PG_UPSERT_CTE: &str = "\
WITH delta_in AS (SELECT $1::text AS k, $2::bigint AS d, $3::bigint AS ts) \
INSERT INTO velda.velda_api_metering (k, consumed, updated_at) \
SELECT k, d, ts FROM delta_in \
ON CONFLICT (k) DO UPDATE \
  SET consumed = velda.velda_api_metering.consumed + EXCLUDED.consumed, \
      updated_at = EXCLUDED.updated_at \
RETURNING consumed;";

const PG_UPSERT_LEN: usize = pg_parse_sync_len(PG_UPSERT_CTE);
const PG_UPSERT_PARSE_SYNC_WIRE: [u8; PG_UPSERT_LEN] = build_pg_parse_sync(PG_UPSERT_CTE);

/// Pushes account credit consumption deltas into PostgreSQL via Extended Query CTE.
pub async fn flush_deltas(
    etp: &Arc<PostgresEtp>,
    key_prefix: &str,
    deltas: &[(u64, i64)],
    io_timeout: Duration,
) -> Result<Vec<i64>, EtpError> {
    if deltas.is_empty() {
        return Ok(Vec::new());
    }

    let now_unix = velda_core::clock::coarse_sec();
    let mut now_buf = [0u8; 24];
    let now_len = fast_write_u64_slice(&mut now_buf, now_unix);
    let now_bytes = &now_buf[..now_len];

    let mut lease = etp.acquire().await?;
    let mut last_err = EtpError::Timeout {
        id: etp.config.id.clone(),
        timeout_ms: u64::try_from(io_timeout.as_millis()).unwrap_or(u64::MAX),
    };

    for attempt in 0..2 {
        let res: Result<Result<Vec<i64>, EtpError>, _> = tokio::time::timeout(io_timeout, async {
            lease.write_all(&PG_UPSERT_PARSE_SYNC_WIRE).await?;
            lease.flush().await?;
            let _ = read_until_ready(&mut lease).await?;

            let mut out = Vec::with_capacity(deltas.len());
            let mut key_buf = Vec::with_capacity(key_prefix.len() + 32);
            let mut delta_buf = Vec::with_capacity(24);
            let mut wire = Vec::with_capacity(256);

            for &(acc_hash, delta) in deltas {
                {
                    let mut kw = BakedWire::cleared(&mut key_buf, key_prefix.len() + 32);
                    kw.push_chunk(key_prefix.as_bytes());
                    kw.push_chunk(b":metering:");
                    kw.push_u64(acc_hash);
                }
                {
                    let mut dw = BakedWire::cleared(&mut delta_buf, 24);
                    dw.push_i64(delta);
                }
                wire.clear();
                append_bind_execute_sync_wire(&mut wire, &key_buf, &delta_buf, now_bytes);
                lease.write_all(&wire).await?;
                lease.flush().await?;
                let total = read_until_ready(&mut lease).await?.unwrap_or(delta);
                out.push(total);
            }
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
