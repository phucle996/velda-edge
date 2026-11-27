//! PostgreSQL wire v3 Extended Query + CTE atomic upsert & readback (`client.rs`).
//!
//! ```text
//!   push_and_read_totals(&etp, key_prefix, deltas)
//!     write const PG_UPSERT_PARSE_SYNC_WIRE (.rodata 'P'+'S')
//!     loop DirtyKeyDelta:
//!       BakedWire key -> Bind('B', [key, delta, exp, now]) + const PG_EXECUTE_SYNC_TAIL -> total
//! ```

use std::sync::Arc;
use std::time::Duration;

use velda_core::lut::decimal::fast_write_u64_slice;
use velda_etp::EtpError;
use velda_etp::postgres::PostgresEtp;

use super::wire::{PgReader, append_bind_execute, append_sync};
use crate::pre_upstream::rate_limit::arena::{DirtyKeyDelta, ReconciledClusterTotal};
use crate::wire_baked::{BakedWire, build_pg_parse_sync, pg_parse_sync_len};

const PG_UPSERT_CTE_SQL: &str = "\
WITH delta_in AS (SELECT $1::text AS k, $2::bigint AS d, $3::bigint AS exp_ts, $4::bigint AS now_ts) \
INSERT INTO velda.velda_rate_limit (k, cnt, exp) \
SELECT k, d, exp_ts FROM delta_in \
ON CONFLICT (k) DO UPDATE \
  SET cnt = CASE WHEN velda.velda_rate_limit.exp < $4::bigint \
            THEN EXCLUDED.cnt ELSE velda.velda_rate_limit.cnt + EXCLUDED.cnt END, \
      exp = EXCLUDED.exp \
RETURNING cnt;";

const PG_UPSERT_PARSE_SYNC_WIRE: [u8; pg_parse_sync_len(PG_UPSERT_CTE_SQL)] =
    build_pg_parse_sync(PG_UPSERT_CTE_SQL);

/// Atomically upserts per-key counters into PostgreSQL via Extended Query CTE (`$1..$4`)
/// and returns authoritative cluster totals.
pub async fn push_and_read_totals(
    etp: &Arc<PostgresEtp>,
    key_prefix: &str,
    deltas: &[DirtyKeyDelta],
    io_timeout: Duration,
) -> Result<Vec<ReconciledClusterTotal>, EtpError> {
    if deltas.is_empty() {
        return Ok(Vec::new());
    }

    let now_unix = velda_core::clock::coarse_sec();
    let exp = now_unix + 60;
    let mut lease = etp.acquire().await?;

    let mut exp_buf = [0u8; 24];
    let exp_len = fast_write_u64_slice(&mut exp_buf, exp);
    let mut now_buf = [0u8; 24];
    let now_len = fast_write_u64_slice(&mut now_buf, now_unix);
    let prefix_b = key_prefix.as_bytes();

    let mut out = Vec::with_capacity(deltas.len());
    let mut reader = PgReader::new();
    let mut last_err = EtpError::Timeout {
        id: etp.config.id.clone(),
        timeout_ms: u64::try_from(io_timeout.as_millis()).unwrap_or(u64::MAX),
    };

    for attempt in 0..2 {
        let pending = &deltas[out.len()..];
        let res: Result<Result<(), EtpError>, _> = tokio::time::timeout(io_timeout, async {
            let mut wire = Vec::with_capacity(128 * 96);
            let mut key_buf = Vec::with_capacity(prefix_b.len() + 48);
            let mut d_buf = [0u8; 24];

            lease.write_all(&PG_UPSERT_PARSE_SYNC_WIRE).await?;
            lease.flush().await?;
            let _ = reader.read_pipelined_until_ready(&mut lease).await?;

            // Pipeline up to 128 Bind+Execute operations per single Sync round-trip.
            for chunk in pending.chunks(128) {
                wire.clear();
                for d in chunk {
                    {
                        let mut kw = BakedWire::cleared(&mut key_buf, prefix_b.len() + 48);
                        kw.push_chunk(prefix_b);
                        kw.push_u64(u64::from(d.tier));
                        kw.push_byte(b':');
                        kw.push_hex16(d.key_hash);
                        kw.push_byte(b':');
                        kw.push_u64(d.window_epoch);
                    }
                    let d_len = fast_write_u64_slice(&mut d_buf, d.delta);
                    append_bind_execute(
                        &mut wire,
                        &[
                            &key_buf,
                            &d_buf[..d_len],
                            &exp_buf[..exp_len],
                            &now_buf[..now_len],
                        ],
                    );
                }
                append_sync(&mut wire);
                lease.write_all(&wire).await?;
                lease.flush().await?;

                let totals = reader.read_pipelined_until_ready(&mut lease).await?;
                for (i, d) in chunk.iter().enumerate() {
                    let total = totals.get(i).copied().flatten().unwrap_or(d.delta);
                    out.push(ReconciledClusterTotal {
                        key_hash: d.key_hash,
                        tier: d.tier,
                        pushed_delta: d.delta,
                        window_epoch: d.window_epoch,
                        cluster_total: total,
                    });
                }
            }
            Ok(())
        })
        .await;

        match res {
            Ok(Ok(())) => return Ok(out),
            Ok(Err(err)) => {
                last_err = err;
                reader.clear();
                if attempt == 0 && lease.reconnect().await.is_ok() {
                    continue;
                }
                lease.recreate_in_background();
                return Err(last_err);
            }
            Err(_) => {
                reader.clear();
                lease.recreate_in_background();
                return Err(last_err);
            }
        }
    }

    Err(last_err)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pre_upstream::rate_limit::providers::postgres::schema::ensure_table;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use velda_etp::{EtpEntryConfig, EtpKind, EtpPoolSettings, EtpTransportSecurity};

    #[tokio::test]
    async fn test_rate_limit_postgres_extended_cte_handles_special_chars_in_prefix() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let saw_parse = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let saw_parse_c = Arc::clone(&saw_parse);

        let srv = tokio::spawn(async move {
            while let Ok((mut s, _)) = listener.accept().await {
                let sp = Arc::clone(&saw_parse_c);
                tokio::spawn(async move {
                    let mut startup = [0u8; 256];
                    let _ = s.read(&mut startup).await;
                    let mut auth = vec![b'R', 0, 0, 0, 8, 0, 0, 0, 0];
                    auth.extend_from_slice(&[b'Z', 0, 0, 0, 5, b'I']);
                    let _ = s.write_all(&auth).await;

                    let mut buf = [0u8; 2048];
                    while let Ok(n) = s.read(&mut buf).await {
                        if n == 0 {
                            break;
                        }
                        if buf[0] == b'Q' {
                            let sql = String::from_utf8_lossy(&buf[5..n]);
                            assert!(sql.contains("CREATE SCHEMA IF NOT EXISTS velda"));
                            assert!(sql.contains("velda.velda_rate_limit"));
                            let _ = s
                                .write_all(&[b'C', 0, 0, 0, 4, b'Z', 0, 0, 0, 5, b'I'])
                                .await;
                        } else if buf[0] == b'P' {
                            sp.store(true, std::sync::atomic::Ordering::Release);
                            let mut rep = vec![b'1', 0, 0, 0, 4];
                            rep.extend_from_slice(&[b'Z', 0, 0, 0, 5, b'I']);
                            let _ = s.write_all(&rep).await;
                        } else if buf[0] == b'B' {
                            let val = b"88";
                            let mut rep = vec![b'2', 0, 0, 0, 4];
                            rep.push(b'D');
                            rep.extend_from_slice(&((4 + 2 + 4 + val.len()) as u32).to_be_bytes());
                            rep.extend_from_slice(&1i16.to_be_bytes());
                            rep.extend_from_slice(&(val.len() as i32).to_be_bytes());
                            rep.extend_from_slice(val);
                            rep.extend_from_slice(&[b'Z', 0, 0, 0, 5, b'I']);
                            let _ = s.write_all(&rep).await;
                        }
                    }
                });
            }
        });

        let entry = EtpEntryConfig {
            id: "pg-rl-test".into(),
            kind: EtpKind::Postgres,
            security: EtpTransportSecurity::Plain,
            pool: EtpPoolSettings {
                pool_size: 2,
                connect_timeout_ms: 500,
                idle_timeout_ms: 5000,
            },
            tls: None,
            settings: serde_json::json!({ "host": "127.0.0.1", "port": port }),
        };
        let pg = Arc::new(PostgresEtp::from_entry(&entry).unwrap());
        ensure_table(&pg, Duration::from_secs(1)).await.unwrap();
        let out = push_and_read_totals(
            &pg,
            "tenant'drop;--\\",
            &[DirtyKeyDelta {
                key_hash: 7,
                tier: 0,
                delta: 3,
                window_epoch: 1,
            }],
            Duration::from_secs(1),
        )
        .await
        .unwrap();
        assert!(saw_parse.load(std::sync::atomic::Ordering::Acquire));
        assert_eq!(out[0].cluster_total, 88);
        srv.abort();
    }
}
