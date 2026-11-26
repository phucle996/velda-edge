//! PostgreSQL Extended Query + CTE atomic non-negative micro-quota lease (`client.rs`).
//!
//! ```text
//!   flush_batch(&etp, slots, batch, batch_i64)
//!     CTE: WITH locked AS (SELECT ... FOR UPDATE),
//!               updated AS (UPDATE SET remaining = GREATEST(0, locked.remaining - $2) RETURNING LEAST(...))
//!          SELECT leased FROM updated;
//!     -> Parameterized [$1=key, $2=batch, $3=now], 0 bank overshoot!
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use velda_core::lut::decimal::fast_write_u64_slice;
use velda_etp::EtpError;
use velda_etp::postgres::PostgresEtp;

use super::wire::{append_bind_execute_sync_with_tail, build_bind_tail, read_until_ready};
use crate::post_response::openai_token_quota::local::{
    FLAG_EXHAUSTED, FLAG_NEEDS_REFILL, UsageSlot,
};
use crate::wire_baked::{BakedWire, build_pg_parse_sync, pg_parse_sync_len};

const PG_LEASE_CTE: &str = "\
WITH locked AS (\
    SELECT k, remaining FROM velda.velda_openai_token_quota WHERE k = $1::text FOR UPDATE\
), updated AS (\
    UPDATE velda.velda_openai_token_quota AS t \
    SET remaining = GREATEST(0, locked.remaining - $2::bigint), \
        updated_at = $3::bigint \
    FROM locked \
    WHERE t.k = locked.k AND locked.remaining > 0 \
    RETURNING LEAST(locked.remaining, $2::bigint) AS leased\
) \
SELECT leased FROM updated;";

const PG_LEASE_LEN: usize = pg_parse_sync_len(PG_LEASE_CTE);
const PG_LEASE_PARSE_SYNC_WIRE: [u8; PG_LEASE_LEN] = build_pg_parse_sync(PG_LEASE_CTE);

pub(crate) async fn flush_batch(
    etp: &Arc<PostgresEtp>,
    slots: &[UsageSlot],
    batch: &[(usize, String)],
    batch_i64: i64,
    io_timeout: Duration,
    connected: &AtomicBool,
) -> bool {
    if batch.is_empty() {
        return true;
    }

    let now_unix = velda_core::clock::coarse_sec();
    let mut now_buf = [0u8; 24];
    let now_len = fast_write_u64_slice(&mut now_buf, now_unix);
    let mut batch_buf = Vec::with_capacity(24);
    {
        let mut bw = BakedWire::new(&mut batch_buf);
        bw.push_i64(batch_i64);
    }
    let bind_tail = build_bind_tail(&batch_buf, &now_buf[..now_len]);

    let Ok(mut lease) = etp.acquire().await else {
        connected.store(false, Ordering::Relaxed);
        return false;
    };

    let mut wire = Vec::with_capacity(256);
    let mut updates = None;
    for attempt in 0..2 {
        let res: Result<Result<Vec<(usize, i64)>, EtpError>, _> =
            tokio::time::timeout(io_timeout, async {
                lease.write_all(&PG_LEASE_PARSE_SYNC_WIRE).await?;
                lease.flush().await?;
                let _ = read_until_ready(&mut lease).await?;

                let mut out = Vec::with_capacity(batch.len());
                for (slot_idx, key) in batch {
                    wire.clear();
                    append_bind_execute_sync_with_tail(&mut wire, key.as_bytes(), &bind_tail);
                    lease.write_all(&wire).await?;
                    lease.flush().await?;
                    let leased = read_until_ready(&mut lease).await?.unwrap_or(0).max(0);
                    out.push((*slot_idx, leased));
                }
                Ok(out)
            })
            .await;

        match res {
            Ok(Ok(vals)) => {
                updates = Some(vals);
                break;
            }
            Ok(Err(EtpError::Io(io_err))) if attempt == 0 && lease.reconnect().await.is_ok() => {
                let _ = io_err;
            }
            Ok(Err(err)) => {
                lease.recreate_in_background();
                tracing::warn!(id = %etp.config.id, error = %err, "Postgres quota lease batch failed");
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::post_response::openai_token_quota::providers::postgres::schema::ensure_table;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use velda_core::clock::init_clock;
    use velda_etp::{EtpEntryConfig, EtpKind, EtpPoolSettings, EtpTransportSecurity};

    use crate::post_response::openai_token_quota::local::{SharedUsageArena, fnv1a_64};
    use crate::post_response::openai_token_quota::scanner::TokenUsage;

    #[tokio::test]
    async fn test_postgres_quota_extended_cte_partial_and_exhausted() {
        init_clock();
        let arena = Arc::new(SharedUsageArena::new(64, 0, 10));
        let usage = TokenUsage {
            prompt_tokens: 1,
            completion_tokens: 1,
            total_tokens: 2,
            cached_prompt_tokens: 0,
        };
        arena.record(fnv1a_64(b"t'quoted\\"), b"t'quoted\\", usage, 1);

        let mut pending = Vec::new();
        let mut kb = [0u8; 62];
        for (i, slot) in arena.slots().iter().enumerate() {
            if slot.consumer_hash.load(Ordering::Relaxed) != 0 {
                let k = SharedUsageArena::read_slot_key(slot, &mut kb).to_string();
                pending.push((i, format!("velda:plugin:openai_token_quota:{k}")));
            }
        }

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let srv = tokio::spawn(async move {
            while let Ok((mut s, _)) = listener.accept().await {
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
                        if buf[0] == b'Q' || buf[0] == b'P' {
                            let mut rep = if buf[0] == b'P' {
                                vec![b'1', 0, 0, 0, 4]
                            } else {
                                vec![b'C', 0, 0, 0, 4]
                            };
                            rep.extend_from_slice(&[b'Z', 0, 0, 0, 5, b'I']);
                            let _ = s.write_all(&rep).await;
                        } else if buf[0] == b'B' {
                            let val = b"30";
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
            id: "pg-quota-test".into(),
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
        let connected = AtomicBool::new(false);
        let ok = flush_batch(
            &pg,
            arena.slots(),
            &pending,
            50,
            Duration::from_secs(1),
            &connected,
        )
        .await;
        assert!(ok);
        assert!(connected.load(Ordering::Relaxed));
        let (_, _, _, _, rem) = arena.read_consumer("t'quoted\\").unwrap();
        assert_eq!(rem, 28);
        srv.abort();
    }
}
