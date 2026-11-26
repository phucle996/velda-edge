//! MySQL Prepared Statement + CTE atomic upsert & readback (`client.rs`).
//!
//! ```text
//!   push_and_read_totals(&etp, key_prefix, deltas)
//!     COM_STMT_PREPARE(upsert CTE with ?) -> loop DirtyKeyDelta: COM_STMT_EXECUTE(params) -> COM_STMT_CLOSE
//!     COM_STMT_PREPARE(SELECT cnt WHERE k=?) -> loop DirtyKeyDelta: COM_STMT_EXECUTE(key) -> COM_STMT_CLOSE
//! ```

use std::fmt::Write as _;
use std::sync::Arc;
use std::time::Duration;

use velda_etp::EtpError;
use velda_etp::mysql::MysqlEtp;

use super::wire::{MysqlParam, stmt_close, stmt_execute, stmt_prepare};
use crate::pre_upstream::rate_limit::arena::{DirtyKeyDelta, ReconciledClusterTotal};

const MY_UPSERT_CTE_SQL: &str = "\
INSERT INTO velda.velda_rate_limit (k, cnt, exp) \
SELECT k, d, exp_ts FROM (SELECT ? AS k, ? AS d, ? AS exp_ts) AS delta_in \
ON DUPLICATE KEY UPDATE \
  cnt = IF(exp < ?, VALUES(cnt), cnt + VALUES(cnt)), \
  exp = VALUES(exp)";

const MY_SELECT_SQL: &str = "SELECT CAST(cnt AS SIGNED) FROM velda.velda_rate_limit WHERE k = ?";

/// Atomically upserts per-key counters into `velda.velda_rate_limit` via prepared statements
/// and reads back cluster totals.
pub async fn push_and_read_totals(
    etp: &Arc<MysqlEtp>,
    key_prefix: &str,
    deltas: &[DirtyKeyDelta],
    io_timeout: Duration,
) -> Result<Vec<ReconciledClusterTotal>, EtpError> {
    if deltas.is_empty() {
        return Ok(Vec::new());
    }

    let now_unix = velda_core::clock::coarse_sec();
    let exp_i64 = i64::try_from(now_unix + 60).unwrap_or(i64::MAX);
    let now_i64 = i64::try_from(now_unix).unwrap_or(i64::MAX);
    let mut lease = etp.acquire().await?;

    let mut last_err = EtpError::Timeout {
        id: etp.config.id.clone(),
        timeout_ms: u64::try_from(io_timeout.as_millis()).unwrap_or(u64::MAX),
    };

    for attempt in 0..2 {
        let res: Result<Result<Vec<ReconciledClusterTotal>, EtpError>, _> =
            tokio::time::timeout(io_timeout, async {
                let mut keys: Vec<String> = Vec::with_capacity(deltas.len());
                for d in deltas {
                    let mut k = String::with_capacity(64);
                    let _ = write!(
                        k,
                        "{key_prefix}{}:{:016x}:{}",
                        d.tier, d.key_hash, d.window_epoch
                    );
                    keys.push(k);
                }

                let upsert_stmt = stmt_prepare(&mut lease, MY_UPSERT_CTE_SQL).await?;
                for (k, d) in keys.iter().zip(deltas.iter()) {
                    let delta_i64 = i64::try_from(d.delta).unwrap_or(i64::MAX);
                    stmt_execute(
                        &mut lease,
                        &upsert_stmt,
                        &[
                            MysqlParam::Str(k),
                            MysqlParam::I64(delta_i64),
                            MysqlParam::I64(exp_i64),
                            MysqlParam::I64(now_i64),
                        ],
                    )
                    .await?;
                }
                stmt_close(&mut lease, upsert_stmt.stmt_id).await?;

                let select_stmt = stmt_prepare(&mut lease, MY_SELECT_SQL).await?;
                let mut out = Vec::with_capacity(deltas.len());
                for (k, d) in keys.iter().zip(deltas.iter()) {
                    let total = stmt_execute(&mut lease, &select_stmt, &[MysqlParam::Str(k)])
                        .await?
                        .and_then(|v| u64::try_from(v).ok())
                        .unwrap_or(d.delta);
                    out.push(ReconciledClusterTotal {
                        key_hash: d.key_hash,
                        tier: d.tier,
                        pushed_delta: d.delta,
                        window_epoch: d.window_epoch,
                        cluster_total: total,
                    });
                }
                stmt_close(&mut lease, select_stmt.stmt_id).await?;
                Ok(out)
            })
            .await;

        match res {
            Ok(Ok(totals)) => return Ok(totals),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pre_upstream::rate_limit::providers::mysql::schema::ensure_table;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use velda_etp::{EtpEntryConfig, EtpKind, EtpPoolSettings, EtpTransportSecurity};

    async fn send_pkt(s: &mut tokio::net::TcpStream, seq: u8, payload: &[u8]) {
        let len = payload.len() as u32;
        let mut p = vec![
            (len & 0xff) as u8,
            ((len >> 8) & 0xff) as u8,
            ((len >> 16) & 0xff) as u8,
            seq,
        ];
        p.extend_from_slice(payload);
        let _ = s.write_all(&p).await;
    }

    #[tokio::test]
    async fn test_rate_limit_mysql_prepared_statement_and_schema_ddl() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        let srv = tokio::spawn(async move {
            while let Ok((mut s, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut hs = vec![0x0a];
                    hs.extend_from_slice(b"8.0.36\0");
                    hs.extend_from_slice(&1u32.to_le_bytes());
                    hs.extend_from_slice(&[0x11u8; 8]);
                    hs.push(0);
                    hs.extend_from_slice(&0xffffu16.to_le_bytes());
                    hs.push(45);
                    hs.extend_from_slice(&2u16.to_le_bytes());
                    hs.extend_from_slice(&0x0008u16.to_le_bytes());
                    hs.push(21);
                    hs.extend_from_slice(&[0u8; 10]);
                    hs.extend_from_slice(&[0x22u8; 12]);
                    hs.push(0);
                    hs.extend_from_slice(b"mysql_native_password\0");
                    send_pkt(&mut s, 0, &hs).await;
                    let mut rbuf = [0u8; 512];
                    let _ = s.read(&mut rbuf).await;
                    send_pkt(&mut s, 2, &[0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00]).await;

                    let mut active_stmt = 0u32;
                    let mut hdr = [0u8; 4];
                    while s.read_exact(&mut hdr).await.is_ok() {
                        let len = (hdr[0] as usize)
                            | ((hdr[1] as usize) << 8)
                            | ((hdr[2] as usize) << 16);
                        let mut body = vec![0u8; len];
                        if len > 0 {
                            let _ = s.read_exact(&mut body).await;
                        }
                        match body[0] {
                            0x03 => {
                                send_pkt(&mut s, 1, &[0x00]).await;
                            }
                            0x16 => {
                                let sql = String::from_utf8_lossy(&body[1..]);
                                if sql.starts_with("SELECT") {
                                    active_stmt = 2;
                                    let mut ok = vec![0x00];
                                    ok.extend_from_slice(&2u32.to_le_bytes());
                                    ok.extend_from_slice(&1u16.to_le_bytes());
                                    ok.extend_from_slice(&1u16.to_le_bytes());
                                    send_pkt(&mut s, 1, &ok).await;
                                    send_pkt(&mut s, 2, &[0x01]).await;
                                    send_pkt(&mut s, 3, &[0xFE]).await;
                                    send_pkt(&mut s, 4, &[0x01]).await;
                                    send_pkt(&mut s, 5, &[0xFE]).await;
                                } else {
                                    active_stmt = 1;
                                    let mut ok = vec![0x00];
                                    ok.extend_from_slice(&1u32.to_le_bytes());
                                    ok.extend_from_slice(&0u16.to_le_bytes());
                                    ok.extend_from_slice(&4u16.to_le_bytes());
                                    send_pkt(&mut s, 1, &ok).await;
                                    for seq in 2..=5 {
                                        send_pkt(&mut s, seq, &[0x01]).await;
                                    }
                                    send_pkt(&mut s, 6, &[0xFE]).await;
                                }
                            }
                            0x17 => {
                                if active_stmt == 1 {
                                    send_pkt(&mut s, 1, &[0x00]).await;
                                } else {
                                    send_pkt(&mut s, 1, &[1]).await;
                                    let mut col = vec![0; 6];
                                    col.push(0x0c);
                                    col.extend_from_slice(&[0u8; 6]);
                                    col.push(0x08);
                                    send_pkt(&mut s, 2, &col).await;
                                    send_pkt(&mut s, 3, &[0xFE]).await;
                                    let mut row = vec![0x00, 0x00];
                                    row.extend_from_slice(&91i64.to_le_bytes());
                                    send_pkt(&mut s, 4, &row).await;
                                    send_pkt(&mut s, 5, &[0xFE]).await;
                                }
                            }
                            _ => {}
                        }
                    }
                });
            }
        });

        let entry = EtpEntryConfig {
            id: "my-rl-test".into(),
            kind: EtpKind::MySql,
            security: EtpTransportSecurity::Plain,
            pool: EtpPoolSettings {
                pool_size: 2,
                connect_timeout_ms: 500,
                idle_timeout_ms: 5000,
            },
            tls: None,
            settings: serde_json::json!({ "host": "127.0.0.1", "port": port }),
        };
        let my = Arc::new(MysqlEtp::from_entry(&entry).unwrap());
        ensure_table(&my, Duration::from_secs(1)).await.unwrap();
        let out = push_and_read_totals(
            &my,
            "t'k\\",
            &[DirtyKeyDelta {
                key_hash: 5,
                tier: 0,
                delta: 4,
                window_epoch: 2,
            }],
            Duration::from_secs(1),
        )
        .await
        .unwrap();
        assert_eq!(out[0].cluster_total, 91);
        srv.abort();
    }
}
