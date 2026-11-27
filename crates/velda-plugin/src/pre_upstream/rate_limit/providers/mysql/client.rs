//! MySQL multi-row `INSERT ON DUPLICATE KEY UPDATE` & `SELECT IN` batch client (`client.rs`).
//!
//! ```text
//!   push_and_read_totals(&etp, key_prefix, deltas)
//!     1. COM_QUERY: multi-row INSERT ... VALUES (...), (...) ON DUPLICATE KEY UPDATE
//!     2. COM_QUERY: SELECT k, CAST(cnt AS UNSIGNED) FROM ... WHERE k IN (...) -> 2 RTT total
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use velda_etp::EtpError;
use velda_etp::mysql::MysqlEtp;

use super::wire::{append_mysql_escaped_str, execute_com_query, execute_com_query_kv_rows};
use crate::pre_upstream::rate_limit::arena::{DirtyKeyDelta, ReconciledClusterTotal};
use crate::wire_baked::BakedWire;

/// Atomically upserts per-key counters into `velda.velda_rate_limit` in 2 batch RTTs
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
    let exp_u64 = now_unix + 60;
    let prefix_b = key_prefix.as_bytes();

    let mut upsert_sql = Vec::with_capacity(256 + deltas.len() * (prefix_b.len() + 72));
    let mut select_sql = Vec::with_capacity(96 + deltas.len() * (prefix_b.len() + 48));
    let mut key_buf = Vec::with_capacity(prefix_b.len() + 48);
    let mut key_map: HashMap<String, usize> = HashMap::with_capacity(deltas.len());

    {
        upsert_sql.extend_from_slice(b"INSERT INTO velda.velda_rate_limit (k, cnt, exp) VALUES ");
        select_sql.extend_from_slice(
            b"SELECT k, CAST(cnt AS UNSIGNED) FROM velda.velda_rate_limit WHERE k IN (",
        );

        for (idx, d) in deltas.iter().enumerate() {
            {
                let mut kw = BakedWire::cleared(&mut key_buf, prefix_b.len() + 48);
                kw.push_chunk(prefix_b);
                kw.push_u64(u64::from(d.tier));
                kw.push_byte(b':');
                kw.push_hex16(d.key_hash);
                kw.push_byte(b':');
                kw.push_u64(d.window_epoch);
            }
            if idx > 0 {
                upsert_sql.push(b',');
                select_sql.push(b',');
            }
            upsert_sql.push(b'(');
            append_mysql_escaped_str(&mut upsert_sql, &key_buf);
            {
                let mut uw = BakedWire::new(&mut upsert_sql);
                uw.push_byte(b',');
                uw.push_u64(d.delta);
                uw.push_byte(b',');
                uw.push_u64(exp_u64);
                uw.push_byte(b')');
            }
            append_mysql_escaped_str(&mut select_sql, &key_buf);
            key_map.insert(String::from_utf8_lossy(&key_buf).into_owned(), idx);
        }

        upsert_sql.extend_from_slice(b" ON DUPLICATE KEY UPDATE cnt = IF(exp < ");
        {
            let mut uw = BakedWire::new(&mut upsert_sql);
            uw.push_u64(now_unix);
            uw.push_chunk(b", VALUES(cnt), cnt + VALUES(cnt)), exp = VALUES(exp)");
        }
        select_sql.push(b')');
    }

    let upsert_str = std::str::from_utf8(&upsert_sql).unwrap_or("");
    let select_str = std::str::from_utf8(&select_sql).unwrap_or("");
    let mut lease = etp.acquire().await?;
    let mut inserted = false;

    let mut last_err = EtpError::Timeout {
        id: etp.config.id.clone(),
        timeout_ms: u64::try_from(io_timeout.as_millis()).unwrap_or(u64::MAX),
    };

    for attempt in 0..2 {
        let res: Result<Result<Vec<ReconciledClusterTotal>, EtpError>, _> =
            tokio::time::timeout(io_timeout, async {
                if !inserted {
                    execute_com_query(&mut lease, upsert_str).await?;
                    inserted = true;
                }
                let rows = execute_com_query_kv_rows(&mut lease, select_str).await?;
                let mut totals: Vec<u64> = deltas.iter().map(|d| d.delta).collect();
                for (k, val) in rows {
                    if let Some(&idx) = key_map.get(&k) {
                        totals[idx] = val;
                    }
                }
                Ok(deltas
                    .iter()
                    .enumerate()
                    .map(|(idx, d)| ReconciledClusterTotal {
                        key_hash: d.key_hash,
                        tier: d.tier,
                        pushed_delta: d.delta,
                        window_epoch: d.window_epoch,
                        cluster_total: totals[idx],
                    })
                    .collect())
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
                break;
            }
            Err(_) => {
                lease.recreate_in_background();
                break;
            }
        }
    }

    if inserted {
        return Ok(deltas
            .iter()
            .map(|d| ReconciledClusterTotal {
                key_hash: d.key_hash,
                tier: d.tier,
                pushed_delta: d.delta,
                window_epoch: d.window_epoch,
                cluster_total: d.delta,
            })
            .collect());
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

    fn push_lenenc_str(buf: &mut Vec<u8>, s: &[u8]) {
        if s.len() < 251 {
            buf.push(s.len() as u8);
        } else {
            buf.push(0xFC);
            buf.extend_from_slice(&(s.len() as u16).to_le_bytes());
        }
        buf.extend_from_slice(s);
    }

    #[tokio::test]
    async fn test_rate_limit_mysql_batch_com_query_and_schema_ddl() {
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

                    let mut hdr = [0u8; 4];
                    while s.read_exact(&mut hdr).await.is_ok() {
                        let len = (hdr[0] as usize)
                            | ((hdr[1] as usize) << 8)
                            | ((hdr[2] as usize) << 16);
                        let mut body = vec![0u8; len];
                        if len > 0 {
                            let _ = s.read_exact(&mut body).await;
                        }
                        if body.first() == Some(&0x03) {
                            let sql = String::from_utf8_lossy(&body[1..]);
                            if sql.starts_with("SELECT") {
                                assert!(sql.contains("t\\'k\\\\0:0000000000000005:2"));
                                send_pkt(&mut s, 1, &[2]).await;
                                send_pkt(&mut s, 2, b"def").await;
                                send_pkt(&mut s, 3, b"def").await;
                                send_pkt(&mut s, 4, &[0xFE]).await;
                                let mut row1 = Vec::new();
                                push_lenenc_str(&mut row1, b"t'k\\0:0000000000000005:2");
                                push_lenenc_str(&mut row1, b"91");
                                send_pkt(&mut s, 5, &row1).await;
                                let mut row2 = Vec::new();
                                push_lenenc_str(&mut row2, b"t'k\\1:0000000000000006:2");
                                push_lenenc_str(&mut row2, b"44");
                                send_pkt(&mut s, 6, &row2).await;
                                send_pkt(&mut s, 7, &[0xFE]).await;
                            } else {
                                send_pkt(&mut s, 1, &[0x00]).await;
                            }
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
            &[
                DirtyKeyDelta {
                    key_hash: 5,
                    tier: 0,
                    delta: 4,
                    window_epoch: 2,
                },
                DirtyKeyDelta {
                    key_hash: 6,
                    tier: 1,
                    delta: 7,
                    window_epoch: 2,
                },
            ],
            Duration::from_secs(1),
        )
        .await
        .unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].cluster_total, 91);
        assert_eq!(out[1].cluster_total, 44);
        srv.abort();
    }
}
