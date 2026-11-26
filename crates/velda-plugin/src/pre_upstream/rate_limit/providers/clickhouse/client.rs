//! ClickHouse HTTP/1.1 `SummingMergeTree` upsert & `sum(delta)` query (`client.rs`).
//!
//! ```text
//!   BakedWire TabSeparated + SQL assembly (0 per-key String allocations):
//!     insert_head + tsv_buf (k\tepoch\tdelta\n)
//!     select_head + sql_buf (SELECT k, sum(delta) FROM db.velda_rate_limits WHERE ...)
//! ```

use std::collections::HashMap;
use std::io::Error;
use std::sync::Arc;
use std::time::Duration;

use velda_etp::EtpError;
use velda_etp::clickhouse::ClickHouseEtp;

use super::http::{format_auth_headers, read_http1_response};
use super::schema::TABLE_NAME;
use crate::pre_upstream::rate_limit::arena::{DirtyKeyDelta, ReconciledClusterTotal};
use crate::wire_baked::BakedWire;

/// Inserts counter deltas via `SummingMergeTree` and queries aggregated totals back.
pub async fn push_and_read_totals(
    etp: &Arc<ClickHouseEtp>,
    key_prefix: &str,
    deltas: &[DirtyKeyDelta],
    io_timeout: Duration,
) -> Result<Vec<ReconciledClusterTotal>, EtpError> {
    if deltas.is_empty() {
        return Ok(Vec::new());
    }

    let db = &etp.config.database;
    let auth = format_auth_headers(&etp.config);
    let safe_prefix: Vec<u8> = key_prefix
        .bytes()
        .filter(|&b| b.is_ascii_alphanumeric() || matches!(b, b':' | b'_' | b'-'))
        .collect();

    let mut tsv_buf = Vec::with_capacity(deltas.len() * (safe_prefix.len() + 48));
    let mut sql_buf = Vec::with_capacity(128 + deltas.len() * (safe_prefix.len() + 36));
    let mut key_map: HashMap<String, usize> = HashMap::with_capacity(deltas.len());

    let mut min_epoch = u64::MAX;
    for d in deltas {
        min_epoch = min_epoch.min(d.window_epoch);
    }

    {
        let mut tw = BakedWire::new(&mut tsv_buf);
        let mut sw = BakedWire::new(&mut sql_buf);
        sw.push_chunk(b"SELECT k, sum(delta) FROM ");
        sw.push_chunk(db.as_bytes());
        sw.push_byte(b'.');
        sw.push_chunk(TABLE_NAME.as_bytes());
        sw.push_chunk(b" WHERE epoch >= ");
        sw.push_u64(min_epoch);
        sw.push_chunk(b" AND k IN (");

        let mut single_key = Vec::with_capacity(safe_prefix.len() + 40);
        for (idx, d) in deltas.iter().enumerate() {
            {
                let mut kw = BakedWire::cleared(&mut single_key, safe_prefix.len() + 40);
                kw.push_chunk(&safe_prefix);
                kw.push_u64(u64::from(d.tier));
                kw.push_byte(b':');
                kw.push_hex16(d.key_hash);
                kw.push_byte(b':');
                kw.push_u64(d.window_epoch);
            }

            tw.push_chunk(&single_key);
            tw.push_byte(b'\t');
            tw.push_u64(d.window_epoch);
            tw.push_byte(b'\t');
            tw.push_u64(d.delta);
            tw.push_byte(b'\n');

            if idx > 0 {
                sw.push_byte(b',');
            }
            sw.push_byte(b'\'');
            sw.push_chunk(&single_key);
            sw.push_byte(b'\'');

            key_map.insert(String::from_utf8_lossy(&single_key).into_owned(), idx);
        }
        sw.push_chunk(b") GROUP BY k FORMAT TabSeparated");
    }

    let insert_prefix = format!(
        "POST {}/?query=INSERT%20INTO%20{db}.{TABLE_NAME}%20FORMAT%20TabSeparated HTTP/1.1\r\n\
         Host: {}\r\nContent-Type: text/tab-separated-values\r\n\
         {auth}Connection: keep-alive\r\nContent-Length: ",
        etp.config.base_path, etp.config.host_header
    );
    let select_prefix = format!(
        "POST {}/ HTTP/1.1\r\nHost: {}\r\nContent-Type: text/plain; charset=utf-8\r\n\
         {auth}Connection: keep-alive\r\nContent-Length: ",
        etp.config.base_path, etp.config.host_header
    );

    let mut insert_head = Vec::with_capacity(insert_prefix.len() + 24);
    {
        let mut hw = BakedWire::new(&mut insert_head);
        hw.push_chunk(insert_prefix.as_bytes());
        hw.push_u64(tsv_buf.len() as u64);
        hw.push_chunk(b"\r\n\r\n");
    }
    let mut select_head = Vec::with_capacity(select_prefix.len() + 24);
    {
        let mut hw = BakedWire::new(&mut select_head);
        hw.push_chunk(select_prefix.as_bytes());
        hw.push_u64(sql_buf.len() as u64);
        hw.push_chunk(b"\r\n\r\n");
    }

    let mut lease = etp.acquire().await?;
    let mut resp_buf = Vec::with_capacity(2048);
    let mut last_err = EtpError::Timeout {
        id: etp.config.id.clone(),
        timeout_ms: u64::try_from(io_timeout.as_millis()).unwrap_or(u64::MAX),
    };

    for attempt in 0..2 {
        let res: Result<Result<Vec<ReconciledClusterTotal>, EtpError>, _> =
            tokio::time::timeout(io_timeout, async {
                lease.write_all(&insert_head).await?;
                lease.write_all(&tsv_buf).await?;
                lease.flush().await?;
                let (st1, b1) = read_http1_response(&mut lease, &mut resp_buf).await?;
                if !(200..300).contains(&st1) {
                    return Err(EtpError::Io(Error::other(format!(
                        "ClickHouse INSERT HTTP {st1}: {}",
                        String::from_utf8_lossy(&b1)
                    ))));
                }

                lease.write_all(&select_head).await?;
                lease.write_all(&sql_buf).await?;
                lease.flush().await?;
                let (st2, b2) = read_http1_response(&mut lease, &mut resp_buf).await?;
                if !(200..300).contains(&st2) {
                    return Err(EtpError::Io(Error::other(format!(
                        "ClickHouse SELECT HTTP {st2}: {}",
                        String::from_utf8_lossy(&b2)
                    ))));
                }

                let mut totals: Vec<u64> = deltas.iter().map(|d| d.delta).collect();
                let txt = String::from_utf8_lossy(&b2);
                for line in txt.lines() {
                    if let Some((k, sum_str)) = line.split_once('\t')
                        && let Some(&idx) = key_map.get(k.trim())
                        && let Ok(val) = sum_str.trim().parse::<u64>()
                    {
                        totals[idx] = val;
                    }
                }

                Ok(deltas
                    .iter()
                    .enumerate()
                    .map(|(i, d)| ReconciledClusterTotal {
                        key_hash: d.key_hash,
                        tier: d.tier,
                        pushed_delta: d.delta,
                        window_epoch: d.window_epoch,
                        cluster_total: totals[i],
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
