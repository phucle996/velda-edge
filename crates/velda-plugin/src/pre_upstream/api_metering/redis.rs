//! Shadow RedisEtp Delta Sync (`api_metering::redis`).
//!
//! ```text
//! Shadow Worker ──► collect_deltas() [delta -> in_flight_delta]
//!   └─► etp.acquire() ─► HINCRBY batch
//!         ├── Ok  ─► reconcile_balances(global - remaining_local_delta)
//!         └── Err ─► reconnect (1x) ─► revert_in_flight_deltas()
//! ```

use std::io::ErrorKind;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use velda_core::lut::decimal::fast_write_u64;
use velda_etp::EtpError;
use velda_etp::redis::RedisEtp;

use super::algorithm::ShardedMeteringState;
use crate::error::PluginError;
use crate::pre_upstream::distributed::DistributedSyncConfig;

fn encode_hincrby_batch(key: &str, deltas: &[(u64, i64)], out: &mut Vec<u8>) {
    out.clear();
    let mut hash_buf = itoa::Buffer::new();
    let mut delta_buf = itoa::Buffer::new();
    let mut len_buf = itoa::Buffer::new();

    for &(acc_hash, delta) in deltas {
        let field = hash_buf.format(acc_hash);
        let delta_str = delta_buf.format(delta);

        out.extend_from_slice(b"*4\r\n$7\r\nHINCRBY\r\n$");
        out.extend_from_slice(len_buf.format(key.len()).as_bytes());
        out.extend_from_slice(b"\r\n");
        out.extend_from_slice(key.as_bytes());
        out.extend_from_slice(b"\r\n$");
        out.extend_from_slice(len_buf.format(field.len()).as_bytes());
        out.extend_from_slice(b"\r\n");
        out.extend_from_slice(field.as_bytes());
        out.extend_from_slice(b"\r\n$");
        out.extend_from_slice(len_buf.format(delta_str.len()).as_bytes());
        out.extend_from_slice(b"\r\n");
        out.extend_from_slice(delta_str.as_bytes());
        out.extend_from_slice(b"\r\n");
    }
}

fn parse_pipelined_resp_ints(
    buf: &mut Vec<u8>,
    results: &mut Vec<i64>,
    expected: usize,
) -> Result<(), String> {
    let mut cursor = 0usize;
    while cursor < buf.len() && results.len() < expected {
        let remaining = &buf[cursor..];
        let Some(pos) = remaining.windows(2).position(|w| w == b"\r\n") else {
            break;
        };
        let line = &remaining[..pos];
        cursor += pos + 2;

        if let Some(num_bytes) = line.strip_prefix(b":") {
            let num_str =
                std::str::from_utf8(num_bytes).map_err(|e| format!("Non-UTF8 integer: {e}"))?;
            let val = num_str
                .parse::<i64>()
                .map_err(|e| format!("Invalid integer '{num_str}': {e}"))?;
            results.push(val);
        } else if let Some(err_bytes) = line.strip_prefix(b"-") {
            let err_msg = std::str::from_utf8(err_bytes).unwrap_or("unknown error");
            tracing::warn!(error = err_msg, "Redis returned error for HINCRBY in batch");
            results.push(0);
        } else {
            return Err(format!(
                "Unexpected RESP line prefix in HINCRBY batch: {:?}",
                line.first()
            ));
        }
    }

    if cursor > 0 {
        buf.drain(..cursor);
    }
    Ok(())
}

async fn flush_deltas_via_etp(
    etp: &Arc<RedisEtp>,
    key: &str,
    deltas: &[(u64, i64)],
    cmd_buf: &mut Vec<u8>,
    read_buf: &mut Vec<u8>,
) -> Result<Vec<i64>, EtpError> {
    encode_hincrby_batch(key, deltas, cmd_buf);
    let mut lease = etp.acquire().await?;

    for attempt in 0..2 {
        let exchange: Result<Result<Vec<i64>, EtpError>, _> =
            tokio::time::timeout(Duration::from_millis(1000), async {
                lease.write_all(cmd_buf).await?;
                lease.flush().await?;

                let mut results = Vec::with_capacity(deltas.len());
                read_buf.clear();
                let mut scratch = [0u8; 4096];

                while results.len() < deltas.len() {
                    let n = lease.read(&mut scratch).await?;
                    if n == 0 {
                        return Err(EtpError::Io(std::io::Error::new(
                            ErrorKind::UnexpectedEof,
                            "Redis closed connection before batch completed",
                        )));
                    }
                    if let Some(chunk) = scratch.get(..n) {
                        read_buf.extend_from_slice(chunk);
                    }
                    if read_buf.len() > 256 * 1024 {
                        return Err(EtpError::Io(std::io::Error::new(
                            ErrorKind::InvalidData,
                            "Redis RESP2 response exceeded 256 KiB",
                        )));
                    }
                    parse_pipelined_resp_ints(read_buf, &mut results, deltas.len()).map_err(
                        |msg| EtpError::Io(std::io::Error::new(ErrorKind::InvalidData, msg)),
                    )?;
                }

                Ok(results)
            })
            .await;

        match exchange {
            Ok(Ok(results)) => return Ok(results),
            Ok(Err(EtpError::Io(io_err)))
                if attempt == 0
                    && matches!(
                        io_err.kind(),
                        ErrorKind::UnexpectedEof
                            | ErrorKind::BrokenPipe
                            | ErrorKind::ConnectionReset
                    ) =>
            {
                if lease.reconnect().await.is_err() {
                    lease.recreate_in_background();
                    return Err(EtpError::Io(io_err));
                }
            }
            Ok(Err(err)) => {
                lease.recreate_in_background();
                return Err(err);
            }
            Err(_) => {
                lease.recreate_in_background();
                return Err(EtpError::Timeout {
                    id: etp.config.id.clone(),
                    timeout_ms: 1000,
                });
            }
        }
    }

    Err(EtpError::Timeout {
        id: etp.config.id.clone(),
        timeout_ms: 1000,
    })
}

/// Asynchronous Shadow Logistics loop reconciling local credit deltas with a [`RedisEtp`] pool.
pub(crate) async fn run_shadow_metering_sync(
    state: Arc<ShardedMeteringState>,
    etp: Arc<RedisEtp>,
    dist_cfg: DistributedSyncConfig,
    initial_balance: i64,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), PluginError> {
    let mut interval =
        tokio::time::interval(Duration::from_millis(dist_cfg.sync_interval_ms.max(10)));
    let key = format!("{}:metering:consumed", dist_cfg.key_prefix);
    let mut cmd_buf = Vec::with_capacity(8192);
    let mut read_buf = Vec::with_capacity(8192);

    // Ensure `fast_write_u64` remains available cleanly without unused import warnings if needed
    let _ = fast_write_u64;

    loop {
        tokio::select! {
            _ = interval.tick() => {}
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    break;
                }
            }
        }

        if *shutdown.borrow() {
            break;
        }

        let deltas = state.collect_deltas();
        if deltas.is_empty() {
            continue;
        }

        match flush_deltas_via_etp(&etp, &key, &deltas, &mut cmd_buf, &mut read_buf).await {
            Ok(results) => {
                let mut updates = Vec::with_capacity(results.len());
                for (idx, &res) in results.iter().enumerate() {
                    let (acc_hash, _) = deltas[idx];
                    let remaining_balance = initial_balance.saturating_sub(res);
                    updates.push((acc_hash, remaining_balance));
                }
                state.reconcile_balances(&updates);
            }
            Err(e) => {
                state.revert_in_flight_deltas(&deltas);
                tracing::warn!(
                    etp_id = %etp.config.id,
                    error = %e,
                    "Shadow logistics failed to flush metering deltas via RedisEtp"
                );
            }
        }
    }

    Ok(())
}
