//! RESP2 `DECRBY`/`INCRBY` wire codec, conservation math & lease group executor (`codec.rs`)
//! for `openai_token_quota` GCP Memorystore client.
//!
//! ```text
//!   encode_decrby_batch() ──► read_expected_replies() ──► compute_lease_and_refund()
//!     └──► refund > 0 ──────► encode_incrby_refunds() ──► Vec<ShardItemOutcome>
//! ```

use std::io::ErrorKind;
use std::sync::Arc;
use std::time::Duration;

use velda_etp::EtpError;
use velda_etp::gcp_memorystore::{GcpMemorystoreEtp, GcpMemorystoreLease};

use crate::wire_baked::BakedWire;

#[derive(Debug, Clone)]
pub(crate) enum LeaseRedirect {
    Moved { slot: u16, addr: String },
    Ask { addr: String },
}

pub(crate) enum LeaseLineReply {
    Int(i64),
    AskingOk,
    Moved { slot: u16, addr: String },
    Ask { addr: String },
    Err(String),
}

pub(crate) enum ShardItemOutcome {
    Leased {
        slot_idx: usize,
        leased: i64,
    },
    Redirect {
        slot_idx: usize,
        key: String,
        redirect: LeaseRedirect,
    },
}

pub(crate) fn encode_decrby_batch(
    batch: &[(usize, String)],
    batch_str: &str,
    with_asking: bool,
    out: &mut Vec<u8>,
) {
    let decr_prefix: &[u8] = if with_asking {
        b"*1\r\n$6\r\nASKING\r\n*3\r\n$6\r\nDECRBY\r\n$"
    } else {
        b"*3\r\n$6\r\nDECRBY\r\n$"
    };
    let mut batch_tail = Vec::with_capacity(24 + batch_str.len());
    {
        let mut tw = BakedWire::new(&mut batch_tail);
        tw.push_chunk(b"\r\n$");
        tw.push_u64(batch_str.len() as u64);
        tw.push_chunk(b"\r\n");
        tw.push_chunk(batch_str.as_bytes());
        tw.push_chunk(b"\r\n");
    }

    let mut w = BakedWire::cleared(
        out,
        batch.len() * (decr_prefix.len() + batch_tail.len() + 48),
    );
    for (_, key) in batch {
        w.push_chunk(decr_prefix);
        w.push_u64(key.len() as u64);
        w.push_chunk(b"\r\n");
        w.push_chunk(key.as_bytes());
        w.push_chunk(&batch_tail);
    }
}

pub(crate) fn encode_incrby_refunds(
    refunds: &[(String, i64)],
    with_asking: bool,
    out: &mut Vec<u8>,
) {
    let incr_prefix: &[u8] = if with_asking {
        b"*1\r\n$6\r\nASKING\r\n*3\r\n$6\r\nINCRBY\r\n$"
    } else {
        b"*3\r\n$6\r\nINCRBY\r\n$"
    };
    let mut w = BakedWire::cleared(out, refunds.len() * 96);
    let mut val_buf = Vec::with_capacity(24);
    for (key, refund) in refunds {
        {
            let mut vw = BakedWire::cleared(&mut val_buf, 24);
            vw.push_i64(*refund);
        }
        w.push_chunk(incr_prefix);
        w.push_u64(key.len() as u64);
        w.push_chunk(b"\r\n");
        w.push_chunk(key.as_bytes());
        w.push_chunk(b"\r\n$");
        w.push_u64(val_buf.len() as u64);
        w.push_chunk(b"\r\n");
        w.push_chunk(&val_buf);
        w.push_chunk(b"\r\n");
    }
}

fn parse_next_line(buf: &mut Vec<u8>) -> Option<Result<LeaseLineReply, String>> {
    let pos = buf.windows(2).position(|w| w == b"\r\n")?;
    let line = buf[..pos].to_vec();
    buf.drain(..pos + 2);

    if line.starts_with(b"+") {
        return Some(Ok(LeaseLineReply::AskingOk));
    }
    if let Some(num_bytes) = line.strip_prefix(b":") {
        let s = match std::str::from_utf8(num_bytes) {
            Ok(v) => v,
            Err(e) => return Some(Err(format!("Non-UTF8 integer: {e}"))),
        };
        let val = match s.parse::<i64>() {
            Ok(v) => v,
            Err(e) => return Some(Err(format!("Invalid integer '{s}': {e}"))),
        };
        return Some(Ok(LeaseLineReply::Int(val)));
    }
    if let Some(err_bytes) = line.strip_prefix(b"-") {
        let err_str = std::str::from_utf8(err_bytes).unwrap_or("unknown error");
        let mut parts = err_str.split_whitespace();
        if let (Some(kind), Some(slot_s), Some(addr)) = (parts.next(), parts.next(), parts.next())
            && let Ok(slot) = slot_s.parse::<u16>()
        {
            if kind.eq_ignore_ascii_case("MOVED") {
                return Some(Ok(LeaseLineReply::Moved {
                    slot,
                    addr: addr.to_string(),
                }));
            }
            if kind.eq_ignore_ascii_case("ASK") {
                return Some(Ok(LeaseLineReply::Ask {
                    addr: addr.to_string(),
                }));
            }
        }
        return Some(Ok(LeaseLineReply::Err(err_str.to_string())));
    }
    Some(Err(format!(
        "Unexpected RESP2 line prefix: {:?}",
        line.first()
    )))
}

async fn read_expected_replies(
    lease: &mut GcpMemorystoreLease,
    cmd_bytes: &[u8],
    expected_commands: usize,
    with_asking: bool,
    resp_buf: &mut Vec<u8>,
) -> Result<Vec<LeaseLineReply>, EtpError> {
    let total_lines = if with_asking {
        expected_commands.saturating_mul(2)
    } else {
        expected_commands
    };

    tokio::time::timeout(Duration::from_millis(500), async {
        lease.write_all(cmd_bytes).await?;
        lease.flush().await?;

        let mut replies = Vec::with_capacity(expected_commands);
        let mut lines_read = 0usize;
        resp_buf.clear();
        let mut tmp = [0u8; 2048];

        while lines_read < total_lines {
            while let Some(parsed) = parse_next_line(resp_buf) {
                lines_read += 1;
                let item = parsed
                    .map_err(|m| EtpError::Io(std::io::Error::new(ErrorKind::InvalidData, m)))?;
                match item {
                    LeaseLineReply::AskingOk => {}
                    other => replies.push(other),
                }
                if lines_read >= total_lines {
                    break;
                }
            }
            if lines_read >= total_lines {
                break;
            }
            let n = lease.read(&mut tmp).await?;
            if n == 0 {
                return Err(EtpError::Io(std::io::Error::new(
                    ErrorKind::UnexpectedEof,
                    "GcpMemorystore closed connection before batch completed",
                )));
            }
            if let Some(chunk) = tmp.get(..n) {
                resp_buf.extend_from_slice(chunk);
            }
            if resp_buf.len() > 64 * 1024 {
                return Err(EtpError::Io(std::io::Error::new(
                    ErrorKind::InvalidData,
                    "GcpMemorystore RESP2 response > 64 KiB",
                )));
            }
        }
        Ok(replies)
    })
    .await
    .map_err(|_| EtpError::Timeout {
        id: lease.pool.id.clone(),
        timeout_ms: 500,
    })?
}

#[inline(always)]
pub(crate) fn compute_lease_and_refund(after: i64, batch_i64: i64) -> (i64, i64) {
    if after >= 0 {
        (batch_i64, 0)
    } else {
        let prev = after.saturating_add(batch_i64);
        if prev > 0 {
            let leased = prev.min(batch_i64);
            (leased, batch_i64 - leased)
        } else {
            (0, batch_i64)
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn execute_lease_group(
    etp: &Arc<GcpMemorystoreEtp>,
    mut lease: GcpMemorystoreLease,
    group: &[(usize, String)],
    batch_i64: i64,
    batch_str: &str,
    with_asking: bool,
    cmd_buf: &mut Vec<u8>,
    resp_buf: &mut Vec<u8>,
) -> Result<Vec<ShardItemOutcome>, EtpError> {
    encode_decrby_batch(group, batch_str, with_asking, cmd_buf);

    let replies = {
        let mut last_err: Option<EtpError> = None;
        let mut parsed = None;
        for attempt in 0..2 {
            match read_expected_replies(&mut lease, cmd_buf, group.len(), with_asking, resp_buf)
                .await
            {
                Ok(r) => {
                    parsed = Some(r);
                    break;
                }
                Err(EtpError::Io(io_err))
                    if attempt == 0 && io_err.kind() != ErrorKind::InvalidData =>
                {
                    if lease.reconnect().await.is_err() {
                        lease.recreate_in_background();
                        return Err(EtpError::Io(io_err));
                    }
                    last_err = Some(EtpError::Io(io_err));
                }
                Err(e) => {
                    lease.recreate_in_background();
                    return Err(e);
                }
            }
        }
        match parsed {
            Some(r) => r,
            None => {
                return Err(last_err.unwrap_or_else(|| EtpError::PoolExhausted {
                    id: etp.config.id.clone(),
                }));
            }
        }
    };

    let mut outcomes = Vec::with_capacity(group.len());
    let mut refunds: Vec<(String, i64)> = Vec::new();

    for ((slot_idx, key), reply) in group.iter().zip(replies) {
        match reply {
            LeaseLineReply::Int(after) => {
                let (leased, refund) = compute_lease_and_refund(after, batch_i64);
                if refund > 0 {
                    refunds.push((key.clone(), refund));
                }
                outcomes.push(ShardItemOutcome::Leased {
                    slot_idx: *slot_idx,
                    leased,
                });
            }
            LeaseLineReply::Moved { slot, addr } => {
                if !etp.is_cluster() {
                    return Err(etp.report_moved(slot, &addr).unwrap_err());
                }
                outcomes.push(ShardItemOutcome::Redirect {
                    slot_idx: *slot_idx,
                    key: key.clone(),
                    redirect: LeaseRedirect::Moved { slot, addr },
                });
            }
            LeaseLineReply::Ask { addr } => {
                if !etp.is_cluster() {
                    return Err(EtpError::Config(format!(
                        "GcpMemorystore ETP '{}' received cluster -ASK {addr} while configured with mode='standalone'; set mode='cluster' in etp.json",
                        etp.config.id
                    )));
                }
                outcomes.push(ShardItemOutcome::Redirect {
                    slot_idx: *slot_idx,
                    key: key.clone(),
                    redirect: LeaseRedirect::Ask { addr },
                });
            }
            LeaseLineReply::Err(err_msg) => {
                tracing::warn!(
                    key = %key,
                    error = %err_msg,
                    "GcpMemorystore DECRBY returned error; treating slot as 0 leased"
                );
                outcomes.push(ShardItemOutcome::Leased {
                    slot_idx: *slot_idx,
                    leased: 0,
                });
            }
            LeaseLineReply::AskingOk => {}
        }
    }

    if !refunds.is_empty() {
        encode_incrby_refunds(&refunds, with_asking, cmd_buf);
        if let Err(err) =
            read_expected_replies(&mut lease, cmd_buf, refunds.len(), with_asking, resp_buf).await
        {
            lease.recreate_in_background();
            tracing::warn!(
                id = %etp.config.id,
                error = %err,
                "Failed to flush compensating INCRBY refunds cleanly"
            );
        }
    }

    Ok(outcomes)
}
