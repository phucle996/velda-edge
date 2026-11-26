//! DragonflyDB RESP2 wire codec & conservation math (`codec.rs`) for `openai_token_quota`.
//!
//! ```text
//!   encode_decrby_batch() / encode_incrby_refunds() -> RESP2 arrays
//!   read_expected_replies()                         -> Vec<LeaseLineReply>
//!   compute_lease_and_refund(after, batch)          -> (leased, refund)
//! ```

use std::io::ErrorKind;
use std::time::Duration;

use velda_etp::EtpError;
use velda_etp::dragonfly::DragonflyLease;

use crate::wire_baked::BakedWire;

pub(crate) enum LeaseLineReply {
    Int(i64),
    Err(String),
}

pub(crate) fn encode_decrby_batch(batch: &[(usize, String)], batch_str: &str, out: &mut Vec<u8>) {
    let mut batch_tail = Vec::with_capacity(24 + batch_str.len());
    {
        let mut tw = BakedWire::new(&mut batch_tail);
        tw.push_chunk(b"\r\n$");
        tw.push_u64(batch_str.len() as u64);
        tw.push_chunk(b"\r\n");
        tw.push_chunk(batch_str.as_bytes());
        tw.push_chunk(b"\r\n");
    }

    let mut w = BakedWire::cleared(out, batch.len() * (32 + batch_tail.len() + 32));
    for (_, key) in batch {
        w.push_chunk(b"*3\r\n$6\r\nDECRBY\r\n$");
        w.push_u64(key.len() as u64);
        w.push_chunk(b"\r\n");
        w.push_chunk(key.as_bytes());
        w.push_chunk(&batch_tail);
    }
}

pub(crate) fn encode_incrby_refunds(refunds: &[(String, i64)], out: &mut Vec<u8>) {
    let mut w = BakedWire::cleared(out, refunds.len() * 80);
    let mut val_buf = Vec::with_capacity(24);
    for (key, refund) in refunds {
        {
            let mut vw = BakedWire::cleared(&mut val_buf, 24);
            vw.push_i64(*refund);
        }
        w.push_chunk(b"*3\r\n$6\r\nINCRBY\r\n$");
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
        return Some(Ok(LeaseLineReply::Err(err_str.to_string())));
    }
    Some(Err(format!(
        "Unexpected Dragonfly RESP2 line prefix: {:?}",
        line.first()
    )))
}

pub(crate) async fn read_expected_replies(
    lease: &mut DragonflyLease,
    cmd_bytes: &[u8],
    expected_commands: usize,
    resp_buf: &mut Vec<u8>,
) -> Result<Vec<LeaseLineReply>, EtpError> {
    tokio::time::timeout(Duration::from_millis(500), async {
        lease.write_all(cmd_bytes).await?;
        lease.flush().await?;

        let mut replies = Vec::with_capacity(expected_commands);
        resp_buf.clear();
        let mut tmp = [0u8; 2048];

        while replies.len() < expected_commands {
            while let Some(parsed) = parse_next_line(resp_buf) {
                let item = parsed
                    .map_err(|m| EtpError::Io(std::io::Error::new(ErrorKind::InvalidData, m)))?;
                replies.push(item);
                if replies.len() >= expected_commands {
                    break;
                }
            }
            if replies.len() >= expected_commands {
                break;
            }
            let n = lease.read(&mut tmp).await?;
            if n == 0 {
                return Err(EtpError::Io(std::io::Error::new(
                    ErrorKind::UnexpectedEof,
                    "Dragonfly closed connection before batch completed",
                )));
            }
            if let Some(chunk) = tmp.get(..n) {
                resp_buf.extend_from_slice(chunk);
            }
            if resp_buf.len() > 64 * 1024 {
                return Err(EtpError::Io(std::io::Error::new(
                    ErrorKind::InvalidData,
                    "Dragonfly RESP2 response > 64 KiB",
                )));
            }
        }
        Ok(replies)
    })
    .await
    .map_err(|_| EtpError::Timeout {
        id: lease.pool.config.id.clone(),
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
