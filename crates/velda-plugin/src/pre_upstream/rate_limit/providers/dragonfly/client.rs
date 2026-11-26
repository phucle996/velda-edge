//! DragonflyDB single-endpoint RESP2 pipelined atomic counter upsert & readback (`client.rs`).
//!
//! ```text
//!   DirtyKeyDelta[0..N] ──► BakedWire RESP2 chunks (*6 SET EX 15 NX + *3 INCRBY) -> 1 RTT
//! ```

use std::io::{Error, ErrorKind};
use std::sync::Arc;
use std::time::Duration;

use velda_core::lut::decimal::fast_write_u64_slice;
use velda_etp::EtpError;
use velda_etp::dragonfly::DragonflyEtp;

use crate::pre_upstream::rate_limit::arena::{DirtyKeyDelta, ReconciledClusterTotal};
use crate::wire_baked::BakedWire;

const SET_HEAD: &[u8] = b"*6\r\n$3\r\nSET\r\n$";
const SET_EX_AND_INCR_HEAD: &[u8] =
    b"\r\n$1\r\n0\r\n$2\r\nEX\r\n$2\r\n15\r\n$2\r\nNX\r\n*3\r\n$6\r\nINCRBY\r\n$";

/// Atomically initializes (if absent with TTL) and increments each key's epoch counter in
/// a single pipelined RTT against DragonflyDB without Lua scripts.
pub async fn push_and_read_totals(
    etp: &Arc<DragonflyEtp>,
    key_prefix: &str,
    deltas: &[DirtyKeyDelta],
    io_timeout: Duration,
) -> Result<Vec<ReconciledClusterTotal>, EtpError> {
    if deltas.is_empty() {
        return Ok(Vec::new());
    }

    let prefix = if key_prefix.is_empty() {
        b"velda:rl:"
    } else {
        key_prefix.as_bytes()
    };
    let mut key_buf = Vec::with_capacity(prefix.len() + 40);
    let mut wire = Vec::with_capacity(deltas.len() * (prefix.len() * 2 + 128));
    let mut d_buf = [0u8; 24];
    let mut klen_buf = [0u8; 24];

    {
        let mut w = BakedWire::new(&mut wire);
        for d in deltas {
            {
                let mut kw = BakedWire::cleared(&mut key_buf, prefix.len() + 40);
                kw.push_chunk(prefix);
                kw.push_u64(u64::from(d.tier));
                kw.push_byte(b':');
                kw.push_hex16(d.key_hash);
                kw.push_byte(b':');
                kw.push_u64(d.window_epoch);
            }
            let d_len = fast_write_u64_slice(&mut d_buf, d.delta);
            let klen_len = fast_write_u64_slice(&mut klen_buf, key_buf.len() as u64);

            w.push_chunk(SET_HEAD);
            w.push_chunk(&klen_buf[..klen_len]);
            w.push_chunk(b"\r\n");
            w.push_chunk(&key_buf);
            w.push_chunk(SET_EX_AND_INCR_HEAD);
            w.push_chunk(&klen_buf[..klen_len]);
            w.push_chunk(b"\r\n");
            w.push_chunk(&key_buf);
            w.push_chunk(b"\r\n$");
            w.push_u64(d_len as u64);
            w.push_chunk(b"\r\n");
            w.push_chunk(&d_buf[..d_len]);
            w.push_chunk(b"\r\n");
        }
    }

    let mut lease = etp.acquire().await?;
    let mut last_err = EtpError::Timeout {
        id: etp.config.id.clone(),
        timeout_ms: u64::try_from(io_timeout.as_millis()).unwrap_or(u64::MAX),
    };

    for attempt in 0..2 {
        let res: Result<Result<Vec<ReconciledClusterTotal>, EtpError>, _> =
            tokio::time::timeout(io_timeout, async {
                lease.write_all(&wire).await?;
                lease.flush().await?;

                let mut out = Vec::with_capacity(deltas.len());
                for d in deltas {
                    let set_reply = read_resp_line(&mut lease).await?;
                    let incr_reply = read_resp_line(&mut lease).await?;
                    if set_reply.starts_with('-') {
                        return Err(EtpError::Io(Error::new(
                            ErrorKind::InvalidData,
                            format!("Dragonfly SET NX error: {set_reply}"),
                        )));
                    }
                    if incr_reply.starts_with('-') {
                        return Err(EtpError::Io(Error::new(
                            ErrorKind::InvalidData,
                            format!("Dragonfly INCRBY error: {incr_reply}"),
                        )));
                    }
                    let total = incr_reply
                        .strip_prefix(':')
                        .and_then(|s| s.parse::<u64>().ok())
                        .ok_or_else(|| {
                            EtpError::Io(Error::new(
                                ErrorKind::InvalidData,
                                format!("Unexpected Dragonfly INCRBY reply: {incr_reply}"),
                            ))
                        })?;
                    out.push(ReconciledClusterTotal {
                        key_hash: d.key_hash,
                        tier: d.tier,
                        pushed_delta: d.delta,
                        window_epoch: d.window_epoch,
                        cluster_total: total,
                    });
                }
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

async fn read_resp_line(stream: &mut velda_etp::connect::EtpStream) -> Result<String, EtpError> {
    let mut buf = Vec::with_capacity(64);
    let mut byte = [0u8; 1];
    loop {
        let n = stream.read(&mut byte).await?;
        if n == 0 {
            return Err(EtpError::Io(Error::new(
                ErrorKind::UnexpectedEof,
                "Dragonfly closed stream while reading RESP2 reply",
            )));
        }
        buf.push(byte[0]);
        if buf.len() >= 2 && buf[buf.len() - 2] == b'\r' && buf[buf.len() - 1] == b'\n' {
            buf.truncate(buf.len() - 2);
            return Ok(String::from_utf8_lossy(&buf).into_owned());
        }
        if buf.len() > 8192 {
            return Err(EtpError::Io(Error::new(
                ErrorKind::InvalidData,
                "Dragonfly RESP2 reply line exceeded 8 KiB",
            )));
        }
    }
}
