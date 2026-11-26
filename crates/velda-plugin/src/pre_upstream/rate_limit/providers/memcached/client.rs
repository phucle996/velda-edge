//! Memcached ASCII protocol `incr` / `add` SoT upsert (`client.rs`).
//!
//! ```text
//!   DirtyKeyDelta -> BakedWire ASCII command assembly (0 per-key String allocations)
//!     ├─► "incr {key} {delta}\r\n" -> total
//!     └─► "NOT_FOUND" -> "add {key} 0 15 {len}\r\n{delta}\r\n"
//! ```

use std::io::{Error, ErrorKind};
use std::sync::Arc;
use std::time::Duration;

use velda_core::lut::decimal::fast_write_u64_slice;
use velda_etp::EtpError;
use velda_etp::memcached::MemcachedEtp;

use crate::pre_upstream::rate_limit::arena::{DirtyKeyDelta, ReconciledClusterTotal};
use crate::wire_baked::BakedWire;

/// Increments cluster rate-limit counters stored in Memcached and returns updated totals.
pub async fn push_and_read_totals(
    etp: &Arc<MemcachedEtp>,
    key_prefix: &str,
    deltas: &[DirtyKeyDelta],
    io_timeout: Duration,
) -> Result<Vec<ReconciledClusterTotal>, EtpError> {
    if deltas.is_empty() {
        return Ok(Vec::new());
    }

    let safe_prefix: Vec<u8> = key_prefix
        .bytes()
        .map(|b| {
            if b.is_ascii_alphanumeric() || b == b'_' || b == b'-' {
                b
            } else {
                b'_'
            }
        })
        .collect();
    let mut lease = etp.acquire().await?;
    let mut key_buf = Vec::with_capacity(safe_prefix.len() + 40);
    let mut incr_buf = Vec::with_capacity(safe_prefix.len() + 72);
    let mut add_buf = Vec::with_capacity(safe_prefix.len() + 96);
    let mut d_buf = [0u8; 24];

    let mut last_err = EtpError::Timeout {
        id: etp.config.id.clone(),
        timeout_ms: u64::try_from(io_timeout.as_millis()).unwrap_or(u64::MAX),
    };

    for attempt in 0..2 {
        let res: Result<Result<Vec<ReconciledClusterTotal>, EtpError>, _> =
            tokio::time::timeout(io_timeout, async {
                let mut out = Vec::with_capacity(deltas.len());
                for d in deltas {
                    {
                        let mut kw = BakedWire::cleared(&mut key_buf, safe_prefix.len() + 40);
                        kw.push_chunk(&safe_prefix);
                        kw.push_u64(u64::from(d.tier));
                        kw.push_byte(b'_');
                        kw.push_hex16(d.key_hash);
                        kw.push_byte(b'_');
                        kw.push_u64(d.window_epoch);
                    }
                    {
                        let mut iw = BakedWire::cleared(&mut incr_buf, key_buf.len() + 32);
                        iw.push_chunk(b"incr ");
                        iw.push_chunk(&key_buf);
                        iw.push_byte(b' ');
                        iw.push_u64(d.delta);
                        iw.push_chunk(b"\r\n");
                    }
                    lease.write_all(&incr_buf).await?;
                    lease.flush().await?;

                    let line = read_ascii_line(&mut lease).await?;
                    let total = if line == "NOT_FOUND" {
                        let dlen = fast_write_u64_slice(&mut d_buf, d.delta);
                        {
                            let mut aw = BakedWire::cleared(&mut add_buf, key_buf.len() + 48);
                            aw.push_chunk(b"add ");
                            aw.push_chunk(&key_buf);
                            aw.push_chunk(b" 0 15 ");
                            aw.push_u64(dlen as u64);
                            aw.push_chunk(b"\r\n");
                            aw.push_chunk(&d_buf[..dlen]);
                            aw.push_chunk(b"\r\n");
                        }
                        lease.write_all(&add_buf).await?;
                        lease.flush().await?;
                        let add_reply = read_ascii_line(&mut lease).await?;
                        if add_reply == "STORED" {
                            d.delta
                        } else if add_reply == "NOT_STORED" {
                            lease.write_all(&incr_buf).await?;
                            lease.flush().await?;
                            read_ascii_line(&mut lease)
                                .await?
                                .trim()
                                .parse::<u64>()
                                .map_err(|_| {
                                    EtpError::Io(Error::new(
                                        ErrorKind::InvalidData,
                                        "Invalid Memcached incr reply after add race",
                                    ))
                                })?
                        } else {
                            return Err(EtpError::Io(Error::new(
                                ErrorKind::InvalidData,
                                format!("Memcached add failed: {add_reply}"),
                            )));
                        }
                    } else {
                        line.trim().parse::<u64>().map_err(|_| {
                            EtpError::Io(Error::new(
                                ErrorKind::InvalidData,
                                format!("Unexpected Memcached incr reply: {line}"),
                            ))
                        })?
                    };

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

async fn read_ascii_line(stream: &mut velda_etp::connect::EtpStream) -> Result<String, EtpError> {
    let mut buf = Vec::with_capacity(64);
    let mut b = [0u8; 1];
    loop {
        let n = stream.read(&mut b).await?;
        if n == 0 {
            return Err(EtpError::Io(Error::new(
                ErrorKind::UnexpectedEof,
                "Memcached closed connection unexpectedly",
            )));
        }
        buf.push(b[0]);
        if buf.len() >= 2 && buf[buf.len() - 2] == b'\r' && buf[buf.len() - 1] == b'\n' {
            buf.truncate(buf.len() - 2);
            return Ok(String::from_utf8_lossy(&buf).into_owned());
        }
        if buf.len() > 4096 {
            return Err(EtpError::Io(Error::new(
                ErrorKind::InvalidData,
                "Memcached response line exceeded 4 KiB",
            )));
        }
    }
}
