//! Memcached ASCII protocol pipelined `incr` / `add` SoT upsert (`client.rs`).
//!
//! ```text
//!   Pipeline Stage 1: N × `incr <key> <delta>\r\n` -> 1 write_all + N buffered reply reads
//!   Pipeline Stage 2: missing (`NOT_FOUND`) -> M × `add <key> 0 15 <len>\r\n<delta>\r\n`
//!   Pipeline Stage 3: add races (`NOT_STORED`) -> R × `incr <key> <delta>\r\n`
//! ```

use std::io::{Error, ErrorKind};
use std::sync::Arc;
use std::time::Duration;

use velda_core::lut::decimal::fast_write_u64_slice;
use velda_etp::EtpError;
use velda_etp::connect::EtpStream;
use velda_etp::memcached::MemcachedEtp;

use crate::pre_upstream::rate_limit::arena::{DirtyKeyDelta, ReconciledClusterTotal};
use crate::wire_baked::BakedWire;

struct MemcachedReader {
    buf: Vec<u8>,
}

impl MemcachedReader {
    fn new() -> Self {
        Self {
            buf: Vec::with_capacity(4096),
        }
    }

    fn clear(&mut self) {
        self.buf.clear();
    }

    async fn read_line(&mut self, stream: &mut EtpStream) -> Result<String, EtpError> {
        loop {
            if let Some(pos) = self.buf.windows(2).position(|w| w == b"\r\n") {
                let line = String::from_utf8_lossy(&self.buf[..pos]).into_owned();
                self.buf.drain(..pos + 2);
                return Ok(line);
            }
            if self.buf.len() > 64 * 1024 {
                return Err(EtpError::Io(Error::new(
                    ErrorKind::InvalidData,
                    "Memcached response buffer exceeded 64 KiB",
                )));
            }
            let mut tmp = [0u8; 4096];
            let n = stream.read(&mut tmp).await?;
            if n == 0 {
                return Err(EtpError::Io(Error::new(
                    ErrorKind::UnexpectedEof,
                    "Memcached closed connection unexpectedly",
                )));
            }
            self.buf.extend_from_slice(&tmp[..n]);
        }
    }
}

/// Increments cluster rate-limit counters stored in Memcached via pipelined ASCII commands.
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
    let key_stride = safe_prefix.len() + 40;
    let mut key_arena = Vec::with_capacity(deltas.len() * key_stride);
    let mut key_spans: Vec<(usize, usize)> = Vec::with_capacity(deltas.len());
    for d in deltas {
        let start = key_arena.len();
        let mut kw = BakedWire::new(&mut key_arena);
        kw.push_chunk(&safe_prefix);
        kw.push_u64(u64::from(d.tier));
        kw.push_byte(b'_');
        kw.push_hex16(d.key_hash);
        kw.push_byte(b'_');
        kw.push_u64(d.window_epoch);
        key_spans.push((start, key_arena.len()));
    }

    let mut lease = etp.acquire().await?;
    let mut reader = MemcachedReader::new();
    let mut wire_buf = Vec::with_capacity(deltas.len() * (key_stride + 32));
    let mut d_buf = [0u8; 24];
    let mut totals = vec![0u64; deltas.len()];
    let mut incr_sent = false;

    let mut last_err = EtpError::Timeout {
        id: etp.config.id.clone(),
        timeout_ms: u64::try_from(io_timeout.as_millis()).unwrap_or(u64::MAX),
    };

    for attempt in 0..2 {
        let res: Result<Result<Vec<ReconciledClusterTotal>, EtpError>, _> =
            tokio::time::timeout(io_timeout, async {
                let mut missing: Vec<usize> = Vec::new();
                if !incr_sent {
                    wire_buf.clear();
                    {
                        let mut w = BakedWire::new(&mut wire_buf);
                        for (idx, d) in deltas.iter().enumerate() {
                            let (s, e) = key_spans[idx];
                            w.push_chunk(b"incr ");
                            w.push_chunk(&key_arena[s..e]);
                            w.push_byte(b' ');
                            w.push_u64(d.delta);
                            w.push_chunk(b"\r\n");
                        }
                    }
                    lease.write_all(&wire_buf).await?;
                    lease.flush().await?;

                    for (idx, total_slot) in totals.iter_mut().enumerate() {
                        let line = reader.read_line(&mut lease).await?;
                        if line == "NOT_FOUND" {
                            missing.push(idx);
                        } else {
                            let val = line.trim().parse::<u64>().map_err(|_| {
                                EtpError::Io(Error::new(
                                    ErrorKind::InvalidData,
                                    format!("Unexpected Memcached incr reply: {line}"),
                                ))
                            })?;
                            *total_slot = val;
                        }
                    }
                    incr_sent = true;
                } else {
                    for (idx, &t) in totals.iter().enumerate() {
                        if t == 0 {
                            missing.push(idx);
                        }
                    }
                }

                if !missing.is_empty() {
                    wire_buf.clear();
                    {
                        let mut w = BakedWire::new(&mut wire_buf);
                        for &idx in &missing {
                            let (s, e) = key_spans[idx];
                            let dlen = fast_write_u64_slice(&mut d_buf, deltas[idx].delta);
                            w.push_chunk(b"add ");
                            w.push_chunk(&key_arena[s..e]);
                            w.push_chunk(b" 0 15 ");
                            w.push_u64(dlen as u64);
                            w.push_chunk(b"\r\n");
                            w.push_chunk(&d_buf[..dlen]);
                            w.push_chunk(b"\r\n");
                        }
                    }
                    lease.write_all(&wire_buf).await?;
                    lease.flush().await?;

                    let mut race_indices: Vec<usize> = Vec::new();
                    for &idx in &missing {
                        let add_reply = reader.read_line(&mut lease).await?;
                        if add_reply == "STORED" {
                            totals[idx] = deltas[idx].delta;
                        } else if add_reply == "NOT_STORED" {
                            race_indices.push(idx);
                        } else {
                            return Err(EtpError::Io(Error::new(
                                ErrorKind::InvalidData,
                                format!("Memcached add failed: {add_reply}"),
                            )));
                        }
                    }

                    if !race_indices.is_empty() {
                        wire_buf.clear();
                        {
                            let mut w = BakedWire::new(&mut wire_buf);
                            for &idx in &race_indices {
                                let (s, e) = key_spans[idx];
                                w.push_chunk(b"incr ");
                                w.push_chunk(&key_arena[s..e]);
                                w.push_byte(b' ');
                                w.push_u64(deltas[idx].delta);
                                w.push_chunk(b"\r\n");
                            }
                        }
                        lease.write_all(&wire_buf).await?;
                        lease.flush().await?;

                        for &idx in &race_indices {
                            let line = reader.read_line(&mut lease).await?;
                            let val = line.trim().parse::<u64>().map_err(|_| {
                                EtpError::Io(Error::new(
                                    ErrorKind::InvalidData,
                                    "Invalid Memcached incr reply after add race",
                                ))
                            })?;
                            totals[idx] = val;
                        }
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
            Ok(Ok(out)) => return Ok(out),
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
