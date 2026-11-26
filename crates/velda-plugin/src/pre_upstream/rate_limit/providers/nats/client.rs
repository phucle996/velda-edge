//! NATS JetStream Key-Value read-modify-publish SoT client (`client.rs`).
//!
//! ```text
//!   DirtyKeyDelta -> BakedWire NATS SUB/PUB command assembly (0 per-key String allocations)
//!     1. SUB _INBOX.vrl.<seq> vrl<seq>\r\nPUB $JS.API.DIRECT.GET.KV_<bkt>.$KV.<bkt>.<key> ...
//!     2. PUB $KV.<bkt>.<key> _INBOX.vrl.<seq> <len>\r\n<new_cnt>\r\nUNSUB vrl<seq>\r\n
//! ```

use std::io::{Error, ErrorKind};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use velda_core::lut::decimal::fast_write_u64_slice;
use velda_etp::EtpError;
use velda_etp::nats::NatsEtp;

use crate::pre_upstream::rate_limit::arena::{DirtyKeyDelta, ReconciledClusterTotal};
use crate::wire_baked::BakedWire;

static INBOX_SEQ: AtomicU64 = AtomicU64::new(1);

/// Reads current JetStream KV counter value (if present) and publishes incremented total.
pub async fn push_and_read_totals(
    etp: &Arc<NatsEtp>,
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
            if b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-') {
                b
            } else {
                b'_'
            }
        })
        .collect();
    let bucket_b = etp.config.bucket.as_bytes();
    let mut lease = etp.acquire().await?;
    let mut key_buf = Vec::with_capacity(safe_prefix.len() + 40);
    let mut wire_buf = Vec::with_capacity(256);
    let mut payload_buf = [0u8; 24];

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

                    let seq = INBOX_SEQ.fetch_add(1, Ordering::Relaxed);
                    {
                        let mut w = BakedWire::cleared(&mut wire_buf, 256);
                        w.push_chunk(b"SUB _INBOX.vrl.");
                        w.push_u64(seq);
                        w.push_chunk(b" vrl");
                        w.push_u64(seq);
                        w.push_chunk(b"\r\nPUB $JS.API.DIRECT.GET.KV_");
                        w.push_chunk(bucket_b);
                        w.push_chunk(b".$KV.");
                        w.push_chunk(bucket_b);
                        w.push_byte(b'.');
                        w.push_chunk(&key_buf);
                        w.push_chunk(b" _INBOX.vrl.");
                        w.push_u64(seq);
                        w.push_chunk(b" 0\r\n\r\n");
                    }
                    lease.write_all(&wire_buf).await?;
                    lease.flush().await?;

                    let existing = read_nats_reply_u64(&mut lease).await?;
                    let next_total = existing.saturating_add(d.delta);
                    let plen = fast_write_u64_slice(&mut payload_buf, next_total);

                    {
                        let mut w = BakedWire::cleared(&mut wire_buf, 192);
                        w.push_chunk(b"PUB $KV.");
                        w.push_chunk(bucket_b);
                        w.push_byte(b'.');
                        w.push_chunk(&key_buf);
                        w.push_chunk(b" _INBOX.vrl.");
                        w.push_u64(seq);
                        w.push_byte(b' ');
                        w.push_u64(plen as u64);
                        w.push_chunk(b"\r\n");
                        w.push_chunk(&payload_buf[..plen]);
                        w.push_chunk(b"\r\nUNSUB vrl");
                        w.push_u64(seq);
                        w.push_chunk(b"\r\n");
                    }
                    lease.write_all(&wire_buf).await?;
                    lease.flush().await?;
                    let _ = read_nats_reply_u64(&mut lease).await?;

                    out.push(ReconciledClusterTotal {
                        key_hash: d.key_hash,
                        tier: d.tier,
                        pushed_delta: d.delta,
                        window_epoch: d.window_epoch,
                        cluster_total: next_total,
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

async fn read_nats_reply_u64(stream: &mut velda_etp::connect::EtpStream) -> Result<u64, EtpError> {
    loop {
        let line = read_crlf_line(stream).await?;
        if line == "PING" {
            stream.write_all(b"PONG\r\n").await?;
            stream.flush().await?;
            continue;
        }
        if line == "+OK" {
            continue;
        }
        if let Some(err) = line.strip_prefix("-ERR") {
            return Err(EtpError::Io(Error::other(format!(
                "NATS error reply: {}",
                err.trim()
            ))));
        }
        if let Some(rest) = line.strip_prefix("MSG ") {
            let nbytes: usize = rest
                .split_whitespace()
                .last()
                .and_then(|s| s.parse::<usize>().ok())
                .unwrap_or(0);
            let mut body = vec![0u8; nbytes + 2];
            if !body.is_empty() {
                stream.read_exact(&mut body).await?;
            }
            let raw = std::str::from_utf8(&body[..nbytes]).unwrap_or("").trim();
            if let Ok(n) = raw.parse::<u64>() {
                return Ok(n);
            }
            return Ok(0);
        }
        if let Some(rest) = line.strip_prefix("HMSG ") {
            let parts: Vec<&str> = rest.split_whitespace().collect();
            let total_len: usize = parts
                .last()
                .and_then(|s| s.parse::<usize>().ok())
                .unwrap_or(0);
            let hdr_len: usize = parts
                .get(parts.len().saturating_sub(2))
                .and_then(|s| s.parse::<usize>().ok())
                .unwrap_or(0);
            let mut body = vec![0u8; total_len + 2];
            if !body.is_empty() {
                stream.read_exact(&mut body).await?;
            }
            let payload_slice = &body[hdr_len.min(total_len)..total_len];
            let raw = std::str::from_utf8(payload_slice).unwrap_or("").trim();
            return Ok(raw.parse::<u64>().unwrap_or(0));
        }
    }
}

async fn read_crlf_line(stream: &mut velda_etp::connect::EtpStream) -> Result<String, EtpError> {
    let mut buf = Vec::with_capacity(128);
    let mut byte = [0u8; 1];
    loop {
        let n = stream.read(&mut byte).await?;
        if n == 0 {
            return Err(EtpError::Io(Error::new(
                ErrorKind::UnexpectedEof,
                "NATS closed connection while reading reply",
            )));
        }
        buf.push(byte[0]);
        if buf.len() >= 2 && buf[buf.len() - 2] == b'\r' && buf[buf.len() - 1] == b'\n' {
            buf.truncate(buf.len() - 2);
            return Ok(String::from_utf8_lossy(&buf).into_owned());
        }
        if buf.len() > 64 * 1024 {
            return Err(EtpError::Io(Error::new(
                ErrorKind::InvalidData,
                "NATS reply header line exceeded 64 KiB",
            )));
        }
    }
}
