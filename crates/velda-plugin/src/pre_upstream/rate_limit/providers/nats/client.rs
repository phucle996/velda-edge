//! NATS JetStream Key-Value CAS read-modify-publish (`DIRECT.GET` + `HPUB`) SoT client (`client.rs`).
//!
//! ```text
//!   DirtyKeyDelta -> BakedWire NATS SUB / DIRECT.GET / HPUB CAS assembly
//!     1. SUB _INBOX.vrl.<seq> vrl<seq>\r\nPUB $JS.API.DIRECT.GET.KV_<bkt>.$KV.<bkt>.<key> ...
//!     2. Parse HMSG (existing u64, Nats-Sequence) -> HPUB with Nats-Expected-Last-Subject-Sequence
//!     3. Retry CAS on err_code 10071 -> consume PubAck -> UNSUB vrl<seq>\r\n
//! ```

use std::io::{Error, ErrorKind};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use velda_core::lut::decimal::fast_write_u64_slice;
use velda_etp::EtpError;
use velda_etp::connect::EtpStream;
use velda_etp::nats::NatsEtp;

use crate::pre_upstream::rate_limit::arena::{DirtyKeyDelta, ReconciledClusterTotal};
use crate::wire_baked::BakedWire;

static INBOX_SEQ: AtomicU64 = AtomicU64::new(1);

enum NatsReply {
    Value {
        existing: u64,
        last_seq: u64,
    },
    PubAck {
        cas_conflict: bool,
        err_msg: Option<String>,
    },
}

struct NatsReader {
    buf: Vec<u8>,
}

impl NatsReader {
    fn new() -> Self {
        Self {
            buf: Vec::with_capacity(4096),
        }
    }

    fn clear(&mut self) {
        self.buf.clear();
    }

    async fn ensure_crlf_line(&mut self, stream: &mut EtpStream) -> Result<String, EtpError> {
        loop {
            if let Some(pos) = self.buf.windows(2).position(|w| w == b"\r\n") {
                let line = String::from_utf8_lossy(&self.buf[..pos]).into_owned();
                self.buf.drain(..pos + 2);
                return Ok(line);
            }
            if self.buf.len() > 64 * 1024 {
                return Err(EtpError::Io(Error::new(
                    ErrorKind::InvalidData,
                    "NATS reply header line exceeded 64 KiB",
                )));
            }
            let mut tmp = [0u8; 4096];
            let n = stream.read(&mut tmp).await?;
            if n == 0 {
                return Err(EtpError::Io(Error::new(
                    ErrorKind::UnexpectedEof,
                    "NATS closed connection while reading reply",
                )));
            }
            self.buf.extend_from_slice(&tmp[..n]);
        }
    }

    async fn read_exact_plus_crlf(
        &mut self,
        stream: &mut EtpStream,
        total_len: usize,
    ) -> Result<Vec<u8>, EtpError> {
        let need = total_len + 2;
        while self.buf.len() < need {
            let mut tmp = [0u8; 4096];
            let n = stream.read(&mut tmp).await?;
            if n == 0 {
                return Err(EtpError::Io(Error::new(
                    ErrorKind::UnexpectedEof,
                    "NATS closed connection while reading frame payload",
                )));
            }
            self.buf.extend_from_slice(&tmp[..n]);
        }
        let payload = self.buf[..total_len].to_vec();
        self.buf.drain(..need);
        Ok(payload)
    }

    async fn read_reply(
        &mut self,
        stream: &mut EtpStream,
        expect_puback: bool,
    ) -> Result<NatsReply, EtpError> {
        loop {
            let line = self.ensure_crlf_line(stream).await?;
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
                let body = self.read_exact_plus_crlf(stream, nbytes).await?;
                let raw = std::str::from_utf8(&body).unwrap_or("").trim();
                if expect_puback {
                    let cas_conflict =
                        raw.contains("\"err_code\":10071") || raw.contains("wrong last sequence");
                    let err_msg = if !cas_conflict && raw.contains("\"error\":") {
                        Some(raw.to_string())
                    } else {
                        None
                    };
                    return Ok(NatsReply::PubAck {
                        cas_conflict,
                        err_msg,
                    });
                }
                return Ok(NatsReply::Value {
                    existing: raw.parse::<u64>().unwrap_or(0),
                    last_seq: 0,
                });
            }
            if let Some(rest) = line.strip_prefix("HMSG ") {
                let mut parts = rest.split_whitespace();
                let mut nums = [0usize; 2];
                let mut count = 0usize;
                for p in parts.by_ref() {
                    if let Ok(v) = p.parse::<usize>() {
                        nums[0] = nums[1];
                        nums[1] = v;
                        count += 1;
                    }
                }
                let (hdr_len, total_len) = if count >= 2 {
                    (nums[0], nums[1])
                } else {
                    (0, nums[1])
                };
                let body = self.read_exact_plus_crlf(stream, total_len).await?;
                let hdr_slice = &body[..hdr_len.min(total_len)];
                let hdr_str = std::str::from_utf8(hdr_slice).unwrap_or("");
                if hdr_str.starts_with("NATS/1.0 404") {
                    return Ok(NatsReply::Value {
                        existing: 0,
                        last_seq: 0,
                    });
                }
                let mut last_seq = 0u64;
                for hline in hdr_str.split("\r\n") {
                    if let Some((k, v)) = hline.split_once(':')
                        && k.trim().eq_ignore_ascii_case("Nats-Sequence")
                        && let Ok(seq) = v.trim().parse::<u64>()
                    {
                        last_seq = seq;
                    }
                }
                let payload_slice = &body[hdr_len.min(total_len)..total_len];
                let existing = std::str::from_utf8(payload_slice)
                    .unwrap_or("")
                    .trim()
                    .parse::<u64>()
                    .unwrap_or(0);
                return Ok(NatsReply::Value { existing, last_seq });
            }
        }
    }
}

/// Reads current JetStream KV counter value & sequence, then updates with CAS `HPUB`.
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
    let mut wire_buf = Vec::with_capacity(384);
    let mut hdr_buf = Vec::with_capacity(96);
    let mut payload_buf = [0u8; 24];
    let mut reader = NatsReader::new();
    let mut out = Vec::with_capacity(deltas.len());

    let mut last_err = EtpError::Timeout {
        id: etp.config.id.clone(),
        timeout_ms: u64::try_from(io_timeout.as_millis()).unwrap_or(u64::MAX),
    };

    for attempt in 0..2 {
        let pending = &deltas[out.len()..];
        let res: Result<Result<(), EtpError>, _> = tokio::time::timeout(io_timeout, async {
            for d in pending {
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
                    let mut sw = BakedWire::cleared(&mut wire_buf, 64);
                    sw.push_chunk(b"SUB _INBOX.vrl.");
                    sw.push_u64(seq);
                    sw.push_chunk(b" vrl");
                    sw.push_u64(seq);
                    sw.push_chunk(b"\r\n");
                }
                lease.write_all(&wire_buf).await?;

                let mut final_total = None;
                for _cas_round in 0..4 {
                    {
                        let mut w = BakedWire::cleared(&mut wire_buf, 256);
                        w.push_chunk(b"PUB $JS.API.DIRECT.GET.KV_");
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

                    let (existing, last_seq) = match reader.read_reply(&mut lease, false).await? {
                        NatsReply::Value { existing, last_seq } => (existing, last_seq),
                        NatsReply::PubAck { .. } => (0, 0),
                    };
                    let next_total = existing.saturating_add(d.delta);
                    let plen = fast_write_u64_slice(&mut payload_buf, next_total);

                    {
                        let mut hw = BakedWire::cleared(&mut hdr_buf, 96);
                        hw.push_chunk(b"NATS/1.0\r\nNats-Expected-Last-Subject-Sequence: ");
                        hw.push_u64(last_seq);
                        hw.push_chunk(b"\r\n\r\n");
                    }
                    {
                        let mut w = BakedWire::cleared(&mut wire_buf, 384);
                        w.push_chunk(b"HPUB $KV.");
                        w.push_chunk(bucket_b);
                        w.push_byte(b'.');
                        w.push_chunk(&key_buf);
                        w.push_chunk(b" _INBOX.vrl.");
                        w.push_u64(seq);
                        w.push_byte(b' ');
                        w.push_u64(hdr_buf.len() as u64);
                        w.push_byte(b' ');
                        w.push_u64((hdr_buf.len() + plen) as u64);
                        w.push_chunk(b"\r\n");
                        w.push_chunk(&hdr_buf);
                        w.push_chunk(&payload_buf[..plen]);
                        w.push_chunk(b"\r\n");
                    }
                    lease.write_all(&wire_buf).await?;
                    lease.flush().await?;

                    match reader.read_reply(&mut lease, true).await? {
                        NatsReply::PubAck {
                            cas_conflict: true, ..
                        } => continue,
                        NatsReply::PubAck {
                            err_msg: Some(msg), ..
                        } => {
                            return Err(EtpError::Io(Error::other(format!(
                                "NATS JetStream KV PubAck error: {msg}"
                            ))));
                        }
                        _ => {
                            final_total = Some(next_total);
                            break;
                        }
                    }
                }

                // Unsubscribe only after consuming JetStream PubAck to avoid racing the reply.
                {
                    let mut u = BakedWire::cleared(&mut wire_buf, 32);
                    u.push_chunk(b"UNSUB vrl");
                    u.push_u64(seq);
                    u.push_chunk(b"\r\n");
                }
                lease.write_all(&wire_buf).await?;
                lease.flush().await?;

                let Some(next_total) = final_total else {
                    return Err(EtpError::Io(Error::other(
                        "NATS JetStream KV CAS conflict persisted after retries",
                    )));
                };

                out.push(ReconciledClusterTotal {
                    key_hash: d.key_hash,
                    tier: d.tier,
                    pushed_delta: d.delta,
                    window_epoch: d.window_epoch,
                    cluster_total: next_total,
                });
            }
            Ok(())
        })
        .await;

        match res {
            Ok(Ok(())) => return Ok(out),
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
