//! RESP2 wire codec and lease round-trip (`codec.rs`) for `api_metering` Redis client.
//!
//! ```text
//!   encode_hincrby_batch() ──► [*1 ASKING?] *4 HINCRBY <key> <acc_hash> <delta> ...
//!   send_hincrby_on_lease() ─► write + CRLF parse loop ─► Values / -MOVED / -ASK
//! ```

use std::io::ErrorKind;
use std::time::Duration;

use velda_core::lut::decimal::fast_write_u64_slice;
use velda_etp::EtpError;
use velda_etp::redis::RedisLease;

use crate::wire_baked::BakedWire;

pub(crate) enum BatchReplyOutcome {
    Values(Vec<i64>),
    Moved { slot: u16, addr: String },
    Ask { addr: String },
}

enum ParsedLine {
    Int(i64),
    AskingOk,
    Moved { slot: u16, addr: String },
    Ask { addr: String },
    Err(String),
}

pub(crate) fn encode_hincrby_batch(
    key: &str,
    deltas: &[(u64, i64)],
    with_asking: bool,
    out: &mut Vec<u8>,
) {
    // Pre-bake the invariant per-command RESP2 prefix once per batch:
    // `[ASKING] *4\r\n$7\r\nHINCRBY\r\n$<klen>\r\n<key>\r\n$`
    let mut prefix = Vec::with_capacity(48 + key.len());
    {
        let mut pw = BakedWire::new(&mut prefix);
        if with_asking {
            pw.push_chunk(b"*1\r\n$6\r\nASKING\r\n");
        }
        pw.push_chunk(b"*4\r\n$7\r\nHINCRBY\r\n$");
        pw.push_u64(key.len() as u64);
        pw.push_chunk(b"\r\n");
        pw.push_chunk(key.as_bytes());
        pw.push_chunk(b"\r\n$");
    }

    let mut w = BakedWire::cleared(out, deltas.len() * (prefix.len() + 48));
    let mut field_buf = [0u8; 24];
    let mut delta_buf = Vec::with_capacity(24);
    for &(acc_hash, delta) in deltas {
        let flen = fast_write_u64_slice(&mut field_buf, acc_hash);
        {
            let mut dw = BakedWire::cleared(&mut delta_buf, 24);
            dw.push_i64(delta);
        }
        w.push_chunk(&prefix);
        w.push_u64(flen as u64);
        w.push_chunk(b"\r\n");
        w.push_chunk(&field_buf[..flen]);
        w.push_chunk(b"\r\n$");
        w.push_u64(delta_buf.len() as u64);
        w.push_chunk(b"\r\n");
        w.push_chunk(&delta_buf);
        w.push_chunk(b"\r\n");
    }
}

fn parse_next_resp_line(buf: &mut Vec<u8>) -> Option<Result<ParsedLine, String>> {
    let pos = buf.windows(2).position(|w| w == b"\r\n")?;
    let line_bytes = buf[..pos].to_vec();
    buf.drain(..pos + 2);

    if line_bytes.starts_with(b"+") {
        return Some(Ok(ParsedLine::AskingOk));
    }
    if let Some(num_bytes) = line_bytes.strip_prefix(b":") {
        let num_str = match std::str::from_utf8(num_bytes) {
            Ok(s) => s,
            Err(e) => return Some(Err(format!("Non-UTF8 integer: {e}"))),
        };
        let val = match num_str.parse::<i64>() {
            Ok(v) => v,
            Err(e) => return Some(Err(format!("Invalid integer '{num_str}': {e}"))),
        };
        return Some(Ok(ParsedLine::Int(val)));
    }
    if let Some(err_bytes) = line_bytes.strip_prefix(b"-") {
        let err_str = std::str::from_utf8(err_bytes).unwrap_or("unknown error");
        let mut parts = err_str.split_whitespace();
        if let (Some(kind), Some(slot_s), Some(addr)) = (parts.next(), parts.next(), parts.next())
            && let Ok(slot) = slot_s.parse::<u16>()
        {
            if kind.eq_ignore_ascii_case("MOVED") {
                return Some(Ok(ParsedLine::Moved {
                    slot,
                    addr: addr.to_string(),
                }));
            }
            if kind.eq_ignore_ascii_case("ASK") {
                return Some(Ok(ParsedLine::Ask {
                    addr: addr.to_string(),
                }));
            }
        }
        return Some(Ok(ParsedLine::Err(err_str.to_string())));
    }

    Some(Err(format!(
        "Unexpected RESP line prefix in HINCRBY batch: {:?}",
        line_bytes.first()
    )))
}

pub(crate) async fn send_hincrby_on_lease(
    lease: &mut RedisLease,
    etp_id: &str,
    cmd_buf: &[u8],
    deltas_len: usize,
    with_asking: bool,
    read_buf: &mut Vec<u8>,
) -> Result<BatchReplyOutcome, EtpError> {
    let expected_lines = if with_asking {
        deltas_len.saturating_mul(2)
    } else {
        deltas_len
    };

    for attempt in 0..2 {
        let exchange: Result<Result<BatchReplyOutcome, EtpError>, _> =
            tokio::time::timeout(Duration::from_millis(1000), async {
                lease.write_all(cmd_buf).await?;
                lease.flush().await?;

                let mut values = Vec::with_capacity(deltas_len);
                let mut redirect: Option<BatchReplyOutcome> = None;
                let mut lines_read = 0usize;
                read_buf.clear();
                let mut scratch = [0u8; 4096];

                while lines_read < expected_lines {
                    while let Some(parsed) = parse_next_resp_line(read_buf) {
                        lines_read += 1;
                        match parsed.map_err(|msg| {
                            EtpError::Io(std::io::Error::new(ErrorKind::InvalidData, msg))
                        })? {
                            ParsedLine::AskingOk => {}
                            ParsedLine::Int(v) => values.push(v),
                            ParsedLine::Moved { slot, addr } => {
                                if redirect.is_none() {
                                    redirect = Some(BatchReplyOutcome::Moved { slot, addr });
                                }
                            }
                            ParsedLine::Ask { addr } => {
                                if redirect.is_none() {
                                    redirect = Some(BatchReplyOutcome::Ask { addr });
                                }
                            }
                            ParsedLine::Err(err_msg) => {
                                tracing::warn!(
                                    error = %err_msg,
                                    "Redis returned error for HINCRBY in batch"
                                );
                                values.push(0);
                            }
                        }
                        if lines_read >= expected_lines {
                            break;
                        }
                    }

                    if lines_read >= expected_lines {
                        break;
                    }

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
                }

                if let Some(redir) = redirect {
                    Ok(redir)
                } else {
                    Ok(BatchReplyOutcome::Values(values))
                }
            })
            .await;

        match exchange {
            Ok(Ok(outcome)) => return Ok(outcome),
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
                    id: etp_id.to_string(),
                    timeout_ms: 1000,
                });
            }
        }
    }

    Err(EtpError::Timeout {
        id: etp_id.to_string(),
        timeout_ms: 1000,
    })
}
