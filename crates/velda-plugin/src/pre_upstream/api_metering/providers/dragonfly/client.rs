//! DragonflyDB single-endpoint RESP2 pipelined `HINCRBY` client (`client.rs`).
//!
//! ```text
//!   flush_deltas(&etp, key, deltas)
//!     └──► etp.acquire() ──► 1 RTT pipelined HINCRBY batch ──► Vec<i64> consumed totals
//! ```

use std::io::ErrorKind;
use std::sync::Arc;
use std::time::Duration;

use velda_core::lut::decimal::fast_write_u64_slice;
use velda_etp::EtpError;
use velda_etp::dragonfly::DragonflyEtp;

use crate::wire_baked::BakedWire;

/// Pushes account credit deltas to DragonflyDB via pipelined `HINCRBY` without Lua scripts.
pub async fn flush_deltas(
    etp: &Arc<DragonflyEtp>,
    key: &str,
    deltas: &[(u64, i64)],
    cmd_buf: &mut Vec<u8>,
    read_buf: &mut Vec<u8>,
) -> Result<Vec<i64>, EtpError> {
    let mut prefix = Vec::with_capacity(40 + key.len());
    {
        let mut pw = BakedWire::new(&mut prefix);
        pw.push_chunk(b"*4\r\n$7\r\nHINCRBY\r\n$");
        pw.push_u64(key.len() as u64);
        pw.push_chunk(b"\r\n");
        pw.push_chunk(key.as_bytes());
        pw.push_chunk(b"\r\n$");
    }

    {
        let mut w = BakedWire::cleared(cmd_buf, deltas.len() * (prefix.len() + 48));
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

    let mut lease = etp.acquire().await?;
    for attempt in 0..2 {
        let exchange: Result<Result<Vec<i64>, EtpError>, _> =
            tokio::time::timeout(Duration::from_millis(1000), async {
                lease.write_all(cmd_buf).await?;
                lease.flush().await?;

                let mut values = Vec::with_capacity(deltas.len());
                read_buf.clear();
                let mut scratch = [0u8; 4096];

                while values.len() < deltas.len() {
                    while let Some(pos) = read_buf.windows(2).position(|w| w == b"\r\n") {
                        let line = read_buf[..pos].to_vec();
                        read_buf.drain(..pos + 2);
                        if let Some(num_bytes) = line.strip_prefix(b":") {
                            let s = std::str::from_utf8(num_bytes).map_err(|e| {
                                EtpError::Io(std::io::Error::new(
                                    ErrorKind::InvalidData,
                                    format!("Non-UTF8 integer: {e}"),
                                ))
                            })?;
                            let v = s.parse::<i64>().map_err(|e| {
                                EtpError::Io(std::io::Error::new(
                                    ErrorKind::InvalidData,
                                    format!("Invalid integer '{s}': {e}"),
                                ))
                            })?;
                            values.push(v);
                        } else if let Some(err_bytes) = line.strip_prefix(b"-") {
                            let err_str = std::str::from_utf8(err_bytes).unwrap_or("unknown error");
                            tracing::warn!(error = %err_str, "DragonflyDB HINCRBY error in batch");
                            values.push(0);
                        } else {
                            return Err(EtpError::Io(std::io::Error::new(
                                ErrorKind::InvalidData,
                                format!("Unexpected Dragonfly RESP prefix: {:?}", line.first()),
                            )));
                        }
                        if values.len() >= deltas.len() {
                            break;
                        }
                    }

                    if values.len() >= deltas.len() {
                        break;
                    }
                    let n = lease.read(&mut scratch).await?;
                    if n == 0 {
                        return Err(EtpError::Io(std::io::Error::new(
                            ErrorKind::UnexpectedEof,
                            "DragonflyDB closed connection before batch completed",
                        )));
                    }
                    read_buf.extend_from_slice(&scratch[..n]);
                    if read_buf.len() > 256 * 1024 {
                        return Err(EtpError::Io(std::io::Error::new(
                            ErrorKind::InvalidData,
                            "DragonflyDB RESP2 response exceeded 256 KiB",
                        )));
                    }
                }
                Ok(values)
            })
            .await;

        match exchange {
            Ok(Ok(vals)) => return Ok(vals),
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
