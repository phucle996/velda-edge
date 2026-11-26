//! PostgreSQL wire v3 frame encoder & response parser (`wire.rs`) for `openai_token_quota`.
//!
//! ```text
//!   const PG_DDL_WIRE / PG_LEASE_PARSE_SYNC_WIRE  ──► .rodata (compiled const)
//!   append_bind_execute_sync_with_tail() ──► BakedWire SIMD ('B' + key + pre-baked batch/now/E/S tail)
//!   Response loop: 'D' (DataRow scalar i64) | 'E' (Error) .. until 'Z' (ReadyForQuery)
//! ```

use std::io::{Error, ErrorKind};

use velda_etp::EtpError;
use velda_etp::connect::EtpStream;

use crate::wire_baked::{BakedWire, PG_EXECUTE_SYNC_TAIL};

/// Pre-bakes the per-batch constant Bind tail (`$2=batch_s`, `$3=now_s`, result formats, Execute, Sync).
pub(crate) fn build_bind_tail(p2_batch: &[u8], p3_now: &[u8]) -> Vec<u8> {
    let mut tail = Vec::with_capacity(32 + p2_batch.len() + p3_now.len());
    let mut w = BakedWire::new(&mut tail);
    w.push_chunk(&(p2_batch.len() as i32).to_be_bytes());
    w.push_chunk(p2_batch);
    w.push_chunk(&(p3_now.len() as i32).to_be_bytes());
    w.push_chunk(p3_now);
    w.push_chunk(&[0, 0]);
    w.push_chunk(&PG_EXECUTE_SYNC_TAIL);
    tail
}

/// Appends Wire v3 Bind (1 varying `$1` key + pre-baked `$2`/`$3` + Execute + Sync tail).
pub(crate) fn append_bind_execute_sync_with_tail(
    buf: &mut Vec<u8>,
    p1_key: &[u8],
    bind_tail: &[u8],
) {
    // `bind_tail` contains `[4B p2_len + p2 + 4B p3_len + p3 + 2B res_fmt] + 15B PG_EXECUTE_SYNC_TAIL`
    let tail_bind_bytes = bind_tail.len() - PG_EXECUTE_SYNC_TAIL.len();
    let bind_len = (4 + 1 + 1 + 2 + 2 + (4 + p1_key.len()) + tail_bind_bytes) as u32;
    let mut w = BakedWire::new(buf);
    w.push_byte(b'B');
    w.push_chunk(&bind_len.to_be_bytes());
    w.push_chunk(&[0, 0, 0, 0, 0, 3]);
    w.push_chunk(&(p1_key.len() as i32).to_be_bytes());
    w.push_chunk(p1_key);
    w.push_chunk(bind_tail);
}

pub(crate) async fn read_until_ready(stream: &mut EtpStream) -> Result<Option<i64>, EtpError> {
    let mut scalar: Option<i64> = None;
    let mut pg_err: Option<String> = None;

    loop {
        let mut hdr = [0u8; 5];
        stream.read_exact(&mut hdr).await?;
        let msg_type = hdr[0];
        let len = u32::from_be_bytes([hdr[1], hdr[2], hdr[3], hdr[4]]);
        if !(4..=16 * 1024 * 1024).contains(&len) {
            return Err(EtpError::Io(Error::new(
                ErrorKind::InvalidData,
                format!("Postgres invalid response length {len}"),
            )));
        }
        let body_len = (len - 4) as usize;
        let mut body = vec![0u8; body_len];
        if body_len > 0 {
            stream.read_exact(&mut body).await?;
        }

        match msg_type {
            b'D' if scalar.is_none() && body.len() >= 6 => {
                let col_len = i32::from_be_bytes([body[2], body[3], body[4], body[5]]);
                if col_len > 0 && body.len() >= 6 + col_len as usize {
                    let text = std::str::from_utf8(&body[6..6 + col_len as usize]).unwrap_or("");
                    scalar = text.trim().parse::<i64>().ok();
                }
            }
            b'E' => {
                pg_err = Some(String::from_utf8_lossy(&body).into_owned());
            }
            b'Z' => break,
            _ => {}
        }
    }

    if let Some(err) = pg_err {
        return Err(EtpError::Io(Error::other(format!(
            "Postgres wire query error: {err}"
        ))));
    }
    Ok(scalar)
}
