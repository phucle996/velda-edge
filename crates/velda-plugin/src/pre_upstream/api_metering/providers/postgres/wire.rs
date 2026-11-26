//! PostgreSQL Wire v3 framing codec (`wire.rs`) for `api_metering`.
//!
//! ```text
//!   const PG_DDL_WIRE / PG_UPSERT_PARSE_SYNC_WIRE  ──► .rodata (compiled const)
//!   append_bind_execute_sync_wire(&mut buf, k, d, ts)
//!     └──► BakedWire SIMD chunks ('B' header + 3 text params + PG_EXECUTE_SYNC_TAIL)
//!   read_until_ready(&mut stream) ──► 'D' col 0 i64 | 'E' err | 'Z' ready
//! ```

use std::io::{Error, ErrorKind};

use velda_etp::EtpError;
use velda_etp::connect::EtpStream;

use crate::wire_baked::{BakedWire, PG_EXECUTE_SYNC_TAIL};

/// Appends Wire v3 Bind (3 text parameters) + Execute + Sync using SIMD `BakedWire`
/// and the compile-time constant `PG_EXECUTE_SYNC_TAIL`.
pub(crate) fn append_bind_execute_sync_wire(
    buf: &mut Vec<u8>,
    p1_key: &[u8],
    p2_delta: &[u8],
    p3_now: &[u8],
) {
    let mut w = BakedWire::new(buf);
    let bind_start = w.len();
    w.push_byte(b'B');
    let len_slot = w.alloc_slot(4);
    // unnamed portal + unnamed stmt + 0 param formats (all text) + 3 param values
    w.push_chunk(&[0, 0, 0, 0, 0, 3]);
    w.push_chunk(&(p1_key.len() as i32).to_be_bytes());
    w.push_chunk(p1_key);
    w.push_chunk(&(p2_delta.len() as i32).to_be_bytes());
    w.push_chunk(p2_delta);
    w.push_chunk(&(p3_now.len() as i32).to_be_bytes());
    w.push_chunk(p3_now);
    // 0 result format codes (text)
    w.push_chunk(&[0, 0]);
    let bind_len = (w.len() - bind_start - 1) as u32;
    w.patch_bytes_at(len_slot, &bind_len.to_be_bytes());
    w.push_chunk(&PG_EXECUTE_SYNC_TAIL);
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
            "Postgres SQL error: {err}"
        ))));
    }
    Ok(scalar)
}
