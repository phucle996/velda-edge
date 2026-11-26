//! PostgreSQL wire v3 frame encoder & response parser (`wire.rs`) for `rate_limit`.
//!
//! ```text
//!   Simple Query:   const PG_DDL_WIRE (.rodata)
//!   Extended Query: const PG_PARSE_SYNC_WIRE (.rodata) -> Bind('B') + const PG_EXECUTE_SYNC_TAIL[15]
//!   Response loop:  'D' (DataRow u64) | 'E' (Error) .. until 'Z' (ReadyForQuery)
//! ```

use std::io::{Error, ErrorKind};

use velda_etp::EtpError;
use velda_etp::connect::EtpStream;

use crate::wire_baked::{BakedWire, PG_EXECUTE_SYNC_TAIL};

pub(crate) fn append_bind_execute_sync(buf: &mut Vec<u8>, params: &[&[u8]]) {
    let params_bytes: usize = params.iter().map(|p| 4 + p.len()).sum();
    // 'B' | i32 len | portal=""\0 | stmt=""\0 | i16 n_fmts=0 | i16 n_params | params... | i16 n_res_fmts=0
    let bind_len = 4 + 1 + 1 + 2 + 2 + params_bytes + 2;
    let mut w = BakedWire::new(buf);
    w.push_byte(b'B');
    w.push_chunk(&(bind_len as u32).to_be_bytes());
    w.push_chunk(&[0, 0, 0, 0]);
    w.push_chunk(&(params.len() as i16).to_be_bytes());
    for p in params {
        w.push_chunk(&(p.len() as i32).to_be_bytes());
        w.push_chunk(p);
    }
    w.push_chunk(&[0, 0]);
    w.push_chunk(&PG_EXECUTE_SYNC_TAIL);
}

pub(crate) async fn read_until_ready(stream: &mut EtpStream) -> Result<Option<u64>, EtpError> {
    let mut scalar: Option<u64> = None;
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
                    scalar = text.trim().parse::<u64>().ok();
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
