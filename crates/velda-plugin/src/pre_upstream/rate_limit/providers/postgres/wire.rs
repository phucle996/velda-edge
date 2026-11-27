//! PostgreSQL wire v3 frame encoder & pipelined response parser (`wire.rs`) for `rate_limit`.
//!
//! ```text
//!   Simple Query:   const PG_DDL_WIRE (.rodata)
//!   Extended Query: const PG_PARSE_SYNC_WIRE (.rodata) -> N × Bind('B')+Execute('E') + Sync('S')
//!   Response loop:  buffered 4KB reads -> 'D' (DataRow u64) .. 'C'/'Z' -> Vec<Option<u64>>
//! ```

use std::io::{Error, ErrorKind};

use velda_etp::EtpError;
use velda_etp::connect::EtpStream;

use crate::wire_baked::{BakedWire, PG_EXECUTE_SYNC_TAIL};

/// Appends `'B'` (`Bind`) + `'E'` (`Execute`) without `'S'` (`Sync`) for batch pipelining.
pub(crate) fn append_bind_execute(buf: &mut Vec<u8>, params: &[&[u8]]) {
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
    w.push_chunk(&PG_EXECUTE_SYNC_TAIL[..10]);
}

/// Appends `'S'` (`Sync`) frame terminator at the end of a pipelined Extended Query batch.
#[inline]
pub(crate) fn append_sync(buf: &mut Vec<u8>) {
    buf.extend_from_slice(&PG_EXECUTE_SYNC_TAIL[10..]);
}

pub(crate) struct PgReader {
    buf: Vec<u8>,
}

impl PgReader {
    pub(crate) fn new() -> Self {
        Self {
            buf: Vec::with_capacity(4096),
        }
    }

    pub(crate) fn clear(&mut self) {
        self.buf.clear();
    }

    async fn read_frame(&mut self, stream: &mut EtpStream) -> Result<(u8, Vec<u8>), EtpError> {
        while self.buf.len() < 5 {
            let mut tmp = [0u8; 4096];
            let n = stream.read(&mut tmp).await?;
            if n == 0 {
                return Err(EtpError::Io(Error::new(
                    ErrorKind::UnexpectedEof,
                    "Postgres closed connection while reading response header",
                )));
            }
            self.buf.extend_from_slice(&tmp[..n]);
        }
        let msg_type = self.buf[0];
        let len = u32::from_be_bytes([self.buf[1], self.buf[2], self.buf[3], self.buf[4]]);
        if !(4..=16 * 1024 * 1024).contains(&len) {
            return Err(EtpError::Io(Error::new(
                ErrorKind::InvalidData,
                format!("Postgres invalid response length {len}"),
            )));
        }
        let frame_total = 1 + (len as usize);
        while self.buf.len() < frame_total {
            let mut tmp = [0u8; 4096];
            let n = stream.read(&mut tmp).await?;
            if n == 0 {
                return Err(EtpError::Io(Error::new(
                    ErrorKind::UnexpectedEof,
                    "Postgres closed connection while reading response payload",
                )));
            }
            self.buf.extend_from_slice(&tmp[..n]);
        }
        let body = self.buf[5..frame_total].to_vec();
        self.buf.drain(..frame_total);
        Ok((msg_type, body))
    }

    /// Reads pipelined Extended Query responses until `'Z'` (`ReadyForQuery`).
    pub(crate) async fn read_pipelined_until_ready(
        &mut self,
        stream: &mut EtpStream,
    ) -> Result<Vec<Option<u64>>, EtpError> {
        let mut results = Vec::new();
        let mut cur_scalar: Option<u64> = None;
        let mut pg_err: Option<String> = None;

        loop {
            let (msg_type, body) = self.read_frame(stream).await?;
            match msg_type {
                b'D' if cur_scalar.is_none() && body.len() >= 6 => {
                    let col_len = i32::from_be_bytes([body[2], body[3], body[4], body[5]]);
                    if col_len > 0 && body.len() >= 6 + col_len as usize {
                        let text =
                            std::str::from_utf8(&body[6..6 + col_len as usize]).unwrap_or("");
                        cur_scalar = text.trim().parse::<u64>().ok();
                    }
                }
                b'C' => {
                    results.push(cur_scalar.take());
                }
                b'E' => {
                    pg_err = Some(String::from_utf8_lossy(&body).into_owned());
                }
                b'Z' => {
                    if let Some(val) = cur_scalar.take() {
                        results.push(Some(val));
                    }
                    break;
                }
                _ => {}
            }
        }

        if let Some(err) = pg_err {
            return Err(EtpError::Io(Error::other(format!(
                "Postgres wire query error: {err}"
            ))));
        }
        Ok(results)
    }
}

pub(crate) async fn read_until_ready(stream: &mut EtpStream) -> Result<Option<u64>, EtpError> {
    let mut reader = PgReader::new();
    let res = reader.read_pipelined_until_ready(stream).await?;
    Ok(res.into_iter().next().flatten())
}
