//! MySQL `COM_QUERY` multi-row wire codec & text resultset parser (`wire.rs`) for `rate_limit`.
//!
//! ```text
//!   COM_QUERY (0x03) -> DDL + multi-row INSERT ON DUPLICATE KEY UPDATE (OK packet)
//!   COM_QUERY (0x03) -> batch SELECT k, cnt WHERE k IN (...) -> (String, u64) rows
//! ```

use std::io::{Error, ErrorKind};

use velda_etp::EtpError;
use velda_etp::connect::EtpStream;

pub(crate) async fn execute_com_query(stream: &mut EtpStream, sql: &str) -> Result<(), EtpError> {
    let mut payload = Vec::with_capacity(1 + sql.len());
    payload.push(0x03);
    payload.extend_from_slice(sql.as_bytes());
    write_mysql_pkt(stream, 0, &payload).await?;
    let first = read_mysql_pkt(stream).await?;
    if first.first() == Some(&0xFF) {
        return Err(EtpError::Io(Error::other(format!(
            "MySQL query error: {}",
            String::from_utf8_lossy(&first[3.min(first.len())..])
        ))));
    }
    Ok(())
}

/// Executes a `COM_QUERY` returning `(k, cnt)` text rows (used by multi-key `SELECT IN`).
pub(crate) async fn execute_com_query_kv_rows(
    stream: &mut EtpStream,
    sql: &str,
) -> Result<Vec<(String, u64)>, EtpError> {
    let mut payload = Vec::with_capacity(1 + sql.len());
    payload.push(0x03);
    payload.extend_from_slice(sql.as_bytes());
    write_mysql_pkt(stream, 0, &payload).await?;

    let first = read_mysql_pkt(stream).await?;
    if first.is_empty() {
        return Err(EtpError::Io(Error::new(
            ErrorKind::InvalidData,
            "MySQL empty COM_QUERY response",
        )));
    }
    match first[0] {
        0x00 => Ok(Vec::new()),
        0xFF => Err(EtpError::Io(Error::other(format!(
            "MySQL query error: {}",
            String::from_utf8_lossy(&first[3.min(first.len())..])
        )))),
        col_count => {
            let cols = col_count as usize;
            for _ in 0..cols {
                let _ = read_mysql_pkt(stream).await?;
            }
            let sep = read_mysql_pkt(stream).await?;
            if sep.first() != Some(&0xFE) {
                return Err(EtpError::Io(Error::new(
                    ErrorKind::InvalidData,
                    "MySQL text resultset expected column EOF",
                )));
            }
            let mut out = Vec::new();
            loop {
                let row = read_mysql_pkt(stream).await?;
                if row.first() == Some(&0xFE) && row.len() < 9 {
                    break;
                }
                if row.first() == Some(&0xFF) {
                    return Err(EtpError::Io(Error::other("MySQL text row ERR")));
                }
                if let Some((klen, kstart)) = read_lenenc_int(&row, 0)
                    && let Some(key_bytes) = row.get(kstart..kstart + klen)
                    && let Some((vlen, vstart)) = read_lenenc_int(&row, kstart + klen)
                    && let Some(val_bytes) = row.get(vstart..vstart + vlen)
                    && let Ok(val) = std::str::from_utf8(val_bytes)
                        .unwrap_or("")
                        .trim()
                        .parse::<u64>()
                {
                    out.push((String::from_utf8_lossy(key_bytes).into_owned(), val));
                }
            }
            Ok(out)
        }
    }
}

/// Appends `'...'` SQL-escaped literal for MySQL `COM_QUERY` batch statements.
pub(crate) fn append_mysql_escaped_str(buf: &mut Vec<u8>, s: &[u8]) {
    buf.push(b'\'');
    for &b in s {
        match b {
            b'\\' => buf.extend_from_slice(b"\\\\"),
            b'\'' => buf.extend_from_slice(b"\\'"),
            0 => buf.extend_from_slice(b"\\0"),
            b'\n' => buf.extend_from_slice(b"\\n"),
            b'\r' => buf.extend_from_slice(b"\\r"),
            0x1a => buf.extend_from_slice(b"\\Z"),
            _ => buf.push(b),
        }
    }
    buf.push(b'\'');
}

fn read_lenenc_int(buf: &[u8], pos: usize) -> Option<(usize, usize)> {
    let first = *buf.get(pos)?;
    match first {
        0..=250 => Some((first as usize, pos + 1)),
        0xFC => {
            let b = buf.get(pos + 1..pos + 3)?;
            Some((u16::from_le_bytes([b[0], b[1]]) as usize, pos + 3))
        }
        0xFD => {
            let b = buf.get(pos + 1..pos + 4)?;
            Some((
                (b[0] as usize) | ((b[1] as usize) << 8) | ((b[2] as usize) << 16),
                pos + 4,
            ))
        }
        0xFE => {
            let b = buf.get(pos + 1..pos + 9)?;
            Some((u64::from_le_bytes(b.try_into().ok()?) as usize, pos + 9))
        }
        _ => None,
    }
}

async fn write_mysql_pkt(stream: &mut EtpStream, seq: u8, payload: &[u8]) -> Result<(), EtpError> {
    let len = payload.len() as u32;
    let mut pkt = Vec::with_capacity(4 + payload.len());
    pkt.push((len & 0xFF) as u8);
    pkt.push(((len >> 8) & 0xFF) as u8);
    pkt.push(((len >> 16) & 0xFF) as u8);
    pkt.push(seq);
    pkt.extend_from_slice(payload);
    stream.write_all(&pkt).await?;
    stream.flush().await?;
    Ok(())
}

async fn read_mysql_pkt(stream: &mut EtpStream) -> Result<Vec<u8>, EtpError> {
    let mut hdr = [0u8; 4];
    stream.read_exact(&mut hdr).await?;
    let len = (hdr[0] as usize) | ((hdr[1] as usize) << 8) | ((hdr[2] as usize) << 16);
    let mut body = vec![0u8; len];
    if len > 0 {
        stream.read_exact(&mut body).await?;
    }
    Ok(body)
}
