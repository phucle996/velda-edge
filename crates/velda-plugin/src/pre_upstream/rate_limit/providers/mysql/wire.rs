//! MySQL binary prepared-statement & COM_QUERY wire codec (`wire.rs`) for `rate_limit`.
//!
//! ```text
//!   COM_QUERY (0x03)       -> DDL statements
//!   COM_STMT_PREPARE (0x16)-> PreparedMeta { stmt_id, num_params }
//!   COM_STMT_EXECUTE (0x17)-> binary params -> binary scalar i64 col0
//!   COM_STMT_CLOSE (0x19)  -> stmt_id release
//! ```

use std::io::{Error, ErrorKind};

use velda_etp::EtpError;
use velda_etp::connect::EtpStream;

pub(crate) enum MysqlParam<'a> {
    Str(&'a str),
    I64(i64),
}

pub(crate) struct PreparedMeta {
    pub(crate) stmt_id: u32,
    pub(crate) num_params: usize,
}

pub(crate) async fn execute_com_query(stream: &mut EtpStream, sql: &str) -> Result<(), EtpError> {
    let mut payload = Vec::with_capacity(1 + sql.len());
    payload.push(0x03);
    payload.extend_from_slice(sql.as_bytes());
    write_mysql_pkt(stream, 0, &payload).await?;
    let first = read_mysql_pkt(stream).await?;
    if first.first() == Some(&0xFF) {
        return Err(EtpError::Io(Error::other(format!(
            "MySQL DDL error: {}",
            String::from_utf8_lossy(&first[3.min(first.len())..])
        ))));
    }
    Ok(())
}

pub(crate) async fn stmt_prepare(
    stream: &mut EtpStream,
    sql: &str,
) -> Result<PreparedMeta, EtpError> {
    let mut payload = Vec::with_capacity(1 + sql.len());
    payload.push(0x16);
    payload.extend_from_slice(sql.as_bytes());
    write_mysql_pkt(stream, 0, &payload).await?;

    let ok = read_mysql_pkt(stream).await?;
    if ok.first() == Some(&0xFF) {
        return Err(EtpError::Io(Error::other(format!(
            "MySQL COM_STMT_PREPARE error: {}",
            String::from_utf8_lossy(&ok[3.min(ok.len())..])
        ))));
    }
    if ok.len() < 9 || ok[0] != 0x00 {
        return Err(EtpError::Io(Error::new(
            ErrorKind::InvalidData,
            "Unexpected MySQL COM_STMT_PREPARE_OK packet",
        )));
    }
    let stmt_id = u32::from_le_bytes([ok[1], ok[2], ok[3], ok[4]]);
    let num_columns = u16::from_le_bytes([ok[5], ok[6]]) as usize;
    let num_params = u16::from_le_bytes([ok[7], ok[8]]) as usize;

    if num_params > 0 {
        for _ in 0..num_params {
            let _ = read_mysql_pkt(stream).await?;
        }
        let eof = read_mysql_pkt(stream).await?;
        if eof.first() != Some(&0xFE) {
            return Err(EtpError::Io(Error::new(
                ErrorKind::InvalidData,
                "MySQL PREPARE expected param EOF",
            )));
        }
    }
    if num_columns > 0 {
        for _ in 0..num_columns {
            let _ = read_mysql_pkt(stream).await?;
        }
        let eof = read_mysql_pkt(stream).await?;
        if eof.first() != Some(&0xFE) {
            return Err(EtpError::Io(Error::new(
                ErrorKind::InvalidData,
                "MySQL PREPARE expected column EOF",
            )));
        }
    }

    Ok(PreparedMeta {
        stmt_id,
        num_params,
    })
}

pub(crate) async fn stmt_execute(
    stream: &mut EtpStream,
    stmt: &PreparedMeta,
    params: &[MysqlParam<'_>],
) -> Result<Option<i64>, EtpError> {
    let mut payload = Vec::with_capacity(64);
    payload.push(0x17);
    payload.extend_from_slice(&stmt.stmt_id.to_le_bytes());
    payload.push(0x00);
    payload.extend_from_slice(&1u32.to_le_bytes());

    if stmt.num_params > 0 {
        let bitmap_len = stmt.num_params.div_ceil(8);
        payload.extend(std::iter::repeat_n(0u8, bitmap_len));
        payload.push(0x01);
        for p in params {
            match p {
                MysqlParam::Str(_) => payload.extend_from_slice(&[0xFD, 0x00]),
                MysqlParam::I64(_) => payload.extend_from_slice(&[0x08, 0x00]),
            }
        }
        for p in params {
            match p {
                MysqlParam::Str(s) => {
                    append_lenenc_int(&mut payload, s.len());
                    payload.extend_from_slice(s.as_bytes());
                }
                MysqlParam::I64(v) => payload.extend_from_slice(&v.to_le_bytes()),
            }
        }
    }

    write_mysql_pkt(stream, 0, &payload).await?;
    let first = read_mysql_pkt(stream).await?;
    if first.is_empty() {
        return Err(EtpError::Io(Error::new(
            ErrorKind::InvalidData,
            "MySQL empty COM_STMT_EXECUTE response",
        )));
    }
    match first[0] {
        0x00 => Ok(None),
        0xFF => Err(EtpError::Io(Error::other(format!(
            "MySQL COM_STMT_EXECUTE error: {}",
            String::from_utf8_lossy(&first[3.min(first.len())..])
        )))),
        col_count => {
            let cols = col_count as usize;
            let mut col0_type = 0xFDu8;
            for idx in 0..cols {
                let col_def = read_mysql_pkt(stream).await?;
                if idx == 0 {
                    col0_type = parse_col_def_type(&col_def);
                }
            }
            let sep = read_mysql_pkt(stream).await?;
            if sep.first() != Some(&0xFE) {
                return Err(EtpError::Io(Error::new(
                    ErrorKind::InvalidData,
                    "MySQL expected column EOF",
                )));
            }
            let mut scalar: Option<i64> = None;
            loop {
                let row = read_mysql_pkt(stream).await?;
                if row.first() == Some(&0xFE) && row.len() < 9 {
                    break;
                }
                if row.first() == Some(&0xFF) {
                    return Err(EtpError::Io(Error::other("MySQL binary row ERR")));
                }
                if scalar.is_none() && row.first() == Some(&0x00) {
                    scalar = decode_binary_col0_i64(&row, cols, col0_type);
                }
            }
            Ok(scalar)
        }
    }
}

pub(crate) async fn stmt_close(stream: &mut EtpStream, stmt_id: u32) -> Result<(), EtpError> {
    let mut payload = [0u8; 5];
    payload[0] = 0x19;
    payload[1..5].copy_from_slice(&stmt_id.to_le_bytes());
    write_mysql_pkt(stream, 0, &payload).await
}

fn append_lenenc_int(buf: &mut Vec<u8>, v: usize) {
    if v < 251 {
        buf.push(v as u8);
    } else if v < 65_536 {
        buf.push(0xFC);
        buf.extend_from_slice(&(v as u16).to_le_bytes());
    } else {
        buf.push(0xFE);
        buf.extend_from_slice(&(v as u64).to_le_bytes());
    }
}

fn parse_col_def_type(pkt: &[u8]) -> u8 {
    let mut pos = 0usize;
    for _ in 0..6 {
        let Some((len, adv)) = read_lenenc_int(pkt, pos) else {
            return 0xFD;
        };
        pos = adv.saturating_add(len);
    }
    pos += 1 + 2 + 4;
    *pkt.get(pos).unwrap_or(&0xFD)
}

fn decode_binary_col0_i64(row: &[u8], col_count: usize, col0_type: u8) -> Option<i64> {
    let bitmap_len = (col_count + 2).div_ceil(8);
    let pos = 1 + bitmap_len;
    if pos > row.len() || (row.get(1).copied().unwrap_or(0) & 0x04) != 0 {
        return None;
    }
    match col0_type {
        0x08 | 0x09 => {
            let b = row.get(pos..pos + 8)?;
            Some(i64::from_le_bytes(b.try_into().ok()?))
        }
        0x03 => {
            let b = row.get(pos..pos + 4)?;
            Some(i64::from(i32::from_le_bytes(b.try_into().ok()?)))
        }
        _ => {
            let (slen, start) = read_lenenc_int(row, pos)?;
            let s = std::str::from_utf8(row.get(start..start + slen)?).ok()?;
            s.trim().parse::<i64>().ok()
        }
    }
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
