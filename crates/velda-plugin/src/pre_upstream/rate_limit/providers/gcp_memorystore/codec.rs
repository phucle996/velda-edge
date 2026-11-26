//! RESP2 `SET EX 15 NX` + `INCRBY` SIMD chunk encoder & reply parser (`codec.rs`)
//! for `rate_limit` GCP Memorystore client.
//!
//! ```text
//!   write_rl_key() / append_set_nx_incrby_item()
//!     └──► BakedWire SIMD chunks (*6 SET EX 15 NX + *3 INCRBY) — 0 Vec<String> allocations!
//! ```

use std::io::{Error, ErrorKind};

use velda_core::lut::decimal::fast_write_u64_slice;
use velda_etp::EtpError;
use velda_etp::connect::EtpStream;

use crate::pre_upstream::rate_limit::arena::{DirtyKeyDelta, ReconciledClusterTotal};
use crate::wire_baked::BakedWire;

const ASKING_CHUNK: &[u8] = b"*1\r\n$6\r\nASKING\r\n";
const SET_HEAD: &[u8] = b"*6\r\n$3\r\nSET\r\n$";
const SET_EX_AND_INCR_HEAD: &[u8] =
    b"\r\n$1\r\n0\r\n$2\r\nEX\r\n$2\r\n15\r\n$2\r\nNX\r\n*3\r\n$6\r\nINCRBY\r\n$";

#[derive(Debug, Clone)]
pub(crate) enum KeyRedirect {
    Moved { slot: u16, addr: String },
    Ask { addr: String },
}

pub(crate) enum SubItemOutcome {
    Done(ReconciledClusterTotal),
    Redirect {
        delta: DirtyKeyDelta,
        key: Vec<u8>,
        redirect: KeyRedirect,
    },
}

#[inline(always)]
pub(crate) fn effective_prefix_bytes(key_prefix: &str) -> &[u8] {
    if key_prefix.is_empty() {
        b"velda:rl:"
    } else {
        key_prefix.as_bytes()
    }
}

#[inline]
pub(crate) fn write_rl_key(prefix: &[u8], d: &DirtyKeyDelta, key_buf: &mut Vec<u8>) {
    let mut kw = BakedWire::cleared(key_buf, prefix.len() + 40);
    kw.push_chunk(prefix);
    kw.push_u64(u64::from(d.tier));
    kw.push_byte(b':');
    kw.push_hex16(d.key_hash);
    kw.push_byte(b':');
    kw.push_u64(d.window_epoch);
}

#[inline]
pub(crate) fn append_set_nx_incrby_item(
    w: &mut BakedWire<'_>,
    key: &[u8],
    delta: u64,
    with_asking: bool,
) {
    let mut d_buf = [0u8; 24];
    let d_len = fast_write_u64_slice(&mut d_buf, delta);
    let mut klen_buf = [0u8; 24];
    let klen_len = fast_write_u64_slice(&mut klen_buf, key.len() as u64);

    if with_asking {
        w.push_chunk(ASKING_CHUNK);
    }
    w.push_chunk(SET_HEAD);
    w.push_chunk(&klen_buf[..klen_len]);
    w.push_chunk(b"\r\n");
    w.push_chunk(key);
    if with_asking {
        w.push_chunk(b"\r\n$1\r\n0\r\n$2\r\nEX\r\n$2\r\n15\r\n$2\r\nNX\r\n");
        w.push_chunk(ASKING_CHUNK);
        w.push_chunk(b"*3\r\n$6\r\nINCRBY\r\n$");
    } else {
        w.push_chunk(SET_EX_AND_INCR_HEAD);
    }
    w.push_chunk(&klen_buf[..klen_len]);
    w.push_chunk(b"\r\n");
    w.push_chunk(key);
    w.push_chunk(b"\r\n$");
    w.push_u64(d_len as u64);
    w.push_chunk(b"\r\n");
    w.push_chunk(&d_buf[..d_len]);
    w.push_chunk(b"\r\n");
}

pub(crate) fn build_pipelined_set_nx_incrby(
    key_prefix: &str,
    deltas: &[DirtyKeyDelta],
    out: &mut Vec<u8>,
) {
    let prefix = effective_prefix_bytes(key_prefix);
    let mut key_buf = Vec::with_capacity(prefix.len() + 40);
    let mut w = BakedWire::cleared(out, deltas.len() * (prefix.len() * 2 + 128));
    for d in deltas {
        write_rl_key(prefix, d, &mut key_buf);
        append_set_nx_incrby_item(&mut w, &key_buf, d.delta, false);
    }
}

pub(crate) fn encode_shard_group(group: &[(Vec<u8>, DirtyKeyDelta)], out: &mut Vec<u8>) {
    let mut w = BakedWire::cleared(out, group.len() * 144);
    for (key, d) in group {
        append_set_nx_incrby_item(&mut w, key, d.delta, false);
    }
}

pub(crate) fn encode_single_redirect(key: &[u8], delta: u64, with_asking: bool, out: &mut Vec<u8>) {
    let mut w = BakedWire::cleared(out, 192 + key.len() * 2);
    append_set_nx_incrby_item(&mut w, key, delta, with_asking);
}

pub(crate) fn parse_redirect(line: &str) -> Option<KeyRedirect> {
    let body = line.strip_prefix('-')?;
    let mut parts = body.split_whitespace();
    let kind = parts.next()?;
    let slot = parts.next()?.parse::<u16>().ok()?;
    let addr = parts.next()?;
    if kind.eq_ignore_ascii_case("MOVED") {
        Some(KeyRedirect::Moved {
            slot,
            addr: addr.to_string(),
        })
    } else if kind.eq_ignore_ascii_case("ASK") {
        let _ = slot;
        Some(KeyRedirect::Ask {
            addr: addr.to_string(),
        })
    } else {
        None
    }
}

pub(crate) fn parse_resp_int(reply: &str) -> Result<u64, EtpError> {
    reply
        .strip_prefix(':')
        .and_then(|s| s.parse::<u64>().ok())
        .ok_or_else(|| {
            EtpError::Io(Error::new(
                ErrorKind::InvalidData,
                format!("Unexpected GcpMemorystore INCRBY reply: {reply}"),
            ))
        })
}

pub(crate) async fn read_resp_line(stream: &mut EtpStream) -> Result<String, EtpError> {
    let mut buf = Vec::with_capacity(64);
    let mut byte = [0u8; 1];
    loop {
        let n = stream.read(&mut byte).await?;
        if n == 0 {
            return Err(EtpError::Io(Error::new(
                ErrorKind::UnexpectedEof,
                "GcpMemorystore closed stream while reading RESP2 reply",
            )));
        }
        buf.push(byte[0]);
        if buf.len() >= 2 && buf[buf.len() - 2] == b'\r' && buf[buf.len() - 1] == b'\n' {
            buf.truncate(buf.len() - 2);
            return Ok(String::from_utf8_lossy(&buf).into_owned());
        }
        if buf.len() > 8192 {
            return Err(EtpError::Io(Error::new(
                ErrorKind::InvalidData,
                "GcpMemorystore RESP2 reply line exceeded 8 KiB",
            )));
        }
    }
}
