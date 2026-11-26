//! Valkey Cluster CRC16 slot hashing, `CLUSTER SLOTS` RESP2 parser, and Hybrid [`SlotMap`].
//!
//! ```text
//!   crc16_slot(key) -> slot (0..=16383)
//!         └─► SlotMap: Ranges (<= 64) | #[cold] Dense4Bit (8 KB) / DenseU8 (16 KB)
//! ```

use std::io::{Error, ErrorKind};

use crate::connect::EtpStream;
use crate::error::EtpError;

pub const MAX_INLINE_RANGES: usize = 64;
const UNASSIGNED_U8: u8 = 0xFF;

const CRC16_TAB: [u16; 256] = [
    0x0000, 0x1021, 0x2042, 0x3063, 0x4084, 0x50a5, 0x60c6, 0x70e7, 0x8108, 0x9129, 0xa14a, 0xb16b,
    0xc18c, 0xd1ad, 0xe1ce, 0xf1ef, 0x1231, 0x0210, 0x3273, 0x2252, 0x52b5, 0x4294, 0x72f7, 0x62d6,
    0x9339, 0x8318, 0xb37b, 0xa35a, 0xd3bd, 0xc39c, 0xf3ff, 0xe3de, 0x2462, 0x3443, 0x0420, 0x1401,
    0x64e6, 0x74c7, 0x44a4, 0x5485, 0xa56a, 0xb54b, 0x8528, 0x9509, 0xe5ee, 0xf5cf, 0xc5ac, 0xd58d,
    0x3653, 0x2672, 0x1611, 0x0630, 0x76d7, 0x66f6, 0x5695, 0x46b4, 0xb75b, 0xa77a, 0x9719, 0x8738,
    0xf7df, 0xe7fe, 0xd79d, 0xc7bc, 0x48c4, 0x58e5, 0x6886, 0x78a7, 0x0840, 0x1861, 0x2802, 0x3823,
    0xc9cc, 0xd9ed, 0xe98e, 0xf9af, 0x8948, 0x9969, 0xa90a, 0xb92b, 0x5af5, 0x4ad4, 0x7ab7, 0x6a96,
    0x1a71, 0x0a50, 0x3a33, 0x2a12, 0xdbfd, 0xcbdc, 0xfbbf, 0xeb9e, 0x9b79, 0x8b58, 0xbb3b, 0xab1a,
    0x6ca6, 0x7c87, 0x4ce4, 0x5cc5, 0x2c22, 0x3c03, 0x0c60, 0x1c41, 0xedae, 0xfd8f, 0xcdec, 0xddcd,
    0xad2a, 0xbd0b, 0x8d68, 0x9d49, 0x7e97, 0x6eb6, 0x5ed5, 0x4ef4, 0x3e13, 0x2e32, 0x1e51, 0x0e70,
    0xff9f, 0xefbe, 0xdfdd, 0xcffc, 0xbf1b, 0xaf3a, 0x9f59, 0x8f78, 0x9188, 0x81a9, 0xb1ca, 0xa1eb,
    0xd10c, 0xc12d, 0xf14e, 0xe16f, 0x1080, 0x00a1, 0x30c2, 0x20e3, 0x5004, 0x4025, 0x7046, 0x6067,
    0x83b9, 0x9398, 0xa3fb, 0xb3da, 0xc33d, 0xd31c, 0xe37f, 0xf35e, 0x02b1, 0x1290, 0x22f3, 0x32d2,
    0x4235, 0x5214, 0x6277, 0x7256, 0xb5ea, 0xa5cb, 0x95a8, 0x8589, 0xf56e, 0xe54f, 0xd52c, 0xc50d,
    0x34e2, 0x24c3, 0x14a0, 0x0481, 0x7466, 0x6447, 0x5424, 0x4405, 0xa7db, 0xb7fa, 0x8799, 0x97b8,
    0xe75f, 0xf77e, 0xc71d, 0xd73c, 0x26d3, 0x36f2, 0x0691, 0x16b0, 0x6657, 0x7676, 0x4615, 0x5634,
    0xd94c, 0xc96d, 0xf90e, 0xe92f, 0x99c8, 0x89e9, 0xb98a, 0xa9ab, 0x5844, 0x4865, 0x7806, 0x6827,
    0x18c0, 0x08e1, 0x3882, 0x28a3, 0xcb7d, 0xdb5c, 0xeb3f, 0xfb1e, 0x8bf9, 0x9bd8, 0xabbb, 0xbb9a,
    0x4a75, 0x5a54, 0x6a37, 0x7a16, 0x0af1, 0x1ad0, 0x2ab3, 0x3a92, 0xfd2e, 0xed0f, 0xdd6c, 0xcd4d,
    0xbdaa, 0xad8b, 0x9de8, 0x8dc9, 0x7c26, 0x6c07, 0x5c64, 0x4c45, 0x3ca2, 0x2c83, 0x1ce0, 0x0cc1,
    0xef1f, 0xff3e, 0xcf5d, 0xdf7c, 0xaf9b, 0xbfba, 0x8fd9, 0x9ff8, 0x6e17, 0x7e36, 0x4e55, 0x5e74,
    0x2e93, 0x3eb2, 0x0ed1, 0x1ef0,
];

#[inline]
pub fn crc16_slot(key: &[u8]) -> u16 {
    let payload = if let Some(open) = key.iter().position(|&b| b == b'{') {
        let rest = &key[open + 1..];
        if let Some(close) = rest.iter().position(|&b| b == b'}')
            && close > 0
        {
            &rest[..close]
        } else {
            key
        }
    } else {
        key
    };
    let mut crc: u16 = 0;
    for &b in payload {
        let idx = ((crc >> 8) as u8 ^ b) as usize;
        crc = (crc << 8) ^ CRC16_TAB[idx];
    }
    crc & 0x3FFF
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(transparent)]
pub struct SlotRange(u64);

impl SlotRange {
    #[inline(always)]
    pub const fn new(start: u16, end: u16, node_idx: u16) -> Self {
        Self((start as u64) | ((end as u64) << 16) | ((node_idx as u64) << 32))
    }
    #[inline(always)]
    pub const fn start(self) -> u16 {
        self.0 as u16
    }
    #[inline(always)]
    pub const fn end(self) -> u16 {
        (self.0 >> 16) as u16
    }
    #[inline(always)]
    pub const fn node_idx(self) -> u16 {
        (self.0 >> 32) as u16
    }
}

#[derive(Clone, Debug)]
pub enum SlotMap {
    Ranges(Box<[SlotRange]>),
    Dense4Bit(Box<[u8]>),
    DenseU8(Box<[u8]>),
}

impl Default for SlotMap {
    fn default() -> Self {
        Self::Ranges(Box::new([]))
    }
}

impl SlotMap {
    pub fn from_ranges(mut raw: Vec<(u16, u16, u16)>, total_nodes: usize) -> Self {
        if raw.is_empty() {
            return Self::default();
        }
        raw.sort_unstable_by_key(|r| r.0);
        let mut merged: Vec<SlotRange> = Vec::with_capacity(raw.len());
        for (start, end, node) in raw {
            let s = start.min(16_383);
            let e = end.min(16_383);
            if s > e {
                continue;
            }
            if let Some(last) = merged.last_mut()
                && last.node_idx() == node
                && last.end().saturating_add(1) >= s
            {
                let new_end = last.end().max(e);
                *last = SlotRange::new(last.start(), new_end, node);
                continue;
            }
            merged.push(SlotRange::new(s, e, node));
        }
        if merged.len() <= MAX_INLINE_RANGES {
            Self::Ranges(merged.into_boxed_slice())
        } else {
            build_dense_cold(&merged, total_nodes)
        }
    }

    #[inline]
    pub fn lookup(&self, slot: u16) -> Option<usize> {
        let s = slot & 0x3FFF;
        match self {
            Self::Ranges(ranges) => {
                if ranges.len() <= 8 {
                    for &r in ranges.iter() {
                        if s >= r.start() && s <= r.end() {
                            return Some(r.node_idx() as usize);
                        }
                    }
                    None
                } else {
                    let pos = ranges.partition_point(|r| r.start() <= s);
                    if pos == 0 {
                        return None;
                    }
                    let r = ranges[pos - 1];
                    if s <= r.end() {
                        Some(r.node_idx() as usize)
                    } else {
                        None
                    }
                }
            }
            Self::Dense4Bit(buf) => lookup_dense4_cold(buf, s),
            Self::DenseU8(buf) => lookup_dense8_cold(buf, s),
        }
    }

    pub fn with_moved_slot(&self, slot: u16, node_idx: u16, total_nodes: usize) -> Self {
        let s = slot & 0x3FFF;
        match self {
            Self::Ranges(ranges) => {
                let mut expanded = Vec::with_capacity(ranges.len() + 2);
                let mut placed = false;
                for &r in ranges.iter() {
                    if s < r.start() || s > r.end() {
                        expanded.push((r.start(), r.end(), r.node_idx()));
                    } else {
                        if s > r.start() {
                            expanded.push((r.start(), s - 1, r.node_idx()));
                        }
                        expanded.push((s, s, node_idx));
                        placed = true;
                        if s < r.end() {
                            expanded.push((s + 1, r.end(), r.node_idx()));
                        }
                    }
                }
                if !placed {
                    expanded.push((s, s, node_idx));
                }
                Self::from_ranges(expanded, total_nodes)
            }
            Self::Dense4Bit(buf) => patch_dense4_cold(buf, s, node_idx, total_nodes),
            Self::DenseU8(buf) => patch_dense8_cold(buf, s, node_idx),
        }
    }
}

#[cold]
#[inline(never)]
fn build_dense_cold(ranges: &[SlotRange], total_nodes: usize) -> SlotMap {
    if total_nodes <= 15 && ranges.iter().all(|r| r.node_idx() < 15) {
        let mut buf = vec![0xFFu8; 8192].into_boxed_slice();
        for &r in ranges {
            let val = (r.node_idx() as u8) & 0x0F;
            for s in r.start()..=r.end() {
                let byte_idx = (s >> 1) as usize;
                if (s & 1) == 0 {
                    buf[byte_idx] = (buf[byte_idx] & 0xF0) | val;
                } else {
                    buf[byte_idx] = (buf[byte_idx] & 0x0F) | (val << 4);
                }
            }
        }
        SlotMap::Dense4Bit(buf)
    } else {
        let mut buf = vec![UNASSIGNED_U8; 16_384].into_boxed_slice();
        for &r in ranges {
            let val = (r.node_idx().min(254)) as u8;
            for s in r.start()..=r.end() {
                buf[s as usize] = val;
            }
        }
        SlotMap::DenseU8(buf)
    }
}

#[cold]
#[inline(never)]
fn lookup_dense4_cold(buf: &[u8], slot: u16) -> Option<usize> {
    let byte = *buf.get((slot >> 1) as usize)?;
    let nib = if (slot & 1) == 0 {
        byte & 0x0F
    } else {
        byte >> 4
    };
    if nib == 0x0F {
        None
    } else {
        Some(nib as usize)
    }
}

#[cold]
#[inline(never)]
fn lookup_dense8_cold(buf: &[u8], slot: u16) -> Option<usize> {
    let val = *buf.get(slot as usize)?;
    if val == UNASSIGNED_U8 {
        None
    } else {
        Some(val as usize)
    }
}

#[cold]
#[inline(never)]
fn patch_dense4_cold(buf: &[u8], slot: u16, node_idx: u16, total_nodes: usize) -> SlotMap {
    if total_nodes <= 15 && node_idx < 15 {
        let mut next = buf.to_vec().into_boxed_slice();
        let byte_idx = (slot >> 1) as usize;
        let val = (node_idx as u8) & 0x0F;
        if (slot & 1) == 0 {
            next[byte_idx] = (next[byte_idx] & 0xF0) | val;
        } else {
            next[byte_idx] = (next[byte_idx] & 0x0F) | (val << 4);
        }
        SlotMap::Dense4Bit(next)
    } else {
        let mut wide = vec![UNASSIGNED_U8; 16_384].into_boxed_slice();
        for s in 0..16_384u16 {
            if let Some(n) = lookup_dense4_cold(buf, s) {
                wide[s as usize] = n as u8;
            }
        }
        wide[slot as usize] = node_idx.min(254) as u8;
        SlotMap::DenseU8(wide)
    }
}

#[cold]
#[inline(never)]
fn patch_dense8_cold(buf: &[u8], slot: u16, node_idx: u16) -> SlotMap {
    let mut next = buf.to_vec().into_boxed_slice();
    next[slot as usize] = node_idx.min(254) as u8;
    SlotMap::DenseU8(next)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoveredSlotRange {
    pub start: u16,
    pub end: u16,
    pub host: String,
    pub port: u16,
}

#[derive(Debug)]
enum RespValue {
    Simple(String),
    Error(String),
    Integer(i64),
    Bulk(Option<Vec<u8>>),
    Array(Vec<RespValue>),
}

/// Parses Valkey `CLUSTER SLOTS` response directly inside `velda-etp::valkey`.
pub async fn fetch_cluster_slots(
    stream: &mut EtpStream,
    fallback_host: &str,
    id: &str,
) -> Result<Vec<DiscoveredSlotRange>, EtpError> {
    stream
        .write_all(b"*2\r\n$7\r\nCLUSTER\r\n$5\r\nSLOTS\r\n")
        .await?;
    stream.flush().await?;
    let root = read_resp_value(stream, 0).await?;
    let RespValue::Array(items) = root else {
        let msg = match root {
            RespValue::Error(e) => e,
            other => format!("{other:?}"),
        };
        return Err(EtpError::Config(format!(
            "Valkey Cluster '{id}' CLUSTER SLOTS failed: {msg}"
        )));
    };
    let mut out = Vec::with_capacity(items.len());
    for entry in items {
        let RespValue::Array(fields) = entry else {
            continue;
        };
        if fields.len() < 3 {
            continue;
        }
        let Some(start) = resp_as_u16(&fields[0]) else {
            continue;
        };
        let Some(end) = resp_as_u16(&fields[1]) else {
            continue;
        };
        let RespValue::Array(master) = &fields[2] else {
            continue;
        };
        if master.len() < 2 {
            continue;
        }
        let mut host = resp_as_str(&master[0])
            .unwrap_or_default()
            .trim()
            .to_string();
        if let Some(h) = extract_hostname_meta(master.get(3)) {
            host = h;
        }
        if host.is_empty() {
            host = fallback_host.to_string();
        }
        let Some(port) = resp_as_u16(&master[1]) else {
            continue;
        };
        if port == 0 {
            continue;
        }
        out.push(DiscoveredSlotRange {
            start,
            end,
            host,
            port,
        });
    }
    if out.is_empty() {
        return Err(EtpError::Config(format!(
            "Valkey Cluster '{id}' CLUSTER SLOTS returned no master ranges"
        )));
    }
    Ok(out)
}

pub(crate) fn format_host_port(host: &str, port: u16) -> String {
    if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

pub(crate) fn split_host_port(
    raw: &str,
    default_port: u16,
    id: &str,
) -> Result<(String, u16), EtpError> {
    let trimmed = raw.trim();
    if let Some(after) = trimmed.strip_prefix('[') {
        let (host, rest) = after.split_once(']').ok_or_else(|| {
            EtpError::Config(format!(
                "Valkey Cluster '{id}' invalid IPv6 node '{trimmed}'"
            ))
        })?;
        let port = if let Some(p) = rest.strip_prefix(':') {
            p.parse::<u16>().map_err(|_| {
                EtpError::Config(format!("Valkey Cluster '{id}' invalid port in '{trimmed}'"))
            })?
        } else {
            default_port
        };
        return Ok((host.to_string(), port));
    }
    if let Some((host, p)) = trimmed.rsplit_once(':')
        && !host.contains(':')
    {
        let port = p.parse::<u16>().map_err(|_| {
            EtpError::Config(format!("Valkey Cluster '{id}' invalid port in '{trimmed}'"))
        })?;
        return Ok((host.to_string(), port));
    }
    Ok((trimmed.to_string(), default_port))
}

fn extract_hostname_meta(meta: Option<&RespValue>) -> Option<String> {
    let Some(RespValue::Array(kv)) = meta else {
        return None;
    };
    for pair in kv.as_chunks::<2>().0 {
        if let Some(k) = resp_as_str(&pair[0])
            && k.eq_ignore_ascii_case("hostname")
            && let Some(v) = resp_as_str(&pair[1])
        {
            let t = v.trim();
            if !t.is_empty() {
                return Some(t.to_string());
            }
        }
    }
    None
}

fn resp_as_u16(val: &RespValue) -> Option<u16> {
    match val {
        RespValue::Integer(i) => u16::try_from(*i).ok(),
        _ => None,
    }
}

fn resp_as_str(val: &RespValue) -> Option<&str> {
    match val {
        RespValue::Bulk(Some(b)) => std::str::from_utf8(b).ok(),
        RespValue::Simple(s) => Some(s.as_str()),
        _ => None,
    }
}

fn read_resp_value<'a>(
    stream: &'a mut EtpStream,
    depth: u8,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<RespValue, EtpError>> + Send + 'a>> {
    Box::pin(async move {
        if depth > 8 {
            return Err(EtpError::Io(Error::new(
                ErrorKind::InvalidData,
                "Valkey CLUSTER SLOTS nesting exceeded max depth",
            )));
        }
        let line = read_crlf_line(stream).await?;
        let Some(prefix) = line.as_bytes().first().copied() else {
            return Err(EtpError::Io(Error::new(
                ErrorKind::InvalidData,
                "Empty RESP2 line",
            )));
        };
        let rest = &line[1..];
        match prefix {
            b'+' => Ok(RespValue::Simple(rest.to_string())),
            b'-' => Ok(RespValue::Error(rest.to_string())),
            b':' => Ok(RespValue::Integer(rest.trim().parse::<i64>().map_err(
                |_| EtpError::Io(Error::new(ErrorKind::InvalidData, "Invalid RESP2 int")),
            )?)),
            b'$' => {
                let len = rest.trim().parse::<isize>().map_err(|_| {
                    EtpError::Io(Error::new(ErrorKind::InvalidData, "Invalid RESP2 bulk len"))
                })?;
                if len < 0 {
                    return Ok(RespValue::Bulk(None));
                }
                let n = len as usize;
                let mut buf = vec![0u8; n + 2];
                if !buf.is_empty() {
                    stream.read_exact(&mut buf).await?;
                }
                buf.truncate(n);
                Ok(RespValue::Bulk(Some(buf)))
            }
            b'*' => {
                let count = rest.trim().parse::<isize>().map_err(|_| {
                    EtpError::Io(Error::new(
                        ErrorKind::InvalidData,
                        "Invalid RESP2 array len",
                    ))
                })?;
                if count <= 0 {
                    return Ok(RespValue::Array(Vec::new()));
                }
                let mut arr = Vec::with_capacity((count as usize).min(4096));
                for _ in 0..count {
                    arr.push(read_resp_value(stream, depth + 1).await?);
                }
                Ok(RespValue::Array(arr))
            }
            other => Err(EtpError::Io(Error::new(
                ErrorKind::InvalidData,
                format!("Unsupported RESP2 prefix byte 0x{other:02x}"),
            ))),
        }
    })
}

async fn read_crlf_line(stream: &mut EtpStream) -> Result<String, EtpError> {
    let mut buf = Vec::with_capacity(64);
    let mut byte = [0u8; 1];
    loop {
        let n = stream.read(&mut byte).await?;
        if n == 0 {
            return Err(EtpError::Io(Error::new(
                ErrorKind::UnexpectedEof,
                "Valkey closed stream during CLUSTER SLOTS",
            )));
        }
        buf.push(byte[0]);
        if buf.len() >= 2 && buf[buf.len() - 2] == b'\r' && buf[buf.len() - 1] == b'\n' {
            buf.truncate(buf.len() - 2);
            return Ok(String::from_utf8_lossy(&buf).into_owned());
        }
        if buf.len() > 32 * 1024 {
            return Err(EtpError::Io(Error::new(
                ErrorKind::InvalidData,
                "RESP2 line exceeded 32 KiB",
            )));
        }
    }
}
