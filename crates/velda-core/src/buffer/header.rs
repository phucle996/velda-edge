//! Domain-Blind Dual-Representation Header Workspace and Adaptive Object Pool.
//!
//! - **Values `<= 16` bytes (Hardware Register `raw: u128`)**: Integers (`U64`), IP addresses
//!   (`Ipv4`, `Ipv6`), 128-bit identifiers (`Hex128`), and short ASCII tokens (`InlineAscii(len)`)
//!   are stored in a 16-entry L1-resident `NumericHeaderEntry` table indexed by a 64-bit integer
//!   key (`key: u64 = header_key(name)`).
//! - **Values `> 16` bytes (Zero-Copy Pointer)**: Long strings (e.g. JWTs, Cookies, User-Agent)
//!   remain as zero-copy `Bytes` pointers inside the underlying [`HeaderMap`].
//! - **Single-Pass Batch Rendering (`render()`)**: Right before wire transmission, all pending
//!   16-byte register entries are materialized into the [`HeaderMap`] in a single pass.

use std::cell::RefCell;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::ops::Deref;

use http::HeaderMap;
use http::header::{AsHeaderName, HeaderName, HeaderValue, IntoHeaderName};

pub use super::{HeaderPool, HeaderWorkspaceLease};

/// Canonical header name for the W3C TraceContext Request Identifier.
pub const X_REQUEST_ID: HeaderName = HeaderName::from_static("x-request-id");

/// Compile-time 64-bit integer key for `x-request-id`.
pub const KEY_X_REQUEST_ID: u64 = header_key(b"x-request-id");

/// Maximum number of Bit-1 (Bypass on Reject) headers tracked on the stack.
pub const MAX_BYPASS_HEADERS: usize = 8;

/// Maximum number of deferred 16-byte register header mutations buffered before batch rendering.
pub const MAX_NUMERIC_HEADERS: usize = 16;

/// Computes a case-insensitive 64-bit FNV-1a integer key for an HTTP header name at compile time or runtime.
#[inline(always)]
pub const fn header_key(name: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf29ce484222325;
    let mut i = 0;
    while i < name.len() {
        let b = name[i];
        let lower = if b >= b'A' && b <= b'Z' { b + 32 } else { b };
        hash ^= lower as u64;
        hash = hash.wrapping_mul(0x100000001b3);
        i += 1;
    }
    hash
}

const DIGITS_1: [u8; 10] = *b"0123456789";

const DIGITS_2: [u8; 180] = {
    let mut buf = [0u8; 180];
    let mut n = 10usize;
    while n < 100 {
        let idx = (n - 10) * 2;
        buf[idx] = b'0' + ((n / 10) as u8);
        buf[idx + 1] = b'0' + ((n % 10) as u8);
        n += 1;
    }
    buf
};

const DIGITS_3: [u8; 2700] = {
    let mut buf = [0u8; 2700];
    let mut n = 100usize;
    while n < 1000 {
        let idx = (n - 100) * 3;
        buf[idx] = b'0' + ((n / 100) as u8);
        buf[idx + 1] = b'0' + (((n / 10) % 10) as u8);
        buf[idx + 2] = b'0' + ((n % 10) as u8);
        n += 1;
    }
    buf
};

thread_local! {
    static U64_HEADER_CACHE: RefCell<(u64, HeaderValue)> = const {
        RefCell::new((0, HeaderValue::from_static("0")))
    };
}

/// Resolves any unsigned 64-bit integer into an HTTP [`HeaderValue`].
/// Values `0..1000` (including HTTP status codes `100..599` and rate-limit counters) resolve in ~0.3 ns from `.rodata`.
#[inline(always)]
pub fn resolve_u64_header_value(val: u64) -> HeaderValue {
    if val < 10 {
        let idx = val as usize;
        return HeaderValue::from_static(unsafe {
            std::str::from_utf8_unchecked(&DIGITS_1[idx..idx + 1])
        });
    }
    if val < 100 {
        let idx = ((val as usize) - 10) * 2;
        return HeaderValue::from_static(unsafe {
            std::str::from_utf8_unchecked(&DIGITS_2[idx..idx + 2])
        });
    }
    if val < 1000 {
        let idx = ((val as usize) - 100) * 3;
        return HeaderValue::from_static(unsafe {
            std::str::from_utf8_unchecked(&DIGITS_3[idx..idx + 3])
        });
    }

    U64_HEADER_CACHE.with(|cell| {
        let mut cache = cell.borrow_mut();
        if cache.0 == val {
            return cache.1.clone();
        }
        let mut buf = [0u8; 20];
        let mut n = val;
        let mut pos = 20usize;
        while n >= 100 {
            let rem = (n % 100) as usize;
            n /= 100;
            pos -= 2;
            if rem < 10 {
                buf[pos] = b'0';
                buf[pos + 1] = DIGITS_1[rem];
            } else {
                let d_idx = (rem - 10) * 2;
                buf[pos] = DIGITS_2[d_idx];
                buf[pos + 1] = DIGITS_2[d_idx + 1];
            }
        }
        if n < 10 {
            pos -= 1;
            buf[pos] = DIGITS_1[n as usize];
        } else {
            let d_idx = ((n as usize) - 10) * 2;
            pos -= 2;
            buf[pos] = DIGITS_2[d_idx];
            buf[pos + 1] = DIGITS_2[d_idx + 1];
        }
        let hv = HeaderValue::from_bytes(&buf[pos..20])
            .unwrap_or_else(|_| HeaderValue::from_static("0"));
        *cache = (val, hv.clone());
        hv
    })
}

/// Fast 32-character hex parser (`&[u8] -> Option<u128>`).
#[inline]
fn parse_hex_128(bytes: &[u8]) -> Option<u128> {
    if bytes.len() != 32 {
        return None;
    }
    let mut acc = 0u128;
    for &b in bytes {
        let nibble = match b {
            b'0'..=b'9' => (b - b'0') as u128,
            b'a'..=b'f' => (b - b'a' + 10) as u128,
            b'A'..=b'F' => (b - b'A' + 10) as u128,
            _ => return None,
        };
        acc = (acc << 4) | nibble;
    }
    Some(acc)
}

/// Wire rendering format for a 16-byte [`NumericHeaderEntry::raw`] register.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NumericFmt {
    /// 64-bit unsigned decimal integer (`raw as u64`, including HTTP status codes `100..=599`).
    U64,
    /// 128-bit W3C identifier rendered as 32 lowercase hexadecimal characters.
    Hex128,
    /// 32-bit IPv4 address (`raw as u32`).
    Ipv4,
    /// 128-bit IPv6 address (`raw`).
    Ipv6,
    /// Short ASCII string `<= 16` bytes packed little-endian into `raw: u128`, with byte length `0..=16`.
    InlineAscii(u8),
}

/// Mutation semantics for a deferred 16-byte register header entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NumericOp {
    Set,
    Append,
    SetBypass,
}

/// Single 16-byte register header entry held in [`HeaderWorkspace`], indexed by `key: u64`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NumericHeaderEntry {
    pub key: u64,
    pub name: HeaderName,
    pub raw: u128,
    pub fmt: NumericFmt,
    pub op: NumericOp,
}

impl NumericHeaderEntry {
    /// Materializes the 16-byte `raw` integer register into an HTTP [`HeaderValue`].
    #[inline(always)]
    pub fn into_header_value(&self) -> HeaderValue {
        match self.fmt {
            NumericFmt::U64 => resolve_u64_header_value(self.raw as u64),
            NumericFmt::Hex128 => {
                let mut hex = [0u8; 32];
                let hi = (self.raw >> 64) as u64;
                let lo = self.raw as u64;
                let (hi_out, lo_out) = hex.split_at_mut(16);
                crate::request_id::format_hex_16(hi, hi_out.try_into().unwrap());
                crate::request_id::format_hex_16(lo, lo_out.try_into().unwrap());
                HeaderValue::from_bytes(&hex).unwrap_or_else(|_| {
                    HeaderValue::from_static("00000000000000000000000000000000")
                })
            }
            NumericFmt::Ipv4 => {
                let octets = Ipv4Addr::from(self.raw as u32).octets();
                let mut buf = [0u8; 15];
                let mut pos = 0usize;
                for (i, &oct) in octets.iter().enumerate() {
                    if i > 0 {
                        buf[pos] = b'.';
                        pos += 1;
                    }
                    let n = oct as usize;
                    if n < 10 {
                        buf[pos] = DIGITS_1[n];
                        pos += 1;
                    } else if n < 100 {
                        let idx = (n - 10) * 2;
                        buf[pos] = DIGITS_2[idx];
                        buf[pos + 1] = DIGITS_2[idx + 1];
                        pos += 2;
                    } else {
                        let idx = (n - 100) * 3;
                        buf[pos] = DIGITS_3[idx];
                        buf[pos + 1] = DIGITS_3[idx + 1];
                        buf[pos + 2] = DIGITS_3[idx + 2];
                        pos += 3;
                    }
                }
                HeaderValue::from_bytes(&buf[..pos])
                    .unwrap_or_else(|_| HeaderValue::from_static("0.0.0.0"))
            }
            NumericFmt::Ipv6 => {
                use std::io::Write;
                let v6 = Ipv6Addr::from(self.raw);
                let mut buf = [0u8; 46];
                let mut cursor = std::io::Cursor::new(&mut buf[..]);
                let _ = write!(cursor, "{v6}");
                let len = cursor.position() as usize;
                HeaderValue::from_bytes(&buf[..len])
                    .unwrap_or_else(|_| HeaderValue::from_static("::"))
            }
            NumericFmt::InlineAscii(len) => {
                let bytes = self.raw.to_le_bytes();
                let n = (len as usize).min(16);
                HeaderValue::from_bytes(&bytes[..n])
                    .unwrap_or_else(|_| HeaderValue::from_static(""))
            }
        }
    }
}

/// Domain-blind dual-representation workspace holding:
/// - `headers`: Pre-allocated [`HeaderMap`] holding zero-copy pointers for `> 16` byte strings and wire headers.
/// - `numeric_entries`: L1-resident 16-byte integer registers (`raw: u128`) indexed by `key: u64` for all values `<= 16` bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeaderWorkspace {
    headers: HeaderMap,
    pub request_id_raw: u128,
    pub ingress_time_sec: u64,
    numeric_entries: [Option<NumericHeaderEntry>; MAX_NUMERIC_HEADERS],
    pub numeric_len: u8,
    bypass_keys: [Option<HeaderName>; MAX_BYPASS_HEADERS],
    bypass_len: u8,
    initial_capacity: usize,
}

impl HeaderWorkspace {
    pub const X_REQUEST_ID: HeaderName = X_REQUEST_ID;
    pub const DEFAULT_CAPACITY: usize = 32;

    /// Creates and pre-warms a header workspace with specified initial bucket capacity.
    pub fn new(capacity: usize) -> Self {
        let mut headers = HeaderMap::with_capacity(capacity);
        headers.reserve(capacity);
        let (id, req_id) = crate::next_request_id();
        headers.insert(X_REQUEST_ID, req_id);
        let mut bypass_keys = [const { None }; MAX_BYPASS_HEADERS];
        bypass_keys[0] = Some(X_REQUEST_ID);
        Self {
            headers,
            request_id_raw: id.0,
            ingress_time_sec: crate::coarse_now_sec(),
            numeric_entries: [const { None }; MAX_NUMERIC_HEADERS],
            numeric_len: 0,
            bypass_keys,
            bypass_len: 1,
            initial_capacity: capacity,
        }
    }

    /// Creates and pre-warms a header workspace with an explicit Request ID and initial capacity.
    #[inline]
    pub fn with_request_id(req_id: HeaderValue, capacity: usize) -> Self {
        let raw = parse_hex_128(req_id.as_bytes()).unwrap_or(0);
        let mut headers = HeaderMap::with_capacity(capacity);
        headers.reserve(capacity);
        headers.insert(X_REQUEST_ID, req_id);
        let mut bypass_keys = [const { None }; MAX_BYPASS_HEADERS];
        bypass_keys[0] = Some(X_REQUEST_ID);
        Self {
            headers,
            request_id_raw: raw,
            ingress_time_sec: crate::coarse_now_sec(),
            numeric_entries: [const { None }; MAX_NUMERIC_HEADERS],
            numeric_len: 0,
            bypass_keys,
            bypass_len: 1,
            initial_capacity: capacity,
        }
    }

    /// Wraps an existing [`HeaderMap`] into a [`HeaderWorkspace`].
    #[inline]
    pub fn from_headers(mut headers: HeaderMap) -> Self {
        let cap = headers.capacity().max(Self::DEFAULT_CAPACITY);
        let raw = if let Some(existing) = headers.get(&X_REQUEST_ID) {
            parse_hex_128(existing.as_bytes()).unwrap_or(0)
        } else {
            let (id, req_id) = crate::next_request_id();
            headers.insert(X_REQUEST_ID, req_id);
            id.0
        };
        let mut bypass_keys = [const { None }; MAX_BYPASS_HEADERS];
        bypass_keys[0] = Some(X_REQUEST_ID);
        Self {
            headers,
            request_id_raw: raw,
            ingress_time_sec: crate::coarse_now_sec(),
            numeric_entries: [const { None }; MAX_NUMERIC_HEADERS],
            numeric_len: 0,
            bypass_keys,
            bypass_len: 1,
            initial_capacity: cap,
        }
    }

    /// Looks up a 16-byte register entry by its 64-bit integer key (`header_key(name)`).
    #[inline(always)]
    pub fn get_raw_by_key(&self, key: u64) -> Option<(u128, NumericFmt)> {
        let len = self.numeric_len as usize;
        for i in (0..len).rev() {
            if let Some(ref entry) = self.numeric_entries[i]
                && entry.key == key
            {
                return Some((entry.raw, entry.fmt));
            }
        }
        None
    }

    #[inline(always)]
    pub fn contains_key<K: AsHeaderName>(&self, name: K) -> bool {
        let key = header_key(name.as_str().as_bytes());
        if self.get_raw_by_key(key).is_some() {
            return true;
        }
        self.headers.contains_key(name)
    }

    #[inline(always)]
    fn push_numeric(&mut self, name: HeaderName, raw: u128, fmt: NumericFmt, op: NumericOp) {
        let key = header_key(name.as_str().as_bytes());
        if !matches!(op, NumericOp::Append) {
            if key == KEY_X_REQUEST_ID && matches!(fmt, NumericFmt::Hex128) {
                self.request_id_raw = raw;
            }
            let len = self.numeric_len as usize;
            for i in 0..len {
                if let Some(ref mut entry) = self.numeric_entries[i]
                    && entry.key == key
                {
                    entry.raw = raw;
                    entry.fmt = fmt;
                    entry.op = op;
                    return;
                }
            }
        }
        let len = self.numeric_len as usize;
        if len < MAX_NUMERIC_HEADERS {
            self.numeric_entries[len] = Some(NumericHeaderEntry {
                key,
                name,
                raw,
                fmt,
                op,
            });
            self.numeric_len += 1;
        } else {
            self.render();
            self.numeric_entries[0] = Some(NumericHeaderEntry {
                key,
                name,
                raw,
                fmt,
                op,
            });
            self.numeric_len = 1;
        }
    }

    #[inline(always)]
    fn remove_numeric_by_key(&mut self, key: u64) {
        let mut len = self.numeric_len as usize;
        let mut i = 0;
        while i < len {
            if self.numeric_entries[i]
                .as_ref()
                .is_some_and(|e| e.key == key)
            {
                for j in i..len - 1 {
                    self.numeric_entries[j] = self.numeric_entries[j + 1].take();
                }
                self.numeric_entries[len - 1] = None;
                len -= 1;
            } else {
                i += 1;
            }
        }
        self.numeric_len = len as u8;
    }

    #[inline(always)]
    pub fn set_u64(&mut self, name: HeaderName, val: u64) {
        self.push_numeric(name, val as u128, NumericFmt::U64, NumericOp::Set);
    }

    #[inline(always)]
    pub fn append_u64(&mut self, name: HeaderName, val: u64) {
        self.push_numeric(name, val as u128, NumericFmt::U64, NumericOp::Append);
    }

    #[inline]
    pub fn insert_bypass_u64(&mut self, name: HeaderName, val: u64) -> bool {
        let b_len = self.bypass_len as usize;
        let exists = (0..b_len).any(|i| self.bypass_keys[i].as_ref() == Some(&name));
        let ok = if !exists {
            if b_len < MAX_BYPASS_HEADERS {
                self.bypass_keys[b_len] = Some(name.clone());
                self.bypass_len += 1;
                true
            } else {
                false
            }
        } else {
            true
        };
        self.push_numeric(name, val as u128, NumericFmt::U64, NumericOp::SetBypass);
        ok
    }

    #[inline(always)]
    pub fn set_hex128(&mut self, name: HeaderName, id: u128) {
        self.push_numeric(name, id, NumericFmt::Hex128, NumericOp::Set);
    }

    #[inline(always)]
    pub fn append_hex128(&mut self, name: HeaderName, id: u128) {
        self.push_numeric(name, id, NumericFmt::Hex128, NumericOp::Append);
    }

    #[inline(always)]
    pub fn set_ip(&mut self, name: HeaderName, ip: IpAddr) {
        match ip {
            IpAddr::V4(v4) => self.push_numeric(
                name,
                u32::from(v4) as u128,
                NumericFmt::Ipv4,
                NumericOp::Set,
            ),
            IpAddr::V6(v6) => {
                self.push_numeric(name, u128::from(v6), NumericFmt::Ipv6, NumericOp::Set)
            }
        }
    }

    #[inline(always)]
    pub fn append_ip(&mut self, name: HeaderName, ip: IpAddr) {
        match ip {
            IpAddr::V4(v4) => self.push_numeric(
                name,
                u32::from(v4) as u128,
                NumericFmt::Ipv4,
                NumericOp::Append,
            ),
            IpAddr::V6(v6) => {
                self.push_numeric(name, u128::from(v6), NumericFmt::Ipv6, NumericOp::Append)
            }
        }
    }

    #[inline(always)]
    pub fn set_inline_ascii(&mut self, name: HeaderName, bytes: &[u8]) {
        debug_assert!(bytes.len() <= 16);
        let len = bytes.len().min(16);
        let mut buf = [0u8; 16];
        buf[..len].copy_from_slice(&bytes[..len]);
        self.push_numeric(
            name,
            u128::from_le_bytes(buf),
            NumericFmt::InlineAscii(len as u8),
            NumericOp::Set,
        );
    }

    #[inline(always)]
    pub fn append_inline_ascii(&mut self, name: HeaderName, bytes: &[u8]) {
        debug_assert!(bytes.len() <= 16);
        let len = bytes.len().min(16);
        let mut buf = [0u8; 16];
        buf[..len].copy_from_slice(&bytes[..len]);
        self.push_numeric(
            name,
            u128::from_le_bytes(buf),
            NumericFmt::InlineAscii(len as u8),
            NumericOp::Append,
        );
    }

    /// Reads a header as a `u64` integer via 64-bit integer key lookup or branchless wire byte parsing.
    #[inline]
    pub fn get_u64<K: AsHeaderName>(&self, name: K) -> Option<u64> {
        let key = header_key(name.as_str().as_bytes());
        if let Some((raw, NumericFmt::U64)) = self.get_raw_by_key(key) {
            return Some(raw as u64);
        }
        let bytes = self.headers.get(name)?.as_bytes();
        if bytes.is_empty() || bytes.len() > 20 {
            return None;
        }
        let mut acc = 0u64;
        for &b in bytes {
            let d = b.wrapping_sub(b'0');
            if d > 9 {
                return None;
            }
            acc = acc.checked_mul(10)?.checked_add(d as u64)?;
        }
        Some(acc)
    }

    /// Reads a header as a 128-bit hexadecimal integer via 64-bit integer key lookup or wire bytes.
    #[inline]
    pub fn get_hex128<K: AsHeaderName>(&self, name: K) -> Option<u128> {
        let key = header_key(name.as_str().as_bytes());
        if let Some((raw, NumericFmt::Hex128)) = self.get_raw_by_key(key) {
            return Some(raw);
        }
        if key == KEY_X_REQUEST_ID && self.request_id_raw != 0 {
            return Some(self.request_id_raw);
        }
        parse_hex_128(self.headers.get(name)?.as_bytes())
    }

    /// Reads a header as a binary [`IpAddr`] via 64-bit integer key lookup or wire bytes.
    #[inline]
    pub fn get_ip<K: AsHeaderName>(&self, name: K) -> Option<IpAddr> {
        let key = header_key(name.as_str().as_bytes());
        if let Some((raw, fmt)) = self.get_raw_by_key(key) {
            return match fmt {
                NumericFmt::Ipv4 => Some(IpAddr::V4(Ipv4Addr::from(raw as u32))),
                NumericFmt::Ipv6 => Some(IpAddr::V6(Ipv6Addr::from(raw))),
                _ => None,
            };
        }
        let bytes = self.headers.get(name)?.as_bytes();
        let first = match bytes.iter().position(|&b| b == b',') {
            Some(idx) => &bytes[..idx],
            None => bytes,
        };
        std::str::from_utf8(first.trim_ascii())
            .ok()?
            .parse::<IpAddr>()
            .ok()
    }

    /// Batch-renders all pending 16-byte register mutations into the underlying [`HeaderMap`].
    #[inline]
    pub fn render(&mut self) {
        let len = self.numeric_len as usize;
        if len == 0 {
            return;
        }
        for i in 0..len {
            if let Some(entry) = self.numeric_entries[i].take() {
                let hv = entry.into_header_value();
                match entry.op {
                    NumericOp::Set | NumericOp::SetBypass => {
                        self.headers.insert(entry.name, hv);
                    }
                    NumericOp::Append => {
                        self.headers.append(entry.name, hv);
                    }
                }
            }
        }
        self.numeric_len = 0;
    }

    #[inline(always)]
    pub fn into_inner(mut self) -> HeaderMap {
        self.render();
        self.headers
    }

    #[inline]
    pub fn insert<K: IntoHeaderName>(
        &mut self,
        name: K,
        value: HeaderValue,
    ) -> Option<HeaderValue> {
        self.headers.insert(name, value)
    }

    #[inline]
    pub fn append<K: IntoHeaderName>(&mut self, name: K, value: HeaderValue) -> bool {
        self.headers.append(name, value)
    }

    #[inline]
    pub fn reserve(&mut self, additional: usize) {
        self.headers.reserve(additional);
    }

    /// Removes a header from the workspace (protecting Slot 0 `x-request-id`).
    #[inline]
    pub fn remove<K: AsHeaderName>(&mut self, name: K) -> Option<HeaderValue> {
        let key = header_key(name.as_str().as_bytes());
        if key == KEY_X_REQUEST_ID {
            return None;
        }
        if self.numeric_len > 0 {
            self.remove_numeric_by_key(key);
        }
        self.headers.remove(name)
    }

    #[inline(always)]
    pub fn request_id(&self) -> &HeaderValue {
        if let Some(val) = self.headers.get(&X_REQUEST_ID) {
            val
        } else {
            static FALLBACK_ID: HeaderValue =
                HeaderValue::from_static("0000000000000000-0000000000000000");
            &FALLBACK_ID
        }
    }

    #[inline]
    pub fn set_request_id(&mut self, req_id: HeaderValue) {
        if let Some(raw) = parse_hex_128(req_id.as_bytes()) {
            self.request_id_raw = raw;
        }
        self.headers.insert(X_REQUEST_ID, req_id);
        self.bypass_keys[0] = Some(X_REQUEST_ID);
        if self.bypass_len == 0 {
            self.bypass_len = 1;
        }
    }

    #[inline]
    pub fn insert_bypass(&mut self, name: HeaderName, value: HeaderValue) -> bool {
        let len = self.bypass_len as usize;
        let exists = (0..len).any(|i| self.bypass_keys[i].as_ref() == Some(&name));
        let ok = if !exists {
            if len < MAX_BYPASS_HEADERS {
                self.bypass_keys[len] = Some(name.clone());
                self.bypass_len += 1;
                true
            } else {
                false
            }
        } else {
            true
        };
        if self.numeric_len > 0 {
            self.remove_numeric_by_key(header_key(name.as_str().as_bytes()));
        }
        self.headers.insert(name, value);
        ok
    }

    pub fn bypass_headers(&self) -> impl Iterator<Item = (&HeaderName, &HeaderValue)> {
        let len = self.bypass_len as usize;
        (0..len).filter_map(move |i| {
            let key = self.bypass_keys[i].as_ref()?;
            let val = self.headers.get(key)?;
            Some((key, val))
        })
    }

    #[inline(always)]
    pub fn take(&mut self) -> HeaderMap {
        self.render();
        self.bypass_keys = [const { None }; MAX_BYPASS_HEADERS];
        let (id, req_id) = crate::next_request_id();
        self.request_id_raw = id.0;
        self.ingress_time_sec = crate::coarse_now_sec();
        self.bypass_keys[0] = Some(X_REQUEST_ID);
        self.bypass_len = 1;
        let mut new_map = HeaderMap::with_capacity(self.initial_capacity);
        new_map.insert(X_REQUEST_ID, req_id);
        std::mem::replace(&mut self.headers, new_map)
    }

    #[inline(always)]
    pub fn reset(&mut self) {
        for i in 0..(self.numeric_len as usize) {
            self.numeric_entries[i] = None;
        }
        self.numeric_len = 0;
        let (id, req_id) = crate::next_request_id();
        self.request_id_raw = id.0;
        self.ingress_time_sec = crate::coarse_now_sec();
        self.bypass_keys = [const { None }; MAX_BYPASS_HEADERS];
        self.bypass_keys[0] = Some(X_REQUEST_ID);
        self.bypass_len = 1;
        if self.headers.capacity() > self.initial_capacity * 4 {
            self.headers = HeaderMap::with_capacity(self.initial_capacity);
        } else {
            self.headers.clear();
        }
        self.headers.insert(X_REQUEST_ID, req_id);
    }
}

impl Default for HeaderWorkspace {
    #[inline(always)]
    fn default() -> Self {
        Self::new(Self::DEFAULT_CAPACITY)
    }
}

impl Deref for HeaderWorkspace {
    type Target = HeaderMap;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.headers
    }
}

impl AsRef<HeaderMap> for HeaderWorkspace {
    #[inline(always)]
    fn as_ref(&self) -> &HeaderMap {
        &self.headers
    }
}

impl From<HeaderMap> for HeaderWorkspace {
    #[inline(always)]
    fn from(headers: HeaderMap) -> Self {
        Self::from_headers(headers)
    }
}

impl From<HeaderWorkspace> for HeaderMap {
    #[inline(always)]
    fn from(ws: HeaderWorkspace) -> Self {
        ws.into_inner()
    }
}

impl FromIterator<(HeaderName, HeaderValue)> for HeaderWorkspace {
    fn from_iter<T: IntoIterator<Item = (HeaderName, HeaderValue)>>(iter: T) -> Self {
        Self::from_headers(HeaderMap::from_iter(iter))
    }
}

impl<'a> IntoIterator for &'a HeaderWorkspace {
    type Item = (&'a HeaderName, &'a HeaderValue);
    type IntoIter = http::header::Iter<'a, HeaderValue>;

    #[inline(always)]
    fn into_iter(self) -> Self::IntoIter {
        self.headers.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::header::{AUTHORIZATION, CONTENT_TYPE, RETRY_AFTER};

    #[test]
    fn test_header_workspace_recycle_preserves_capacity() {
        let mut ws = HeaderWorkspace::new(32);
        assert!(ws.capacity() >= 32);
        assert_ne!(ws.request_id_raw, 0);
        assert!(ws.ingress_time_sec > 1_700_000_000);

        ws.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        ws.insert(AUTHORIZATION, HeaderValue::from_static("Bearer token123"));
        assert_eq!(ws.len(), 3);
        assert!(ws.remove(&HeaderWorkspace::X_REQUEST_ID).is_none());

        ws.reset();
        assert_eq!(ws.len(), 1);
        assert!(ws.capacity() >= 32);
    }

    #[test]
    fn test_header_workspace_arithmetic_defer_and_batch_render() {
        let mut ws = HeaderWorkspace::new(32);
        let req_id_u128 = ws.request_id_raw;
        let ts = ws.ingress_time_sec;

        let h_limit = HeaderName::from_static("x-ratelimit-limit");
        let h_rem = HeaderName::from_static("x-ratelimit-remaining");
        let h_status = HeaderName::from_static("x-upstream-status");
        let h_trace = HeaderName::from_static("x-trace-id");
        let h_ip = HeaderName::from_static("x-real-ip");
        let h_ts = HeaderName::from_static("x-edge-ts");

        ws.set_u64(h_limit.clone(), 100);
        ws.set_u64(h_rem.clone(), 99);
        ws.set_u64(h_status.clone(), 200);
        ws.set_hex128(h_trace.clone(), req_id_u128);
        ws.set_ip(h_ip.clone(), IpAddr::V4(Ipv4Addr::new(10, 24, 0, 5)));
        ws.set_u64(h_ts.clone(), ts);

        assert_eq!(ws.numeric_len, 6);
        assert_eq!(ws.get_u64(&h_limit), Some(100));
        assert_eq!(ws.get_u64(&h_rem), Some(99));
        assert_eq!(ws.get_u64(&h_status), Some(200));
        assert_eq!(ws.get_hex128(&h_trace), Some(req_id_u128));
        assert_eq!(
            ws.get_ip(&h_ip),
            Some(IpAddr::V4(Ipv4Addr::new(10, 24, 0, 5)))
        );
        assert_eq!(ws.get_u64(&h_ts), Some(ts));
        assert!(ws.contains_key(&h_limit));

        ws.render();
        assert_eq!(ws.numeric_len, 0);
        assert_eq!(ws.get(&h_limit).unwrap(), "100");
        assert_eq!(ws.get(&h_rem).unwrap(), "99");
        assert_eq!(ws.get(&h_status).unwrap(), "200");
        assert_eq!(ws.get(&h_trace).unwrap(), ws.request_id());
        assert_eq!(ws.get(&h_ip).unwrap(), "10.24.0.5");
        assert_eq!(ws.get(&h_ts).unwrap(), &ts.to_string());

        ws.set_ip(h_ip.clone(), IpAddr::V6(Ipv6Addr::LOCALHOST));
        ws.render();
        assert_eq!(ws.get(&h_ip).unwrap(), "::1");

        const KEY_PROTO: u64 = header_key(b"x-forwarded-proto");
        let h_proto = HeaderName::from_static("x-forwarded-proto");
        ws.set_inline_ascii(h_proto.clone(), b"https");
        assert!(matches!(
            ws.get_raw_by_key(KEY_PROTO),
            Some((_, NumericFmt::InlineAscii(5)))
        ));
        ws.render();
        assert_eq!(ws.get(&h_proto).unwrap(), "https");
    }

    #[test]
    fn test_header_workspace_bypass_tagging() {
        let mut ws = HeaderWorkspace::default();
        assert_eq!(ws.bypass_headers().count(), 1);
        ws.insert(
            HeaderName::from_static("x-internal-secret"),
            HeaderValue::from_static("secret123"),
        );
        ws.insert_bypass_u64(RETRY_AFTER, 60);
        assert_eq!(ws.get_u64(RETRY_AFTER), Some(60));
        ws.render();
        assert_eq!(ws.bypass_headers().count(), 2);

        ws.reset();
        assert_eq!(ws.len(), 1);
        assert_eq!(ws.bypass_headers().count(), 1);
    }
}
