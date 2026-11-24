//! RFC 7541 HPACK Header Compression Encoder & Decoder over [`HeaderWorkspace`].
//!
//! ```text
//! RFC 7541 Index Space:
//!   [1 ..= 61]                  ──▶ STATIC_TABLE (RFC 7541 Appendix A, prehashed FNV-1a)
//!   [62 ..= 61 + dyn.len()]     ──▶ Dynamic Table (FIFO VecDeque, size = name + val + 32)
//!
//! 1-Pass HPACK Ingress Decode (`decode_hpack_ingress`):
//!   [pre-seed x-request-id (Bit-63 bypass) + x-real-ip/x-forwarded-*]
//!        │
//!        ▼
//!   [HPACK Wire Loop]
//!     ├─ 1xxxxxxx (7-bit) : Indexed Header Field
//!     ├─ 01xxxxxx (6-bit) : Literal with Incremental Indexing ──▶ insert into Dynamic Table
//!     ├─ 001xxxxx (5-bit) : Dynamic Table Size Update (header-block prefix only)
//!     ├─ 0001xxxx (4-bit) : Literal Never Indexed
//!     └─ 0000xxxx (4-bit) : Literal Without Indexing
//!        │
//!        ├─ :method / :scheme / :authority / :path / :status ──▶ Request/Response Head
//!        ├─ hop-by-hop (RFC 9113 §8.2.2) & spoofed x-forwarded-* ──▶ Skip
//!        ├─ x-forwarded-for ──▶ append to XFF chain ──▶ finalize slot in-place
//!        └─ normal header ──▶ ws.push_prehashed(fnv1a, HeaderName, HeaderValue)
//! ```

use std::borrow::Cow;
use std::collections::VecDeque;

use bytes::{BufMut, Bytes, BytesMut};
use http::header::{HeaderName, HeaderValue};
use http::{Method, StatusCode, Uri};
use velda_core::buffer::header::{
    DEFAULT_HEADER_CAPACITY, HeaderWorkspace, MAX_WIRE_HEADERS, X_REQUEST_ID, header_key,
};

use crate::proto::frame::H2ErrorCode;
use crate::proto::huffman::{decode_huffman_bytes, encode_huffman, encoded_huffman_len};

/// Default HTTP/2 `SETTINGS_HEADER_TABLE_SIZE` per RFC 9113 §6.5.2 (4,096 bytes).
pub const DEFAULT_HEADER_TABLE_SIZE: usize = 4096;

/// RFC 7541 Appendix A Static Table (61 entries, 1-indexed via `index - 1`).
#[rustfmt::skip]
pub static STATIC_TABLE: [(&str, &str); 61] = [
    (":authority", ""),                 // 1
    (":method", "GET"),                 // 2
    (":method", "POST"),                // 3
    (":path", "/"),                     // 4
    (":path", "/index.html"),           // 5
    (":scheme", "http"),                // 6
    (":scheme", "https"),               // 7
    (":status", "200"),                 // 8
    (":status", "204"),                 // 9
    (":status", "206"),                 // 10
    (":status", "304"),                 // 11
    (":status", "400"),                 // 12
    (":status", "404"),                 // 13
    (":status", "500"),                 // 14
    ("accept-charset", ""),             // 15
    ("accept-encoding", "gzip, deflate"), // 16
    ("accept-language", ""),            // 17
    ("accept-ranges", ""),              // 18
    ("accept", ""),                     // 19
    ("access-control-allow-origin", ""), // 20
    ("age", ""),                        // 21
    ("allow", ""),                      // 22
    ("authorization", ""),              // 23
    ("cache-control", ""),              // 24
    ("content-disposition", ""),        // 25
    ("content-encoding", ""),           // 26
    ("content-language", ""),           // 27
    ("content-length", ""),             // 28
    ("content-location", ""),           // 29
    ("content-range", ""),              // 30
    ("content-type", ""),               // 31
    ("cookie", ""),                     // 32
    ("date", ""),                       // 33
    ("etag", ""),                       // 34
    ("expect", ""),                     // 35
    ("expires", ""),                    // 36
    ("from", ""),                       // 37
    ("host", ""),                       // 38
    ("if-match", ""),                   // 39
    ("if-modified-since", ""),          // 40
    ("if-none-match", ""),              // 41
    ("if-range", ""),                   // 42
    ("if-unmodified-since", ""),        // 43
    ("last-modified", ""),              // 44
    ("link", ""),                       // 45
    ("location", ""),                   // 46
    ("max-forwards", ""),               // 47
    ("proxy-authenticate", ""),         // 48
    ("proxy-authorization", ""),        // 49
    ("range", ""),                      // 50
    ("referer", ""),                    // 51
    ("refresh", ""),                    // 52
    ("retry-after", ""),                // 53
    ("server", ""),                     // 54
    ("set-cookie", ""),                 // 55
    ("strict-transport-security", ""),  // 56
    ("transfer-encoding", ""),          // 57
    ("user-agent", ""),                 // 58
    ("vary", ""),                       // 59
    ("via", ""),                        // 60
    ("www-authenticate", ""),           // 61
];

/// Compile-time 63-bit FNV-1a keys for all 61 RFC 7541 static table header names.
const STATIC_TABLE_KEYS: [u64; 61] = {
    let mut keys = [0u64; 61];
    let mut i = 0;
    while i < 61 {
        keys[i] = header_key(STATIC_TABLE[i].0.as_bytes());
        i += 1;
    }
    keys
};

const KEY_X_REQUEST_ID: u64 = header_key(b"x-request-id");
const KEY_HOST: u64 = header_key(b"host");
const KEY_X_REAL_IP: u64 = header_key(b"x-real-ip");
const KEY_X_FORWARDED_FOR: u64 = header_key(b"x-forwarded-for");
const KEY_X_FORWARDED_PROTO: u64 = header_key(b"x-forwarded-proto");
const KEY_X_FORWARDED_PORT: u64 = header_key(b"x-forwarded-port");
const KEY_X_FORWARDED_HOST: u64 = header_key(b"x-forwarded-host");
const KEY_FORWARDED: u64 = header_key(b"forwarded");
const KEY_CONNECTION: u64 = header_key(b"connection");
const KEY_KEEP_ALIVE: u64 = header_key(b"keep-alive");
const KEY_PROXY_CONNECTION: u64 = header_key(b"proxy-connection");
const KEY_TRANSFER_ENCODING: u64 = header_key(b"transfer-encoding");
const KEY_UPGRADE: u64 = header_key(b"upgrade");
const KEY_TE: u64 = header_key(b"te");

const X_REAL_IP: HeaderName = HeaderName::from_static("x-real-ip");
const X_FORWARDED_FOR: HeaderName = HeaderName::from_static("x-forwarded-for");
const X_FORWARDED_PROTO: HeaderName = HeaderName::from_static("x-forwarded-proto");
const X_FORWARDED_PORT: HeaderName = HeaderName::from_static("x-forwarded-port");
const X_FORWARDED_HOST: HeaderName = HeaderName::from_static("x-forwarded-host");
const FORWARDED: HeaderName = HeaderName::from_static("forwarded");

/// HPACK decode error carrying RFC 9113 connection/stream error code (`COMPRESSION_ERROR` / `PROTOCOL_ERROR`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HpackError {
    pub code: H2ErrorCode,
    pub msg: &'static str,
}

impl std::fmt::Display for HpackError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "HPACK error ({:#x}): {}", self.code.0, self.msg)
    }
}

impl std::error::Error for HpackError {}

/// Network address bundle for 1-pass proxy forwarding enrichment during HTTP/2 HPACK ingress decode.
#[derive(Debug, Clone, Copy)]
pub struct H2ForwardingAddrs {
    pub peer_ip: u128,
    pub local_ip: u128,
    pub local_port_be: u16,
    pub is_tls: bool,
}

/// Parameters for 1-pass HTTP/2 HPACK ingress decode pre-seeding `x-request-id` (Bit-63 bypass)
/// and optional proxy forwarding headers (`x-forwarded-*`, `x-real-ip`, `forwarded`).
#[derive(Debug, Clone)]
pub struct H2IngressEnrich {
    pub request_id: HeaderValue,
    pub forwarding: Option<H2ForwardingAddrs>,
}

/// Decoded HTTP/2 request or response header block backed directly by [`HeaderWorkspace`].
#[derive(Debug, Clone)]
pub struct DecodedH2Headers {
    pub method: Option<Method>,
    pub uri: Option<Uri>,
    pub status: Option<StatusCode>,
    pub headers: HeaderWorkspace,
}

impl Default for DecodedH2Headers {
    #[inline]
    fn default() -> Self {
        Self {
            method: None,
            uri: None,
            status: None,
            headers: HeaderWorkspace::empty(DEFAULT_HEADER_CAPACITY),
        }
    }
}

/// Single entry inside an RFC 7541 HPACK dynamic table.
#[derive(Debug, Clone)]
struct DynamicEntry {
    key_hash: u64,
    name: Bytes,
    value: Bytes,
    hname: Option<HeaderName>,
    wire_size: usize,
}

/// RFC 7541 §2.3 / §4 Dynamic Table with FIFO eviction and `name + value + 32` size accounting.
#[derive(Debug, Clone)]
pub struct HpackDynamicTable {
    entries: VecDeque<DynamicEntry>,
    size: usize,
    max_size: usize,
}

impl HpackDynamicTable {
    pub fn new(max_size: usize) -> Self {
        Self {
            entries: VecDeque::new(),
            size: 0,
            max_size,
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    #[inline]
    pub fn size(&self) -> usize {
        self.size
    }

    #[inline]
    pub fn max_size(&self) -> usize {
        self.max_size
    }

    /// Adjusts `max_size` and evicts oldest entries until `size <= max_size` (RFC 7541 §4.3).
    pub fn set_max_size(&mut self, new_max_size: usize) {
        self.max_size = new_max_size;
        while self.size > self.max_size {
            let Some(evicted) = self.entries.pop_back() else {
                self.size = 0;
                break;
            };
            self.size = self.size.saturating_sub(evicted.wire_size);
        }
    }

    /// Inserts `(name, value)` at the front of the dynamic table (RFC 7541 §4.4).
    /// Entries larger than `max_size` clear the table without causing an error.
    pub fn insert(&mut self, name: Bytes, value: Bytes, hname: Option<HeaderName>, key_hash: u64) {
        let entry_size = name.len().saturating_add(value.len()).saturating_add(32);
        if entry_size > self.max_size {
            self.entries.clear();
            self.size = 0;
            return;
        }
        while self.size + entry_size > self.max_size {
            let Some(evicted) = self.entries.pop_back() else {
                self.size = 0;
                break;
            };
            self.size = self.size.saturating_sub(evicted.wire_size);
        }
        self.entries.push_front(DynamicEntry {
            key_hash,
            name,
            value,
            hname,
            wire_size: entry_size,
        });
        self.size += entry_size;
    }

    /// Searches dynamic table for matching `(name, value)` or `name` only.
    /// Returns `(hpack_index, exact_value_match)` where `hpack_index >= 62`.
    #[inline]
    fn find(&self, name: &[u8], value: &[u8]) -> Option<(usize, bool)> {
        let mut name_only_match = None;
        for (i, entry) in self.entries.iter().enumerate() {
            if entry.name.as_ref() == name {
                let hpack_idx = 62 + i;
                if entry.value.as_ref() == value {
                    return Some((hpack_idx, true));
                }
                if name_only_match.is_none() {
                    name_only_match = Some((hpack_idx, false));
                }
            }
        }
        name_only_match
    }
}

/// Stateful RFC 7541 HPACK Decoder maintaining the peer-synchronized dynamic table.
#[derive(Debug, Clone)]
pub struct HpackDecoder {
    table: HpackDynamicTable,
    protocol_max_table_size: usize,
    max_header_list_size: usize,
}

impl Default for HpackDecoder {
    fn default() -> Self {
        Self::new(DEFAULT_HEADER_TABLE_SIZE, 64 * 1024)
    }
}

impl HpackDecoder {
    /// Creates an HPACK decoder with negotiated `SETTINGS_HEADER_TABLE_SIZE` and
    /// `SETTINGS_MAX_HEADER_LIST_SIZE`.
    pub fn new(max_table_size: usize, max_header_list_size: usize) -> Self {
        Self {
            table: HpackDynamicTable::new(max_table_size),
            protocol_max_table_size: max_table_size,
            max_header_list_size,
        }
    }

    /// Updates local `SETTINGS_HEADER_TABLE_SIZE` ceiling.
    pub fn set_max_table_size(&mut self, max_table_size: usize) {
        self.protocol_max_table_size = max_table_size;
        if self.table.max_size() > max_table_size {
            self.table.set_max_size(max_table_size);
        }
    }

    #[inline]
    pub fn dynamic_table(&self) -> &HpackDynamicTable {
        &self.table
    }

    /// Decodes a complete HPACK header block directly into [`HeaderWorkspace`].
    pub fn decode(&mut self, block: &[u8]) -> Result<DecodedH2Headers, HpackError> {
        self.decode_internal(block, None)
    }

    /// Single-pass ingress HPACK decode pre-seeding `x-request-id` (Bit-63 bypass),
    /// optional proxy forwarding headers (`x-real-ip`, `x-forwarded-*`, `forwarded`),
    /// and stripping RFC 9113 §8.2.2 hop-by-hop + spoofed forwarding headers.
    pub fn decode_ingress(
        &mut self,
        block: &[u8],
        enrich: H2IngressEnrich,
    ) -> Result<DecodedH2Headers, HpackError> {
        self.decode_internal(block, Some(enrich))
    }

    fn decode_internal(
        &mut self,
        mut slice: &[u8],
        enrich: Option<H2IngressEnrich>,
    ) -> Result<DecodedH2Headers, HpackError> {
        let mut decoded = DecodedH2Headers::default();
        let is_ingress = enrich.is_some();
        let forwarding = enrich.as_ref().and_then(|e| e.forwarding);
        let has_forwarding = forwarding.is_some();
        let mut rem_slots = MAX_WIRE_HEADERS;

        let mut peer_ip_buf = [0u8; 48];
        let mut peer_ip_len = 0usize;
        let mut xff_slot_idx: Option<usize> = None;
        let mut xff_chain: Vec<u8> = Vec::new();

        if let Some(meta) = enrich {
            decoded
                .headers
                .push_prehashed_bypass(KEY_X_REQUEST_ID, X_REQUEST_ID, meta.request_id);
            if let Some(fwd) = meta.forwarding {
                let (ip_buf, ip_len) = velda_core::lut::ip::format_ip_bytes(fwd.peer_ip);
                let len = ip_len.min(peer_ip_buf.len());
                peer_ip_buf[..len].copy_from_slice(&ip_buf[..len]);
                peer_ip_len = len;

                let proto = if fwd.is_tls {
                    HeaderValue::from_static("https")
                } else {
                    HeaderValue::from_static("http")
                };

                if let Ok(ip_val) = HeaderValue::from_bytes(&peer_ip_buf[..len]) {
                    decoded
                        .headers
                        .push_prehashed(KEY_X_REAL_IP, X_REAL_IP, ip_val.clone());
                    xff_slot_idx = Some(decoded.headers.len());
                    decoded
                        .headers
                        .push_prehashed(KEY_X_FORWARDED_FOR, X_FORWARDED_FOR, ip_val);
                }
                decoded
                    .headers
                    .push_prehashed(KEY_X_FORWARDED_PROTO, X_FORWARDED_PROTO, proto);
                decoded.headers.push_prehashed(
                    KEY_X_FORWARDED_PORT,
                    X_FORWARDED_PORT,
                    HeaderValue::from(u16::from_be(fwd.local_port_be)),
                );
                // Reserve 1 (`x-request-id`) + 4 pre-seeded + 2 (`x-forwarded-host`, `forwarded`).
                rem_slots -= 7;
            } else {
                rem_slots -= 1;
            }
        }

        let mut saw_regular_header = false;
        let mut allow_table_size_update = true;
        let mut header_list_bytes = 0usize;
        let mut pseudo_mask = 0u8;
        let mut raw_path: Option<String> = None;
        let mut raw_authority: Option<String> = None;
        let mut raw_scheme: Option<String> = None;

        while !slice.is_empty() {
            let first = slice[0];

            if (first & 0x80) != 0 {
                // RFC 7541 §6.1: Indexed Header Field (1xxxxxxx)
                allow_table_size_update = false;
                let (idx, consumed) = decode_prefixed_int(slice, 7).ok_or(HpackError {
                    code: H2ErrorCode::COMPRESSION_ERROR,
                    msg: "malformed HPACK indexed field integer",
                })?;
                slice = &slice[consumed..];

                let idx_usize = usize::try_from(idx).map_err(|_| HpackError {
                    code: H2ErrorCode::COMPRESSION_ERROR,
                    msg: "HPACK index overflow",
                })?;
                let resolved = self.resolve_indexed(idx_usize)?;
                header_list_bytes = account_header_size(
                    header_list_bytes,
                    resolved.wire_size,
                    self.max_header_list_size,
                )?;

                apply_resolved_header(
                    resolved,
                    is_ingress,
                    has_forwarding,
                    &mut saw_regular_header,
                    &mut pseudo_mask,
                    &mut rem_slots,
                    &mut decoded,
                    &mut xff_chain,
                    &mut raw_path,
                    &mut raw_authority,
                    &mut raw_scheme,
                )?;
            } else if (first & 0xc0) == 0x40 {
                // RFC 7541 §6.2.1: Literal Header Field with Incremental Indexing (01xxxxxx)
                allow_table_size_update = false;
                let (name_idx, consumed) = decode_prefixed_int(slice, 6).ok_or(HpackError {
                    code: H2ErrorCode::COMPRESSION_ERROR,
                    msg: "malformed HPACK incremental-indexing integer",
                })?;
                slice = &slice[consumed..];

                let (name_bytes, name_key, pre_hname) =
                    self.decode_name_spec(name_idx, &mut slice)?;
                let val_bytes = decode_hpack_string(&mut slice)?;
                let wire_size = name_bytes
                    .len()
                    .saturating_add(val_bytes.len())
                    .saturating_add(32);
                header_list_bytes =
                    account_header_size(header_list_bytes, wire_size, self.max_header_list_size)?;

                self.table.insert(
                    name_bytes.clone(),
                    val_bytes.clone(),
                    pre_hname.clone(),
                    name_key,
                );

                let resolved = ResolvedHeader {
                    key_hash: name_key,
                    name_bytes,
                    val_bytes,
                    hname: pre_hname,
                    wire_size,
                };
                apply_resolved_header(
                    resolved,
                    is_ingress,
                    has_forwarding,
                    &mut saw_regular_header,
                    &mut pseudo_mask,
                    &mut rem_slots,
                    &mut decoded,
                    &mut xff_chain,
                    &mut raw_path,
                    &mut raw_authority,
                    &mut raw_scheme,
                )?;
            } else if (first & 0xe0) == 0x20 {
                // RFC 7541 §6.3: Dynamic Table Size Update (001xxxxx)
                if !allow_table_size_update {
                    return Err(HpackError {
                        code: H2ErrorCode::COMPRESSION_ERROR,
                        msg: "dynamic table size update after header field",
                    });
                }
                let (new_size_u64, consumed) = decode_prefixed_int(slice, 5).ok_or(HpackError {
                    code: H2ErrorCode::COMPRESSION_ERROR,
                    msg: "malformed HPACK dynamic table size update",
                })?;
                slice = &slice[consumed..];

                let new_size = usize::try_from(new_size_u64).map_err(|_| HpackError {
                    code: H2ErrorCode::COMPRESSION_ERROR,
                    msg: "dynamic table size overflow",
                })?;
                if new_size > self.protocol_max_table_size {
                    return Err(HpackError {
                        code: H2ErrorCode::COMPRESSION_ERROR,
                        msg: "dynamic table size exceeds SETTINGS_HEADER_TABLE_SIZE",
                    });
                }
                self.table.set_max_size(new_size);
            } else {
                // RFC 7541 §6.2.2 (0000xxxx Without Indexing) & §6.2.3 (0001xxxx Never Indexed)
                allow_table_size_update = false;
                let (name_idx, consumed) = decode_prefixed_int(slice, 4).ok_or(HpackError {
                    code: H2ErrorCode::COMPRESSION_ERROR,
                    msg: "malformed HPACK literal integer",
                })?;
                slice = &slice[consumed..];

                let (name_bytes, name_key, pre_hname) =
                    self.decode_name_spec(name_idx, &mut slice)?;
                let val_bytes = decode_hpack_string(&mut slice)?;
                let wire_size = name_bytes
                    .len()
                    .saturating_add(val_bytes.len())
                    .saturating_add(32);
                header_list_bytes =
                    account_header_size(header_list_bytes, wire_size, self.max_header_list_size)?;

                let resolved = ResolvedHeader {
                    key_hash: name_key,
                    name_bytes,
                    val_bytes,
                    hname: pre_hname,
                    wire_size,
                };
                apply_resolved_header(
                    resolved,
                    is_ingress,
                    has_forwarding,
                    &mut saw_regular_header,
                    &mut pseudo_mask,
                    &mut rem_slots,
                    &mut decoded,
                    &mut xff_chain,
                    &mut raw_path,
                    &mut raw_authority,
                    &mut raw_scheme,
                )?;
            }
        }

        // Assemble URI from pseudo-headers (same rule as HTTP/3 & HTTP/2)
        if let Some(path) = raw_path {
            let uri_str = if let (Some(scheme), Some(auth)) = (raw_scheme, raw_authority) {
                Cow::Owned(format!("{scheme}://{auth}{path}"))
            } else {
                Cow::Borrowed(path.as_str())
            };
            if let Ok(u) = uri_str.parse::<Uri>() {
                decoded.uri = Some(u);
            }
        } else if let Some(auth) = raw_authority
            && let Ok(u) = auth.parse::<Uri>()
        {
            decoded.uri = Some(u);
        }

        if let Some(fwd) = forwarding {
            if let Some(idx) = xff_slot_idx
                && !xff_chain.is_empty()
                && peer_ip_len > 0
            {
                xff_chain.extend_from_slice(b", ");
                xff_chain.extend_from_slice(&peer_ip_buf[..peer_ip_len]);
                if let Ok(comb_val) = HeaderValue::from_maybe_shared(Bytes::from(xff_chain)) {
                    decoded.headers.set_value_at(idx, comb_val);
                }
            }

            let host_hdr = decoded.headers.get(http::header::HOST).cloned();
            let host_str = host_hdr.as_ref().and_then(|v| v.to_str().ok()).or_else(|| {
                decoded
                    .uri
                    .as_ref()
                    .and_then(|u| u.authority().map(|a| a.as_str()).or_else(|| u.host()))
            });

            if let Some(h) = host_str
                && let Ok(hv) = HeaderValue::from_str(h)
            {
                decoded
                    .headers
                    .push_prehashed(KEY_X_FORWARDED_HOST, X_FORWARDED_HOST, hv);
            }

            let mut fwd_buf = [0u8; 256];
            let mut rem = &mut fwd_buf[..];
            let push = |dst: &mut &mut [u8], src: &[u8]| {
                if let Some((slot, rest)) = std::mem::take(dst).split_at_mut_checked(src.len()) {
                    slot.copy_from_slice(src);
                    *dst = rest;
                }
            };

            let peer_bytes = &peer_ip_buf[..peer_ip_len];
            push(&mut rem, b"for=");
            if velda_core::lut::ip::is_v4_u128(fwd.peer_ip) {
                push(&mut rem, peer_bytes);
            } else {
                push(&mut rem, b"\"[");
                push(&mut rem, peer_bytes);
                push(&mut rem, b"]\"");
            }

            if fwd.is_tls {
                push(&mut rem, b";proto=https;by=");
            } else {
                push(&mut rem, b";proto=http;by=");
            }
            let (local_buf, local_len) = velda_core::lut::ip::format_ip_bytes(fwd.local_ip);
            if let Some(local_bytes) = local_buf.get(..local_len) {
                if velda_core::lut::ip::is_v4_u128(fwd.local_ip) {
                    push(&mut rem, local_bytes);
                } else {
                    push(&mut rem, b"\"[");
                    push(&mut rem, local_bytes);
                    push(&mut rem, b"]\"");
                }
            }

            if let Some(h) = host_str {
                let h_bytes = h.as_bytes();
                if rem.len() >= 8 + h_bytes.len() {
                    push(&mut rem, b";host=\"");
                    push(&mut rem, h_bytes);
                    push(&mut rem, b"\"");
                }
            }

            let written = 256 - rem.len();
            if let Some(fwd_bytes) = fwd_buf.get(..written)
                && let Ok(val) = HeaderValue::from_bytes(fwd_bytes)
            {
                decoded
                    .headers
                    .push_prehashed(KEY_FORWARDED, FORWARDED, val);
            }
        }

        Ok(decoded)
    }

    fn resolve_indexed(&self, idx: usize) -> Result<ResolvedHeader, HpackError> {
        if idx == 0 {
            return Err(HpackError {
                code: H2ErrorCode::COMPRESSION_ERROR,
                msg: "HPACK index 0 is illegal",
            });
        }
        if idx <= STATIC_TABLE.len() {
            let t_idx = idx - 1;
            let (name, val) = STATIC_TABLE[t_idx];
            let hname = (!name.starts_with(':')).then(|| HeaderName::from_static(name));
            let wire_size = name.len() + val.len() + 32;
            return Ok(ResolvedHeader {
                key_hash: STATIC_TABLE_KEYS[t_idx],
                name_bytes: Bytes::from_static(name.as_bytes()),
                val_bytes: Bytes::from_static(val.as_bytes()),
                hname,
                wire_size,
            });
        }
        let dyn_idx = idx - STATIC_TABLE.len() - 1;
        let entry = self.table.entries.get(dyn_idx).ok_or(HpackError {
            code: H2ErrorCode::COMPRESSION_ERROR,
            msg: "HPACK dynamic table index out of bounds",
        })?;
        Ok(ResolvedHeader {
            key_hash: entry.key_hash,
            name_bytes: entry.name.clone(),
            val_bytes: entry.value.clone(),
            hname: entry.hname.clone(),
            wire_size: entry.wire_size,
        })
    }

    fn decode_name_spec(
        &self,
        name_idx: u64,
        slice: &mut &[u8],
    ) -> Result<(Bytes, u64, Option<HeaderName>), HpackError> {
        if name_idx == 0 {
            let name_bytes = decode_hpack_string(slice)?;
            if name_bytes.iter().any(|b| b.is_ascii_uppercase()) {
                return Err(HpackError {
                    code: H2ErrorCode::PROTOCOL_ERROR,
                    msg: "HTTP/2 header name contains uppercase ASCII",
                });
            }
            let hname = (!name_bytes.starts_with(b":"))
                .then(|| HeaderName::from_bytes(&name_bytes).ok())
                .flatten();
            if !name_bytes.starts_with(b":") && hname.is_none() {
                return Err(HpackError {
                    code: H2ErrorCode::PROTOCOL_ERROR,
                    msg: "invalid HTTP/2 header field name",
                });
            }
            let key_hash = header_key(&name_bytes);
            return Ok((name_bytes, key_hash, hname));
        }

        let idx_usize = usize::try_from(name_idx).map_err(|_| HpackError {
            code: H2ErrorCode::COMPRESSION_ERROR,
            msg: "HPACK name index overflow",
        })?;
        if idx_usize <= STATIC_TABLE.len() {
            let t_idx = idx_usize - 1;
            let (name, _) = STATIC_TABLE[t_idx];
            let hname = (!name.starts_with(':')).then(|| HeaderName::from_static(name));
            return Ok((
                Bytes::from_static(name.as_bytes()),
                STATIC_TABLE_KEYS[t_idx],
                hname,
            ));
        }
        let dyn_idx = idx_usize - STATIC_TABLE.len() - 1;
        let entry = self.table.entries.get(dyn_idx).ok_or(HpackError {
            code: H2ErrorCode::COMPRESSION_ERROR,
            msg: "HPACK dynamic name index out of bounds",
        })?;
        Ok((entry.name.clone(), entry.key_hash, entry.hname.clone()))
    }
}

struct ResolvedHeader {
    key_hash: u64,
    name_bytes: Bytes,
    val_bytes: Bytes,
    hname: Option<HeaderName>,
    wire_size: usize,
}

#[inline]
fn account_header_size(current: usize, entry: usize, limit: usize) -> Result<usize, HpackError> {
    let next = current.saturating_add(entry);
    if next > limit {
        return Err(HpackError {
            code: H2ErrorCode::ENHANCE_YOUR_CALM,
            msg: "decoded header list exceeds max_header_list_size",
        });
    }
    Ok(next)
}

#[allow(clippy::too_many_arguments)]
#[inline]
fn apply_resolved_header(
    resolved: ResolvedHeader,
    is_ingress: bool,
    has_forwarding: bool,
    saw_regular_header: &mut bool,
    pseudo_mask: &mut u8,
    rem_slots: &mut usize,
    decoded: &mut DecodedH2Headers,
    xff_chain: &mut Vec<u8>,
    raw_path: &mut Option<String>,
    raw_authority: &mut Option<String>,
    raw_scheme: &mut Option<String>,
) -> Result<(), HpackError> {
    if resolved.name_bytes.starts_with(b":") {
        if *saw_regular_header {
            return Err(HpackError {
                code: H2ErrorCode::PROTOCOL_ERROR,
                msg: "pseudo-header appeared after regular header",
            });
        }
        match resolved.name_bytes.as_ref() {
            b":method" => {
                if (*pseudo_mask & 0x01) != 0 {
                    return Err(HpackError {
                        code: H2ErrorCode::PROTOCOL_ERROR,
                        msg: "duplicate :method pseudo-header",
                    });
                }
                *pseudo_mask |= 0x01;
                decoded.method =
                    Some(
                        Method::from_bytes(&resolved.val_bytes).map_err(|_| HpackError {
                            code: H2ErrorCode::PROTOCOL_ERROR,
                            msg: "invalid :method pseudo-header",
                        })?,
                    );
            }
            b":path" => {
                if (*pseudo_mask & 0x02) != 0 || resolved.val_bytes.is_empty() {
                    return Err(HpackError {
                        code: H2ErrorCode::PROTOCOL_ERROR,
                        msg: "invalid or duplicate :path pseudo-header",
                    });
                }
                *pseudo_mask |= 0x02;
                let s = std::str::from_utf8(&resolved.val_bytes).map_err(|_| HpackError {
                    code: H2ErrorCode::PROTOCOL_ERROR,
                    msg: "non-UTF8 :path pseudo-header",
                })?;
                *raw_path = Some(s.to_owned());
            }
            b":scheme" => {
                if (*pseudo_mask & 0x04) != 0 {
                    return Err(HpackError {
                        code: H2ErrorCode::PROTOCOL_ERROR,
                        msg: "duplicate :scheme pseudo-header",
                    });
                }
                *pseudo_mask |= 0x04;
                let s = std::str::from_utf8(&resolved.val_bytes).map_err(|_| HpackError {
                    code: H2ErrorCode::PROTOCOL_ERROR,
                    msg: "non-UTF8 :scheme pseudo-header",
                })?;
                *raw_scheme = Some(s.to_owned());
            }
            b":authority" => {
                if (*pseudo_mask & 0x08) != 0 {
                    return Err(HpackError {
                        code: H2ErrorCode::PROTOCOL_ERROR,
                        msg: "duplicate :authority pseudo-header",
                    });
                }
                *pseudo_mask |= 0x08;
                let s = std::str::from_utf8(&resolved.val_bytes).map_err(|_| HpackError {
                    code: H2ErrorCode::PROTOCOL_ERROR,
                    msg: "non-UTF8 :authority pseudo-header",
                })?;
                *raw_authority = Some(s.to_owned());
                if let Ok(hv) = HeaderValue::from_maybe_shared(resolved.val_bytes)
                    && !decoded.headers.contains_key(http::header::HOST)
                {
                    let next = rem_slots.checked_sub(1).ok_or(HpackError {
                        code: H2ErrorCode::ENHANCE_YOUR_CALM,
                        msg: "header count exceeds MAX_WIRE_HEADERS",
                    })?;
                    *rem_slots = next;
                    decoded
                        .headers
                        .push_prehashed(KEY_HOST, http::header::HOST, hv);
                }
            }
            b":status" => {
                if (*pseudo_mask & 0x10) != 0 {
                    return Err(HpackError {
                        code: H2ErrorCode::PROTOCOL_ERROR,
                        msg: "duplicate :status pseudo-header",
                    });
                }
                *pseudo_mask |= 0x10;
                let s = std::str::from_utf8(&resolved.val_bytes).map_err(|_| HpackError {
                    code: H2ErrorCode::PROTOCOL_ERROR,
                    msg: "non-UTF8 :status pseudo-header",
                })?;
                let code = s.parse::<u16>().map_err(|_| HpackError {
                    code: H2ErrorCode::PROTOCOL_ERROR,
                    msg: "invalid :status code",
                })?;
                let sc = StatusCode::from_u16(code).map_err(|_| HpackError {
                    code: H2ErrorCode::PROTOCOL_ERROR,
                    msg: "invalid :status range",
                })?;
                decoded.status = Some(sc);
            }
            _ => {
                return Err(HpackError {
                    code: H2ErrorCode::PROTOCOL_ERROR,
                    msg: "unknown HTTP/2 pseudo-header",
                });
            }
        }
        return Ok(());
    }

    *saw_regular_header = true;
    let Some(hname) = resolved.hname else {
        return Err(HpackError {
            code: H2ErrorCode::PROTOCOL_ERROR,
            msg: "invalid HTTP/2 header field name",
        });
    };

    if is_ingress {
        if resolved.key_hash == KEY_X_REQUEST_ID && hname == X_REQUEST_ID {
            return Ok(());
        }
        // Strip RFC 9113 §8.2.2 hop-by-hop headers unconditionally at ingress/egress
        if (resolved.key_hash == KEY_CONNECTION && hname == http::header::CONNECTION)
            || (resolved.key_hash == KEY_KEEP_ALIVE && hname.as_str() == "keep-alive")
            || (resolved.key_hash == KEY_PROXY_CONNECTION && hname.as_str() == "proxy-connection")
            || (resolved.key_hash == KEY_TRANSFER_ENCODING
                && hname == http::header::TRANSFER_ENCODING)
            || (resolved.key_hash == KEY_UPGRADE && hname == http::header::UPGRADE)
        {
            return Ok(());
        }
        if has_forwarding {
            if resolved.key_hash == KEY_X_FORWARDED_FOR && hname == X_FORWARDED_FOR {
                if !xff_chain.is_empty() {
                    xff_chain.extend_from_slice(b", ");
                }
                xff_chain.extend_from_slice(&resolved.val_bytes);
                return Ok(());
            }
            if (resolved.key_hash == KEY_X_REAL_IP && hname == X_REAL_IP)
                || (resolved.key_hash == KEY_X_FORWARDED_PROTO && hname == X_FORWARDED_PROTO)
                || (resolved.key_hash == KEY_X_FORWARDED_PORT && hname == X_FORWARDED_PORT)
                || (resolved.key_hash == KEY_X_FORWARDED_HOST && hname == X_FORWARDED_HOST)
                || (resolved.key_hash == KEY_FORWARDED && hname == FORWARDED)
            {
                return Ok(());
            }
        }
    }

    let hval = HeaderValue::from_maybe_shared(resolved.val_bytes).map_err(|_| HpackError {
        code: H2ErrorCode::PROTOCOL_ERROR,
        msg: "invalid HTTP/2 header value bytes",
    })?;

    if is_ingress
        && resolved.key_hash == KEY_TE
        && hname == http::header::TE
        && !hval
            .to_str()
            .is_ok_and(|s| s.trim().eq_ignore_ascii_case("trailers"))
    {
        return Ok(());
    }

    let next = rem_slots.checked_sub(1).ok_or(HpackError {
        code: H2ErrorCode::ENHANCE_YOUR_CALM,
        msg: "header count exceeds MAX_WIRE_HEADERS",
    })?;
    *rem_slots = next;

    decoded
        .headers
        .push_prehashed(resolved.key_hash, hname, hval);
    Ok(())
}

/// Stateful RFC 7541 HPACK Encoder with dynamic table tracking and optional Huffman compression.
#[derive(Debug, Clone)]
pub struct HpackEncoder {
    table: HpackDynamicTable,
    peer_max_table_size: usize,
}

impl Default for HpackEncoder {
    fn default() -> Self {
        Self::new(DEFAULT_HEADER_TABLE_SIZE)
    }
}

impl HpackEncoder {
    pub fn new(peer_max_table_size: usize) -> Self {
        Self {
            table: HpackDynamicTable::new(peer_max_table_size),
            peer_max_table_size,
        }
    }

    pub fn set_peer_max_table_size(&mut self, max_size: usize) {
        self.peer_max_table_size = max_size;
        if self.table.max_size() > max_size {
            self.table.set_max_size(max_size);
        }
    }

    /// Encodes HTTP/2 request pseudo-headers + [`HeaderWorkspace`] headers into `dst`.
    /// Strips RFC 9113 §8.2.2 disallowed hop-by-hop headers in the same pass.
    pub fn encode_request(
        &mut self,
        method: &Method,
        uri: &Uri,
        headers: &HeaderWorkspace,
        dst: &mut BytesMut,
    ) {
        // 1. :method
        match *method {
            Method::GET => encode_prefixed_int(2, 7, 0x80, dst),
            Method::POST => encode_prefixed_int(3, 7, 0x80, dst),
            _ => self.encode_pair(b":method", method.as_str().as_bytes(), dst),
        }

        // 2. :scheme
        let scheme = uri.scheme_str().unwrap_or("https");
        match scheme {
            "http" => encode_prefixed_int(6, 7, 0x80, dst),
            "https" => encode_prefixed_int(7, 7, 0x80, dst),
            _ => self.encode_pair(b":scheme", scheme.as_bytes(), dst),
        }

        // 3. :path
        if *method != Method::CONNECT {
            let path = uri.path_and_query().map(|pq| pq.as_str()).unwrap_or("/");
            match path {
                "/" => encode_prefixed_int(4, 7, 0x80, dst),
                "/index.html" => encode_prefixed_int(5, 7, 0x80, dst),
                _ => self.encode_pair(b":path", path.as_bytes(), dst),
            }
        }

        // 4. :authority
        if let Some(auth) = uri.authority() {
            self.encode_pair(b":authority", auth.as_str().as_bytes(), dst);
        } else if let Some(host) = headers.get(http::header::HOST) {
            self.encode_pair(b":authority", host.as_bytes(), dst);
        }

        // 5. Regular headers (skip HOST and disallowed RFC 9113 §8.2.2 hop-by-hop headers)
        self.encode_workspace_headers(headers, true, dst);
    }

    /// Encodes HTTP/2 response `:status` + [`HeaderWorkspace`] headers into `dst`.
    pub fn encode_response(
        &mut self,
        status: StatusCode,
        headers: &HeaderWorkspace,
        dst: &mut BytesMut,
    ) {
        let static_idx = match status.as_u16() {
            200 => Some(8),
            204 => Some(9),
            206 => Some(10),
            304 => Some(11),
            400 => Some(12),
            404 => Some(13),
            500 => Some(14),
            _ => None,
        };
        if let Some(idx) = static_idx {
            encode_prefixed_int(idx, 7, 0x80, dst);
        } else {
            self.encode_pair(b":status", status.as_str().as_bytes(), dst);
        }

        self.encode_workspace_headers(headers, false, dst);
    }

    /// Encodes trailing [`HeaderWorkspace`] entries (without pseudo-headers) into `dst`.
    pub fn encode_trailers(&mut self, trailers: &HeaderWorkspace, dst: &mut BytesMut) {
        self.encode_workspace_headers(trailers, false, dst);
    }

    fn encode_workspace_headers(
        &mut self,
        headers: &HeaderWorkspace,
        skip_host: bool,
        dst: &mut BytesMut,
    ) {
        for (name, val) in headers.iter() {
            let s = name.as_str();
            if s.starts_with(':') {
                continue;
            }
            if skip_host && name == http::header::HOST {
                continue;
            }
            if is_disallowed_wire_header(name, val) {
                continue;
            }
            self.encode_pair(s.as_bytes(), val.as_bytes(), dst);
        }
    }

    /// Encodes one header `(name, value)` using static/dynamic indexed match or incremental indexing.
    pub fn encode_pair(&mut self, name: &[u8], value: &[u8], dst: &mut BytesMut) {
        let static_hit = lookup_static_for_encode(name, value);
        if let Some((idx, true)) = static_hit {
            encode_prefixed_int(idx as u64, 7, 0x80, dst);
            return;
        }
        let dyn_hit = self.table.find(name, value);
        if let Some((dyn_idx, true)) = dyn_hit {
            encode_prefixed_int(dyn_idx as u64, 7, 0x80, dst);
            return;
        }

        let name_idx = static_hit
            .map(|(i, _)| i)
            .or_else(|| dyn_hit.map(|(i, _)| i));

        // Use incremental indexing when the pair fits comfortably in dynamic table
        let entry_size = name.len().saturating_add(value.len()).saturating_add(32);
        let should_index = entry_size <= self.table.max_size()
            && name != b"authorization"
            && name != b"cookie"
            && name != b"set-cookie";

        let (prefix_bits, prefix_mask) = if should_index { (6, 0x40) } else { (4, 0x00) };

        if let Some(idx) = name_idx {
            encode_prefixed_int(idx as u64, prefix_bits, prefix_mask, dst);
        } else {
            encode_prefixed_int(0, prefix_bits, prefix_mask, dst);
            encode_hpack_string(name, dst);
        }
        encode_hpack_string(value, dst);

        if should_index {
            let hname = (!name.starts_with(b":"))
                .then(|| HeaderName::from_bytes(name).ok())
                .flatten();
            let key_hash = header_key(name);
            self.table.insert(
                Bytes::copy_from_slice(name),
                Bytes::copy_from_slice(value),
                hname,
                key_hash,
            );
        }
    }
}

#[inline]
fn is_disallowed_wire_header(name: &HeaderName, val: &HeaderValue) -> bool {
    let s = name.as_str();
    match s.len() {
        2 if name == http::header::TE => !val
            .to_str()
            .is_ok_and(|v| v.trim().eq_ignore_ascii_case("trailers")),
        7 => name == http::header::UPGRADE,
        10 => name == http::header::CONNECTION || s == "keep-alive",
        16 => s == "proxy-connection",
        17 => name == http::header::TRANSFER_ENCODING,
        _ => false,
    }
}

/// Encodes an RFC 7541 `:status` pseudo-header without modifying the dynamic table.
#[inline]
pub fn encode_stateless_status(status: StatusCode, dst: &mut BytesMut) {
    let static_idx = match status.as_u16() {
        200 => Some(8),
        204 => Some(9),
        206 => Some(10),
        304 => Some(11),
        400 => Some(12),
        404 => Some(13),
        500 => Some(14),
        _ => None,
    };
    if let Some(idx) = static_idx {
        encode_prefixed_int(idx, 7, 0x80, dst);
    } else {
        // Static table index 8 has name `:status` (Literal Without Indexing, 4-bit prefix).
        encode_prefixed_int(8, 4, 0x00, dst);
        encode_hpack_string(status.as_str().as_bytes(), dst);
    }
}

/// Encodes one `(name, value)` pair using RFC 7541 Static Indexing or Literal Without Indexing.
///
/// Never mutates any HPACK dynamic table, making pre-encoded slices and per-request metadata
/// safe to splice into any connection at any point in its lifetime.
#[inline]
pub fn encode_stateless_literal(name: &[u8], value: &[u8], dst: &mut BytesMut) {
    if let Some((idx, exact)) = lookup_static_for_encode(name, value) {
        if exact {
            encode_prefixed_int(idx as u64, 7, 0x80, dst);
            return;
        }
        encode_prefixed_int(idx as u64, 4, 0x00, dst);
    } else {
        encode_prefixed_int(0, 4, 0x00, dst);
        encode_hpack_string(name, dst);
    }
    encode_hpack_string(value, dst);
}

#[inline]
fn lookup_static_for_encode(name: &[u8], value: &[u8]) -> Option<(usize, bool)> {
    let mut name_only = None;
    for (idx, &(s_name, s_val)) in STATIC_TABLE.iter().enumerate() {
        if s_name.as_bytes() == name {
            let hpack_idx = idx + 1;
            if s_val.as_bytes() == value {
                return Some((hpack_idx, true));
            }
            if name_only.is_none() {
                name_only = Some((hpack_idx, false));
            }
        }
    }
    name_only
}

/// Decodes an RFC 7541 §5.1 integer with `prefix_bits` (1..=8).
#[inline]
pub fn decode_prefixed_int(slice: &[u8], prefix_bits: u8) -> Option<(u64, usize)> {
    if !(1..=8).contains(&prefix_bits) {
        return None;
    }
    let &first_byte = slice.first()?;
    let max_prefix = (1u64 << prefix_bits) - 1;
    let first = (first_byte & (max_prefix as u8)) as u64;
    if first < max_prefix {
        return Some((first, 1));
    }
    let mut val = first;
    let mut m = 0u32;
    for (offset, &b) in slice[1..].iter().enumerate() {
        let added = ((b & 0x7f) as u64).checked_shl(m)?;
        val = val.checked_add(added)?;
        if (b & 0x80) == 0 {
            return Some((val, offset + 2));
        }
        m += 7;
        if m >= 63 {
            return None;
        }
    }
    None
}

/// Encodes an RFC 7541 §5.1 integer with `prefix_bits` into `dst`.
#[inline]
pub fn encode_prefixed_int(val: u64, prefix_bits: u8, first_byte_prefix: u8, dst: &mut BytesMut) {
    let max_prefix = (1u64 << prefix_bits) - 1;
    if val < max_prefix {
        dst.put_u8(first_byte_prefix | (val as u8));
    } else {
        dst.put_u8(first_byte_prefix | (max_prefix as u8));
        let mut rem = val - max_prefix;
        while rem >= 128 {
            dst.put_u8(((rem & 0x7f) as u8) | 0x80);
            rem >>= 7;
        }
        dst.put_u8(rem as u8);
    }
}

#[inline]
fn decode_hpack_string(slice: &mut &[u8]) -> Result<Bytes, HpackError> {
    let &first = slice.first().ok_or(HpackError {
        code: H2ErrorCode::COMPRESSION_ERROR,
        msg: "truncated HPACK string length",
    })?;
    let is_huffman = (first & 0x80) != 0;
    let (str_len_u64, consumed) = decode_prefixed_int(slice, 7).ok_or(HpackError {
        code: H2ErrorCode::COMPRESSION_ERROR,
        msg: "malformed HPACK string length",
    })?;
    let len = usize::try_from(str_len_u64).map_err(|_| HpackError {
        code: H2ErrorCode::COMPRESSION_ERROR,
        msg: "HPACK string length overflow",
    })?;
    let rest = &slice[consumed..];
    let Some((raw, tail)) = rest.split_at_checked(len) else {
        return Err(HpackError {
            code: H2ErrorCode::COMPRESSION_ERROR,
            msg: "truncated HPACK string payload",
        });
    };
    *slice = tail;

    if is_huffman {
        let decoded = decode_huffman_bytes(raw).ok_or(HpackError {
            code: H2ErrorCode::COMPRESSION_ERROR,
            msg: "invalid HPACK Huffman string",
        })?;
        Ok(Bytes::from(decoded))
    } else {
        Ok(Bytes::copy_from_slice(raw))
    }
}

#[inline]
fn encode_hpack_string(raw: &[u8], dst: &mut BytesMut) {
    let huff_len = encoded_huffman_len(raw);
    if huff_len < raw.len() {
        encode_prefixed_int(huff_len as u64, 7, 0x80, dst);
        encode_huffman(raw, dst);
    } else {
        encode_prefixed_int(raw.len() as u64, 7, 0x00, dst);
        dst.put_slice(raw);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// RFC 7541 Appendix C.2.1 (Literal Header Field with Indexing, New Name)
    #[test]
    fn test_rfc7541_c2_1_literal_with_indexing() {
        let wire: [u8; 26] = [
            0x40, 0x0a, 0x63, 0x75, 0x73, 0x74, 0x6f, 0x6d, 0x2d, 0x6b, 0x65, 0x79, 0x0d, 0x63,
            0x75, 0x73, 0x74, 0x6f, 0x6d, 0x2d, 0x68, 0x65, 0x61, 0x64, 0x65, 0x72,
        ];
        let mut dec = HpackDecoder::default();
        let out = dec.decode(&wire).unwrap();
        assert_eq!(out.headers.get("custom-key").unwrap(), "custom-header");
        assert_eq!(dec.dynamic_table().len(), 1);
        assert_eq!(dec.dynamic_table().size(), 55);
    }

    /// RFC 7541 Appendix C.2.2 (Literal Without Indexing, Indexed Name :path)
    #[test]
    fn test_rfc7541_c2_2_literal_without_indexing() {
        let wire: [u8; 14] = [
            0x04, 0x0c, 0x2f, 0x73, 0x61, 0x6d, 0x70, 0x6c, 0x65, 0x2f, 0x70, 0x61, 0x74, 0x68,
        ];
        let mut dec = HpackDecoder::default();
        let out = dec.decode(&wire).unwrap();
        assert_eq!(out.uri.unwrap().path(), "/sample/path");
        assert!(dec.dynamic_table().is_empty());
    }

    /// RFC 7541 Appendix C.2.4 (Indexed Header Field :method: GET)
    #[test]
    fn test_rfc7541_c2_4_indexed_static() {
        let mut dec = HpackDecoder::default();
        let out = dec.decode(&[0x82]).unwrap();
        assert_eq!(out.method, Some(Method::GET));
    }

    /// RFC 7541 Appendix C.4 (Request Examples with Huffman Coding & Dynamic Table)
    #[test]
    fn test_rfc7541_c4_request_sequence_with_dynamic_table() {
        let mut dec = HpackDecoder::default();

        // C.4.1 First Request: GET http://www.example.com/
        let req1: [u8; 17] = [
            0x82, 0x86, 0x84, 0x41, 0x8c, 0xf1, 0xe3, 0xc2, 0xe5, 0xf2, 0x3a, 0x6b, 0xa0, 0xab,
            0x90, 0xf4, 0xff,
        ];
        let out1 = dec.decode(&req1).unwrap();
        assert_eq!(out1.method, Some(Method::GET));
        assert_eq!(out1.uri.unwrap().to_string(), "http://www.example.com/");
        assert_eq!(dec.dynamic_table().size(), 57);

        // C.4.2 Second Request: adds `cache-control: no-cache` (index 62 = :authority preserved)
        let req2: [u8; 12] = [
            0x82, 0x86, 0x84, 0xbe, 0x58, 0x86, 0xa8, 0xeb, 0x10, 0x64, 0x9c, 0xbf,
        ];
        let out2 = dec.decode(&req2).unwrap();
        assert_eq!(out2.method, Some(Method::GET));
        assert_eq!(out2.headers.get("cache-control").unwrap(), "no-cache");
        assert_eq!(out2.headers.get("host").unwrap(), "www.example.com");
        assert_eq!(dec.dynamic_table().len(), 2);
        assert_eq!(dec.dynamic_table().size(), 110);

        // C.4.3 Third Request: adds `custom-key: custom-value`
        let req3: [u8; 22] = [
            0x82, 0x87, 0x85, 0xbf, 0x40, 0x88, 0x25, 0xa8, 0x49, 0xe9, 0x5b, 0xa9, 0x7d, 0x7f,
            0x89, 0x25, 0xa8, 0x49, 0xe9, 0x5b, 0xb8, 0xe8,
        ];
        let mut req3_full = req3.to_vec();
        req3_full.extend_from_slice(&[0xb4, 0xbf]);
        let out3 = dec.decode(&req3_full).unwrap();
        assert_eq!(out3.method, Some(Method::GET));
        assert_eq!(
            out3.uri.unwrap().to_string(),
            "https://www.example.com/index.html"
        );
        assert_eq!(out3.headers.get("custom-key").unwrap(), "custom-value");
        assert_eq!(dec.dynamic_table().len(), 3);
        assert_eq!(dec.dynamic_table().size(), 164);
    }

    #[test]
    fn test_hpack_encoder_decoder_roundtrip_and_dynamic_reuse() {
        let mut enc = HpackEncoder::default();
        let mut dec = HpackDecoder::default();

        let mut ws = HeaderWorkspace::empty(4);
        ws.insert(
            HeaderName::from_static("x-custom-trace"),
            HeaderValue::from_static("trace-abc-123"),
        );
        ws.insert(
            http::header::CONNECTION,
            HeaderValue::from_static("keep-alive"),
        );
        ws.insert(http::header::TE, HeaderValue::from_static("trailers"));

        let mut buf = BytesMut::new();
        let uri: Uri = "https://api.velda.dev/v1/users?active=true"
            .parse()
            .unwrap();
        enc.encode_request(&Method::POST, &uri, &ws, &mut buf);

        let first_len = buf.len();
        let out = dec.decode(&buf).unwrap();
        assert_eq!(out.method, Some(Method::POST));
        assert_eq!(
            out.uri.unwrap().to_string(),
            "https://api.velda.dev/v1/users?active=true"
        );
        assert_eq!(out.headers.get("x-custom-trace").unwrap(), "trace-abc-123");
        assert!(out.headers.get("connection").is_none());
        assert_eq!(out.headers.get("te").unwrap(), "trailers");

        // Second identical request should compress smaller via dynamic table reuse
        buf.clear();
        enc.encode_request(&Method::POST, &uri, &ws, &mut buf);
        assert!(buf.len() < first_len);
        let out2 = dec.decode(&buf).unwrap();
        assert_eq!(out2.headers.get("x-custom-trace").unwrap(), "trace-abc-123");
    }

    #[test]
    fn test_hpack_single_pass_ingress_enrichment_xff_chain_and_anti_spoof() {
        let mut enc = HpackEncoder::default();
        let mut dec = HpackDecoder::default();

        let mut client_ws = HeaderWorkspace::empty(8);
        client_ws.insert(
            HeaderName::from_static("x-request-id"),
            HeaderValue::from_static("spoofed-id"),
        );
        client_ws.insert(
            HeaderName::from_static("x-real-ip"),
            HeaderValue::from_static("1.2.3.4"),
        );
        client_ws.insert(
            HeaderName::from_static("x-forwarded-for"),
            HeaderValue::from_static("203.0.113.5"),
        );
        client_ws.insert(
            HeaderName::from_static("forwarded"),
            HeaderValue::from_static("for=1.2.3.4"),
        );
        client_ws.insert(http::header::TE, HeaderValue::from_static("gzip"));
        client_ws.insert(
            http::header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );

        let mut wire = BytesMut::new();
        let uri: Uri = "https://edge.velda.internal/orders".parse().unwrap();
        enc.encode_request(&Method::POST, &uri, &client_ws, &mut wire);

        let decoded = dec
            .decode_ingress(
                &wire,
                H2IngressEnrich {
                    request_id: HeaderValue::from_static("auth-h2-req-id"),
                    forwarding: Some(H2ForwardingAddrs {
                        peer_ip: velda_core::lut::ip::v4_to_u128(0xC0A8_010A), // 192.168.1.10
                        local_ip: velda_core::lut::ip::v4_to_u128(0x0A00_0001), // 10.0.0.1
                        local_port_be: 443u16.to_be(),
                        is_tls: true,
                    }),
                },
            )
            .unwrap();

        assert_eq!(
            decoded.headers.get("x-request-id").unwrap(),
            "auth-h2-req-id"
        );
        assert!(
            decoded
                .headers
                .bypass_headers()
                .any(|(n, v)| n == X_REQUEST_ID && v == "auth-h2-req-id")
        );
        assert_eq!(decoded.headers.get("x-real-ip").unwrap(), "192.168.1.10");
        assert_eq!(
            decoded.headers.get("x-forwarded-for").unwrap(),
            "203.0.113.5, 192.168.1.10"
        );
        assert_eq!(decoded.headers.get("x-forwarded-proto").unwrap(), "https");
        assert_eq!(decoded.headers.get("x-forwarded-port").unwrap(), "443");
        assert_eq!(
            decoded.headers.get("x-forwarded-host").unwrap(),
            "edge.velda.internal"
        );
        assert_eq!(
            decoded.headers.get("forwarded").unwrap(),
            "for=192.168.1.10;proto=https;by=10.0.0.1;host=\"edge.velda.internal\""
        );
        assert!(decoded.headers.get("te").is_none());
        assert_eq!(
            decoded.headers.get("content-type").unwrap(),
            "application/json"
        );
    }

    #[test]
    fn test_hpack_rejects_protocol_violations() {
        let mut dec = HpackDecoder::default();
        // Index 0 -> COMPRESSION_ERROR
        assert_eq!(
            dec.decode(&[0x80]).unwrap_err().code,
            H2ErrorCode::COMPRESSION_ERROR
        );
        // Dynamic table update (4097) larger than protocol_max_table_size (4096) -> COMPRESSION_ERROR
        assert_eq!(
            dec.decode(&[0x3f, 0xe2, 0x1f]).unwrap_err().code,
            H2ErrorCode::COMPRESSION_ERROR
        );
        // Pseudo-header after regular header -> PROTOCOL_ERROR
        let wire = [
            0x40, 0x01, b'a', 0x01, b'1', // a: 1
            0x82, // :method: GET
        ];
        assert_eq!(
            dec.decode(&wire).unwrap_err().code,
            H2ErrorCode::PROTOCOL_ERROR
        );
    }

    #[tokio::test]
    async fn test_hpack_and_frame_interop_with_reference_h2_crate() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        // 1. Our frame + HpackEncoder raw client stream -> reference `h2::server::handshake`
        let (mut raw_client, server_io) = tokio::io::duplex(64 * 1024);
        let server_task = tokio::spawn(async move {
            let mut conn = h2::server::handshake(server_io).await.unwrap();
            let (req, mut respond) = conn.accept().await.unwrap().unwrap();
            let uri_str = req.uri().to_string();
            let cookie_val = req
                .headers()
                .get(http::header::COOKIE)
                .unwrap()
                .to_str()
                .unwrap()
                .to_owned();
            let custom_val = req
                .headers()
                .get("x-velda-phase1")
                .unwrap()
                .to_str()
                .unwrap()
                .to_owned();
            let mut resp = http::Response::new(());
            *resp.status_mut() = StatusCode::CREATED;
            resp.headers_mut().insert(
                HeaderName::from_static("x-server-reply"),
                HeaderValue::from_static("ok-from-h2"),
            );
            let _ = respond.send_response(resp, true).unwrap();
            // Keep connection alive briefly so client reads response
            while conn.accept().await.is_some() {}
            (req.method().clone(), uri_str, cookie_val, custom_val)
        });

        // Write HTTP/2 preface + empty SETTINGS + HEADERS built by our Phase 1 codecs
        raw_client
            .write_all(crate::proto::frame::CONNECTION_PREFACE)
            .await
            .unwrap();
        let mut out = BytesMut::new();
        crate::proto::frame::write_settings(&[], &mut out);

        let mut enc = HpackEncoder::default();
        let mut ws = HeaderWorkspace::empty(4);
        ws.insert(http::header::COOKIE, HeaderValue::from_static("sid=xyz"));
        ws.insert(
            HeaderName::from_static("x-velda-phase1"),
            HeaderValue::from_static("codec-check"),
        );
        let mut hpack_block = BytesMut::new();
        let uri: Uri = "https://velda.test/api/v2/items?p=1".parse().unwrap();
        enc.encode_request(&Method::POST, &uri, &ws, &mut hpack_block);
        crate::proto::frame::write_headers_frames(
            1,
            &hpack_block,
            true,
            crate::proto::frame::DEFAULT_MAX_FRAME_SIZE as usize,
            &mut out,
        );
        raw_client.write_all(&out).await.unwrap();

        // Read `h2` server's SETTINGS + SETTINGS ACK + HEADERS response and decode with Phase 1
        let mut recv_buf = BytesMut::with_capacity(4096);
        let mut dec = HpackDecoder::default();
        let mut decoded_resp: Option<DecodedH2Headers> = None;
        let mut tmp = [0u8; 2048];
        while decoded_resp.is_none() {
            let n = raw_client.read(&mut tmp).await.unwrap();
            assert!(n > 0);
            recv_buf.extend_from_slice(&tmp[..n]);
            while let Some((frame, consumed)) = crate::proto::frame::decode_frame(
                &recv_buf,
                crate::proto::frame::DEFAULT_MAX_FRAME_SIZE,
            )
            .unwrap()
            {
                let _ = recv_buf.split_to(consumed);
                match frame {
                    crate::proto::frame::H2Frame::Settings { ack: false, .. } => {
                        let mut ack = BytesMut::new();
                        crate::proto::frame::write_settings_ack(&mut ack);
                        raw_client.write_all(&ack).await.unwrap();
                    }
                    crate::proto::frame::H2Frame::Headers {
                        stream_id: 1,
                        header_block,
                        ..
                    } => {
                        decoded_resp = Some(dec.decode(&header_block).unwrap());
                    }
                    _ => {}
                }
            }
        }
        drop(raw_client);

        let (m, u, c, x) = server_task.await.unwrap();
        assert_eq!(m, Method::POST);
        assert_eq!(u, "https://velda.test/api/v2/items?p=1");
        assert_eq!(c, "sid=xyz");
        assert_eq!(x, "codec-check");

        let resp = decoded_resp.unwrap();
        assert_eq!(resp.status, Some(StatusCode::CREATED));
        assert_eq!(resp.headers.get("x-server-reply").unwrap(), "ok-from-h2");
    }

    #[test]
    fn test_hpack_dynamic_table_eviction_roundtrip() {
        let mut enc = HpackEncoder::new(128);
        let mut dec = HpackDecoder::new(128, 64 * 1024);

        for i in 0..16u32 {
            let mut ws = HeaderWorkspace::empty(4);
            let val = format!("value-{i:04}-payload-for-eviction-testing");
            ws.insert(
                HeaderName::from_bytes(format!("x-hdr-{i}").as_bytes()).unwrap(),
                HeaderValue::from_str(&val).unwrap(),
            );
            let mut wire = BytesMut::new();
            let status = if i % 2 == 0 {
                StatusCode::OK
            } else {
                StatusCode::CREATED
            };
            enc.encode_response(status, &ws, &mut wire);

            let out = dec.decode(&wire).unwrap();
            assert_eq!(out.status, Some(status));
            assert_eq!(
                out.headers.get(format!("x-hdr-{i}")).unwrap().as_bytes(),
                val.as_bytes()
            );
            assert!(dec.dynamic_table().size() <= 128);
        }
    }

    #[test]
    fn test_decode_prefixed_int_8bit_prefix_boundary() {
        assert_eq!(decode_prefixed_int(&[0xFF, 0x00], 8), Some((255, 2)));
        assert_eq!(decode_prefixed_int(&[0xFF, 0x01], 8), Some((256, 2)));
        assert_eq!(decode_prefixed_int(&[0xFF, 0x7F], 8), Some((382, 2)));
        assert_eq!(decode_prefixed_int(&[0xFF, 0x80, 0x01], 8), Some((383, 3)));
        assert_eq!(decode_prefixed_int(&[0x7F, 0x7F], 7), Some((254, 2)));
        assert_eq!(decode_prefixed_int(&[0x7F], 0), None);
        assert_eq!(decode_prefixed_int(&[0x7F], 9), None);
    }
}
