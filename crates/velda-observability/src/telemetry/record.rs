//! Canonical 256-Byte Cacheline-Aligned Telemetry Record (`velda_observability::telemetry::record`).
//!
//! ```text
//! ┌────────────────────────────────────────────────────────────────────────────────────────┐
//! │                  TELEMETRYRECORD MEMORY LAYOUT (256 BYTES, 64-BYTE ALIGNED)            │
//! ├────────────────────────────────────────────────────────────────────────────────────────┤
//! │ CACHE LINE 0 (Bytes 0..64): Unified Execution Scalars & Trace Span Fields              │
//! │ 0..8   : request_id (u64 entropy)       │ 8..16  : duration_ns (u64 elapsed)           │
//! │ 16..32 : peer_ip (u128 RFC 4291 mapped) │ 32..40 : ingress_time_nanos (u64 epoch nano) │
//! │ 40..44 : bytes_rx (u32)                 │ 44..48 : bytes_tx (u32)                      │
//! │ 48..52 : upstream_ip (u32 IPv4 bits)    │ 52..54 : upstream_port (u16)                 │
//! │ 54..56 : _cl0_pad (u16)                 │ 56..58 : route_id (u16)                      │
//! │ 58..60 : upstream_id (u16)              │ 60..61 : method (HttpMethod: u8)             │
//! │ 61..62 : pipe (PipeKind: u8)            │ 62..63 : internal_status (u8)            │
//! │ 63..64 : _reserved (u8)                 │                                              │
//! ├────────────────────────────────────────────────────────────────────────────────────────┤
//! │ CACHE LINE 1 (Bytes 64..128): Client Classification & URL Buffer Head                  │
//! │ 64..66 : ua_major_version (u16)         │ 66..68 : rpc_msgs_or_flags (u16)             │
//! │ 68..69 : ua_family (UaFamily: u8)       │ 69..70 : ua_os (UaOs: u8)                    │
//! │ 70..72 : url_len (u16: 0..=184)         │ 72..128: url_bytes[0..56] (56 bytes)         │
//! ├────────────────────────────────────────────────────────────────────────────────────────┤
//! │ CACHE LINE 2 (Bytes 128..192): URL Buffer Body                                         │
//! │ 128..192: url_bytes[56..120] (64 bytes contiguous URL payload)                         │
//! ├────────────────────────────────────────────────────────────────────────────────────────┤
//! │ CACHE LINE 3 (Bytes 192..256): URL Buffer Tail                                         │
//! │ 192..256: url_bytes[120..184] (64 bytes contiguous URL payload)                        │
//! └────────────────────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Hardware Layout & Rationale
//! Exactly 256 bytes (`#[repr(C, align(64))]`, exactly 4 CPU cache lines) with zero compiler padding
//! and zero heap pointers. All fields read by trace exporters (`request_id`, `ingress_time_nanos`,
//! `duration_ns`, `peer_ip`, `internal_status`, `route_id`, `upstream_id`, `method`) sit in Cache Line 0
//! so trace serialization touches only the first 64-byte line of each record while log shippers read
//! the full 256-byte record.

use velda_core::lut::{HttpMethod, InternalStatus, PipeKind, UaFamily, UaOs, WireProtocol};

/// Canonical 256-byte (exactly 4 CPU cache lines) pure POD Access Log + Trace Record.
///
/// # Memory Layout (256 bytes, 64-byte aligned, zero compiler padding)
/// - `0..8`:    `request_id: u64` (low 64-bit entropy of W3C RequestId / SpanId)
/// - `8..16`:   `duration_ns: u64` (lossless nanoseconds, prevents 32-bit overflow on streaming/L4)
/// - `16..32`:  `peer_ip: u128` (16-byte aligned at offset 16; lossless IPv6 or RFC 4291 IPv4-mapped)
/// - `32..40`:  `ingress_time_nanos: u64` (true hardware Unix epoch nanoseconds at request arrival)
/// - `40..44`:  `bytes_rx: u32`
/// - `44..48`:  `bytes_tx: u32`
/// - `48..52`:  `upstream_ip: u32`
/// - `52..54`:  `upstream_port: u16`
/// - `54..56`:  `_cl0_pad: u16` (cacheline alignment padding)
/// - `56..58`:  `route_id: u16`
/// - `58..60`:  `upstream_id: u16`
/// - `60..61`:  `method: HttpMethod` (`#[repr(u8)]`, 1 byte)
/// - `61..62`:  `pipe: PipeKind` (`#[repr(u8)]`, 1 byte, strongly typed pipeline variant)
/// - `62..63`:  `internal_status: InternalStatus` (`#[repr(u8)]`, 1 byte, canonical internal status)
/// - `63..64`:  `_reserved: u8` (cacheline alignment padding)
/// - `64..66`:  `ua_major_version: u16`
/// - `66..68`:  `rpc_msgs_or_flags: u16`
/// - `68..69`:  `ua_family: UaFamily` (`#[repr(u8)]`, 1 byte)
/// - `69..70`:  `ua_os: UaOs` (`#[repr(u8)]`, 1 byte)
/// - `70..72`:  `url_len: u16`
/// - `72..256`: `url_bytes: [u8; 184]` (184 bytes monolithic URL: scheme + host + path_and_query)
#[repr(C, align(64))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TelemetryRecord {
    pub request_id: u64,
    pub duration_ns: u64,
    pub peer_ip: u128,
    pub ingress_time_nanos: u64,
    pub bytes_rx: u32,
    pub bytes_tx: u32,
    pub upstream_ip: u32,
    pub upstream_port: u16,
    pub _cl0_pad: u16,
    pub route_id: u16,
    pub upstream_id: u16,
    pub method: HttpMethod,
    pub pipe: PipeKind,
    pub internal_status: InternalStatus,
    pub _reserved: u8,
    pub ua_major_version: u16,
    pub rpc_msgs_or_flags: u16,
    pub ua_family: UaFamily,
    pub ua_os: UaOs,
    pub url_len: u16,
    pub url_bytes: [u8; 184],
}

const _: () = assert!(std::mem::size_of::<TelemetryRecord>() == 256);
const _: () = assert!(std::mem::align_of::<TelemetryRecord>() == 64);
const _: () = assert!(std::mem::offset_of!(TelemetryRecord, duration_ns) == 8);
const _: () = assert!(std::mem::offset_of!(TelemetryRecord, peer_ip) == 16);
const _: () = assert!(std::mem::offset_of!(TelemetryRecord, ingress_time_nanos) == 32);
const _: () = assert!(std::mem::offset_of!(TelemetryRecord, bytes_rx) == 40);
const _: () = assert!(std::mem::offset_of!(TelemetryRecord, bytes_tx) == 44);
const _: () = assert!(std::mem::offset_of!(TelemetryRecord, upstream_ip) == 48);
const _: () = assert!(std::mem::offset_of!(TelemetryRecord, upstream_port) == 52);
const _: () = assert!(std::mem::offset_of!(TelemetryRecord, _cl0_pad) == 54);
const _: () = assert!(std::mem::offset_of!(TelemetryRecord, route_id) == 56);
const _: () = assert!(std::mem::offset_of!(TelemetryRecord, upstream_id) == 58);
const _: () = assert!(std::mem::offset_of!(TelemetryRecord, method) == 60);
const _: () = assert!(std::mem::offset_of!(TelemetryRecord, pipe) == 61);
const _: () = assert!(std::mem::offset_of!(TelemetryRecord, internal_status) == 62);
const _: () = assert!(std::mem::offset_of!(TelemetryRecord, _reserved) == 63);
const _: () = assert!(std::mem::offset_of!(TelemetryRecord, ua_major_version) == 64);
const _: () = assert!(std::mem::offset_of!(TelemetryRecord, rpc_msgs_or_flags) == 66);
const _: () = assert!(std::mem::offset_of!(TelemetryRecord, ua_family) == 68);
const _: () = assert!(std::mem::offset_of!(TelemetryRecord, ua_os) == 69);
const _: () = assert!(std::mem::offset_of!(TelemetryRecord, url_len) == 70);
const _: () = assert!(std::mem::offset_of!(TelemetryRecord, url_bytes) == 72);

pub const BATCH_FLAG_HAS_ERRORS: u8 = 1 << 0;

/// Compact per-batch summary stamped into Cache Line header of [`TelemetryBatchSlot`]
/// during ring draining so background exporters can skip/pre-size payloads in 1 cache line read.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TelemetryBatchSummary {
    pub first_ts_ns: u64,
    pub last_ts_ns: u64,
    pub error_count: u32,
    pub payload_bytes: u32,
    pub url_bytes_sum: u32,
    pub flags: u8,
}

impl TelemetryBatchSummary {
    /// Computes summary fields over an arbitrary slice (used for unpooled fallback/test batches).
    pub fn from_slice(records: &[TelemetryRecord]) -> Self {
        let Some(first) = records.first() else {
            return Self::default();
        };
        let mut error_count: u32 = 0;
        let mut payload_bytes: u32 = 0;
        let mut url_bytes_sum: u32 = 0;
        for rec in records {
            if rec.internal_status != velda_core::lut::InternalStatus::Completed {
                error_count += 1;
            }
            payload_bytes = payload_bytes.saturating_add(rec.bytes_rx.saturating_add(rec.bytes_tx));
            url_bytes_sum = url_bytes_sum.saturating_add((rec.url_len as u32).min(184));
        }
        Self {
            first_ts_ns: first.ingress_time_nanos,
            last_ts_ns: records
                .last()
                .map_or(first.ingress_time_nanos, |r| r.ingress_time_nanos),
            error_count,
            payload_bytes,
            url_bytes_sum,
            flags: if error_count > 0 {
                BATCH_FLAG_HAS_ERRORS
            } else {
                0
            },
        }
    }
}

impl Default for TelemetryRecord {
    #[inline(always)]
    fn default() -> Self {
        Self::zeroed()
    }
}

impl TelemetryRecord {
    #[inline(always)]
    pub const fn zeroed() -> Self {
        Self {
            request_id: 0,
            duration_ns: 0,
            peer_ip: 0,
            ingress_time_nanos: 0,
            bytes_rx: 0,
            bytes_tx: 0,
            upstream_ip: 0,
            upstream_port: 0,
            _cl0_pad: 0,
            route_id: velda_core::UNMATCHED_ROUTE_ID,
            upstream_id: velda_core::UNASSIGNED_UPSTREAM_ID,
            method: HttpMethod::Unknown,
            pipe: PipeKind::BufferedH1H1,
            internal_status: InternalStatus::Completed,
            _reserved: 0,
            ua_major_version: 0,
            rpc_msgs_or_flags: 0,
            ua_family: UaFamily::None,
            ua_os: UaOs::Unknown,
            url_len: 0,
            url_bytes: [0; 184],
        }
    }

    /// Reconstructs the 16-byte W3C trace ID from Edge-minted timestamp and entropy.
    ///
    /// Edge is zero-trust for trace correlation: incoming client trace IDs are never adopted.
    #[inline(always)]
    pub const fn trace_id_bytes(&self) -> [u8; 16] {
        (((self.ingress_time_nanos as u128) << 64) | (self.request_id as u128)).to_be_bytes()
    }

    /// Reconstructs the 8-byte W3C span ID from the Edge-minted request entropy seed.
    #[inline(always)]
    pub const fn span_id_bytes(&self) -> [u8; 8] {
        self.request_id.to_be_bytes()
    }

    /// Accesses the stored monolithic URL as a string slice (or `"-"` if empty/unset).
    #[inline(always)]
    pub fn url(&self) -> &str {
        if self.url_len == 0 {
            "-"
        } else {
            let len = (self.url_len as usize).min(184);
            std::str::from_utf8(&self.url_bytes[..len]).unwrap_or("-")
        }
    }

    /// Sets the request URL, copying up to 184 bytes without allocation.
    #[inline(always)]
    pub fn set_url(&mut self, url: &str) {
        let b = url.as_bytes();
        let len = b.len().min(184);
        self.url_len = len as u16;
        self.url_bytes[..len].copy_from_slice(&b[..len]);
        if len < 184 {
            self.url_bytes[len..].fill(0);
        }
    }

    /// Formats a complete URL (scheme + host + path_and_query) directly into the 184-byte buffer
    /// with zero heap allocations.
    #[inline(always)]
    pub fn encode_url(is_tls: bool, host: Option<&str>, path_and_query: &str) -> (u16, [u8; 184]) {
        let mut bytes = [0u8; 184];
        let mut len = 0;

        if let Some(h) = host {
            let scheme: &[u8] = if is_tls { b"https://" } else { b"http://" };
            let copy_s = scheme.len().min(184 - len);
            let mut i = 0;
            while i < copy_s {
                bytes[len + i] = scheme[i];
                i += 1;
            }
            len += copy_s;

            let h_bytes = h.as_bytes();
            let copy_h = h_bytes.len().min(184 - len);
            let mut i = 0;
            while i < copy_h {
                bytes[len + i] = h_bytes[i];
                i += 1;
            }
            len += copy_h;
        }

        let pq_bytes = path_and_query.as_bytes();
        let copy_pq = pq_bytes.len().min(184 - len);
        let mut i = 0;
        while i < copy_pq {
            bytes[len + i] = pq_bytes[i];
            i += 1;
        }
        len += copy_pq;

        (len as u16, bytes)
    }

    /// Encodes a raw byte slice into the fixed 184-byte URL buffer without heap allocation.
    #[inline(always)]
    pub const fn from_url_bytes(url: &[u8]) -> (u16, [u8; 184]) {
        let len = if url.len() > 184 { 184 } else { url.len() };
        let mut bytes = [0u8; 184];
        let mut i = 0;
        while i < len {
            bytes[i] = url[i];
            i += 1;
        }
        (len as u16, bytes)
    }

    /// Returns the concise, canonical pipeline execution identifier (e.g. `"buffered_h1_h1"`,
    /// `"server_stream_h2_h2"`, `"buffered_grpc_tcp_tcp"`, `"tcp_tcp"`).
    #[inline(always)]
    pub const fn pipe_name(&self) -> &'static str {
        self.pipe.as_str()
    }

    /// Returns the downstream wire protocol derived directly from the `pipe` discriminant.
    #[inline(always)]
    pub const fn downstream_protocol(&self) -> WireProtocol {
        self.pipe.downstream_protocol()
    }

    /// Returns the upstream wire protocol derived directly from the `pipe` discriminant.
    #[inline(always)]
    pub const fn upstream_protocol(&self) -> WireProtocol {
        self.pipe.upstream_protocol()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_geometry_and_alignment() {
        assert_eq!(std::mem::size_of::<TelemetryRecord>(), 256);
        assert_eq!(std::mem::align_of::<TelemetryRecord>(), 64);
        assert_eq!(std::mem::offset_of!(TelemetryRecord, upstream_id), 58);
        assert_eq!(std::mem::offset_of!(TelemetryRecord, method), 60);
        assert_eq!(std::mem::offset_of!(TelemetryRecord, url_bytes), 72);

        let mut rec = TelemetryRecord::zeroed();
        rec.ingress_time_nanos = 0x1122_3344_5566_7788;
        rec.request_id = 0x99aa_bbcc_ddee_ff00;
        assert_eq!(
            rec.trace_id_bytes(),
            [
                0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee,
                0xff, 0x00
            ]
        );
        assert_eq!(
            rec.span_id_bytes(),
            [0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x00]
        );
    }

    #[test]
    fn test_record_url_mutation() {
        let mut rec = TelemetryRecord::zeroed();
        assert_eq!(rec.url(), "-");

        rec.set_url("/api/v1/checkout?user=123");
        assert_eq!(rec.url(), "/api/v1/checkout?user=123");
        assert_eq!(rec.url_len, 25);
    }
}
