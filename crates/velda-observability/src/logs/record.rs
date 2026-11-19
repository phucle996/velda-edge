//! Canonical 256-Byte Cacheline-Aligned Access Log Record (`velda_observability::logs::record`).
//!
//! ```text
//! ┌────────────────────────────────────────────────────────────────────────────────────────┐
//! │                 ACCESSLOGRECORD MEMORY LAYOUT (256 BYTES, 64-BYTE ALIGNED)             │
//! ├────────────────────────────────────────────────────────────────────────────────────────┤
//! │ CACHE LINE 0 (Bytes 0..64): Execution Scalars & Ingress Timestamps                     │
//! │ 0..8   : request_id (u64 entropy)       │ 8..16  : duration_ns (u64 elapsed)           │
//! │ 16..32 : peer_ip (u128 RFC 4291 mapped) │ 32..40 : ingress_time_nanos (u64 epoch nano) │
//! │ 40..44 : bytes_rx (u32)                 │ 44..48 : bytes_tx (u32)                      │
//! │ 48..52 : upstream_ip (u32 IPv4 bits)    │ 52..54 : upstream_port (u16)                 │
//! │ 54..56 : status_code (u16)              │ 56..58 : route_id (u16)                      │
//! │ 58..60 : ua_major_version (u16)         │ 60..62 : rpc_msgs_or_flags (u16)             │
//! │ 62..63 : pipe (PipeKind: u8)            │ 63..64 : completion (CompletionReason: u8)   │
//! ├────────────────────────────────────────────────────────────────────────────────────────┤
//! │ CACHE LINE 1 (Bytes 64..128): Identifiers & URL Buffer Head                            │
//! │ 64..65 : method (HttpMethod: u8)        │ 65..66 : grpc_status (GrpcStatus: u8)        │
//! │ 66..67 : ua_family (UaFamily: u8)       │ 67..68 : ua_os (UaOs: u8)                    │
//! │ 68..70 : url_len (u16: 0..=186)         │ 70..128: url_bytes[0..58] (58 bytes)         │
//! ├────────────────────────────────────────────────────────────────────────────────────────┤
//! │ CACHE LINE 2 (Bytes 128..192): URL Buffer Body                                         │
//! │ 128..192: url_bytes[58..122] (64 bytes contiguous URL payload)                        │
//! ├────────────────────────────────────────────────────────────────────────────────────────┤
//! │ CACHE LINE 3 (Bytes 192..256): URL Buffer Tail                                         │
//! │ 192..256: url_bytes[122..186] (64 bytes contiguous URL payload)                       │
//! └────────────────────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Hardware Layout & Rationale
//! Exactly 256 bytes (`#[repr(C, align(64))]`, exactly 4 CPU cache lines) with zero compiler padding
//! and zero heap pointers. Every field is a fixed-width integer, monolithic URL buffer, or finite enum discriminant
//! so that emitting a log record is a pure 256-byte value copy (`Copy`).

use velda_core::lut::{
    CompletionReason, GrpcStatus, HttpMethod, PipeKind, UaFamily, UaOs, WireProtocol,
};

/// Canonical 256-byte (exactly 4 CPU cache lines) pure POD Access Log Record.
///
/// # Memory Layout (256 bytes, 64-byte aligned, zero compiler padding)
/// - `0..8`:    `request_id: u64` (low 64-bit entropy of W3C RequestId)
/// - `8..16`:   `duration_ns: u64` (lossless nanoseconds, prevents 32-bit overflow on streaming/L4)
/// - `16..32`:  `peer_ip: u128` (16-byte aligned at offset 16; lossless IPv6 or RFC 4291 IPv4-mapped)
/// - `32..40`:  `ingress_time_nanos: u64` (true hardware Unix epoch nanoseconds at request arrival)
/// - `40..44`:  `bytes_rx: u32`
/// - `44..48`:  `bytes_tx: u32`
/// - `48..52`:  `upstream_ip: u32`
/// - `52..54`:  `upstream_port: u16`
/// - `54..56`:  `status_code: u16`
/// - `56..58`:  `route_id: u16`
/// - `58..60`:  `ua_major_version: u16`
/// - `60..62`:  `rpc_msgs_or_flags: u16`
/// - `62..63`:  `pipe: PipeKind` (`#[repr(u8)]`, 1 byte, strongly typed pipeline variant)
/// - `63..64`:  `completion: CompletionReason` (`#[repr(u8)]`, 1 byte)
/// - `64..65`:  `method: HttpMethod` (`#[repr(u8)]`, 1 byte)
/// - `65..66`:  `grpc_status: GrpcStatus` (`#[repr(u8)]`, 1 byte)
/// - `66..67`:  `ua_family: UaFamily` (`#[repr(u8)]`, 1 byte)
/// - `67..68`:  `ua_os: UaOs` (`#[repr(u8)]`, 1 byte)
/// - `68..70`:  `url_len: u16`
/// - `70..256`: `url_bytes: [u8; 186]` (186 bytes monolithic URL: scheme + host + path_and_query)
#[repr(C, align(64))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AccessLogRecord {
    pub request_id: u64,
    pub duration_ns: u64,
    pub peer_ip: u128,
    pub ingress_time_nanos: u64,
    pub bytes_rx: u32,
    pub bytes_tx: u32,
    pub upstream_ip: u32,
    pub upstream_port: u16,
    pub status_code: u16,
    pub route_id: u16,
    pub ua_major_version: u16,
    pub rpc_msgs_or_flags: u16,
    pub pipe: PipeKind,
    pub completion: CompletionReason,
    pub method: HttpMethod,
    pub grpc_status: GrpcStatus,
    pub ua_family: UaFamily,
    pub ua_os: UaOs,
    pub url_len: u16,
    pub url_bytes: [u8; 186],
}

const _: () = assert!(std::mem::size_of::<AccessLogRecord>() == 256);
const _: () = assert!(std::mem::align_of::<AccessLogRecord>() == 64);
const _: () = assert!(std::mem::offset_of!(AccessLogRecord, duration_ns) == 8);
const _: () = assert!(std::mem::offset_of!(AccessLogRecord, peer_ip) == 16);
const _: () = assert!(std::mem::offset_of!(AccessLogRecord, ingress_time_nanos) == 32);
const _: () = assert!(std::mem::offset_of!(AccessLogRecord, bytes_rx) == 40);
const _: () = assert!(std::mem::offset_of!(AccessLogRecord, bytes_tx) == 44);
const _: () = assert!(std::mem::offset_of!(AccessLogRecord, upstream_ip) == 48);
const _: () = assert!(std::mem::offset_of!(AccessLogRecord, upstream_port) == 52);
const _: () = assert!(std::mem::offset_of!(AccessLogRecord, status_code) == 54);
const _: () = assert!(std::mem::offset_of!(AccessLogRecord, route_id) == 56);
const _: () = assert!(std::mem::offset_of!(AccessLogRecord, ua_major_version) == 58);
const _: () = assert!(std::mem::offset_of!(AccessLogRecord, rpc_msgs_or_flags) == 60);
const _: () = assert!(std::mem::offset_of!(AccessLogRecord, pipe) == 62);
const _: () = assert!(std::mem::offset_of!(AccessLogRecord, completion) == 63);
const _: () = assert!(std::mem::offset_of!(AccessLogRecord, method) == 64);
const _: () = assert!(std::mem::offset_of!(AccessLogRecord, grpc_status) == 65);
const _: () = assert!(std::mem::offset_of!(AccessLogRecord, ua_family) == 66);
const _: () = assert!(std::mem::offset_of!(AccessLogRecord, ua_os) == 67);
const _: () = assert!(std::mem::offset_of!(AccessLogRecord, url_len) == 68);
const _: () = assert!(std::mem::offset_of!(AccessLogRecord, url_bytes) == 70);

impl Default for AccessLogRecord {
    #[inline(always)]
    fn default() -> Self {
        Self::zeroed()
    }
}

impl AccessLogRecord {
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
            status_code: 0,
            route_id: velda_core::UNMATCHED_ROUTE_ID,
            ua_major_version: 0,
            rpc_msgs_or_flags: 0,
            pipe: PipeKind::BufferedH1H1,
            completion: CompletionReason::Complete,
            method: HttpMethod::Unknown,
            grpc_status: GrpcStatus::NonGrpc,
            ua_family: UaFamily::None,
            ua_os: UaOs::Unknown,
            url_len: 0,
            url_bytes: [0; 186],
        }
    }

    /// Accesses the stored monolithic URL as a string slice (or `"-"` if empty/unset).
    #[inline(always)]
    pub fn url(&self) -> &str {
        if self.url_len == 0 {
            "-"
        } else {
            let len = (self.url_len as usize).min(186);
            std::str::from_utf8(&self.url_bytes[..len]).unwrap_or("-")
        }
    }

    /// Sets the request URL, copying up to 186 bytes without allocation.
    #[inline(always)]
    pub fn set_url(&mut self, url: &str) {
        let b = url.as_bytes();
        let len = b.len().min(186);
        self.url_len = len as u16;
        self.url_bytes[..len].copy_from_slice(&b[..len]);
        if len < 186 {
            self.url_bytes[len..].fill(0);
        }
    }

    /// Formats a complete URL (scheme + host + path_and_query) directly into the 186-byte buffer
    /// with zero heap allocations.
    #[inline(always)]
    pub fn encode_url(is_tls: bool, host: Option<&str>, path_and_query: &str) -> (u16, [u8; 186]) {
        let mut bytes = [0u8; 186];
        let mut len = 0;

        if let Some(h) = host {
            let scheme: &[u8] = if is_tls { b"https://" } else { b"http://" };
            let copy_s = scheme.len().min(186 - len);
            let mut i = 0;
            while i < copy_s {
                bytes[len + i] = scheme[i];
                i += 1;
            }
            len += copy_s;

            let h_bytes = h.as_bytes();
            let copy_h = h_bytes.len().min(186 - len);
            let mut i = 0;
            while i < copy_h {
                bytes[len + i] = h_bytes[i];
                i += 1;
            }
            len += copy_h;
        }

        let pq_bytes = path_and_query.as_bytes();
        let copy_pq = pq_bytes.len().min(186 - len);
        let mut i = 0;
        while i < copy_pq {
            bytes[len + i] = pq_bytes[i];
            i += 1;
        }
        len += copy_pq;

        (len as u16, bytes)
    }

    /// Encodes a raw byte slice into the fixed 186-byte URL buffer without heap allocation.
    #[inline(always)]
    pub const fn from_url_bytes(url: &[u8]) -> (u16, [u8; 186]) {
        let len = if url.len() > 186 { 186 } else { url.len() };
        let mut bytes = [0u8; 186];
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
        assert_eq!(std::mem::size_of::<AccessLogRecord>(), 256);
        assert_eq!(std::mem::align_of::<AccessLogRecord>(), 64);
        assert_eq!(std::mem::offset_of!(AccessLogRecord, url_bytes), 70);
    }

    #[test]
    fn test_record_url_mutation() {
        let mut rec = AccessLogRecord::zeroed();
        assert_eq!(rec.url(), "-");

        rec.set_url("/api/v1/checkout?user=123");
        assert_eq!(rec.url(), "/api/v1/checkout?user=123");
        assert_eq!(rec.url_len, 25);
    }
}
