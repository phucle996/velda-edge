//! Canonical 256-Byte Cacheline-Aligned Access Log Record (`velda_observability::logs::record`).
//!
//! # Hardware Layout & Rationale
//! Exactly 256 bytes (`#[repr(C, align(64))]`, exactly 4 CPU cache lines) with zero compiler padding
//! and zero heap pointers. Every field is a fixed-width integer, monolithic URL buffer, or finite enum discriminant
//! so that emitting a log record is a pure 256-byte value copy (`Copy`).

use std::net::SocketAddr;

use velda_core::pipe::PipeKind;

use super::types::{
    LogCompletionReason, LogGrpcStatus, LogHttpMethod, LogProtocol, encode_ip_u128,
    encode_upstream_ip_u32,
};
use super::user_agent::{UaFamily, UaOs, classify_user_agent};

/// Canonical 256-byte (exactly 4 CPU cache lines) pure POD Access Log Record.
///
/// # Memory Layout (256 bytes, 64-byte aligned, zero compiler padding)
/// - `0..8`:    `request_id: u64`
/// - `8..16`:   `duration_ns: u64` (lossless nanoseconds, prevents 32-bit overflow on streaming/L4)
/// - `16..32`:  `peer_ip: u128` (16-byte aligned at offset 16; lossless IPv6 or RFC 4291 IPv4-mapped)
/// - `32..36`:  `timestamp_sec: u32`
/// - `36..40`:  `bytes_rx: u32`
/// - `40..44`:  `bytes_tx: u32`
/// - `44..48`:  `upstream_ip: u32`
/// - `48..50`:  `peer_port: u16`
/// - `50..52`:  `upstream_port: u16`
/// - `52..54`:  `status_code: u16`
/// - `54..56`:  `route_id: u16`
/// - `56..58`:  `ua_major_version: u16`
/// - `58..60`:  `rpc_msgs_or_flags: u16`
/// - `60..61`:  `pipe: PipeKind` (`#[repr(u8)]`, 1 byte, strongly typed pipeline variant)
/// - `61..66`:  5 × `u8` enum discriminants (`completion`, `method`, `grpc_status`, `ua_family`, `ua_os`)
/// - `66..68`:  `url_len: u16`
/// - `68..256`: `url_bytes: [u8; 188]` (188 bytes monolithic URL: scheme + host + path_and_query)
#[repr(C, align(64))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AccessLogRecord {
    pub request_id: u64,
    pub duration_ns: u64,
    pub peer_ip: u128,
    pub timestamp_sec: u32,
    pub bytes_rx: u32,
    pub bytes_tx: u32,
    pub upstream_ip: u32,
    pub peer_port: u16,
    pub upstream_port: u16,
    pub status_code: u16,
    pub route_id: u16,
    pub ua_major_version: u16,
    pub rpc_msgs_or_flags: u16,
    pub pipe: PipeKind,
    pub completion: u8,
    pub method: u8,
    pub grpc_status: u8,
    pub ua_family: u8,
    pub ua_os: u8,
    pub url_len: u16,
    pub url_bytes: [u8; 188],
}

const _: () = assert!(std::mem::size_of::<AccessLogRecord>() == 256);
const _: () = assert!(std::mem::align_of::<AccessLogRecord>() == 64);
const _: () = assert!(std::mem::offset_of!(AccessLogRecord, duration_ns) == 8);
const _: () = assert!(std::mem::offset_of!(AccessLogRecord, peer_ip) == 16);
const _: () = assert!(std::mem::offset_of!(AccessLogRecord, timestamp_sec) == 32);
const _: () = assert!(std::mem::offset_of!(AccessLogRecord, bytes_rx) == 36);
const _: () = assert!(std::mem::offset_of!(AccessLogRecord, peer_port) == 48);
const _: () = assert!(std::mem::offset_of!(AccessLogRecord, status_code) == 52);
const _: () = assert!(std::mem::offset_of!(AccessLogRecord, pipe) == 60);
const _: () = assert!(std::mem::offset_of!(AccessLogRecord, url_len) == 66);
const _: () = assert!(std::mem::offset_of!(AccessLogRecord, url_bytes) == 68);

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
            timestamp_sec: 0,
            bytes_rx: 0,
            bytes_tx: 0,
            upstream_ip: 0,
            peer_port: 0,
            upstream_port: 0,
            status_code: 0,
            route_id: 0,
            ua_major_version: 0,
            rpc_msgs_or_flags: 0,
            pipe: PipeKind::BufferedH1H1,
            completion: LogCompletionReason::Complete as u8,
            method: LogHttpMethod::Unknown as u8,
            grpc_status: LogGrpcStatus::NonGrpc as u8,
            ua_family: UaFamily::None as u8,
            ua_os: UaOs::Unknown as u8,
            url_len: 0,
            url_bytes: [0; 188],
        }
    }

    /// Accesses the stored monolithic URL as a string slice (or `"-"` if empty/unset).
    #[inline(always)]
    pub fn url(&self) -> &str {
        if self.url_len == 0 {
            "-"
        } else {
            let len = (self.url_len as usize).min(188);
            std::str::from_utf8(&self.url_bytes[..len]).unwrap_or("-")
        }
    }

    /// Backward-compatible alias for accessing the stored URL/path.
    #[inline(always)]
    pub fn path(&self) -> &str {
        self.url()
    }

    /// Sets the request URL, copying up to 188 bytes without allocation.
    #[inline(always)]
    pub fn set_url(&mut self, url: &str) {
        let b = url.as_bytes();
        let len = b.len().min(188);
        self.url_len = len as u16;
        self.url_bytes[..len].copy_from_slice(&b[..len]);
        if len < 188 {
            self.url_bytes[len..].fill(0);
        }
    }

    /// Backward-compatible alias for `set_url`.
    #[inline(always)]
    pub fn set_path(&mut self, path: &str) {
        self.set_url(path);
    }

    /// Builder pattern helper to set the request URL.
    #[inline(always)]
    pub fn with_url(mut self, url: &str) -> Self {
        self.set_url(url);
        self
    }

    /// Backward-compatible alias for `with_url`.
    #[inline(always)]
    pub fn with_path(mut self, path: &str) -> Self {
        self.set_url(path);
        self
    }

    /// Formats a complete URL (scheme + host + path_and_query) directly into the 188-byte buffer
    /// with zero heap allocations.
    #[inline(always)]
    pub fn encode_url(is_tls: bool, host: Option<&str>, path_and_query: &str) -> (u16, [u8; 188]) {
        let mut bytes = [0u8; 188];
        let mut len = 0;

        if let Some(h) = host {
            let scheme: &[u8] = if is_tls { b"https://" } else { b"http://" };
            let copy_s = scheme.len().min(188 - len);
            let mut i = 0;
            while i < copy_s {
                bytes[len + i] = scheme[i];
                i += 1;
            }
            len += copy_s;

            let h_bytes = h.as_bytes();
            let copy_h = h_bytes.len().min(188 - len);
            let mut i = 0;
            while i < copy_h {
                bytes[len + i] = h_bytes[i];
                i += 1;
            }
            len += copy_h;
        }

        let pq_bytes = path_and_query.as_bytes();
        let copy_pq = pq_bytes.len().min(188 - len);
        let mut i = 0;
        while i < copy_pq {
            bytes[len + i] = pq_bytes[i];
            i += 1;
        }
        len += copy_pq;

        (len as u16, bytes)
    }

    /// Encodes a raw byte slice into the fixed 188-byte URL buffer without heap allocation.
    #[inline(always)]
    pub const fn from_url_bytes(url: &[u8]) -> (u16, [u8; 188]) {
        let len = if url.len() > 188 { 188 } else { url.len() };
        let mut bytes = [0u8; 188];
        let mut i = 0;
        while i < len {
            bytes[i] = url[i];
            i += 1;
        }
        (len as u16, bytes)
    }

    /// Backward-compatible alias for `from_url_bytes`.
    #[inline(always)]
    pub const fn from_path_bytes(path: &[u8]) -> (u16, [u8; 188]) {
        Self::from_url_bytes(path)
    }

    /// Returns the concise, canonical pipeline execution identifier (e.g. `"buffered_h1_h1"`,
    /// `"server_stream_h2_h2"`, `"buffered_grpc_tcp_tcp"`, `"tcp_tcp"`).
    #[inline(always)]
    pub const fn pipe_name(&self) -> &'static str {
        self.pipe.as_str()
    }

    /// Returns the strongly-typed canonical pipeline execution kind.
    #[inline(always)]
    pub const fn pipe_kind(&self) -> PipeKind {
        self.pipe
    }

    /// Returns the downstream wire protocol derived directly from the `pipe` discriminant.
    #[inline(always)]
    pub const fn downstream_protocol(&self) -> LogProtocol {
        LogProtocol::from_u8(self.pipe.downstream_protocol_u8())
    }

    /// Returns the upstream wire protocol derived directly from the `pipe` discriminant.
    #[inline(always)]
    pub const fn upstream_protocol(&self) -> LogProtocol {
        LogProtocol::from_u8(self.pipe.upstream_protocol_u8())
    }

    /// Constructs an integer record from request execution scalars.
    #[allow(clippy::too_many_arguments)]
    #[inline]
    pub fn from_scalars(
        request_id: u64,
        timestamp_sec: u32,
        duration_ns: u64,
        peer: SocketAddr,
        upstream: Option<SocketAddr>,
        bytes_rx: u32,
        bytes_tx: u32,
        status_code: u16,
        route_id: u16,
        rpc_msgs_or_flags: u16,
        pipe: PipeKind,
        completion: LogCompletionReason,
        method: LogHttpMethod,
        grpc_status: LogGrpcStatus,
        user_agent_bytes: &[u8],
    ) -> Self {
        let (ua_family, ua_os, ua_major_version) = classify_user_agent(user_agent_bytes);
        let (upstream_ip, upstream_port) = match upstream {
            Some(addr) => (encode_upstream_ip_u32(addr.ip()), addr.port()),
            None => (0, 0),
        };

        Self {
            request_id,
            duration_ns,
            peer_ip: encode_ip_u128(peer.ip()),
            timestamp_sec,
            bytes_rx,
            bytes_tx,
            upstream_ip,
            peer_port: peer.port(),
            upstream_port,
            status_code,
            route_id,
            ua_major_version,
            rpc_msgs_or_flags,
            pipe,
            completion: completion as u8,
            method: method as u8,
            grpc_status: grpc_status as u8,
            ua_family: ua_family as u8,
            ua_os: ua_os as u8,
            url_len: 0,
            url_bytes: [0; 188],
        }
    }
}
