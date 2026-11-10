//! Canonical 64-Byte Cacheline Access Log Record (`velda_observability::logs::record`).
//!
//! # Hardware Layout & Rationale
//! Exactly 64 bytes (`#[repr(C, align(64))]`, 1 CPU cache line) with zero compiler padding and
//! zero heap pointers. Every field is a fixed-width integer or finite enum discriminant so that
//! emitting a log record is a single 64-byte value copy (`Copy`).

use std::net::SocketAddr;

use crate::metrics::PipeStrategy;

use super::types::{
    LogCompletionReason, LogGrpcStatus, LogHttpMethod, LogProtocol, encode_ip_u128,
    encode_upstream_ip_u32,
};
use super::user_agent::{UaFamily, UaOs, classify_user_agent};

/// Canonical 64-byte (single CPU cache line) pure-integer Access Log Record.
///
/// # Memory Layout (64 bytes, 64-byte aligned, zero compiler padding)
/// - `0..8`:   `request_id: u64`
/// - `8..12`:  `timestamp_sec: u32`
/// - `12..16`: `duration_us: u32`
/// - `16..32`: `peer_ip: u128` (16-byte aligned at offset 16; lossless IPv6 or RFC 4291 IPv4-mapped)
/// - `32..36`: `bytes_rx: u32`
/// - `36..40`: `bytes_tx: u32`
/// - `40..44`: `upstream_ip: u32`
/// - `44..46`: `peer_port: u16`
/// - `46..48`: `upstream_port: u16`
/// - `48..50`: `status_code: u16`
/// - `50..52`: `route_id: u16`
/// - `52..54`: `ua_major_version: u16`
/// - `54..56`: `rpc_msgs_or_flags: u16`
/// - `56..64`: 8 × `u8` enum discriminants (`downstream_proto`, `upstream_proto`, `pipe_strategy`,
///   `completion`, `method`, `grpc_status`, `ua_family`, `ua_os`)
#[repr(C, align(64))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AccessLogRecord {
    pub request_id: u64,
    pub timestamp_sec: u32,
    pub duration_us: u32,
    pub peer_ip: u128,
    pub bytes_rx: u32,
    pub bytes_tx: u32,
    pub upstream_ip: u32,
    pub peer_port: u16,
    pub upstream_port: u16,
    pub status_code: u16,
    pub route_id: u16,
    pub ua_major_version: u16,
    pub rpc_msgs_or_flags: u16,
    pub downstream_proto: u8,
    pub upstream_proto: u8,
    pub pipe_strategy: u8,
    pub completion: u8,
    pub method: u8,
    pub grpc_status: u8,
    pub ua_family: u8,
    pub ua_os: u8,
}

const _: () = assert!(std::mem::size_of::<AccessLogRecord>() == 64);
const _: () = assert!(std::mem::align_of::<AccessLogRecord>() == 64);
const _: () = assert!(std::mem::offset_of!(AccessLogRecord, peer_ip) == 16);
const _: () = assert!(std::mem::offset_of!(AccessLogRecord, bytes_rx) == 32);
const _: () = assert!(std::mem::offset_of!(AccessLogRecord, status_code) == 48);
const _: () = assert!(std::mem::offset_of!(AccessLogRecord, downstream_proto) == 56);

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
            timestamp_sec: 0,
            duration_us: 0,
            peer_ip: 0,
            bytes_rx: 0,
            bytes_tx: 0,
            upstream_ip: 0,
            peer_port: 0,
            upstream_port: 0,
            status_code: 0,
            route_id: 0,
            ua_major_version: 0,
            rpc_msgs_or_flags: 0,
            downstream_proto: LogProtocol::H1 as u8,
            upstream_proto: LogProtocol::H1 as u8,
            pipe_strategy: PipeStrategy::Buffered as u8,
            completion: LogCompletionReason::Complete as u8,
            method: LogHttpMethod::Unknown as u8,
            grpc_status: LogGrpcStatus::NonGrpc as u8,
            ua_family: UaFamily::None as u8,
            ua_os: UaOs::Unknown as u8,
        }
    }

    /// Constructs a 64-byte integer record from request execution scalars.
    #[allow(clippy::too_many_arguments)]
    #[inline]
    pub fn from_scalars(
        request_id: u64,
        timestamp_sec: u32,
        duration_us: u32,
        peer: SocketAddr,
        upstream: Option<SocketAddr>,
        bytes_rx: u32,
        bytes_tx: u32,
        status_code: u16,
        route_id: u16,
        rpc_msgs_or_flags: u16,
        downstream_proto: LogProtocol,
        upstream_proto: LogProtocol,
        pipe_strategy: PipeStrategy,
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
            timestamp_sec,
            duration_us,
            peer_ip: encode_ip_u128(peer.ip()),
            bytes_rx,
            bytes_tx,
            upstream_ip,
            peer_port: peer.port(),
            upstream_port,
            status_code,
            route_id,
            ua_major_version,
            rpc_msgs_or_flags,
            downstream_proto: downstream_proto as u8,
            upstream_proto: upstream_proto as u8,
            pipe_strategy: pipe_strategy as u8,
            completion: completion as u8,
            method: method as u8,
            grpc_status: grpc_status as u8,
            ua_family: ua_family as u8,
            ua_os: ua_os as u8,
        }
    }
}
