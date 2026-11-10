//! Finite Enum Discriminants & Lossless Integer IP Codecs (`velda_observability::logs::types`).
//!
//! # Mechanism & Rationale
//! Converts dynamic wire attributes (protocol family, HTTP method, gRPC status, stream completion,
//! and IPv4/IPv6 addresses) into fixed-width integer values so that [`super::AccessLogRecord`]
//! remains a pure 64-byte `Copy` struct with zero heap pointers.

use std::net::{IpAddr, Ipv6Addr};

/// Unified wire protocol discriminant for access log records.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum LogProtocol {
    H1 = 0,
    H2 = 1,
    H3 = 2,
    GrpcTcp = 3,
    GrpcUdp = 4,
    RawTcp = 5,
    RawUdp = 6,
}

impl LogProtocol {
    #[inline(always)]
    pub const fn from_u8(v: u8) -> Self {
        match v {
            0 => Self::H1,
            1 => Self::H2,
            2 => Self::H3,
            3 => Self::GrpcTcp,
            4 => Self::GrpcUdp,
            5 => Self::RawTcp,
            6 => Self::RawUdp,
            _ => Self::H1,
        }
    }

    #[inline(always)]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::H1 => "http/1.1",
            Self::H2 => "http/2",
            Self::H3 => "http/3",
            Self::GrpcTcp => "grpc/tcp",
            Self::GrpcUdp => "grpc/udp",
            Self::RawTcp => "raw/tcp",
            Self::RawUdp => "raw/udp",
        }
    }

    /// Maps the wire protocol to its coarse family label (`"http"`, `"grpc"`, `"raw"`)
    /// so multi-stream log indexers (such as Loki) group streams without per-plugin branching.
    #[inline(always)]
    pub const fn family_str(self) -> &'static str {
        match self {
            Self::H1 | Self::H2 | Self::H3 => "http",
            Self::GrpcTcp | Self::GrpcUdp => "grpc",
            Self::RawTcp | Self::RawUdp => "raw",
        }
    }
}

/// Finite HTTP method discriminant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum LogHttpMethod {
    Unknown = 0,
    Get = 1,
    Post = 2,
    Put = 3,
    Delete = 4,
    Head = 5,
    Options = 6,
    Patch = 7,
    Connect = 8,
    Trace = 9,
}

impl LogHttpMethod {
    #[inline(always)]
    pub const fn from_u8(v: u8) -> Self {
        match v {
            1 => Self::Get,
            2 => Self::Post,
            3 => Self::Put,
            4 => Self::Delete,
            5 => Self::Head,
            6 => Self::Options,
            7 => Self::Patch,
            8 => Self::Connect,
            9 => Self::Trace,
            _ => Self::Unknown,
        }
    }

    /// Classifies an ASCII method slice by length and leading bytes to avoid byte-by-byte loops.
    #[inline(always)]
    pub const fn from_bytes(b: &[u8]) -> Self {
        match b.len() {
            3 => match (b[0], b[1], b[2]) {
                (b'G', b'E', b'T') => Self::Get,
                (b'P', b'U', b'T') => Self::Put,
                _ => Self::Unknown,
            },
            4 => match (b[0], b[1], b[2], b[3]) {
                (b'P', b'O', b'S', b'T') => Self::Post,
                (b'H', b'E', b'A', b'D') => Self::Head,
                _ => Self::Unknown,
            },
            5 => match (b[0], b[1], b[2], b[3], b[4]) {
                (b'P', b'A', b'T', b'C', b'H') => Self::Patch,
                (b'T', b'R', b'A', b'C', b'E') => Self::Trace,
                _ => Self::Unknown,
            },
            6 => match (b[0], b[1], b[2], b[3], b[4], b[5]) {
                (b'D', b'E', b'L', b'E', b'T', b'E') => Self::Delete,
                _ => Self::Unknown,
            },
            7 => match (b[0], b[1], b[2], b[3]) {
                (b'O', b'P', b'T', b'I') => Self::Options,
                (b'C', b'O', b'N', b'N') => Self::Connect,
                _ => Self::Unknown,
            },
            _ => Self::Unknown,
        }
    }

    #[inline(always)]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unknown => "NONE",
            Self::Get => "GET",
            Self::Post => "POST",
            Self::Put => "PUT",
            Self::Delete => "DELETE",
            Self::Head => "HEAD",
            Self::Options => "OPTIONS",
            Self::Patch => "PATCH",
            Self::Connect => "CONNECT",
            Self::Trace => "TRACE",
        }
    }
}

/// Canonical gRPC status code discriminant (0..16, or 255 when non-gRPC).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum LogGrpcStatus {
    Ok = 0,
    Cancelled = 1,
    Unknown = 2,
    InvalidArgument = 3,
    DeadlineExceeded = 4,
    NotFound = 5,
    AlreadyExists = 6,
    PermissionDenied = 7,
    ResourceExhausted = 8,
    FailedPrecondition = 9,
    Aborted = 10,
    OutOfRange = 11,
    Unimplemented = 12,
    Internal = 13,
    Unavailable = 14,
    DataLoss = 15,
    Unauthenticated = 16,
    NonGrpc = 255,
}

impl LogGrpcStatus {
    #[inline(always)]
    pub const fn from_u8(v: u8) -> Self {
        match v {
            0 => Self::Ok,
            1 => Self::Cancelled,
            2 => Self::Unknown,
            3 => Self::InvalidArgument,
            4 => Self::DeadlineExceeded,
            5 => Self::NotFound,
            6 => Self::AlreadyExists,
            7 => Self::PermissionDenied,
            8 => Self::ResourceExhausted,
            9 => Self::FailedPrecondition,
            10 => Self::Aborted,
            11 => Self::OutOfRange,
            12 => Self::Unimplemented,
            13 => Self::Internal,
            14 => Self::Unavailable,
            15 => Self::DataLoss,
            16 => Self::Unauthenticated,
            _ => Self::NonGrpc,
        }
    }

    #[inline(always)]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "OK",
            Self::Cancelled => "CANCELLED",
            Self::Unknown => "UNKNOWN",
            Self::InvalidArgument => "INVALID_ARGUMENT",
            Self::DeadlineExceeded => "DEADLINE_EXCEEDED",
            Self::NotFound => "NOT_FOUND",
            Self::AlreadyExists => "ALREADY_EXISTS",
            Self::PermissionDenied => "PERMISSION_DENIED",
            Self::ResourceExhausted => "RESOURCE_EXHAUSTED",
            Self::FailedPrecondition => "FAILED_PRECONDITION",
            Self::Aborted => "ABORTED",
            Self::OutOfRange => "OUT_OF_RANGE",
            Self::Unimplemented => "UNIMPLEMENTED",
            Self::Internal => "INTERNAL",
            Self::Unavailable => "UNAVAILABLE",
            Self::DataLoss => "DATA_LOSS",
            Self::Unauthenticated => "UNAUTHENTICATED",
            Self::NonGrpc => "NONE",
        }
    }
}

/// Stream termination disposition discriminant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum LogCompletionReason {
    Complete = 0,
    ClientCancel = 1,
    UpstreamError = 2,
    Timeout = 3,
    Rejected = 4,
}

impl LogCompletionReason {
    #[inline(always)]
    pub const fn from_u8(v: u8) -> Self {
        match v {
            0 => Self::Complete,
            1 => Self::ClientCancel,
            2 => Self::UpstreamError,
            3 => Self::Timeout,
            4 => Self::Rejected,
            _ => Self::Complete,
        }
    }

    #[inline(always)]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Complete => "complete",
            Self::ClientCancel => "client_cancel",
            Self::UpstreamError => "upstream_error",
            Self::Timeout => "timeout",
            Self::Rejected => "rejected",
        }
    }
}

/// Encodes an [`IpAddr`] losslessly into a 128-bit integer using RFC 4291 IPv4-mapped encoding
/// (`0x0000_0000_0000_0000_0000_FFFF_<ipv4_u32>`) so both IPv4 and IPv6 occupy a single
/// integer field without an extra version discriminator flag.
#[inline(always)]
pub const fn encode_ip_u128(ip: IpAddr) -> u128 {
    match ip {
        IpAddr::V4(v4) => v4.to_ipv6_mapped().to_bits(),
        IpAddr::V6(v6) => v6.to_bits(),
    }
}

/// Restores the canonical [`IpAddr`] (`IpAddr::V4` or `IpAddr::V6`) from its RFC 4291 `u128` integer.
#[inline(always)]
pub const fn decode_ip_u128(bits: u128) -> IpAddr {
    Ipv6Addr::from_bits(bits).to_canonical()
}

/// Encodes an upstream backend [`IpAddr`] into a 32-bit integer (native IPv4 bits or FNV1a-32 of IPv6).
#[inline(always)]
pub const fn encode_upstream_ip_u32(ip: IpAddr) -> u32 {
    match ip {
        IpAddr::V4(v4) => v4.to_bits(),
        IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
            Some(v4) => v4.to_bits(),
            None => {
                let octets = v6.octets();
                let mut h: u32 = 0x811c_9dc5;
                let mut i = 0;
                while i < 16 {
                    h ^= octets[i] as u32;
                    h = h.wrapping_mul(0x0100_0193);
                    i += 1;
                }
                h
            }
        },
    }
}
