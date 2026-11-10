//! Lock-Free 64-Byte Integer Access Log Engine (`velda_observability::access_log`).
//!
//! # Architectural Design & Hardware Rationale
//! 1. **Single 64-Byte Cacheline Record (`AccessLogRecord`)**:
//!    Every field—including protocol family, HTTP method, gRPC status, client IP (RFC 4291
//!    `u128` IPv4-mapped/IPv6), and User-Agent (`UaFamily`, `UaOs`, `ua_major_version`)—is
//!    encoded as a fixed-width integer or finite enum discriminant. Eliminating heap strings
//!    and pointers shrinks each record to exactly 64 bytes (1 CPU cache line) and guarantees
//!    `Copy` value semantics so Frontline workers can immediately clear `HeaderWorkspace`
//!    after emitting without Use-After-Free risks.
//! 2. **L1 Cache Preservation ("L1 is Gold")**:
//!    - **Frontline Push (`SpscLogRing::push`)**: Writes the 64-byte record into the core-local
//!      pre-allocated SPSC ring using Non-Temporal Streaming Stores (`_mm256_stream_si256` +
//!      `_mm_sfence()` on x86_64). Data routes through Write-Combining buffers directly to
//!      L3/RAM without allocating cache lines in L1d, preserving L1d exclusively for routing
//!      tables and active socket state machines.
//!    - **Shadow Drain (`SpscLogRing::drain_batch`)**: Issues `_mm_prefetch(..., _MM_HINT_T1)`
//!      to bring queued slots into L2/L3 only, never polluting L1d during background draining.
//! 3. **Reference-Counted Multi-Exporter Fan-Out (`Arc<[AccessLogRecord]>`)**:
//!    Shadow Workers drain raw SPSC rings into immutable `Arc<[AccessLogRecord]>` slices whose
//!    atomic reference counter equals the number of active background exporter plugins. Each
//!    exporter reads and formats independently; when the last exporter drops its `Arc`, Rust
//!    reclaims the batch memory automatically with zero torn reads and zero lock contention.

use std::cell::UnsafeCell;
use std::collections::VecDeque;
use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use crate::store::{MAX_METRIC_WORKERS, current_worker_lane};
use crate::types::PipeStrategy;

/// Default per-core SPSC ring capacity (4096 records × 64B = 256 KB per core).
pub const DEFAULT_CORE_RING_CAPACITY: usize = 4096;

#[repr(align(64))]
struct CachePadded<T>(T);

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

/// Finite User-Agent client family classification.
///
/// Bounding User-Agent to a closed enum eliminates variable-length heap strings on the hot
/// path and prevents high-cardinality index explosions in downstream log stores (such as Loki).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum UaFamily {
    None = 0,
    Chrome = 1,
    Safari = 2,
    Firefox = 3,
    Edge = 4,
    Opera = 5,
    Curl = 6,
    Wget = 7,
    Postman = 8,
    GoHttp = 9,
    GrpcGo = 10,
    Tonic = 11,
    GrpcJava = 12,
    Python = 13,
    Node = 14,
    Reqwest = 15,
    JavaHttp = 16,
    KubeProbe = 17,
    VeldaHealth = 18,
    AwsElb = 19,
    BotGoogle = 20,
    BotBing = 21,
    BotCloudflare = 22,
    Scanner = 23,
    Custom = 255,
}

impl UaFamily {
    #[inline(always)]
    pub const fn from_u8(v: u8) -> Self {
        match v {
            0 => Self::None,
            1 => Self::Chrome,
            2 => Self::Safari,
            3 => Self::Firefox,
            4 => Self::Edge,
            5 => Self::Opera,
            6 => Self::Curl,
            7 => Self::Wget,
            8 => Self::Postman,
            9 => Self::GoHttp,
            10 => Self::GrpcGo,
            11 => Self::Tonic,
            12 => Self::GrpcJava,
            13 => Self::Python,
            14 => Self::Node,
            15 => Self::Reqwest,
            16 => Self::JavaHttp,
            17 => Self::KubeProbe,
            18 => Self::VeldaHealth,
            19 => Self::AwsElb,
            20 => Self::BotGoogle,
            21 => Self::BotBing,
            22 => Self::BotCloudflare,
            23 => Self::Scanner,
            _ => Self::Custom,
        }
    }

    #[inline(always)]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Chrome => "chrome",
            Self::Safari => "safari",
            Self::Firefox => "firefox",
            Self::Edge => "edge",
            Self::Opera => "opera",
            Self::Curl => "curl",
            Self::Wget => "wget",
            Self::Postman => "postman",
            Self::GoHttp => "go-http-client",
            Self::GrpcGo => "grpc-go",
            Self::Tonic => "tonic",
            Self::GrpcJava => "grpc-java",
            Self::Python => "python",
            Self::Node => "node",
            Self::Reqwest => "reqwest",
            Self::JavaHttp => "java-http",
            Self::KubeProbe => "kube-probe",
            Self::VeldaHealth => "velda-healthcheck",
            Self::AwsElb => "aws-elb",
            Self::BotGoogle => "googlebot",
            Self::BotBing => "bingbot",
            Self::BotCloudflare => "cloudflare",
            Self::Scanner => "scanner",
            Self::Custom => "custom",
        }
    }
}

/// Finite User-Agent operating system / platform classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum UaOs {
    Unknown = 0,
    Linux = 1,
    Windows = 2,
    MacOS = 3,
    Android = 4,
    Ios = 5,
    BotOrCli = 6,
}

impl UaOs {
    #[inline(always)]
    pub const fn from_u8(v: u8) -> Self {
        match v {
            1 => Self::Linux,
            2 => Self::Windows,
            3 => Self::MacOS,
            4 => Self::Android,
            5 => Self::Ios,
            6 => Self::BotOrCli,
            _ => Self::Unknown,
        }
    }

    #[inline(always)]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::Linux => "linux",
            Self::Windows => "windows",
            Self::MacOS => "macos",
            Self::Android => "android",
            Self::Ios => "ios",
            Self::BotOrCli => "cli",
        }
    }
}

/// Classifies a raw `User-Agent` header byte slice while it is cache-hot in L1d into
/// `(UaFamily, UaOs, major_version)`.
#[inline]
pub fn classify_user_agent(ua: &[u8]) -> (UaFamily, UaOs, u16) {
    if ua.is_empty() {
        return (UaFamily::None, UaOs::Unknown, 0);
    }

    // Fast prefix check for CLI, RPC, and infrastructure health tools
    if starts_with_ignore_case(ua, b"curl/") {
        return (UaFamily::Curl, UaOs::BotOrCli, parse_leading_u16(&ua[5..]));
    }
    if starts_with_ignore_case(ua, b"Velda-HealthCheck") {
        return (UaFamily::VeldaHealth, UaOs::BotOrCli, 1);
    }
    if starts_with_ignore_case(ua, b"kube-probe/") {
        return (
            UaFamily::KubeProbe,
            UaOs::BotOrCli,
            parse_leading_u16(&ua[11..]),
        );
    }
    if starts_with_ignore_case(ua, b"Go-http-client/") {
        return (
            UaFamily::GoHttp,
            UaOs::BotOrCli,
            parse_leading_u16(&ua[15..]),
        );
    }
    if starts_with_ignore_case(ua, b"grpc-go/") {
        return (
            UaFamily::GrpcGo,
            UaOs::BotOrCli,
            parse_leading_u16(&ua[8..]),
        );
    }
    if starts_with_ignore_case(ua, b"tonic/") {
        return (UaFamily::Tonic, UaOs::BotOrCli, parse_leading_u16(&ua[6..]));
    }
    if starts_with_ignore_case(ua, b"grpc-java") {
        return (UaFamily::GrpcJava, UaOs::BotOrCli, 1);
    }
    if starts_with_ignore_case(ua, b"PostmanRuntime/") {
        return (
            UaFamily::Postman,
            UaOs::BotOrCli,
            parse_leading_u16(&ua[15..]),
        );
    }
    if starts_with_ignore_case(ua, b"Wget/") {
        return (UaFamily::Wget, UaOs::BotOrCli, parse_leading_u16(&ua[5..]));
    }
    if starts_with_ignore_case(ua, b"python-requests/") || starts_with_ignore_case(ua, b"Python/") {
        return (UaFamily::Python, UaOs::BotOrCli, 3);
    }
    if starts_with_ignore_case(ua, b"reqwest/") {
        return (
            UaFamily::Reqwest,
            UaOs::BotOrCli,
            parse_leading_u16(&ua[8..]),
        );
    }
    if starts_with_ignore_case(ua, b"node") || starts_with_ignore_case(ua, b"undici") {
        return (UaFamily::Node, UaOs::BotOrCli, 20);
    }
    if starts_with_ignore_case(ua, b"ELB-HealthChecker/") {
        return (
            UaFamily::AwsElb,
            UaOs::BotOrCli,
            parse_leading_u16(&ua[18..]),
        );
    }

    // Browser & Bot scanning over bounded window (first 128 bytes max)
    let scan = if ua.len() > 128 { &ua[..128] } else { ua };

    let os = if find_subslice(scan, b"Android").is_some() {
        UaOs::Android
    } else if find_subslice(scan, b"iPhone").is_some() || find_subslice(scan, b"iPad").is_some() {
        UaOs::Ios
    } else if find_subslice(scan, b"Windows").is_some() {
        UaOs::Windows
    } else if find_subslice(scan, b"Macintosh").is_some()
        || find_subslice(scan, b"Mac OS").is_some()
    {
        UaOs::MacOS
    } else if find_subslice(scan, b"Linux").is_some() {
        UaOs::Linux
    } else {
        UaOs::Unknown
    };

    if let Some(pos) = find_subslice(scan, b"Edg/") {
        return (UaFamily::Edge, os, parse_leading_u16(&scan[pos + 4..]));
    }
    if let Some(pos) = find_subslice(scan, b"OPR/") {
        return (UaFamily::Opera, os, parse_leading_u16(&scan[pos + 4..]));
    }
    if let Some(pos) = find_subslice(scan, b"Chrome/") {
        return (UaFamily::Chrome, os, parse_leading_u16(&scan[pos + 7..]));
    }
    if let Some(pos) = find_subslice(scan, b"Firefox/") {
        return (UaFamily::Firefox, os, parse_leading_u16(&scan[pos + 8..]));
    }
    if let Some(pos) = find_subslice(scan, b"Version/")
        && find_subslice(scan, b"Safari/").is_some()
    {
        return (UaFamily::Safari, os, parse_leading_u16(&scan[pos + 8..]));
    }
    if find_subslice(scan, b"Googlebot").is_some() {
        return (UaFamily::BotGoogle, UaOs::BotOrCli, 2);
    }
    if find_subslice(scan, b"bingbot").is_some() {
        return (UaFamily::BotBing, UaOs::BotOrCli, 2);
    }
    if find_subslice(scan, b"Cloudflare").is_some() {
        return (UaFamily::BotCloudflare, UaOs::BotOrCli, 1);
    }
    if find_subslice(scan, b"sqlmap").is_some()
        || find_subslice(scan, b"nikto").is_some()
        || find_subslice(scan, b"nuclei").is_some()
        || find_subslice(scan, b"zgrab").is_some()
    {
        return (UaFamily::Scanner, UaOs::BotOrCli, 1);
    }

    (UaFamily::Custom, os, 0)
}

#[inline(always)]
fn starts_with_ignore_case(haystack: &[u8], prefix: &[u8]) -> bool {
    if haystack.len() < prefix.len() {
        return false;
    }
    let mut i = 0;
    while i < prefix.len() {
        if !haystack[i].eq_ignore_ascii_case(&prefix[i]) {
            return false;
        }
        i += 1;
    }
    true
}

#[inline(always)]
fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || haystack.len() < needle.len() {
        return None;
    }
    let first = needle[0];
    let max = haystack.len() - needle.len();
    let mut i = 0;
    while i <= max {
        if haystack[i] == first && &haystack[i..i + needle.len()] == needle {
            return Some(i);
        }
        i += 1;
    }
    None
}

#[inline(always)]
fn parse_leading_u16(bytes: &[u8]) -> u16 {
    let mut val: u16 = 0;
    let mut i = 0;
    while i < bytes.len() && i < 5 {
        let b = bytes[i];
        if b.is_ascii_digit() {
            val = val.saturating_mul(10).saturating_add((b - b'0') as u16);
        } else {
            break;
        }
        i += 1;
    }
    val
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

/// Pre-allocated per-core Single-Producer Single-Consumer (SPSC) 64-byte aligned ring buffer.
///
/// `velda-worker-{id}` is the single producer; `velda-shadow-{id}` is the single consumer.
/// Because `AccessLogRecord` is `#[repr(C, align(64))]`, Rust's allocator guarantees that
/// `Box<[UnsafeCell<AccessLogRecord>]>` has a base alignment of 64 bytes and stride of 64 bytes.
pub struct SpscLogRing {
    capacity: usize,
    mask: usize,
    buffer: Box<[UnsafeCell<AccessLogRecord>]>,
    head: CachePadded<AtomicUsize>,
    cached_tail: CachePadded<AtomicUsize>,
    tail: CachePadded<AtomicUsize>,
    cached_head: CachePadded<AtomicUsize>,
    pub dropped: AtomicU64,
}

unsafe impl Send for SpscLogRing {}
unsafe impl Sync for SpscLogRing {}

impl SpscLogRing {
    pub fn new(capacity: usize) -> Self {
        let cap = capacity.max(64).next_power_of_two();
        let mask = cap - 1;
        let mut vec = Vec::with_capacity(cap);
        for _ in 0..cap {
            vec.push(UnsafeCell::new(AccessLogRecord::zeroed()));
        }

        Self {
            capacity: cap,
            mask,
            buffer: vec.into_boxed_slice(),
            head: CachePadded(AtomicUsize::new(0)),
            cached_tail: CachePadded(AtomicUsize::new(0)),
            tail: CachePadded(AtomicUsize::new(0)),
            cached_head: CachePadded(AtomicUsize::new(0)),
            dropped: AtomicU64::new(0),
        }
    }

    /// Writes a 64-byte [`AccessLogRecord`] into the SPSC ring using Non-Temporal Streaming Stores
    /// (`VMOVNTDQ` / `MOVNTDQ` + `_mm_sfence()`) to bypass L1d cache allocation ("L1 is gold").
    ///
    /// Non-temporal stores bypass x86 Total Store Order (TSO) via Write-Combining buffers, so
    /// `_mm_sfence()` is required before the `Ordering::Release` store on `head` to prevent the
    /// Shadow Worker from observing an updated `head` before the 64-byte slot lands in L3/RAM.
    #[inline]
    pub fn push(&self, record: &AccessLogRecord) -> bool {
        let head = self.head.0.load(Ordering::Relaxed);
        let mut tail = self.cached_tail.0.load(Ordering::Relaxed);

        if head.wrapping_sub(tail) >= self.capacity {
            tail = self.tail.0.load(Ordering::Acquire);
            self.cached_tail.0.store(tail, Ordering::Relaxed);
            if head.wrapping_sub(tail) >= self.capacity {
                self.dropped.fetch_add(1, Ordering::Relaxed);
                return false;
            }
        }

        let idx = head & self.mask;
        unsafe {
            let dst = self.buffer.get_unchecked(idx).get();
            debug_assert!((dst as usize) & 63 == 0);

            #[cfg(target_arch = "x86_64")]
            {
                use core::arch::x86_64::*;
                if std::is_x86_feature_detected!("avx2") {
                    let s = record as *const AccessLogRecord as *const __m256i;
                    let d = dst as *mut __m256i;
                    let v0 = _mm256_load_si256(s);
                    let v1 = _mm256_load_si256(s.add(1));
                    _mm256_stream_si256(d, v0);
                    _mm256_stream_si256(d.add(1), v1);
                    _mm_sfence();
                } else {
                    let s = record as *const AccessLogRecord as *const __m128i;
                    let d = dst as *mut __m128i;
                    let v0 = _mm_load_si128(s);
                    let v1 = _mm_load_si128(s.add(1));
                    let v2 = _mm_load_si128(s.add(2));
                    let v3 = _mm_load_si128(s.add(3));
                    _mm_stream_si128(d, v0);
                    _mm_stream_si128(d.add(1), v1);
                    _mm_stream_si128(d.add(2), v2);
                    _mm_stream_si128(d.add(3), v3);
                    _mm_sfence();
                }
            }

            #[cfg(not(target_arch = "x86_64"))]
            {
                std::ptr::copy_nonoverlapping(record as *const AccessLogRecord, dst, 1);
                std::sync::atomic::fence(Ordering::Release);
            }
        }

        self.head.0.store(head.wrapping_add(1), Ordering::Release);
        true
    }

    /// Discards all queued records in $\mathcal{O}(1)$ when no exporter plugins are active.
    #[inline]
    pub fn discard_all(&self) -> usize {
        let tail = self.tail.0.load(Ordering::Relaxed);
        let head = self.head.0.load(Ordering::Acquire);
        let count = head.wrapping_sub(tail);
        if count > 0 {
            self.cached_head.0.store(head, Ordering::Relaxed);
            self.tail.0.store(head, Ordering::Release);
        }
        count
    }

    /// Drains up to `max_records` from the ring using `_MM_HINT_T1` (L2/L3 cache prefetch only)
    /// so background draining on Shadow Worker never evicts hot L1d lines of the sibling thread.
    pub fn drain_batch(&self, max_records: usize) -> Vec<AccessLogRecord> {
        let tail = self.tail.0.load(Ordering::Relaxed);
        let mut head = self.cached_head.0.load(Ordering::Relaxed);

        let mut avail = head.wrapping_sub(tail);
        if avail == 0 {
            head = self.head.0.load(Ordering::Acquire);
            self.cached_head.0.store(head, Ordering::Relaxed);
            avail = head.wrapping_sub(tail);
            if avail == 0 {
                return Vec::new();
            }
        }

        let to_take = avail.min(max_records);
        let mut out = Vec::with_capacity(to_take);

        for i in 0..to_take {
            let idx = tail.wrapping_add(i) & self.mask;
            #[cfg(target_arch = "x86_64")]
            if i + 4 < to_take {
                let prefetch_idx = tail.wrapping_add(i + 4) & self.mask;
                unsafe {
                    use core::arch::x86_64::*;
                    let slot = self.buffer.get_unchecked(prefetch_idx).get() as *const i8;
                    _mm_prefetch(slot, _MM_HINT_T1);
                }
            }
            unsafe {
                let ptr = self.buffer.get_unchecked(idx).get();
                out.push(std::ptr::read(ptr));
            }
        }

        self.tail
            .0
            .store(tail.wrapping_add(to_take), Ordering::Release);
        out
    }
}

struct SubscriberInner {
    id: u64,
    max_batches: usize,
    queue: Mutex<VecDeque<Arc<[AccessLogRecord]>>>,
    pub dropped_batches: AtomicU64,
}

/// RAII handle held by an active background log exporter plugin.
///
/// Each received batch is an `Arc<[AccessLogRecord]>` shared across all active exporters.
/// Dropping the batch decrements its reference counter; when the last exporter finishes,
/// Rust automatically deallocates the batch slice. Dropping `LogSubscriber` itself
/// unregisters the exporter from [`LogStore`].
pub struct LogSubscriber {
    inner: Arc<SubscriberInner>,
    store: Arc<LogStore>,
}

impl LogSubscriber {
    /// Pops the next reference-counted batch of [`AccessLogRecord`]s if available.
    pub fn try_recv_batch(&self) -> Option<Arc<[AccessLogRecord]>> {
        self.inner.queue.lock().unwrap().pop_front()
    }

    /// Drains up to `max_batches` currently queued batches for bulk export.
    pub fn drain_batches(&self, max_batches: usize) -> Vec<Arc<[AccessLogRecord]>> {
        let mut guard = self.inner.queue.lock().unwrap();
        let take = guard.len().min(max_batches);
        let mut out = Vec::with_capacity(take);
        for _ in 0..take {
            if let Some(b) = guard.pop_front() {
                out.push(b);
            }
        }
        out
    }
}

impl Drop for LogSubscriber {
    fn drop(&mut self) {
        let mut subs = self.store.subscribers.lock().unwrap();
        subs.retain(|s| s.id != self.inner.id);
        self.store
            .active_subscriber_count
            .store(subs.len(), Ordering::Release);
    }
}

/// Central Lock-Free Access Log Store owned by `velda-observability`.
///
/// Owns per-core SPSC rings for Frontline Workers and distributes reference-counted
/// `Arc<[AccessLogRecord]>` batches to registered background Exporter Plugins.
pub struct LogStore {
    pub shards: Vec<SpscLogRing>,
    pub active_cores: usize,
    subscribers: Mutex<Vec<Arc<SubscriberInner>>>,
    pub active_subscriber_count: AtomicUsize,
    next_subscriber_id: AtomicU64,
    pub emitted_total: AtomicU64,
}

impl Default for LogStore {
    fn default() -> Self {
        Self::new()
    }
}

impl LogStore {
    pub fn new() -> Self {
        let detected_cores = velda_core::global_hardware_topology().cpu.available_cores;
        Self::with_cores_and_capacity(detected_cores, DEFAULT_CORE_RING_CAPACITY)
    }

    pub fn with_cores_and_capacity(cores: usize, ring_capacity: usize) -> Self {
        let active_cores = cores.clamp(1, MAX_METRIC_WORKERS);
        let mut shards = Vec::with_capacity(active_cores);
        for _ in 0..active_cores {
            shards.push(SpscLogRing::new(ring_capacity));
        }

        Self {
            shards,
            active_cores,
            subscribers: Mutex::new(Vec::new()),
            active_subscriber_count: AtomicUsize::new(0),
            next_subscriber_id: AtomicU64::new(1),
            emitted_total: AtomicU64::new(0),
        }
    }

    /// Emits a 64-byte [`AccessLogRecord`] onto the calling thread's core-local SPSC ring.
    #[inline]
    pub fn emit(&self, record: &AccessLogRecord) -> bool {
        self.emit_on_core(current_worker_lane(), record)
    }

    /// Emits a 64-byte [`AccessLogRecord`] onto an explicit worker core's SPSC ring.
    #[inline]
    pub fn emit_on_core(&self, core_id: usize, record: &AccessLogRecord) -> bool {
        let idx = core_id % self.active_cores;
        let ok = unsafe { self.shards.get_unchecked(idx).push(record) };
        if ok {
            self.emitted_total.fetch_add(1, Ordering::Relaxed);
        }
        ok
    }

    /// Registers a new background exporter plugin subscriber.
    pub fn subscribe(self: &Arc<Self>, max_batches: usize) -> LogSubscriber {
        let id = self.next_subscriber_id.fetch_add(1, Ordering::Relaxed);
        let inner = Arc::new(SubscriberInner {
            id,
            max_batches: max_batches.max(16),
            queue: Mutex::new(VecDeque::with_capacity(max_batches.min(256))),
            dropped_batches: AtomicU64::new(0),
        });

        let mut subs = self.subscribers.lock().unwrap();
        subs.push(Arc::clone(&inner));
        self.active_subscriber_count
            .store(subs.len(), Ordering::Release);
        drop(subs);

        LogSubscriber {
            inner,
            store: Arc::clone(self),
        }
    }

    /// Executed by Shadow Worker (`velda-shadow-{lane}`) to drain its core-local SPSC ring
    /// and fan out reference-counted `Arc<[AccessLogRecord]>` batches to active exporters.
    ///
    /// If zero exporters are registered, discards queued records in $\mathcal{O}(1)$ without
    /// reading slots or allocating memory.
    pub fn flush_lane(&self, lane: usize, max_batch_size: usize) -> usize {
        let idx = lane % self.active_cores;
        let ring = unsafe { self.shards.get_unchecked(idx) };

        if self.active_subscriber_count.load(Ordering::Acquire) == 0 {
            return ring.discard_all();
        }

        let records = ring.drain_batch(max_batch_size.max(64));
        if records.is_empty() {
            return 0;
        }

        let count = records.len();
        // Convert Vec<AccessLogRecord> into reference-counted Arc<[AccessLogRecord]> once.
        // Each active exporter receives an 8-byte Arc clone incrementing the strong count.
        let shared_batch: Arc<[AccessLogRecord]> = Arc::from(records);

        let subs = self.subscribers.lock().unwrap();
        for sub in subs.iter() {
            let mut q = sub.queue.lock().unwrap();
            if q.len() >= sub.max_batches {
                q.pop_front();
                sub.dropped_batches.fetch_add(1, Ordering::Relaxed);
            }
            q.push_back(Arc::clone(&shared_batch));
        }

        count
    }

    /// Flushes all worker core lanes into registered exporter subscribers.
    pub fn flush_all_lanes(&self, max_batch_per_lane: usize) -> usize {
        let mut total = 0;
        for lane in 0..self.active_cores {
            total += self.flush_lane(lane, max_batch_per_lane);
        }
        total
    }

    pub fn ring_dropped_total(&self) -> u64 {
        self.shards
            .iter()
            .map(|s| s.dropped.load(Ordering::Relaxed))
            .sum()
    }
}

static GLOBAL_LOG_STORE: OnceLock<Arc<LogStore>> = OnceLock::new();

/// Returns the globally shared in-memory access log store.
#[inline]
pub fn global_log_store() -> Arc<LogStore> {
    GLOBAL_LOG_STORE
        .get_or_init(|| Arc::new(LogStore::new()))
        .clone()
}

/// Initializes the globally shared access log store at bootstrap.
pub fn init_global_log_store(store: Arc<LogStore>) -> Result<(), Arc<LogStore>> {
    GLOBAL_LOG_STORE.set(store)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_size_and_ipv4_ipv6_encoding() {
        assert_eq!(std::mem::size_of::<AccessLogRecord>(), 64);
        assert_eq!(std::mem::align_of::<AccessLogRecord>(), 64);

        let ipv4: IpAddr = "192.168.1.42".parse().unwrap();
        let bits_v4 = encode_ip_u128(ipv4);
        assert_eq!(decode_ip_u128(bits_v4), ipv4);

        let ipv6: IpAddr = "2001:db8:85a3::8a2e:370:7334".parse().unwrap();
        let bits_v6 = encode_ip_u128(ipv6);
        assert_eq!(decode_ip_u128(bits_v6), ipv6);
    }

    #[test]
    fn test_user_agent_classification() {
        let (fam, os, ver) = classify_user_agent(
            b"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/134.0.0.0 Safari/537.36",
        );
        assert_eq!(fam, UaFamily::Chrome);
        assert_eq!(os, UaOs::Linux);
        assert_eq!(ver, 134);

        let (fam, os, ver) = classify_user_agent(b"curl/8.7.1");
        assert_eq!(fam, UaFamily::Curl);
        assert_eq!(os, UaOs::BotOrCli);
        assert_eq!(ver, 8);

        let (fam, os, ver) = classify_user_agent(b"grpc-go/1.64.0");
        assert_eq!(fam, UaFamily::GrpcGo);
        assert_eq!(os, UaOs::BotOrCli);
        assert_eq!(ver, 1);

        let (fam, _, _) = classify_user_agent(b"MyProprietaryClient/4.2");
        assert_eq!(fam, UaFamily::Custom);
    }

    #[test]
    fn test_ref_counted_multi_exporter_fanout_and_auto_drop() {
        let store = Arc::new(LogStore::with_cores_and_capacity(2, 128));

        let sub_stdout = store.subscribe(32);
        let sub_loki = store.subscribe(32);
        assert_eq!(store.active_subscriber_count.load(Ordering::Acquire), 2);

        let rec = AccessLogRecord::from_scalars(
            0x1234_5678_9abc_def0,
            1_700_000_000,
            450,
            "10.0.0.12:54321".parse().unwrap(),
            Some("10.0.1.99:8080".parse().unwrap()),
            256,
            1024,
            200,
            7,
            0,
            LogProtocol::H2,
            LogProtocol::H1,
            PipeStrategy::Buffered,
            LogCompletionReason::Complete,
            LogHttpMethod::Get,
            LogGrpcStatus::NonGrpc,
            b"curl/8.5.0",
        );

        assert!(store.emit_on_core(0, &rec));
        assert_eq!(store.flush_lane(0, 64), 1);

        let batch1 = sub_stdout.try_recv_batch().expect("stdout receives batch");
        let batch2 = sub_loki.try_recv_batch().expect("loki receives batch");

        assert_eq!(Arc::strong_count(&batch1), 2);
        assert_eq!(batch1[0].request_id, 0x1234_5678_9abc_def0);
        assert_eq!(batch2[0].status_code, 200);
        assert_eq!(UaFamily::from_u8(batch2[0].ua_family), UaFamily::Curl);

        drop(batch1);
        assert_eq!(Arc::strong_count(&batch2), 1);

        drop(batch2);

        drop(sub_stdout);
        assert_eq!(store.active_subscriber_count.load(Ordering::Acquire), 1);
        drop(sub_loki);
        assert_eq!(store.active_subscriber_count.load(Ordering::Acquire), 0);
    }
}
