//! Canonical In-Memory Metrics Dimensional Enums.
//!
//! Provides strictly bounded finite enumerations for downstream protocols,
//! upstream protocols, pipeline pipe strategies, ALPN negotiations, and HTTP status classes.
//!
//! # Architectural Invariants
//! 1. Bounded Cardinality: Every dimension is a closed enum with a fixed variant count,
//!    preventing time-series explosion in internal metrics storage.
//! 2. Zero Dynamic Allocations: Metric dimensions are mapped via static integer discriminants
//!    and string slices (`&'static str`), never allocating heap memory on the hot path.
//! 3. Clean Monorepo Vocabulary: Mirrors canonical definitions from `velda-core`.

pub use velda_core::lut::{PipeKind, WireProtocol};

/// Negotiated Application-Layer Protocol Negotiation (ALPN) token.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum AlpnProtocol {
    /// HTTP/2 ALPN (`h2`).
    H2 = 0,
    /// HTTP/1.1 ALPN (`http/1.1`).
    Http11 = 1,
    /// HTTP/3 ALPN (`h3`).
    H3 = 2,
    /// No ALPN negotiated (plaintext L4 or default fallback).
    None = 3,
}

impl AlpnProtocol {
    /// Number of distinct ALPN variants.
    pub const COUNT: usize = 4;

    /// Returns the canonical static string slice for this ALPN protocol.
    #[inline(always)]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::H2 => "h2",
            Self::Http11 => "http/1.1",
            Self::H3 => "h3",
            Self::None => "none",
        }
    }
}

/// Coarse HTTP status code classification for bounded in-memory slot counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum StatusClass {
    /// 2xx Success (200 OK, 201 Created, 204 No Content, etc.).
    Success2xx = 0,
    /// 3xx Redirection (301 Moved, 302 Found, 304 Not Modified, etc.).
    Redirect3xx = 1,
    /// 4xx Client Error (400 Bad Request, 404 Not Found, 429 Too Many Requests, etc.).
    ClientError4xx = 2,
    /// 5xx Server Error (500 Internal Error, 502 Bad Gateway, 503 Unavailable, etc.).
    ServerError5xx = 3,
    /// Non-standard / informational status codes (<200 or >=600).
    Other = 4,
}

impl StatusClass {
    /// Classifies an HTTP status code into its corresponding coarse class.
    #[inline(always)]
    pub const fn from_status_code(code: u16) -> Self {
        if code >= 200 && code < 300 {
            Self::Success2xx
        } else if code >= 300 && code < 400 {
            Self::Redirect3xx
        } else if code >= 400 && code < 500 {
            Self::ClientError4xx
        } else if code >= 500 && code < 600 {
            Self::ServerError5xx
        } else {
            Self::Other
        }
    }

    /// Returns the canonical static string representation for Prometheus metric labels.
    #[inline(always)]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Success2xx => "2xx",
            Self::Redirect3xx => "3xx",
            Self::ClientError4xx => "4xx",
            Self::ServerError5xx => "5xx",
            Self::Other => "other",
        }
    }
}

/// Point-in-time parameters captured at stream termination.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamCompletion {
    /// HTTP status code or gRPC trailer status.
    pub status_code: u16,
    /// Total bytes received from downstream for this stream.
    pub bytes_rx: u64,
    /// Total bytes transmitted to downstream for this stream.
    pub bytes_tx: u64,
    /// Total stream duration from start to finish in nanoseconds.
    pub duration_ns: u64,
    /// Measured time to first byte from upstream in nanoseconds (0 if not captured).
    pub ttfb_ns: u64,
}

const _: () = assert!(std::mem::size_of::<StreamCompletion>() == 40);

impl StreamCompletion {
    /// Creates a new `StreamCompletion` record.
    #[inline(always)]
    pub const fn new(
        status_code: u16,
        bytes_rx: u64,
        bytes_tx: u64,
        duration_ns: u64,
        ttfb_ns: u64,
    ) -> Self {
        Self {
            status_code,
            bytes_rx,
            bytes_tx,
            duration_ns,
            ttfb_ns,
        }
    }
}
