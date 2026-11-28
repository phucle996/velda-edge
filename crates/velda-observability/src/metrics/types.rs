//! Canonical In-Memory Metrics Dimensional Enums.
//!
//! Provides strictly bounded finite enumerations for downstream/upstream wire protocols,
//! pipeline pipe strategies, ALPN negotiations, and canonical internal state outcomes.

pub use velda_core::lut::{InternalStatus, PipeKind, WireProtocol};

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

/// Point-in-time parameters captured at stream termination.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamCompletion {
    /// Canonical internal state outcome (`velda_core::InternalStatus`).
    pub internal_status: InternalStatus,
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
        internal_status: InternalStatus,
        bytes_rx: u64,
        bytes_tx: u64,
        duration_ns: u64,
        ttfb_ns: u64,
    ) -> Self {
        Self {
            internal_status,
            bytes_rx,
            bytes_tx,
            duration_ns,
            ttfb_ns,
        }
    }
}
