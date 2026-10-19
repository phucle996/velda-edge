//! Response Compression Post-Response Subsystem.
//!
//! Provides RFC 9110 content-negotiated wire compression (Zstandard, Brotli, Gzip, Deflate)
//! on outgoing response payloads.

pub mod algorithm;
pub mod plugin;

pub use algorithm::{
    CompressionAlgorithm, compress_payload, negotiate_algorithm, parse_accept_encoding,
};
pub use plugin::{ResponseCompressionConfig, ResponseCompressionPlugin};
