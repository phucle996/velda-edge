//! Response Compression Post-Response Subsystem.
//!
//! Provides RFC 9110 content-negotiated wire compression (Zstandard, Brotli, Gzip, Deflate)
//! on outgoing response payloads.

pub mod config;
pub mod dictionary;
pub mod engine;
pub mod negotiation;
pub mod plugin;
pub mod simd;

pub use config::ResponseCompressionConfig;
pub use dictionary::{BUILTIN_COMMON_JSON_REST_DICT, BuiltinDictionary, ZstdDictionaryConfig};
pub use engine::{
    compress_payload_with_dict, decode_zstd_with_dict, encode_brotli, encode_deflate, encode_gzip,
    encode_zstd, encode_zstd_with_dict,
};
pub use negotiation::{
    ClientPreference, CompressionAlgorithm, is_identity_acceptable, negotiate_algorithm,
    parse_accept_encoding, parse_accept_encoding_stack,
};
pub use plugin::ResponseCompressionPlugin;
pub use simd::{STREAMING_COPY, StreamingCopyFn, streaming_to_bytes};
