//! Crate `velda-http1`
//!
//! Dedicated high-performance Layer 7 HTTP/1.1 Protocol Engine for Velda Edge (RFC 9112).
//!
//! Architectural Structure (Minh bạch 2 chiều dữ liệu theo góc nhìn Ingress):
//! - `server/`: Downstream Ingress (Gateway <-> Client). Owns `Http1ServerRequest` & `Http1ServerResponse`,
//!   downstream request wire decoding, response serialization, and `Http1ServerConnection`.
//! - `client/`: Upstream Ingress (Gateway <-> Backend). Owns `Http1ClientRequest` & `Http1ClientResponse`,
//!   outbound request serialization, response decoding, and upstream connection mechanics.
//! - `pipe/`: Bidirectional stream pump connecting downstream and upstream according to streaming policy.
//! - `config/`: Buffer sizing scaled continuously by [`velda_core::HardwareSizing`].
//! - `error/`: HTTP/1.1 protocol error definitions.

pub mod client;
pub mod config;
pub mod error;
pub mod server;
pub mod strategy;
pub mod wire;

pub use config::Http1Config;
pub use error::Http1Error;
pub use strategy::Http1PipeStrategy;
pub use velda_core::Body;

// RFC 9112 Wire Framing & Codec re-exports
pub use wire::{
    ParsedChunk, ProgressiveChunkDecoder, ProgressiveChunkState, decode_chunk, encode_chunk,
    encode_chunked_end, encode_headers, find_crlf, parse_ascii_digits, parse_chunked_body,
    parse_hex_usize, parse_single_chunk, send_chunk, send_chunked_end, send_coalesced_chunks,
};

// Downstream Server re-exports
pub use server::connection::Http1ServerConnection;
pub use server::header::{enrich_headers, extract_host, match_header_name};
pub use server::path::{is_clean_path, normalize_path};
pub use server::request::{
    Http1BodyFraming, Http1ServerRequest, Http1ServerRequestHead, decode_body, decode_request,
    decode_request_head, parse_request_head,
};
pub use server::response::{
    HeadEncodeOptions, Http1ServerResponse, Http1ServerResponseHead, encode_chunked_head,
    encode_head, encode_response, encode_response_head, encode_response_head_chunked,
    encode_status_line, is_connection_close, send_chunked_head, send_head_coalesced, send_parts,
    send_response, send_response_head_chunked, send_response_head_chunked_coalesced,
    send_response_parts,
};

pub use client::connector::{Http1AccelerationPath, UpstreamHttp1Stream, connect};
pub use client::header::{HOP_BY_HOP, extract_sni, resolve_sni, sanitize_headers, strip_port};
pub use client::request::{
    Http1ClientRequest, Http1ClientRequestHead, encode_request, encode_request_head,
    encode_request_head_chunked, encode_request_line, forward_request, send_request,
    send_request_head_chunked, send_request_parts,
};
pub use client::response::{
    Http1ClientResponse, Http1ClientResponseHead, decode_response, decode_response_head,
    parse_response_head, read_chunk_sized, read_next_chunk, read_response_head,
};
