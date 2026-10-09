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
//! - `config/`: Buffer sizing scaled by [`velda_core::hardware::MemoryTier`].
//! - `error/`: HTTP/1.1 protocol error definitions.

pub mod client;
pub mod config;
pub mod error;
pub mod pipe;
pub mod server;
pub mod wire;

pub use config::Http1Config;
pub use error::Http1Error;
pub use velda_core::Body;

// RFC 9112 Wire Framing & Codec re-exports
pub use wire::{
    ParsedChunk, decode_chunk, encode_chunk, encode_chunked_end, encode_headers, find_crlf,
    parse_ascii_digits, parse_chunked_body, parse_hex_usize, parse_single_chunk, send_chunk,
    send_chunked_end,
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
    Http1ServerResponse, Http1ServerResponseHead, encode_response, encode_response_head,
    encode_response_head_chunked, encode_status_line, send_response, send_response_head_chunked,
    send_response_parts,
};

// Upstream Client re-exports
pub use client::connector::{
    Http1AccelerationPath, UpstreamHttp1Stream, connect, notsent_lowat_for_mem_tier,
};
pub use client::header::{extract_sni, resolve_sni, sanitize_headers, strip_port};
pub use client::request::{
    Http1ClientRequest, Http1ClientRequestHead, encode_request, encode_request_head,
    encode_request_line, forward_request, send_request, send_request_head_chunked,
    send_request_parts,
};
pub use client::response::{
    Http1ClientResponse, Http1ClientResponseHead, decode_response, decode_response_head,
    parse_response_head, read_chunk_sized, read_next_chunk, read_response_head,
};

// Streaming Pipe re-exports
pub use pipe::{
    HOP_BY_HOP, Http1PipeStrategy, pipe_buffered, pipe_client_stream, pipe_duplex,
    pipe_server_stream,
};
