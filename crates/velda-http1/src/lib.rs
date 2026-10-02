//! Crate `velda-http1`
//!
//! Dedicated high-performance Layer 7 HTTP/1.1 Protocol Engine for Velda Edge (RFC 9112).
//!
//! Architectural Structure (Minh bạch 2 chiều dữ liệu theo góc nhìn Ingress):
//! - `server/`: Downstream Ingress (Gateway <-> Client). Owns `Http1Request` domain entity,
//!   request parsing/decoding, downstream response serialization, and `Http1ServerConnection`.
//! - `client/`: Upstream Ingress (Gateway <-> Backend). Owns `Http1Response` domain entity,
//!   outbound request serialization, response parsing/decoding, and upstream forwarding.
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
    ParsedChunk, encode_chunk, encode_chunked_end, encode_headers, find_crlf, parse_ascii_digits,
    parse_chunked_body, parse_hex_usize, parse_single_chunk, send_chunk, send_chunked_end,
};

// Downstream Server re-exports
pub use server::connection::Http1ServerConnection;
pub use server::decode::{decode_body, decode_request, decode_request_head, parse_request_head};
pub use server::encode::{
    encode_response, encode_response_head, encode_status_line, send_response,
    send_response_head_chunked, send_response_parts,
};
pub use server::request::{Http1BodyFraming, Http1Request, Http1RequestHead};

// Upstream Client re-exports
pub use client::connector::{
    forward_request, read_chunk_sized, read_next_chunk, read_response_head, send_request,
    send_request_head_chunked,
};
pub use client::decode::{decode_response, decode_response_head, parse_response_head};
pub use client::encode::{encode_request, encode_request_head, encode_request_line};
pub use client::response::{Http1Response, Http1ResponseHead};
pub use client::stream::{UpstreamHttp1Stream, connect_stream};

// Streaming Pipe re-exports
pub use pipe::{
    HOP_BY_HOP_HEADERS, Http1PipeStrategy, pipe_buffered, pipe_client_stream, pipe_duplex,
    pipe_server_stream, sanitize_hop_by_hop_headers,
};
