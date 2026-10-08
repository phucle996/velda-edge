//! Downstream HTTP/1.1 Server Subsystem.
//!
//! Architectural Perspective (Góc nhìn Ingress):
//! Owns the downstream connection lifecycle (Gateway <-> Client):
//! - `request`: Request entity model & stream decoding owned by ingress receiver.
//! - `response`: Response entity model & wire serialization to downstream clients.
//! - `connection`: Downstream connection worker and keep-alive loop.
//! - `header`: Downstream header enrichment and anti-spoofing.
//! - `path`: Downstream URI path normalization and security validation.

pub mod connection;
pub mod header;
pub mod path;
pub mod request;
pub mod response;

pub use connection::Http1ServerConnection;
pub use header::{enrich_headers, extract_host, match_header_name};
pub use path::{is_clean_path, normalize_path};
pub use request::{
    Http1BodyFraming, Http1ServerRequest, Http1ServerRequestHead, ParsedChunk, decode_body,
    decode_request, decode_request_head, find_crlf, parse_ascii_digits, parse_chunked_body,
    parse_hex_usize, parse_request_head, parse_single_chunk,
};
pub use response::{
    Http1ServerResponse, Http1ServerResponseHead, encode_chunk, encode_chunked_end, encode_headers,
    encode_response, encode_response_head, encode_response_head_ext, encode_status_line,
    send_chunk, send_chunked_end, send_response, send_response_head_chunked, send_response_parts,
};
