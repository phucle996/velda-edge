//! Downstream HTTP/1.1 Server Subsystem.
//!
//! Architectural Perspective (Góc nhìn Ingress):
//! Owns the downstream connection lifecycle (Gateway <-> Client):
//! - `request`: Request entity model owned by ingress receiver.
//! - `parse`: Non-destructive parsing of request head & chunked framing.
//! - `decode`: Stateful stream buffer decoding of requests.
//! - `encode`: Response serialization and vectored writing to clients.
//! - `connection`: Downstream connection worker and keep-alive loop.

pub mod connection;
pub mod decode;
pub mod encode;
pub mod parse;
pub mod request;

pub use connection::Http1ServerConnection;
pub use decode::{decode_body, decode_chunked_body, decode_request, decode_request_head};
pub use encode::{
    encode_chunk, encode_chunked_end, encode_headers, encode_response, encode_response_head,
    encode_status_line, send_chunk, send_chunked_end, send_response, send_response_head_chunked,
    send_response_parts,
};
pub use parse::{
    find_crlf, parse_ascii_digits, parse_chunked_body, parse_hex_usize, parse_request_head,
};
pub use request::{Http1BodyFraming, Http1Request, Http1RequestHead};
