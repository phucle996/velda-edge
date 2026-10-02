//! Upstream HTTP/1.1 Client Subsystem.
//!
//! Architectural Perspective (Góc nhìn Ingress):
//! Owns the upstream communication lifecycle (Gateway <-> Backend Microservice):
//! - `response`: Response entity model owned by ingress receiver.
//! - `encode`: Outbound request serialization towards upstream targets.
//! - `parse`: Non-destructive parsing of incoming backend responses.
//! - `decode`: Stateful stream buffer decoding of responses.
//! - `connector`: Forwarding request and receiving response on established streams.

pub mod connector;
pub mod decode;
pub mod encode;
pub mod parse;
pub mod response;

pub use connector::{
    forward_request, read_chunk_sized, read_next_chunk, read_response_head, send_request,
    send_request_head_chunked,
};
pub use decode::{decode_response, decode_response_head};
pub use encode::{encode_headers, encode_request, encode_request_head, encode_request_line};
pub use parse::{
    find_crlf, parse_ascii_digits, parse_chunked_body, parse_hex_usize, parse_response_head,
    parse_single_chunk,
};
pub use response::{Http1Response, Http1ResponseHead};
