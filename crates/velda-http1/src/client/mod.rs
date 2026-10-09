//! Upstream HTTP/1.1 Client Subsystem.
//!
//! Architectural Perspective (Góc nhìn Ingress):
//! Owns the upstream communication lifecycle (Gateway <-> Backend Microservice):
//! - `request`: Request entity model & wire serialization towards upstream targets.
//! - `response`: Response entity model & stream decoding from backend targets.
//! - `connector`: Upstream plain TCP and encrypted TLS stream handling and connection initiation.
//! - `header`: Outbound header sanitization and SNI resolution.

pub mod connector;
pub mod header;
pub mod request;
pub mod response;

pub use connector::{
    Http1AccelerationPath, UpstreamHttp1Stream, connect, notsent_lowat_for_mem_tier,
};
pub use header::{extract_sni, resolve_sni, sanitize_headers, strip_port};
pub use request::{
    Http1ClientRequest, Http1ClientRequestHead, encode_request, encode_request_head,
    encode_request_line, forward_request, send_request, send_request_head_chunked,
    send_request_parts,
};
pub use response::{
    Http1ClientResponse, Http1ClientResponseHead, decode_response, decode_response_head,
    parse_response_head, read_chunk_sized, read_next_chunk, read_response_head,
};
