//! Crate `velda-http3`
//!
//! Dedicated high-performance Layer 7 HTTP/3 Protocol Engine for Velda Edge (RFC 9114).
//!
//! Architecture (Transparent Two-Way Ingress Model):
//! - `server/`: Downstream Ingress (Gateway acting as H3 Server). Manages QUIC connection state
//!   machine, packet ingestion, stream reassembly, and per-stream response transmission.
//! - `client/`: Upstream Ingress (Gateway acting as H3 Client). Manages outbound request
//!   framing, flow-controlled response ingestion, and backend QUIC connection reuse.
//! - `pipe/`: Wire pipe modules for discrete streaming strategies (buffered, server stream, client stream, duplex).
//! - `config`: Tunable parameters for QUIC transport, stream receive windows, and timeouts.
//! - `error`: Strongly typed HTTP/3, frame, and QPACK errors.
//! - `frame`: RFC 9114 HTTP/3 framing and RFC 9000 variable-length integer codecs.
//! - `qpack`: RFC 9204 QPACK encoder/decoder with static table.
//! - `huffman`: RFC 7541 / RFC 9204 canonical Huffman codec.

pub mod client;
pub mod config;
pub mod error;
pub mod frame;
pub mod huffman;
pub mod pipe;
pub mod qpack;
pub mod server;

// Top-level public re-exports
pub use client::{Http3Client, connect, extract_sni, forward_request, resolve_sni, strip_port};
pub use config::Http3Config;
pub use error::Http3Error;
pub use frame::{
    FrameType, Http3Frame, decode_frame, decode_varint, decode_varint_slice, encode_frame,
    encode_varint, error_code, frame_id, settings_id, stream_type,
};
pub use huffman::{decode_huffman, encode_huffman};
pub use pipe::{
    Http3PipeStrategy, pipe_buffered, pipe_client_stream, pipe_duplex, pipe_server_stream,
};
pub use qpack::{DecodedHeaders, decode_qpack, encode_qpack_request, encode_qpack_response};
pub use quinn_proto;
pub use server::{
    Http3Engine, Http3RequestEvent, OutgoingDatagram, build_edge_response_frames, send_response,
};
