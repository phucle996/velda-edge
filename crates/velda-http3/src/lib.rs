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
//! - `proto/`: Sans-IO HTTP/3 protocol core (`frame`, `qpack`, `huffman`).

pub mod client;
pub mod config;
pub mod error;
pub mod headers;
pub mod proto;
pub mod server;
pub mod slot;
pub mod strategy;

pub use client::{
    Http3Client, Http3StreamResponse, Http3StreamSender, connect, default_client_config,
    resolve_sni, strip_port,
};
pub use config::Http3Config;
pub use error::Http3Error;
pub use headers::sanitize_headers;
pub use proto::frame::{
    FrameType, Http3Frame, decode_frame, decode_varint, decode_varint_slice, encode_frame,
    encode_varint, error_code, frame_id, settings_id, stream_type,
};
pub use proto::huffman::{decode_huffman, decode_huffman_bytes, encode_huffman};
pub use proto::qpack::{
    DecodedHeaders, H3ForwardingAddrs, H3IngressEnrich, decode_qpack, decode_qpack_ingress,
    encode_qpack_request, encode_qpack_response,
};
pub use quinn_proto;
pub use server::{
    Http3Engine, Http3RequestEvent, OutgoingDatagram, build_edge_response_frames, extract_host,
    is_clean_path, normalize_path, precompiled_early_response, reject_response, send_response,
};
pub use slot::Http3HeaderSlot;
pub use strategy::Http3PipeStrategy;
