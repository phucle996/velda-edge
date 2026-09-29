//! Crate `velda-http3`
//!
//! Dedicated high-performance Layer 7 HTTP/3 Protocol Engine for Velda Edge (RFC 9114).
//!
//! Subsystems:
//! - `frame`: HTTP/3 frame parsing and variable-length integer encoding/decoding.
//! - `composer_parse/`: Packet-driven HTTP/3 server state machine for `velda-composer`.
//! - `upstream_connector/`: Upstream HTTP/3 client connector for `velda-upstream` and pool.

pub mod composer_parse;
pub mod edge_response;
pub mod error;
pub mod frame;
pub mod upstream_connector;

pub use composer_parse::{Http3Engine, Http3RequestEvent, OutgoingDatagram};
pub use edge_response::{encode_minimal_headers, send_response as send_http3_response};
pub use error::Http3Error;
pub use frame::{Http3Frame, decode_frame, decode_varint, encode_frame, encode_varint};
pub use upstream_connector::Http3UpstreamConnector;
