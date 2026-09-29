//! Crate `velda-http1`
//!
//! Dedicated high-performance Layer 7 HTTP/1.1 Protocol Engine for Velda Edge (RFC 9112).
//!
//! Subsystems:
//! - `composer_parse/`: Ingress stream parsing and connection loop for `velda-composer`.
//! - `upstream_connector/`: Upstream HTTP/1.1 client connector for `velda-upstream` and pool.
//! - `codec`: Zero-copy request decoding and response encoding.

pub mod codec;
pub mod composer_parse;
pub mod edge_response;
pub mod error;
pub mod upstream_connector;

pub use codec::{decode_request, decode_response, encode_request, encode_response};
pub use composer_parse::Http1ServerConnection;
pub use edge_response::send_response as send_http1_response;
pub use error::Http1Error;
pub use upstream_connector::Http1UpstreamConnector;
