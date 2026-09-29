//! Crate `velda-http2`
//!
//! Dedicated high-performance Layer 7 HTTP/2 Protocol Engine for Velda Edge (RFC 9113).
//!
//! Subsystems:
//! - `composer_parse/`: Ingress stream parsing and multiplexed stream handler for `velda-composer`.
//! - `upstream_connector/`: Upstream HTTP/2 client connector for `velda-upstream` and pool.

pub mod composer_parse;
pub mod edge_response;
pub mod error;
pub mod upstream_connector;

pub use composer_parse::Http2ServerConnection;
pub use edge_response::Http2Responder;
pub use error::Http2Error;
pub use upstream_connector::Http2UpstreamConnector;
