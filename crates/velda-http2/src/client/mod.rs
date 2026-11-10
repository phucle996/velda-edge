//! Upstream Ingress Subsystem (Gateway acting as HTTP/2 Client).
//!
//! Owns upstream connection establishment, client requests, and response decoding.

pub mod connector;
pub mod header;
pub mod request;
pub mod response;

pub use connector::{Http2AccelerationPath, connect, connect_stream};
pub use header::{is_disallowed_h2_header, sanitize_h2_headers};
pub use request::{Http2ClientRequest, Http2ClientRequestHead};
pub use response::{Http2ClientResponse, Http2ClientResponseHead};
