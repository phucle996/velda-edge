//! Upstream Ingress Subsystem (Gateway acting as HTTP/2 Client).
//!
//! Owns upstream connection establishment and response types.

pub mod connector;
pub mod response;

pub use connector::{connect, connect_stream};
pub use response::{Http2Response, Http2ResponseHead};
