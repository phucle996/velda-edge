//! Upstream Ingress Subsystem (Gateway acting as HTTP/3 Client).
//!
//! Owns upstream connection establishment and request forwarding.

pub mod connector;
pub(crate) mod driver;
pub mod header;
pub mod request;
pub mod response;

pub use connector::{
    Http3Client, connect, default_client_config, extract_sni, forward_request, resolve_sni,
    strip_port,
};
pub use header::sanitize_headers;
pub use request::{Http3ClientRequest, Http3ClientRequestHead};
pub use response::{Http3ClientResponse, Http3ClientResponseHead};
