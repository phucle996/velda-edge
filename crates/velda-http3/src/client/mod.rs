//! Upstream Ingress Subsystem (Gateway acting as HTTP/3 Client).
//!
//! Owns upstream connection establishment and request forwarding.

pub mod connector;
pub(crate) mod driver;
pub mod header;
pub mod response;

pub use connector::{
    Http3Client, Http3StreamSender, connect, default_client_config, resolve_sni, strip_port,
};
pub use header::sanitize_headers;
pub use response::Http3StreamResponse;
