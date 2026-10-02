//! Upstream Ingress Subsystem (Gateway acting as HTTP/3 Client).
//!
//! Owns upstream connection establishment and request forwarding.

pub mod connector;
pub(crate) mod driver;

pub use connector::{
    Http3Client, connect, default_client_config, extract_sni, forward_request, resolve_sni,
    strip_port,
};
