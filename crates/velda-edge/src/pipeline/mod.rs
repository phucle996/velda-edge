//! Traffic pipeline modules handling stream dispatch and L7 protocol workflows.

pub mod dispatcher;
pub mod http;

pub use dispatcher::{dispatch_l4, dispatch_tcp_l7, dispatch_udp_l4, dispatch_udp_l7};
pub use http::handle_http_stream;
