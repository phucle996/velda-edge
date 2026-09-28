//! Traffic pipeline modules handling stream dispatch and L7 protocol workflows.

pub mod http;
pub mod l4_dispatch;
pub mod l7_dispatch;

pub use http::handle_http_stream;
pub use l4_dispatch::{dispatch_l4, dispatch_udp_l4};
pub use l7_dispatch::{dispatch_tcp_l7, dispatch_udp_l7};
