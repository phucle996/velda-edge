//! Traffic pipeline modules handling stream dispatch and protocol workflows.

pub mod context;
pub mod l4;
pub mod l7;

pub use context::{IngressContext, TlsMetadata};
pub use l4::{handle_l4_tcp, handle_l4_udp};
pub use l7::{handle_tcp_l7, handle_udp_l7};
