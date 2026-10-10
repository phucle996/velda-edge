//! Layer 7 HTTP/2 pipeline: downstream stream worker, route evaluation, and upstream forwarding.
//!
//! Operates strictly for listeners explicitly configured with `protocol = "http2"` or
//! dual-ALPN HTTP listeners negotiated to "h2".
//!
//! Subsystems:
//! - [`downstream`]: Ingress TLS handshake, accept loop, URI normalization, and route dispatch.
//! - [`pipe`]: Dedicated streaming pipe strategies (4 HTTP/2 multiplexed + 4 HTTP/1.1 bridge).

pub mod downstream;
pub mod pipe;

pub use downstream::{handle_http2_stream, run_http2_loop};
