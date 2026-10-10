//! Layer 7 HTTP/1.1 pipeline: downstream stream worker, route evaluation, and upstream forwarding.
//!
//! Operates strictly for listeners explicitly configured with `protocol = "http1"`.
//!
//! Subsystems:
//! - [`downstream`]: Ingress TLS handshake, keep-alive loop, URI normalization, and route dispatch.
//! - [`pipe`]: Dedicated streaming pipe strategies (Buffered, ServerStream, ClientStream, Duplex).

pub mod downstream;
pub mod pipe;

pub use downstream::{handle_http1_stream, run_http1_loop};
