//! Layer 7 HTTP Upstreams: HTTP/1.1, HTTP/2, and HTTP/3.
//!
//! Pre-compiles wire pipe strategies, connection pools, and TLS identities for each protocol.

pub mod http1;
pub mod http2;
pub mod http3;

pub use http1::{Http1ClientResource, Http1Lease, Http1Upstream, UpstreamHttp1Stream};
pub use http2::{Http2ClientResource, Http2Upstream};
pub use http3::{Http3ClientResource, Http3Upstream};
