//! Protocol-isolated in-memory upstream tables and pipeline handoff.
//!
//! Separates backend upstreams into 6 dedicated, typed, protocol-isolated modules:
//! - `tcp`: L4 raw TCP connection pool & streaming handoff
//! - `udp`: L4 raw UDP endpoints & session tracking
//! - `http1`: L7 HTTP/1.1 backend endpoints, connection pooling & pipe handoff
//! - `http2`: L7 HTTP/2 backend endpoints, persistent client multiplexing & pipe handoff
//! - `http3`: L7 HTTP/3 backend endpoints, QUIC client multiplexing & request handoff
//! - `grpc`: L7 gRPC backend endpoints, bidirectional streaming & unary handoff
//!
//! Pre-compiles upstream TLS client engines and streaming policies into static pipeline
//! processors, eliminating dynamic lookups and protocol guessing on the request hot path.

pub mod grpc;
pub mod http1;
pub mod http2;
pub mod http3;
pub mod lb;
pub mod table;
pub mod tcp;
pub mod udp;

pub use grpc::{GrpcTcpUpstream, GrpcUdpUpstream};
pub use http1::{Http1Upstream, UpstreamHttp1Stream};
pub use http2::Http2Upstream;
pub use http3::Http3Upstream;
pub use lb::{EdgeUpstream, LbAlgorithm};
pub use table::{SubUpstreamTable, UpstreamTable, build_upstreams, build_upstreams_default};
pub use tcp::{TcpAccelerationPath, TcpUpstream};
pub use udp::UdpUpstream;
