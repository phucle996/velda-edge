//! Protocol-isolated in-memory upstream tables and pipeline forwarding.
//!
//! Separates backend upstreams into 3 dedicated protocol families:
//! - `raw`: L4 raw TCP streams and UDP datagrams
//! - `http`: L7 HTTP/1.1, HTTP/2, and HTTP/3 web traffic
//! - `grpc`: L7 gRPC RPC endpoints over TCP (HTTP/2) and UDP (QUIC)
//!
//! Pre-compiles upstream TLS client engines and streaming policies into static pipeline
//! processors, eliminating dynamic lookups and protocol guessing on the request hot path.

pub mod builder;
pub mod grpc;
pub mod http;
pub mod lb;
pub mod raw;
pub mod table;

// Core public re-exports (zero breaking changes across monorepo):
pub use builder::{build_upstreams, build_upstreams_default};
pub use grpc::{GrpcTcpUpstream, GrpcUdpUpstream};
pub use http::{Http1Upstream, Http2Upstream, Http3Upstream, UpstreamHttp1Stream};
pub use lb::{EdgeUpstream, LbAlgorithm};
pub use raw::{TcpAccelerationPath, TcpUpstream, UdpAccelerationPath, UdpUpstream};
pub use table::{GrpcUpstream, HttpUpstream, RawUpstream, SubUpstreamTable, UpstreamTable};
