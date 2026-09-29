//! Layer 7 (L7) protocol services: each file represents an isolated protocol pipeline.

pub mod grpc;
pub mod http1;
pub mod http2;
pub mod http3;

pub use grpc::{forward_grpc_unary_request, handle_grpc_stream, process_grpc_request};
pub use http1::{forward_http1_request, handle_http1_stream, process_http1_request};
pub use http2::{forward_http2_request, handle_http2_stream, process_http2_request};
pub use http3::{
    clear_h3_engines, forward_http3_request, handle_grpc_udp_handoff, handle_http3_handoff,
    has_h3_engine, init_h3_engine, process_http3_request,
};
