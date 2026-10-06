//! Downstream gRPC server pipeline modules.
//!
//! Submodules:
//! - `connection`: HTTP/2 connection handshake and stream accept loop.
//! - `stream`: Active downstream stream wrapper (`GrpcServerStream`).
//! - `responder`: Response encoder, LPM framing, and trailers serializer (`GrpcResponder`).

pub mod connection;
pub mod responder;
pub mod stream;

pub use connection::GrpcServerConnection;
pub use responder::GrpcResponder;
pub use stream::GrpcServerStream;
