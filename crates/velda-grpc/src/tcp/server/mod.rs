//! Downstream gRPC server pipeline modules.
//!
//! Submodules:
//! - `connection`: HTTP/2 connection handshake and stream accept loop.
//! - `stream`: Active downstream stream wrapper (`GrpcServerStream`).
//! - `responder`: Response encoder, LPM framing, and trailers serializer (`GrpcResponder`).

pub mod connection;
pub mod header;
pub mod path;
pub mod responder;
pub mod stream;

pub use connection::GrpcServerConnection;
pub use header::{enrich_headers, extract_authority};
pub use path::{parse_grpc_path, validate_grpc_path};
pub use responder::{GrpcResponder, GrpcServerResponse, GrpcServerResponseHead};
pub use stream::{GrpcServerRequest, GrpcServerRequestHead, GrpcServerStream};
