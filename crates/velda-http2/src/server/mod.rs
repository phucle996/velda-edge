//! Downstream Ingress Subsystem (Gateway acting as HTTP/2 Server).
//!
//! Owns incoming requests from downstream clients, flow control decoding,
//! stream response dispatch, and server connection lifecycle.

pub mod connection;
pub mod decode;
pub mod encode;
pub mod request;

pub use connection::Http2ServerConnection;
pub use decode::{Http2StreamReceiver, decode_request, decode_request_body};
pub use encode::{Http2Responder, Http2StreamSender};
pub use request::{Http2Request, Http2RequestHead};
