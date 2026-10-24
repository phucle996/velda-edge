//! Downstream Ingress Subsystem (Gateway acting as HTTP/2 Server).
//!
//! Owns incoming requests from downstream clients, flow control decoding,
//! stream response dispatch, and server connection lifecycle.

pub mod connection;
pub mod early_response;
pub mod header;
pub mod path;
pub mod request;
pub mod response;

pub use connection::{Http2FloodTracker, Http2ServerConnection};
pub use early_response::{
    precompiled_early_response, precompiled_reject_response, send_early_error,
    send_early_error_and_cancel_upload,
};
pub use header::{enrich_headers, extract_host};
pub use path::{is_clean_path, normalize_path};
pub use request::{
    Http2Priority, Http2ServerRequest, Http2ServerRequestHead, Http2StreamReceiver, decode_request,
};
pub use response::{
    Http2Responder, Http2ServerResponse, Http2ServerResponseHead, Http2StreamSender,
    build_h2_response,
};
