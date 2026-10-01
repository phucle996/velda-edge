//! Upstream Ingress Subsystem (Gateway acting as HTTP/2 Client).
//!
//! Owns outbound request framing, flow control handling on incoming responses,
//! and multiplexed upstream connection management.

pub mod connector;
pub mod decode;
pub mod encode;
pub mod response;

pub use connector::Http2UpstreamConnector;
pub use decode::{
    decode_l7_response, decode_response, decode_response_body, decode_streaming_response,
};
pub use encode::{
    encode_and_send_request, send_h2_request, send_l7_request, start_streaming_request,
};
pub use response::{Http2Response, Http2ResponseHead};
