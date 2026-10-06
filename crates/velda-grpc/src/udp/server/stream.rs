//! Downstream gRPC stream representation over UDP.

use quinn_proto::{ConnectionHandle, StreamId};
use std::net::SocketAddr;
use velda_core::L7Request;

/// Represents an active downstream gRPC request stream received over UDP.
#[derive(Debug)]
pub struct GrpcUdpServerStream {
    /// QUIC connection handle.
    pub handle: ConnectionHandle,
    /// Stream identifier.
    pub stream_id: StreamId,
    /// Associated L7 request.
    pub request: L7Request,
    /// Remote client address.
    pub peer: SocketAddr,
}

impl GrpcUdpServerStream {
    /// Creates a new downstream stream wrapper.
    pub fn new(
        handle: ConnectionHandle,
        stream_id: StreamId,
        request: L7Request,
        peer: SocketAddr,
    ) -> Self {
        Self {
            handle,
            stream_id,
            request,
            peer,
        }
    }
}
