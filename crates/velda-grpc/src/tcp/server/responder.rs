//! Downstream gRPC response serialization and streaming responder.
//!
//! Encodes canonical gRPC status codes, Length-Prefixed Message (LPM) data frames,
//! and HTTP/2 response headers/trailers over the downstream send stream.

use bytes::{Bytes, BytesMut};
use h2::server::SendResponse;
use http::header::CONTENT_TYPE;
use http::{HeaderMap, Response, StatusCode};

use crate::error::GrpcError;
use crate::frame::encode_grpc_frame;
use crate::status::GrpcStatus;
use crate::wire::GrpcWire;

/// Streaming responder for an active downstream gRPC call.
///
/// Encodes response payload or status, serializing headers, LPM data frames,
/// and HTTP/2 trailers over the underlying H2 send stream.
pub struct GrpcResponder {
    pub respond: SendResponse<Bytes>,
}

impl GrpcResponder {
    /// Creates a new [`GrpcResponder`] wrapping an active H2 send handle.
    pub fn new(respond: SendResponse<Bytes>) -> Self {
        Self { respond }
    }

    /// Sends a standard Unary response: HTTP 200 + 1 LPM frame + trailers with `grpc-status`.
    pub fn send_unary_response(
        &mut self,
        status: GrpcStatus,
        payload: Option<&[u8]>,
        extra_headers: Option<HeaderMap>,
    ) -> Result<(), GrpcError> {
        let mut resp_builder = Response::builder()
            .status(StatusCode::OK)
            .version(http::Version::HTTP_2)
            .header(CONTENT_TYPE, GrpcWire::CONTENT_TYPE_VALUE);

        if let Some(headers) = extra_headers {
            for (name, val) in headers {
                if let Some(name) = name {
                    resp_builder = resp_builder.header(name, val);
                }
            }
        }

        let is_trailers_only = payload.is_none() && status != GrpcStatus::Ok;

        if is_trailers_only {
            let trailers = status.to_trailers(None);
            for (name, val) in trailers {
                if let Some(name) = name {
                    resp_builder = resp_builder.header(name, val);
                }
            }
            let resp = resp_builder.body(()).map_err(GrpcError::Http)?;
            self.respond
                .send_response(resp, true)
                .map_err(GrpcError::H2)?;
            return Ok(());
        }

        let resp = resp_builder.body(()).map_err(GrpcError::Http)?;
        let mut send_stream = self
            .respond
            .send_response(resp, false)
            .map_err(GrpcError::H2)?;

        // Send payload LPM frame
        if let Some(data) = payload {
            let mut buf = BytesMut::new();
            encode_grpc_frame(data, false, &mut buf);
            send_stream
                .send_data(buf.freeze(), false)
                .map_err(GrpcError::H2)?;
        }

        // Send trailers with status code
        let trailers = status.to_trailers(None);
        send_stream.send_trailers(trailers).map_err(GrpcError::H2)?;

        Ok(())
    }

    /// Responds immediately with a Trailers-Only gRPC response (e.g. for routing errors).
    pub fn send_trailers_only(
        &mut self,
        status: GrpcStatus,
        message: Option<&str>,
    ) -> Result<(), GrpcError> {
        let mut builder = Response::builder()
            .status(StatusCode::OK)
            .version(http::Version::HTTP_2)
            .header(CONTENT_TYPE, GrpcWire::CONTENT_TYPE_VALUE);

        let trailers = status.to_trailers(message);
        for (name, val) in trailers {
            if let Some(name) = name {
                builder = builder.header(name, val);
            }
        }

        let resp = builder.body(()).map_err(GrpcError::Http)?;
        self.respond
            .send_response(resp, true)
            .map_err(GrpcError::H2)?;
        Ok(())
    }

    /// Sends an HTTP/2 response header frame on the underlying H2 send stream.
    pub fn send_response(
        &mut self,
        response: Response<()>,
        end_of_stream: bool,
    ) -> Result<h2::SendStream<Bytes>, h2::Error> {
        self.respond.send_response(response, end_of_stream)
    }
}
