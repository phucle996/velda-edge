//! Layer 7 gRPC response serialization and streaming responder.
//!
//! Operates on borrowed payload and metadata references without holding or retaining
//! response business data inside the protocol engine.

use bytes::{Bytes, BytesMut};
use h2::server::SendResponse;
use http::header::CONTENT_TYPE;
use http::{HeaderMap, HeaderValue, Response, StatusCode};
use velda_core::{Body, L7Response};

use crate::error::GrpcError;
use crate::frame::encode_grpc_frame;
use crate::status::GrpcStatus;

/// Streaming responder for an active downstream gRPC call.
///
/// Pure encoder capability: caller provides response payload or status;
/// the responder serializes headers, LPM data frames, and HTTP/2 trailers
/// over the underlying H2 send stream.
pub struct GrpcResponder {
    respond: SendResponse<Bytes>,
}

impl GrpcResponder {
    /// Creates a new [`GrpcResponder`] from an underlying H2 send handle.
    pub fn new(respond: SendResponse<Bytes>) -> Self {
        Self { respond }
    }

    /// Sends a standard Unary response: HTTP 200 + 1 LPM frame + trailers with grpc-status.
    pub fn send_unary_response(
        &mut self,
        status: GrpcStatus,
        payload: Option<&[u8]>,
        extra_headers: Option<HeaderMap>,
    ) -> Result<(), GrpcError> {
        let mut resp_builder = Response::builder()
            .status(StatusCode::OK)
            .version(http::Version::HTTP_2)
            .header(CONTENT_TYPE, "application/grpc");

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

        // Send trailers
        let mut trailers = status.to_trailers(None);
        if let Ok(val) = HeaderValue::from_str(&format!("{}", status as u32)) {
            trailers.insert("grpc-status", val);
        }

        send_stream.send_trailers(trailers).map_err(GrpcError::H2)?;

        Ok(())
    }

    /// Responds immediately with a Trailers-Only gRPC response (e.g. for routing errors).
    pub fn send_trailers_only(
        mut self,
        status: GrpcStatus,
        message: Option<&str>,
    ) -> Result<(), GrpcError> {
        let mut builder = Response::builder()
            .status(StatusCode::OK)
            .version(http::Version::HTTP_2)
            .header(CONTENT_TYPE, "application/grpc");

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

    /// Translates an [`L7Response`] into gRPC frames and sends to downstream client.
    pub fn send_l7_response(mut self, response: &L7Response) -> Result<(), GrpcError> {
        let status = response
            .headers
            .get("grpc-status")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u32>().ok())
            .map(GrpcStatus::from_code)
            .unwrap_or(GrpcStatus::Ok);

        let payload = match response.body {
            Body::Bytes(ref b) => Some(b.as_ref()),
            Body::Empty => None,
        };

        self.send_unary_response(status, payload, Some(response.headers.clone()))
    }
}
