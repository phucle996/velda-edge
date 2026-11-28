//! Downstream HTTP/2 Server Response Entity and Multiplexed Stream Responder (RFC 9113).
//!
//! Dispatches outgoing HTTP/2 `HEADERS`, `DATA`, `TRAILERS`, and `RST_STREAM` frames directly
//! from [`HeaderWorkspace`] through the connection driver with zero `HeaderMap` conversions.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use http::StatusCode;
use http::header::HeaderValue;
use tokio::sync::{Notify, mpsc};
use velda_core::buffer::header::{HeaderWorkspace, X_REQUEST_ID};
use velda_core::{Body, L7Response};

use super::connection::ServerCmd;
use crate::headers::sanitize_workspace_headers;
use crate::outcome::Http2Outcome;
use crate::proto::frame::H2ErrorCode;

/// Responder handle for an active HTTP/2 multiplexed stream.
#[derive(Debug)]
pub struct Http2Responder {
    pub(crate) cmd_tx: mpsc::UnboundedSender<ServerCmd>,
    pub(crate) max_send_buffer_size: usize,
    pub stream_id: u32,
    pub request_id: Option<HeaderValue>,
    pub alt_svc: Option<HeaderValue>,
    /// Status code of the response sent on this stream (`0` if none sent yet).
    pub last_status: u16,
    /// Total response body bytes sent on this stream (for single-shot responses).
    pub last_bytes_tx: u64,
}

impl Http2Responder {
    #[inline]
    fn inject_metadata_headers(&self, headers: &mut HeaderWorkspace) {
        if let Some(ref req_id) = self.request_id {
            headers.insert_bypass(X_REQUEST_ID, req_id.clone());
        }
        if let Some(ref alt_svc) = self.alt_svc
            && !headers.contains_key(http::header::ALT_SVC)
        {
            headers.insert(http::header::ALT_SVC, alt_svc.clone());
        }
    }

    /// Sends a response using the canonical [`L7Response`] type.
    pub fn send_response(&mut self, response: &L7Response) -> Result<(), Http2Outcome> {
        self.send_parts(response.status, &response.headers, &response.body)
    }

    /// Sends a pre-compiled canonical early error response directly over this HTTP/2 stream.
    #[inline]
    pub fn send_early_error(
        &mut self,
        status: StatusCode,
        cancel_upload: bool,
    ) -> Result<(), Http2Outcome> {
        self.last_status = status.as_u16();
        self.cmd_tx
            .send(ServerCmd::SendEarlyError {
                stream_id: self.stream_id,
                status,
                request_id: self.request_id.clone(),
                alt_svc: self.alt_svc.clone(),
                cancel_upload,
            })
            .map_err(|_| Http2Outcome::ConnectionClosed)
    }

    /// Sends a response using an owned [`L7Response`].
    pub fn send_response_owned(&mut self, response: L7Response) -> Result<(), Http2Outcome> {
        self.send_parts(response.status, &response.headers, &response.body)
    }

    /// Sends a response and immediately resets the incoming stream to cancel any pending peer upload.
    pub fn send_response_and_cancel_upload(
        &mut self,
        response: &L7Response,
    ) -> Result<(), Http2Outcome> {
        self.send_internal(response.status, &response.headers, &response.body, true)
    }

    /// Aborts the stream immediately with an HTTP/2 `RST_STREAM` frame.
    #[inline]
    pub fn send_reset(&mut self, code: H2ErrorCode) {
        let _ = self.cmd_tx.send(ServerCmd::ResetStream {
            stream_id: self.stream_id,
            code,
        });
    }

    /// Sends an HTTP/2 response given discrete parts.
    pub fn send_parts(
        &mut self,
        status: StatusCode,
        headers: &HeaderWorkspace,
        body: &Body,
    ) -> Result<(), Http2Outcome> {
        self.send_internal(status, headers, body, false)
    }

    #[inline]
    fn send_internal(
        &mut self,
        status: StatusCode,
        headers: &HeaderWorkspace,
        body: &Body,
        cancel_upload: bool,
    ) -> Result<(), Http2Outcome> {
        self.last_status = status.as_u16();
        let is_no_body = status.is_informational()
            || status == StatusCode::NO_CONTENT
            || status == StatusCode::NOT_MODIFIED;
        let has_body = !is_no_body && !body.is_empty();
        let body_bytes = if has_body {
            self.last_bytes_tx += body.len() as u64;
            body.to_bytes()
        } else {
            Bytes::new()
        };

        let mut ws = sanitize_workspace_headers(headers);
        self.inject_metadata_headers(&mut ws);

        self.cmd_tx
            .send(ServerCmd::SendResponse {
                stream_id: self.stream_id,
                status,
                headers: ws,
                body: body_bytes,
                end_stream: true,
                cancel_upload,
            })
            .map_err(|_| Http2Outcome::ConnectionClosed)
    }

    /// Sends the response head and returns a progressive stream sender for response chunks.
    pub fn send_stream_response(
        &mut self,
        status: StatusCode,
        headers: &HeaderWorkspace,
    ) -> Result<Http2StreamSender, Http2Outcome> {
        self.last_status = status.as_u16();
        let mut ws = sanitize_workspace_headers(headers);
        self.inject_metadata_headers(&mut ws);

        let queued_send_bytes = Arc::new(AtomicUsize::new(0));
        let send_notify = Arc::new(Notify::new());
        self.cmd_tx
            .send(ServerCmd::StartStreamResponse {
                stream_id: self.stream_id,
                status,
                headers: ws,
                queued_send_bytes: Arc::clone(&queued_send_bytes),
                send_notify: Arc::clone(&send_notify),
            })
            .map_err(|_| Http2Outcome::ConnectionClosed)?;

        Ok(Http2StreamSender {
            stream_id: self.stream_id,
            cmd_tx: self.cmd_tx.clone(),
            queued_send_bytes,
            send_notify,
            max_send_buffer_size: self.max_send_buffer_size,
            bytes_sent: 0,
            finished: false,
        })
    }
}

/// Progressive stream sender for transmitting HTTP/2 response `DATA` chunks downstream.
#[derive(Debug)]
pub struct Http2StreamSender {
    stream_id: u32,
    cmd_tx: mpsc::UnboundedSender<ServerCmd>,
    queued_send_bytes: Arc<AtomicUsize>,
    send_notify: Arc<Notify>,
    max_send_buffer_size: usize,
    pub bytes_sent: u64,
    finished: bool,
}

impl Http2StreamSender {
    /// Sends a `DATA` chunk downstream with bounded driver queue backpressure.
    pub async fn send_chunk(&mut self, chunk: Bytes) -> Result<(), Http2Outcome> {
        if chunk.is_empty() || self.finished {
            return Ok(());
        }
        while self.queued_send_bytes.load(Ordering::Acquire) >= self.max_send_buffer_size {
            if self.cmd_tx.is_closed() {
                return Err(Http2Outcome::ConnectionClosed);
            }
            self.send_notify.notified().await;
        }
        let len = chunk.len();
        self.queued_send_bytes.fetch_add(len, Ordering::Release);
        self.cmd_tx
            .send(ServerCmd::SendData {
                stream_id: self.stream_id,
                data: chunk,
                end_stream: false,
            })
            .map_err(|_| Http2Outcome::ConnectionClosed)?;
        self.bytes_sent += len as u64;
        Ok(())
    }

    /// Sends trailing headers directly from [`HeaderWorkspace`] to conclude the stream.
    pub fn send_trailers(&mut self, trailers: HeaderWorkspace) -> Result<(), Http2Outcome> {
        if self.finished {
            return Ok(());
        }
        self.finished = true;
        self.cmd_tx
            .send(ServerCmd::SendTrailers {
                stream_id: self.stream_id,
                trailers: sanitize_workspace_headers(&trailers),
            })
            .map_err(|_| Http2Outcome::ConnectionClosed)
    }

    /// Explicitly closes the stream by sending an empty `DATA` frame with `END_STREAM`.
    pub fn finish(&mut self) -> Result<(), Http2Outcome> {
        if self.finished {
            return Ok(());
        }
        self.finished = true;
        self.cmd_tx
            .send(ServerCmd::SendData {
                stream_id: self.stream_id,
                data: Bytes::new(),
                end_stream: true,
            })
            .map_err(|_| Http2Outcome::ConnectionClosed)
    }

    /// Explicitly resets the stream with an HTTP/2 error code.
    #[inline]
    pub fn reset(&mut self, code: H2ErrorCode) {
        self.finished = true;
        let _ = self.cmd_tx.send(ServerCmd::ResetStream {
            stream_id: self.stream_id,
            code,
        });
    }
}
