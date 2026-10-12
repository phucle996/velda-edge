//! Layer 7 HTTP/2 to HTTP/3 Client-Stream Bridge Pipeline.
//!
//! Handles progressive client streaming uploads (e.g. file uploads, log batch ingest) where
//! downstream HTTP/2 DATA frames are progressively streamed to an upstream HTTP/3 QUIC connection,
//! returning a single response with zero whole-payload buffering in RAM.

use std::sync::Arc;

use bytes::BytesMut;
use http::StatusCode;
use http::header::{CONTENT_TYPE, HeaderValue};
use velda_core::L7Response;
use velda_http2::server::{Http2Responder, Http2ServerRequestHead, Http2StreamReceiver};
use velda_http3::{Http3Config, sanitize_headers};

use crate::pipeline::DownstreamMeta;
use crate::upstream::Http3Upstream;

/// Serves a client-streaming HTTP/2 downstream stream bridged to an HTTP/3 QUIC backend.
pub async fn serve(
    mut head: Http2ServerRequestHead,
    mut receiver: Http2StreamReceiver,
    mut responder: Http2Responder,
    meta: Arc<DownstreamMeta>,
    upstream: &Arc<Http3Upstream>,
) {
    // 1. Enrich proxy forwarding headers
    velda_http2::server::header::enrich_headers_precomputed(
        &mut head.headers,
        &head.uri,
        &meta.client_ip_header,
        &meta.client_port_header,
        meta.is_tls,
    );

    // 2. Sanitize connection-specific headers forbidden by RFC 9114 §4.2
    sanitize_headers(&mut head.headers);

    let h3_config = Http3Config::auto();

    // 3. Acquire pooled HTTP/3 client
    let (client, _lease) = match upstream.acquire(&h3_config).await {
        Ok(res) => res,
        Err(e) => {
            tracing::warn!(
                error = %e,
                upstream = %upstream.id(),
                "Failed to acquire upstream HTTP/3 client"
            );
            let err_resp = L7Response::from_bytes(
                StatusCode::BAD_GATEWAY,
                format!("502 Bad Gateway: {e}\n").into_bytes(),
            )
            .with_header(
                CONTENT_TYPE,
                HeaderValue::from_static("text/plain; charset=utf-8"),
            );
            let _ = responder.send_response(&err_resp);
            return;
        }
    };

    // 4. Open progressive HTTP/3 stream
    let (sender, response_rx) = match client
        .open_stream(head.method, head.uri, head.headers)
        .await
    {
        Ok(parts) => parts,
        Err(e) => {
            tracing::warn!(
                error = %e,
                upstream = %upstream.id(),
                "Failed to open HTTP/3 progressive request stream"
            );
            let err_resp = L7Response::from_bytes(
                StatusCode::BAD_GATEWAY,
                format!("502 Bad Gateway: {e}\n").into_bytes(),
            );
            let _ = responder.send_response(&err_resp);
            return;
        }
    };

    // 4. Progressively stream client DATA frames directly to HTTP/3 without whole-body buffering
    while let Ok(Some(chunk)) = receiver.recv_chunk().await {
        if !chunk.is_empty() && sender.send_chunk(chunk).await.is_err() {
            tracing::warn!("Failed to stream DATA chunk to HTTP/3 upstream");
            let err_resp = L7Response::from_bytes(
                StatusCode::BAD_GATEWAY,
                b"502 Bad Gateway: upstream stream disconnected\n".to_vec(),
            );
            let _ = responder.send_response(&err_resp);
            return;
        }
    }

    let _ = sender.finish().await;

    // 5. Await response headers from upstream HTTP/3
    let mut resp_stream = match response_rx.await {
        Ok(Ok(r)) => r,
        Ok(Err(e)) => {
            tracing::warn!(error = %e, "Upstream HTTP/3 stream returned error");
            let err_resp = L7Response::from_bytes(
                StatusCode::BAD_GATEWAY,
                format!("502 Bad Gateway: {e}\n").into_bytes(),
            );
            let _ = responder.send_response(&err_resp);
            return;
        }
        Err(_) => {
            let err_resp = L7Response::from_bytes(
                StatusCode::BAD_GATEWAY,
                b"502 Bad Gateway: upstream stream disconnected\n".to_vec(),
            );
            let _ = responder.send_response(&err_resp);
            return;
        }
    };

    // 6. Consume finite response body and send downstream
    let mut resp_body = BytesMut::new();
    while let Some(chunk_res) = resp_stream.next_chunk().await {
        match chunk_res {
            Ok(chunk) => resp_body.extend_from_slice(&chunk),
            Err(e) => {
                tracing::warn!(error = %e, "Error reading response chunk from HTTP/3 upstream");
                let err_resp = L7Response::from_bytes(
                    StatusCode::BAD_GATEWAY,
                    format!("502 Bad Gateway: {e}\n").into_bytes(),
                );
                let _ = responder.send_response(&err_resp);
                return;
            }
        }
    }

    let body = if resp_body.is_empty() {
        velda_core::Body::Empty
    } else {
        velda_core::Body::Bytes(resp_body.freeze())
    };

    let resp = L7Response::new(
        resp_stream.status,
        http::Version::HTTP_2,
        resp_stream.headers,
        body,
    );
    let _ = responder.send_response(&resp);
}
