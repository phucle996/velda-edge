//! Layer 7 HTTP/2 to HTTP/3 Duplex Bridge Pipeline.
//!
//! Handles full-duplex bidirectional streaming (e.g. WebTransport / tunnel over QUIC)
//! between downstream HTTP/2 client and upstream HTTP/3 QUIC connection with true
//! concurrent streaming pumps and zero whole-payload buffering.

use std::sync::Arc;

use http::StatusCode;
use http::header::{CONTENT_TYPE, HeaderValue};
use velda_core::L7Response;
use velda_http2::server::{Http2Responder, Http2ServerRequestHead, Http2StreamReceiver};
use velda_http3::{Http3Config, sanitize_headers};

use crate::pipeline::DownstreamMeta;
use crate::upstream::Http3Upstream;

/// Serves a full-duplex HTTP/2 downstream stream bridged to an HTTP/3 QUIC backend.
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
                "Failed to acquire upstream HTTP/3 client for duplex stream"
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

    // 4. Open progressive bidirectional HTTP/3 stream
    let (sender, response_rx) = match client
        .open_stream(head.method, head.uri, head.headers, None, false)
        .await
    {
        Ok(parts) => parts,
        Err(e) => {
            tracing::warn!(
                error = %e,
                upstream = %upstream.id(),
                "Failed to open HTTP/3 duplex stream"
            );
            let err_resp = L7Response::from_bytes(
                StatusCode::BAD_GATEWAY,
                format!("502 Bad Gateway: {e}\n").into_bytes(),
            );
            let _ = responder.send_response(&err_resp);
            return;
        }
    };

    // Task 1: Upload Pump (Downstream H2 -> Upstream H3 QUIC)
    let upload_task = async {
        while let Ok(Some(chunk)) = receiver.recv_chunk().await {
            if !chunk.is_empty() && sender.send_chunk(chunk).await.is_err() {
                return Err(());
            }
        }
        let _ = sender.finish().await;
        Ok::<(), ()>(())
    };

    // Task 2: Download Pump (Upstream H3 QUIC -> Downstream H2 DATA frames)
    let download_task = async {
        let mut resp_stream = match response_rx.await {
            Ok(Ok(r)) => r,
            _ => return Err(()),
        };

        let mut stream_sender =
            match responder.send_stream_response(resp_stream.status, &resp_stream.headers) {
                Ok(s) => s,
                Err(_) => return Err(()),
            };

        while let Some(chunk_res) = resp_stream.next_chunk().await {
            match chunk_res {
                Ok(chunk) => {
                    if stream_sender.send_chunk(chunk).await.is_err() {
                        return Err(());
                    }
                }
                Err(_) => return Err(()),
            }
        }

        Ok::<(), ()>(())
    };

    let _ = tokio::try_join!(upload_task, download_task);
}
