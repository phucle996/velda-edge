//! Layer 7 HTTP/2 to HTTP/3 Duplex Bridge Pipeline.
//!
//! Handles full-duplex bidirectional streaming (e.g. WebTransport / tunnel over QUIC)
//! between downstream HTTP/2 client and upstream HTTP/3 QUIC connection with true
//! concurrent streaming pumps and zero whole-payload buffering.

use std::sync::Arc;

use http::StatusCode;
use http::header::{CONTENT_TYPE, HeaderValue};
use velda_core::L7Response;
use velda_http2::server::request::{Http2ServerRequestHead, Http2StreamReceiver};
use velda_http2::server::response::Http2Responder;
use velda_http3::headers::sanitize_headers;

use crate::pipeline::{DownstreamMeta, PipeOutcome};
use crate::upstream::Http3Upstream;

/// Serves a full-duplex HTTP/2 downstream stream bridged to an HTTP/3 QUIC backend.
pub async fn serve(
    mut req_head: Http2ServerRequestHead,
    mut receiver: Http2StreamReceiver,
    mut responder: Http2Responder,
    meta: Arc<DownstreamMeta>,
    upstream: &Arc<Http3Upstream>,
    req_state: &velda_core::RequestState,
    plan: &velda_plugin::RoutePluginPlan,
) -> PipeOutcome {
    // 1. Sanitize connection-specific headers forbidden by RFC 9114 §4.2
    sanitize_headers(&mut req_head.headers);

    // 3. Acquire pooled HTTP/3 client
    let (client, _lease) = match upstream.acquire(&upstream.config).await {
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
            return PipeOutcome::http(
                receiver.bytes_received as u64,
                responder.last_bytes_tx,
                None,
                velda_core::InternalStatus::UpstreamError,
            );
        }
    };
    let upstream_addr = Some(client.target());

    // 4. Fast-path: Ingest initial payload with opportunistic coalescing up to 64KB
    let (first_chunk, mut is_fin) = match receiver.recv_coalesced_chunk(65_536).await {
        Ok(Some(c)) => {
            let fin = receiver.is_end_stream();
            (Some(c), fin)
        }
        Ok(None) => (None, true),
        Err(e) => {
            let err_resp = L7Response::from_bytes(
                StatusCode::BAD_REQUEST,
                format!("400 Bad Request: {e}\n").into_bytes(),
            );
            let _ = responder.send_response(&err_resp);
            return PipeOutcome::http(
                receiver.bytes_received as u64,
                responder.last_bytes_tx,
                upstream_addr,
                velda_core::InternalStatus::BadRequest,
            );
        }
    };

    // Open progressive bidirectional HTTP/3 stream with fused initial chunk and optional FIN
    let h3_req_headers = if plan.post_response_hooks.is_some() {
        req_head.headers.clone()
    } else {
        std::mem::take(&mut req_head.headers)
    };
    let (sender, response_rx) = match client
        .open_stream(
            req_head.method,
            req_head.uri,
            h3_req_headers,
            first_chunk,
            is_fin,
        )
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
            return PipeOutcome::http(
                receiver.bytes_received as u64,
                responder.last_bytes_tx,
                upstream_addr,
                velda_core::InternalStatus::UpstreamError,
            );
        }
    };

    let failed = {
        // Task 1: Upload Pump (Downstream H2 -> Upstream H3 QUIC)
        let upload_task = async {
            if !is_fin {
                let coalesce_limit = (upstream.config.max_body_size / 32).clamp(65_536, 262_144);
                while let Ok(Some(chunk)) = receiver.recv_coalesced_chunk(coalesce_limit).await {
                    let chunk_fin = receiver.is_end_stream();
                    if !chunk.is_empty() && sender.send_chunk_fin(chunk, chunk_fin).await.is_err() {
                        return Err(());
                    }
                    if chunk_fin {
                        is_fin = true;
                        break;
                    }
                }
                if !is_fin {
                    let _ = sender.finish().await;
                }
            }
            Ok::<(), ()>(())
        };

        // Task 2: Download Pump (Upstream H3 QUIC -> Downstream H2 DATA frames)
        let download_task = async {
            let mut resp_stream = match response_rx.await {
                Ok(Ok(r)) => r,
                _ => return Err(()),
            };

            let mut resp_ws = std::mem::take(&mut resp_stream.headers);
            if let Some(post_hooks) = &plan.post_response_hooks {
                let mut l7_resp = L7Response::new(
                    resp_stream.status,
                    http::Version::HTTP_2,
                    resp_ws,
                    velda_core::Body::Empty,
                );
                velda_plugin::run_post_response_plugins(
                    post_hooks,
                    req_state,
                    &req_head.headers,
                    &mut l7_resp,
                );
                resp_stream.status = l7_resp.status;
                resp_ws = l7_resp.headers;
            }
            meta.inject_response_headers(&mut resp_ws);

            let mut stream_sender =
                match responder.send_stream_response(resp_stream.status, &resp_ws) {
                    Ok(s) => s,
                    Err(_) => return Err(()),
                };

            while let Some(chunk_res) = resp_stream.next_chunk().await {
                match chunk_res {
                    Ok(chunk) => {
                        if stream_sender.send_chunk(chunk).await.is_err() {
                            responder.last_bytes_tx = stream_sender.bytes_sent;
                            return Err(());
                        }
                    }
                    Err(_) => {
                        responder.last_bytes_tx = stream_sender.bytes_sent;
                        return Err(());
                    }
                }
            }

            responder.last_bytes_tx = stream_sender.bytes_sent;
            let _ = stream_sender.finish();
            Ok::<(), ()>(())
        };

        let mut upload_pinned = std::pin::pin!(upload_task);
        let mut download_pinned = std::pin::pin!(download_task);

        tokio::select! {
            res = &mut download_pinned => {
                res.is_err()
            }
            res = &mut upload_pinned => {
                let up_err = res.is_err();
                download_pinned.await.is_err() || up_err
            }
        }
    };
    PipeOutcome::http(
        receiver.bytes_received as u64,
        responder.last_bytes_tx,
        upstream_addr,
        if failed {
            velda_core::InternalStatus::UpstreamError
        } else {
            velda_core::InternalStatus::Completed
        },
    )
}
