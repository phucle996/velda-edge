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
use velda_http3::sanitize_headers;

use crate::pipeline::DownstreamMeta;
use crate::upstream::Http3Upstream;

/// Serves a client-streaming HTTP/2 downstream stream bridged to an HTTP/3 QUIC backend.
pub async fn serve(
    mut req_head: Http2ServerRequestHead,
    mut receiver: Http2StreamReceiver,
    mut responder: Http2Responder,
    meta: Arc<DownstreamMeta>,
    upstream: &Arc<Http3Upstream>,
    plan: &velda_plugin::RoutePluginPlan,
) {
    // 1. Enrich proxy forwarding headers
    velda_http2::server::header::enrich_headers_precomputed(
        &mut req_head.headers,
        &req_head.uri,
        &meta.client_ip_header,
        &meta.client_port_header,
        meta.is_tls,
    );

    // 2. Sanitize connection-specific headers forbidden by RFC 9114 §4.2
    sanitize_headers(&mut req_head.headers);

    let req_headers_ref = if plan.post_response_hooks.is_some() {
        Some(req_head.headers.clone())
    } else {
        None
    };

    // 3. Acquire pooled HTTP/3 client
    let (client, _lease) = match upstream.acquire(&upstream.config).await {
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

    // 4. Fast-path: Ingest initial payload with opportunistic coalescing
    let (first_chunk, mut is_fin) = match receiver
        .recv_coalesced_chunk(upstream.config.stream_receive_window as usize)
        .await
    {
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
            return;
        }
    };

    // Open progressive HTTP/3 stream with fused initial chunk and optional FIN
    let (sender, response_rx) = match client
        .open_stream(
            req_head.method,
            req_head.uri,
            req_head.headers,
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

    let mut response_rx = std::pin::pin!(response_rx);
    let mut early_resp = None;

    // 5. Progressively stream any remaining client DATA frames directly to HTTP/3
    if !is_fin {
        let coalesce_limit = (upstream.config.max_body_size / 32).clamp(65_536, 262_144);
        'upload: while !receiver.is_end_stream() {
            tokio::select! {
                early = &mut response_rx => {
                    early_resp = Some(early);
                    break 'upload;
                }
                res = receiver.recv_coalesced_chunk(coalesce_limit) => {
                    match res {
                        Ok(Some(chunk)) => {
                            let chunk_fin = receiver.is_end_stream();
                            if !chunk.is_empty() && sender.send_chunk_fin(chunk, chunk_fin).await.is_err() {
                                tracing::warn!("Failed to stream DATA chunk to HTTP/3 upstream");
                                let err_resp = L7Response::from_bytes(
                                    StatusCode::BAD_GATEWAY,
                                    b"502 Bad Gateway: upstream stream disconnected\n".to_vec(),
                                );
                                let _ = responder.send_response(&err_resp);
                                return;
                            }
                            if chunk_fin {
                                is_fin = true;
                                break 'upload;
                            }
                        }
                        Ok(None) => break 'upload,
                        Err(_) => return,
                    }
                }
            }
        }
        if !is_fin && early_resp.is_none() {
            let _ = sender.finish().await;
        }
    }

    // 6. Await response headers from upstream HTTP/3
    let mut resp_stream = match early_resp {
        Some(res) => match res {
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
        },
        None => match response_rx.await {
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
        },
    };

    // 6. Consume finite response body and send downstream
    let max_body = upstream.config.max_body_size;
    let mut resp_body = BytesMut::new();
    while let Some(chunk_res) = resp_stream.next_chunk().await {
        match chunk_res {
            Ok(chunk) => {
                if resp_body.len() + chunk.len() > max_body {
                    tracing::warn!("HTTP/3 upstream response exceeded max_body_size");
                    let err_resp = L7Response::from_bytes(
                        StatusCode::PAYLOAD_TOO_LARGE,
                        b"413 Payload Too Large\n".to_vec(),
                    );
                    let _ = responder.send_response(&err_resp);
                    return;
                }
                resp_body.extend_from_slice(&chunk);
            }
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

    let mut resp = L7Response::new(
        resp_stream.status,
        http::Version::HTTP_2,
        resp_stream.headers,
        body,
    );
    if let Some(post_hooks) = &plan.post_response_hooks
        && let Some(ref req_headers) = req_headers_ref
    {
        velda_plugin::run_post_response_plugins(post_hooks, req_headers, &mut resp);
    }
    let _ = responder.send_response(&resp);
    if !receiver.is_end_stream() {
        tokio::spawn(async move { while let Ok(Some(_)) = receiver.recv_chunk().await {} });
    }
}
