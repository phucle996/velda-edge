//! Layer 7 HTTP/2 pipeline: downstream stream worker, route evaluation, and upstream forwarding.
//!
//! Operates strictly for listeners explicitly configured with `protocol = "http2"`.
//! Routes matched via `velda-router::Http2Router`, forwarded via `velda-http2`.

use http::StatusCode;
use http::header::{CONTENT_TYPE, HeaderValue};
use tokio::io::{AsyncRead, AsyncWrite};
use velda_core::L7Response;
use velda_http2::config::Http2Config;
use velda_http2::pipe::{
    Http2PipeStrategy, pipe_buffered, pipe_client_stream, pipe_duplex, pipe_server_stream,
};
use velda_http2::server::{
    Http2RequestHead, Http2Responder, Http2ServerConnection, Http2StreamReceiver,
};
use velda_router::Http2RouteRequest;
use velda_tls::TlsServerEngine;
use velda_transport::Connection;

use std::net::SocketAddr;
use std::sync::Arc;

use crate::runtime::SharedRuntime;

/// Enriches HTTP/2 request headers with RFC 7239 and standard proxy forwarding metadata.
fn enrich_http2_forwarded_headers(
    headers: &mut http::HeaderMap,
    peer: SocketAddr,
    local_addr: SocketAddr,
    is_tls: bool,
    host: Option<&str>,
) {
    use http::header::{HeaderName, HeaderValue};

    let client_ip = peer.ip();
    let proto = if is_tls { "https" } else { "http" };

    // Format peer IP directly into stack buffer
    let mut ip_buf = [0u8; 64];
    let ip_len = {
        use std::io::Write;
        let mut cursor = std::io::Cursor::new(&mut ip_buf[..]);
        let _ = write!(cursor, "{}", client_ip);
        cursor.position() as usize
    };
    let client_ip_bytes = &ip_buf[..ip_len];

    // 1. X-Forwarded-For
    let x_forwarded_for = HeaderName::from_static("x-forwarded-for");
    if let Some(existing) = headers.get(&x_forwarded_for) {
        if let Ok(existing_bytes) = existing.to_str() {
            let mut combined = Vec::with_capacity(existing_bytes.len() + 2 + ip_len);
            combined.extend_from_slice(existing_bytes.as_bytes());
            combined.extend_from_slice(b", ");
            combined.extend_from_slice(client_ip_bytes);
            if let Ok(val) = HeaderValue::from_bytes(&combined) {
                headers.insert(x_forwarded_for, val);
            }
        }
    } else if let Ok(val) = HeaderValue::from_bytes(client_ip_bytes) {
        headers.insert(x_forwarded_for, val);
    }

    // 2. X-Forwarded-Proto
    headers.insert(
        HeaderName::from_static("x-forwarded-proto"),
        HeaderValue::from_static(proto),
    );

    // 3. X-Forwarded-Port
    let mut port_buf = [0u8; 8];
    let port_len = {
        use std::io::Write;
        let mut cursor = std::io::Cursor::new(&mut port_buf[..]);
        let _ = write!(cursor, "{}", local_addr.port());
        cursor.position() as usize
    };
    if let Ok(val) = HeaderValue::from_bytes(&port_buf[..port_len]) {
        headers.insert(HeaderName::from_static("x-forwarded-port"), val);
    }

    // 4. X-Forwarded-Host
    let x_forwarded_host = HeaderName::from_static("x-forwarded-host");
    if !headers.contains_key(&x_forwarded_host)
        && let Some(h) = host
        && let Ok(val) = HeaderValue::from_str(h)
    {
        headers.insert(x_forwarded_host, val);
    }

    // 5. X-Real-IP
    let x_real_ip = HeaderName::from_static("x-real-ip");
    if !headers.contains_key(&x_real_ip)
        && let Ok(val) = HeaderValue::from_bytes(client_ip_bytes)
    {
        headers.insert(x_real_ip, val);
    }

    // 6. Standard: RFC 7239 (Official IETF "Forwarded" HTTP Extension)
    let forwarded = HeaderName::from_static("forwarded");
    let mut fwd_buf = [0u8; 256];
    let fwd_len = {
        use std::io::Write;
        let mut cursor = std::io::Cursor::new(&mut fwd_buf[..]);
        match client_ip {
            std::net::IpAddr::V4(v4) => {
                let _ = write!(cursor, "for={v4}");
            }
            std::net::IpAddr::V6(v6) => {
                let _ = write!(cursor, "for=\"[{v6}]\"");
            }
        }
        let _ = write!(cursor, ";proto={proto};by=");
        match local_addr.ip() {
            std::net::IpAddr::V4(v4) => {
                let _ = write!(cursor, "{v4}");
            }
            std::net::IpAddr::V6(v6) => {
                let _ = write!(cursor, "\"[{v6}]\"");
            }
        }
        if let Some(h) = host {
            let _ = write!(cursor, ";host=\"{h}\"");
        }
        cursor.position() as usize
    };
    let fwd_bytes = &fwd_buf[..fwd_len];

    if let Some(existing) = headers.get(&forwarded) {
        if let Ok(existing_bytes) = existing.to_str() {
            let mut combined = Vec::with_capacity(existing_bytes.len() + 2 + fwd_len);
            combined.extend_from_slice(existing_bytes.as_bytes());
            combined.extend_from_slice(b", ");
            combined.extend_from_slice(fwd_bytes);
            if let Ok(val) = HeaderValue::from_bytes(&combined) {
                headers.insert(forwarded, val);
            }
        }
    } else if let Ok(val) = HeaderValue::from_bytes(fwd_bytes) {
        headers.insert(forwarded, val);
    }
}

/// Asynchronous stream worker dispatching incoming HTTP/2 connections.
///
/// Inspects listener configuration to determine whether downstream TLS termination
/// is active. If TLS is required, negotiates handshake, validates ALPN against "http2",
/// and passes the encrypted stream to the HTTP/2 loop.
pub async fn handle_http2_stream(
    connection: Connection,
    listener_id: Arc<str>,
    config: Http2Config,
    tls_enabled: bool,
    runtime: SharedRuntime,
) {
    let rt = runtime.load();
    let peer = connection.peer();
    let local_addr = connection.local_addr();

    if tls_enabled {
        let Some(tls_server) = rt.tls_server.as_ref() else {
            tracing::error!(
                listener = %listener_id,
                peer = %peer,
                "TLS required for listener, but no TLS server engine is compiled; dropping connection"
            );
            return;
        };

        match tls_server.accept_with_timeout(connection).await {
            Ok(tls_stream) => {
                let handshake_info = TlsServerEngine::extract_handshake_info(&tls_stream);
                tracing::debug!(
                    listener = %listener_id,
                    peer = %peer,
                    sni = ?handshake_info.sni,
                    alpn = ?handshake_info.alpn,
                    "Downstream TLS handshake succeeded"
                );

                if let Some(alpn) = handshake_info.alpn.as_deref()
                    && alpn != "h2"
                {
                    tracing::warn!(
                        listener = %listener_id,
                        peer = %peer,
                        expected = "h2",
                        actual = alpn,
                        "Dropping connection due to protocol ALPN mismatch"
                    );
                    return;
                }

                run_http2_loop(
                    tls_stream,
                    listener_id,
                    peer,
                    local_addr,
                    true,
                    config,
                    runtime,
                )
                .await;
            }
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    listener = %listener_id,
                    peer = %peer,
                    "Downstream TLS handshake failed"
                );
            }
        }
    } else {
        run_http2_loop(
            connection,
            listener_id,
            peer,
            local_addr,
            false,
            config,
            runtime,
        )
        .await;
    }
}

/// Core HTTP/2 downstream request-response loop decoupled from transport layer.
pub async fn run_http2_loop<IO>(
    stream: IO,
    listener_id: Arc<str>,
    peer: SocketAddr,
    local_addr: SocketAddr,
    is_tls: bool,
    config: Http2Config,
    runtime: SharedRuntime,
) where
    IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let timeout_duration = std::time::Duration::from_millis(config.idle_timeout_ms);
    let mut consecutive_not_founds: u32 = 0;
    const MAX_CONSECUTIVE_NOT_FOUNDS: u32 = 100;

    match Http2ServerConnection::handshake(stream, config).await {
        Ok(mut conn) => loop {
            let accept_result =
                tokio::time::timeout(timeout_duration, conn.accept_streaming_request()).await;
            match accept_result {
                Ok(Ok(Some((head, receiver, responder)))) => {
                    let host_hdr = head.headers.get(http::header::HOST).cloned();
                    let host_str = host_hdr
                        .as_ref()
                        .and_then(|v| v.to_str().ok())
                        .or_else(|| head.uri.authority().map(|a| a.as_str()))
                        .or_else(|| head.uri.host());

                    let mut http_req = Http2RouteRequest::new(head.path());
                    if let Some(h) = host_str {
                        http_req = http_req.with_host(h);
                    }
                    http_req = http_req.with_method(head.method.as_str());

                    let rt = runtime.load();
                    let matched_route = rt.router.route_http2(&listener_id, &http_req).cloned();

                    // ========================================================
                    // FAST-PATH 404 EVALUATION (Zero Tokio Task Allocation)
                    //
                    // Non-matching routes (including misdirected gRPC requests
                    // or scanner path-fuzzing) are rejected immediately in-place
                    // without spawning a new task or allocating heap futures.
                    // ========================================================
                    let Some(route) = matched_route else {
                        consecutive_not_founds += 1;
                        if consecutive_not_founds > MAX_CONSECUTIVE_NOT_FOUNDS {
                            tracing::warn!(
                                listener = %listener_id,
                                peer = %peer,
                                consecutive = consecutive_not_founds,
                                "Aborting HTTP/2 connection: excessive consecutive non-matching routes (possible scan/flood)"
                            );
                            break;
                        }

                        let is_grpc = head
                            .headers
                            .get(CONTENT_TYPE)
                            .is_some_and(|ct| ct.as_bytes().starts_with(b"application/grpc"));

                        let body_text = if is_grpc {
                            b"404 Not Found: no matching HTTP/2 route (this listener serves HTTP/2 web traffic, not gRPC)\n".as_slice()
                        } else {
                            b"404 Not Found: no matching route\n".as_slice()
                        };

                        let not_found =
                            L7Response::from_bytes(StatusCode::NOT_FOUND, body_text.to_vec())
                                .with_header(
                                    CONTENT_TYPE,
                                    HeaderValue::from_static("text/plain; charset=utf-8"),
                                );

                        // If peer has not finished transmitting the request body,
                        // immediately issue RST_STREAM to terminate peer upload and
                        // free socket buffers instantly.
                        if !receiver.is_end_stream() {
                            let _ = responder.send_response_and_cancel_upload(&not_found);
                        } else {
                            let _ = responder.send_response(&not_found);
                        }
                        continue;
                    };

                    consecutive_not_founds = 0;
                    let lid_clone = listener_id.clone();
                    let rt_clone = runtime.clone();
                    let cfg_clone = config;
                    tokio::spawn(async move {
                        serve_http2_stream(
                            head, receiver, responder, lid_clone, peer, local_addr, is_tls,
                            cfg_clone, rt_clone, route,
                        )
                        .await;
                    });
                }
                Ok(Ok(None)) => break,
                Ok(Err(e)) => {
                    tracing::debug!(error = %e, "HTTP/2 stream accept error");
                    break;
                }
                Err(_) => {
                    tracing::debug!(
                        listener = %listener_id,
                        timeout_ms = config.idle_timeout_ms,
                        "HTTP/2 stream accept timed out"
                    );
                    break;
                }
            }
        },
        Err(e) => {
            tracing::warn!(error = %e, "Failed to complete HTTP/2 server handshake");
        }
    }
}

/// Serves an individual HTTP/2 downstream multiplexed stream top-to-bottom.
#[allow(clippy::too_many_arguments)]
async fn serve_http2_stream(
    mut head: Http2RequestHead,
    receiver: Http2StreamReceiver,
    responder: Http2Responder,
    listener_id: Arc<str>,
    peer: SocketAddr,
    local_addr: SocketAddr,
    is_tls: bool,
    config: Http2Config,
    runtime: SharedRuntime,
    route: velda_router::Http2Route,
) {
    let rt = runtime.load();
    let Some(upstream) = rt.upstreams.http2.get(&route.upstream_name) else {
        tracing::error!(
            listener = %listener_id,
            route = %route.id,
            upstream = %route.upstream_name,
            "No healthy backend endpoints available for HTTP/2 upstream"
        );
        let no_backend = L7Response::from_bytes(
            StatusCode::SERVICE_UNAVAILABLE,
            b"503 Service Unavailable: upstream not configured\n".to_vec(),
        )
        .with_header(
            CONTENT_TYPE,
            HeaderValue::from_static("text/plain; charset=utf-8"),
        );
        if !receiver.is_end_stream() {
            let _ = responder.send_response_and_cancel_upload(&no_backend);
        } else {
            let _ = responder.send_response(&no_backend);
        }
        return;
    };

    let strategy = upstream.strategy;
    let host_hdr = head.headers.get(http::header::HOST).cloned();
    let host_str = host_hdr
        .as_ref()
        .and_then(|v| v.to_str().ok())
        .or_else(|| head.uri.authority().map(|a| a.as_str()))
        .or_else(|| head.uri.host());

    enrich_http2_forwarded_headers(&mut head.headers, peer, local_addr, is_tls, host_str);

    // HTTP/2 Pipeline Invariant (RFC 9113):
    // The Edge pipeline hands off the request processing closure directly to the upstream.
    // The upstream manages single-round Load Balancing selection, persistent multiplexed client
    // reuse, ready check, error recovery, and zero-overhead binary framing.
    let upstream_cfg = config;
    let pipe_res = upstream
        .dispatch_pipe(&upstream_cfg, move |mut client| async move {
            match strategy {
                Http2PipeStrategy::Buffered => {
                    pipe_buffered(head, receiver, responder, &mut client, &config).await
                }
                Http2PipeStrategy::ServerStream => {
                    pipe_server_stream(head, receiver, responder, &mut client, &config).await
                }
                Http2PipeStrategy::ClientStream => {
                    pipe_client_stream(head, receiver, responder, &mut client, &config).await
                }
                Http2PipeStrategy::Duplex => {
                    pipe_duplex(head, receiver, responder, &mut client, &config).await
                }
            }
        })
        .await;

    if let Err(e) = pipe_res {
        tracing::warn!(
            error = %e,
            upstream = %route.upstream_name,
            "HTTP/2 upstream dispatch pipe failed"
        );
    }
}
