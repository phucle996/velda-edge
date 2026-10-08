//! Layer 7 gRPC over TCP: downstream stream worker, route evaluation, and streaming forwarding.
//!
//! Powered entirely by `velda-grpc`. Operates strictly for listeners explicitly configured
//! with `transport = "tcp"` and `protocol = "grpc"`. Zero HTTP fallback, zero header sniffing,
//! and full bidirectional streaming.

use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncWrite};
use velda_grpc::GrpcConfig;
use velda_grpc::GrpcStatus;
use velda_grpc::tcp::pipe::{
    GrpcPipeStrategy, pipe_buffered, pipe_client_stream, pipe_duplex, pipe_server_stream,
};
use velda_grpc::tcp::server::{GrpcServerConnection, GrpcServerStream};
use velda_router::GrpcRouteRequest;
use velda_tls::TlsServerEngine;
use velda_transport::Connection;

use crate::pre_compile::pipeline::DownstreamMeta;
use crate::runtime::SharedRuntime;

/// Asynchronous stream worker dispatching incoming gRPC connections over TCP.
///
/// Inspects the pre-compiled listener metadata to determine whether downstream
/// TLS termination is active. If TLS is required, negotiates the handshake, validates
/// ALPN against "h2", and passes the encrypted stream to the gRPC TCP loop.
pub async fn handle_grpc_tcp(
    connection: Connection,
    listener_id: Arc<str>,
    config: GrpcConfig,
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

                let meta = DownstreamMeta {
                    listener_id,
                    peer,
                    local_addr,
                    is_tls: true,
                };
                run_grpc_tcp_loop(tls_stream, meta, config, runtime).await;
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
        let meta = DownstreamMeta {
            listener_id,
            peer,
            local_addr,
            is_tls: false,
        };
        run_grpc_tcp_loop(connection, meta, config, runtime).await;
    }
}

/// Core gRPC downstream stream worker loop over TCP decoupled from transport layer.
pub async fn run_grpc_tcp_loop<IO>(
    stream: IO,
    meta: DownstreamMeta,
    config: GrpcConfig,
    runtime: SharedRuntime,
) where
    IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let mut conn = match GrpcServerConnection::handshake(stream, &config).await {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(
                error = %e,
                listener = %meta.listener_id,
                "Failed downstream gRPC server handshake"
            );
            return;
        }
    };

    let timeout_duration = std::time::Duration::from_millis(config.idle_timeout_ms);

    while let Ok(accept_result) = tokio::time::timeout(timeout_duration, conn.accept()).await {
        match accept_result {
            Ok(Some(server_stream)) => {
                let meta = meta.clone();
                let rt = runtime.clone();
                let cfg = config;

                tokio::spawn(async move {
                    dispatch_grpc_tcp_request_stream(server_stream, meta, &cfg, &rt).await;
                });
            }
            Ok(None) => break,
            Err(e) => {
                tracing::debug!(error = %e, "gRPC stream accept error");
                break;
            }
        }
    }
}

/// Dispatches a single downstream gRPC request stream through the router to upstream backend.
async fn dispatch_grpc_tcp_request_stream(
    mut server_stream: GrpcServerStream,
    meta: DownstreamMeta,
    config: &GrpcConfig,
    runtime: &SharedRuntime,
) {
    server_stream.enrich_forwarded_headers(meta.peer, meta.local_addr, meta.is_tls);
    let authority_str = server_stream.authority();
    let path = server_stream.parts.uri.path();

    let Some(grpc_req) = GrpcRouteRequest::from_path(path, authority_str) else {
        let _ = server_stream.respond.send_trailers_only(
            GrpcStatus::Unimplemented,
            Some("no route matched for empty path"),
        );
        return;
    };

    let rt = runtime.load();
    let Some(route) = rt.router.route_grpc(&meta.listener_id, &grpc_req) else {
        let _ = server_stream.respond.send_trailers_only(
            GrpcStatus::Unimplemented,
            Some("no route matched for service"),
        );
        return;
    };

    let Some(upstream) = rt
        .upstreams
        .grpc
        .get(&route.upstream_name)
        .and_then(|u| u.as_tcp())
    else {
        tracing::error!(
            listener = %meta.listener_id,
            route = %route.id,
            upstream = %route.upstream_name,
            "No healthy backend endpoints available for gRPC upstream"
        );
        let _ = server_stream
            .respond
            .send_trailers_only(GrpcStatus::Unavailable, Some("upstream not configured"));
        return;
    };

    let strategy = upstream.strategy;
    let mut client = match upstream.acquire(config).await {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(
                error = %e,
                upstream = %route.upstream_name,
                "Failed to acquire upstream gRPC over TCP connection"
            );
            let _ = server_stream
                .respond
                .send_trailers_only(GrpcStatus::Unavailable, Some("upstream unavailable"));
            return;
        }
    };

    tracing::debug!(
        listener = %meta.listener_id,
        route = %route.id,
        upstream = %route.upstream_name,
        strategy = ?strategy,
        service = %grpc_req.service,
        method = ?grpc_req.method,
        "Piping gRPC bidirectional stream to upstream backend"
    );

    let pipe_res = match strategy {
        GrpcPipeStrategy::Buffered => {
            let req_data = match server_stream
                .read_raw_message(config.max_message_size)
                .await
            {
                Ok(data) => data,
                Err(_) => {
                    let _ = server_stream.respond.send_trailers_only(
                        GrpcStatus::ResourceExhausted,
                        Some("request message size exceeds limit"),
                    );
                    return;
                }
            };
            let mut res = pipe_buffered(
                &server_stream.parts,
                &req_data,
                &mut server_stream.respond,
                &mut client,
                config,
            )
            .await;

            // Self-Healing Retry (RFC 9113 §8.1.4):
            // If upstream connection dropped or remote peer returned REFUSED_STREAM,
            // retry transparently 1-shot on a fresh connection.
            if let Err(ref e) = res
                && e.is_stale_or_refused()
            {
                tracing::debug!(
                    upstream = %route.upstream_name,
                    error = %e,
                    "gRPC upstream connection dropped or refused; self-healing with fresh connection"
                );
                if let Ok(mut fresh_client) = upstream.acquire_fresh(config).await {
                    res = pipe_buffered(
                        &server_stream.parts,
                        &req_data,
                        &mut server_stream.respond,
                        &mut fresh_client,
                        config,
                    )
                    .await;
                }
            }
            res
        }
        GrpcPipeStrategy::ServerStream => {
            pipe_server_stream(server_stream, &mut client, config).await
        }
        GrpcPipeStrategy::ClientStream => {
            pipe_client_stream(server_stream, &mut client, config).await
        }
        GrpcPipeStrategy::Duplex => pipe_duplex(server_stream, &mut client, config).await,
    };

    if let Err(e) = pipe_res {
        tracing::warn!(
            error = %e,
            upstream = %route.upstream_name,
            "gRPC stream pipe terminated with error"
        );
    }
}

#[cfg(test)]
mod tests {
    use http::HeaderMap;
    use std::net::SocketAddr;
    use velda_grpc::GrpcStatus;
    use velda_grpc::tcp::server::enrich_headers;

    #[test]
    fn test_enrich_forwarded_headers() {
        let mut headers = HeaderMap::new();
        // Client attempts to spoof their IP, host, and SSL status
        headers.insert("x-forwarded-for", "203.0.113.195".parse().unwrap());
        headers.insert("x-real-ip", "203.0.113.195".parse().unwrap());
        headers.insert("x-forwarded-host", "evil.attacker.com".parse().unwrap());
        headers.insert("x-forwarded-proto", "http".parse().unwrap());
        headers.insert("x-forwarded-ssl", "off".parse().unwrap());
        headers.insert("connection", "close".parse().unwrap());
        headers.insert("keep-alive", "timeout=5".parse().unwrap());
        headers.insert("forwarded", "for=203.0.113.195;proto=http".parse().unwrap());

        let peer: SocketAddr = "192.0.2.40:50051".parse().unwrap();
        let local: SocketAddr = "10.0.0.1:443".parse().unwrap();

        enrich_headers(&mut headers, peer, local, true, Some("grpc.example.com"));

        // Client spoofed values MUST be completely replaced with authoritative values
        assert_eq!(headers.get("x-forwarded-for").unwrap(), "192.0.2.40");
        assert_eq!(headers.get("x-real-ip").unwrap(), "192.0.2.40");
        assert_eq!(headers.get("x-forwarded-proto").unwrap(), "https");
        assert_eq!(headers.get("x-forwarded-port").unwrap(), "443");
        assert_eq!(headers.get("x-forwarded-host").unwrap(), "grpc.example.com");
        assert_eq!(
            headers.get("forwarded").unwrap(),
            "for=192.0.2.40;proto=https;by=10.0.0.1;host=\"grpc.example.com\""
        );
        // Untrusted extra x-forwarded-* headers MUST be stripped
        assert!(headers.get("x-forwarded-ssl").is_none());
        // RFC 9113 hop-by-hop headers MUST be stripped
        assert!(headers.get("connection").is_none());
        assert!(headers.get("keep-alive").is_none());
    }

    #[test]
    fn test_stale_or_refused_detection() {
        assert!(velda_grpc::GrpcError::Protocol("connection closed".into()).is_stale_or_refused());
        assert!(
            velda_grpc::GrpcError::Io(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "broken pipe"
            ))
            .is_stale_or_refused()
        );
        assert!(
            velda_grpc::GrpcError::Io(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "connection reset"
            ))
            .is_stale_or_refused()
        );

        // Status errors are business responses, not stale connections
        assert!(
            !velda_grpc::GrpcError::Status(GrpcStatus::NotFound, "not found".into())
                .is_stale_or_refused()
        );
        assert!(!velda_grpc::GrpcError::PayloadTooLarge(100).is_stale_or_refused());
    }
}
