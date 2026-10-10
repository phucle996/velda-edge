//! Layer 7 gRPC over TCP downstream stream worker, route evaluation, and streaming forwarding.
//!
//! Powered entirely by `velda-grpc`. Operates strictly for listeners explicitly configured
//! with `transport = "tcp"` and `protocol = "grpc"`. Zero HTTP fallback, zero header sniffing,
//! and full bidirectional streaming.

use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncWrite};
use velda_grpc::tcp::pipe::GrpcPipeStrategy;
use velda_grpc::tcp::server::{GrpcServerConnection, GrpcServerStream};
use velda_grpc::{GrpcConfig, GrpcStatus};
use velda_router::GrpcRouteRequest;
use velda_tls::TlsServerEngine;
use velda_transport::Connection;

use super::pipe;
use crate::pipeline::DownstreamMeta;
use crate::runtime::SharedRuntime;
use crate::upstream::GrpcUpstream;

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

                let meta = DownstreamMeta::new(listener_id, peer, local_addr, true);
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
        let meta = DownstreamMeta::new(listener_id, peer, local_addr, false);
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

    let Some(upstream_target) = rt.upstreams.grpc.get(&route.upstream_name) else {
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

    // Resource Saturation Circuit Breaker (Overload Protection)
    let overload_lvl = rt.overload.level();
    if overload_lvl.is_shedding() {
        tracing::warn!(
            route = %route.id,
            upstream = %route.upstream_name,
            overload = ?overload_lvl,
            "Shedding gRPC TCP request due to memory saturation"
        );
        let _ = server_stream
            .respond
            .send_trailers_only(GrpcStatus::ResourceExhausted, Some("resource exhausted"));
        return;
    }

    let pipe_res = match upstream_target.as_ref() {
        GrpcUpstream::Tcp(upstream) => match upstream.strategy {
            GrpcPipeStrategy::Buffered => {
                pipe::buffered_tcp_tcp::serve(server_stream, upstream, config).await
            }
            GrpcPipeStrategy::ServerStream => {
                pipe::server_stream_tcp_tcp::serve(server_stream, upstream, config).await
            }
            GrpcPipeStrategy::ClientStream => {
                pipe::client_stream_tcp_tcp::serve(server_stream, upstream, config).await
            }
            GrpcPipeStrategy::Duplex => {
                pipe::duplex_tcp_tcp::serve(server_stream, upstream, config).await
            }
        },
        GrpcUpstream::Udp(upstream) => match upstream.strategy {
            velda_grpc::udp::pipe::GrpcUdpPipeStrategy::Buffered => {
                pipe::buffered_tcp_udp::serve(server_stream, upstream, config).await
            }
            velda_grpc::udp::pipe::GrpcUdpPipeStrategy::ServerStream => {
                pipe::server_stream_tcp_udp::serve(server_stream, upstream, config).await
            }
            velda_grpc::udp::pipe::GrpcUdpPipeStrategy::ClientStream => {
                pipe::client_stream_tcp_udp::serve(server_stream, upstream, config).await
            }
            velda_grpc::udp::pipe::GrpcUdpPipeStrategy::Duplex => {
                pipe::duplex_tcp_udp::serve(server_stream, upstream, config).await
            }
        },
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
        headers.insert("x-forwarded-for", "203.0.113.195".parse().unwrap());
        headers.insert("x-real-ip", "203.0.113.195".parse().unwrap());
        headers.insert("x-forwarded-host", "evil.attacker.com".parse().unwrap());
        headers.insert("x-forwarded-proto", "http".parse().unwrap());
        headers.insert("connection", "close".parse().unwrap());
        headers.insert("keep-alive", "timeout=5".parse().unwrap());
        headers.insert("forwarded", "for=203.0.113.195;proto=http".parse().unwrap());

        let peer: SocketAddr = "192.0.2.40:50051".parse().unwrap();
        let local: SocketAddr = "10.0.0.1:443".parse().unwrap();

        enrich_headers(&mut headers, peer, local, true, Some("grpc.example.com"));

        assert_eq!(headers.get("x-forwarded-for").unwrap(), "192.0.2.40");
        assert_eq!(headers.get("x-real-ip").unwrap(), "192.0.2.40");
        assert_eq!(headers.get("x-forwarded-proto").unwrap(), "https");
        assert_eq!(headers.get("x-forwarded-port").unwrap(), "443");
        assert_eq!(headers.get("x-forwarded-host").unwrap(), "grpc.example.com");
        assert_eq!(
            headers.get("forwarded").unwrap(),
            "for=192.0.2.40;proto=https;by=10.0.0.1;host=\"grpc.example.com\""
        );
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

        assert!(
            !velda_grpc::GrpcError::Status(GrpcStatus::NotFound, "not found".into())
                .is_stale_or_refused()
        );
        assert!(!velda_grpc::GrpcError::PayloadTooLarge(100).is_stale_or_refused());
    }
}
