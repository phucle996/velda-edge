//! Layer 7 gRPC pipeline: downstream stream worker, route evaluation, and streaming forwarding.
//!
//! Powered entirely by `velda-grpc`. Operates strictly for listeners explicitly configured
//! with `protocol = "grpc"`. Zero HTTP fallback, zero header sniffing, and bidirectional streaming.

use tokio::io::{AsyncRead, AsyncWrite};
use velda_core::{L7Request, L7Response};
use velda_grpc::GrpcConfig;
use velda_grpc::GrpcStatus;
use velda_grpc::server::{GrpcServerConnection, GrpcServerStream};
use velda_router::GrpcRouteRequest;
use velda_tls::TlsServerEngine;
use velda_transport::Connection;

use crate::pipeline::context::{IngressContext, TlsMetadata};
use crate::runtime::SharedRuntime;

/// Asynchronous stream worker dispatching incoming gRPC connections.
///
/// Inspects the pre-compiled listener metadata to determine whether downstream
/// TLS termination is active. If TLS is required, negotiates the handshake, validates
/// ALPN against the declared protocol, and passes the encrypted stream to the gRPC loop.
pub async fn handle_grpc_stream(
    connection: Connection,
    context: IngressContext,
    config: GrpcConfig,
    runtime: SharedRuntime,
) {
    let rt = runtime.load();

    if context.tls_enabled {
        let Some(tls_server) = rt.tls_server.as_ref() else {
            tracing::error!(
                listener = %context.listener_id,
                peer = %context.peer,
                "TLS required for listener, but no TLS server engine is compiled; dropping connection"
            );
            return;
        };

        match tls_server.accept_with_timeout(connection).await {
            Ok(tls_stream) => {
                let handshake_info = TlsServerEngine::extract_handshake_info(&tls_stream);
                tracing::debug!(
                    listener = %context.listener_id,
                    peer = %context.peer,
                    sni = ?handshake_info.sni,
                    alpn = ?handshake_info.alpn,
                    "Downstream TLS handshake succeeded"
                );
                let enriched_context =
                    context.with_tls_metadata(TlsMetadata::from_handshake(handshake_info));

                if let Err(err) = enriched_context.validate_alpn("grpc") {
                    tracing::warn!(
                        error = %err,
                        listener = %enriched_context.listener_id,
                        peer = %enriched_context.peer,
                        "Dropping connection due to protocol ALPN mismatch"
                    );
                    return;
                }

                run_grpc_loop(tls_stream, enriched_context, config, runtime).await;
            }
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    listener = %context.listener_id,
                    peer = %context.peer,
                    "Downstream TLS handshake failed"
                );
            }
        }
    } else {
        run_grpc_loop(connection, context, config, runtime).await;
    }
}

/// Core gRPC downstream stream worker loop decoupled from transport layer.
pub async fn run_grpc_loop<IO>(
    stream: IO,
    context: IngressContext,
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
                listener = %context.listener_id,
                "Failed downstream gRPC server handshake"
            );
            return;
        }
    };

    let timeout_duration = std::time::Duration::from_millis(config.idle_timeout_ms);

    while let Ok(accept_result) = tokio::time::timeout(timeout_duration, conn.accept()).await {
        match accept_result {
            Ok(Some(server_stream)) => {
                let ctx = context.clone();
                let rt = runtime.clone();
                let cfg = config;

                tokio::spawn(async move {
                    dispatch_grpc_request_stream(server_stream, &ctx, &cfg, &rt).await;
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
async fn dispatch_grpc_request_stream(
    mut server_stream: GrpcServerStream,
    context: &IngressContext,
    config: &GrpcConfig,
    runtime: &SharedRuntime,
) {
    let path = server_stream.parts.uri.path();
    let authority = server_stream
        .parts
        .headers
        .get(":authority")
        .and_then(|v| v.to_str().ok())
        .or_else(|| server_stream.parts.uri.authority().map(|a| a.as_str()));

    let Some(grpc_req) = GrpcRouteRequest::from_path(path, authority) else {
        let _ = server_stream.respond.send_trailers_only(
            GrpcStatus::Unimplemented,
            Some("no route matched for empty path"),
        );
        return;
    };

    let rt = runtime.load();
    let Some(route) = rt.router.route_grpc(&context.listener_id, &grpc_req) else {
        let _ = server_stream.respond.send_trailers_only(
            GrpcStatus::Unimplemented,
            Some("no route matched for service"),
        );
        return;
    };

    let Some(upstream) = rt.upstreams.grpc.get(&route.upstream_name) else {
        tracing::error!(
            listener = %context.listener_id,
            route = %route.id,
            upstream = %route.upstream_name,
            "No healthy backend endpoints available for gRPC upstream"
        );
        let _ = server_stream
            .respond
            .send_trailers_only(GrpcStatus::Unavailable, Some("upstream not configured"));
        return;
    };

    tracing::debug!(
        listener = %context.listener_id,
        route = %route.id,
        upstream = %route.upstream_name,
        service = %grpc_req.service,
        method = ?grpc_req.method,
        "Piping gRPC bidirectional stream to upstream backend"
    );

    if let Err(e) = upstream.dispatch_stream(server_stream, config).await {
        tracing::warn!(
            error = %e,
            upstream = %route.upstream_name,
            "gRPC stream pipe terminated with error"
        );
    }
}

/// Dispatches a single unary gRPC request through `GrpcRouter` to the upstream backend.
/// Used for unary RPC execution or UDP L7 datagram handoff when the listener explicitly declares protocol = "grpc".
pub async fn process_grpc_request(
    req: &L7Request,
    context: &IngressContext,
    config: &GrpcConfig,
    runtime: &SharedRuntime,
) -> L7Response {
    let authority = req
        .headers
        .get(":authority")
        .and_then(|v| v.to_str().ok())
        .or_else(|| req.host().and_then(|h| h.to_str().ok()))
        .or_else(|| req.uri.authority().map(|a| a.as_str()));

    let Some(grpc_req) = GrpcRouteRequest::from_path(req.path(), authority) else {
        return GrpcStatus::Unimplemented.to_l7_response(Some("no route matched for empty path"));
    };

    let rt = runtime.load();
    let Some(route) = rt.router.route_grpc(&context.listener_id, &grpc_req) else {
        return GrpcStatus::Unimplemented.to_l7_response(Some("no route matched for service"));
    };

    let Some(upstream) = rt.upstreams.grpc.get(&route.upstream_name) else {
        return GrpcStatus::Unavailable.to_l7_response(Some("upstream not configured"));
    };

    match upstream.dispatch_unary(req, config).await {
        Ok(resp) => resp,
        Err(e) => GrpcStatus::Unavailable.to_l7_response(Some(&format!("upstream error: {e}"))),
    }
}
