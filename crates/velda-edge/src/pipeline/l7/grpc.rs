//! Layer 7 gRPC pipeline: downstream stream worker, route evaluation, and streaming forwarding.
//!
//! Powered entirely by `velda-grpc`. Operates strictly for listeners explicitly configured
//! with `protocol = "grpc"`. Zero HTTP fallback, zero header sniffing, and full duplex streaming.

use std::net::SocketAddr;
use tokio::io::{AsyncRead, AsyncWrite};
use velda_composer::ComposerContext;
use velda_core::{L7Request, L7Response};
use velda_grpc::composer_parse::{GrpcServerConnection, GrpcServerStream};
use velda_grpc::upstream_connector::GrpcUpstreamConnector;
use velda_grpc::{GrpcStatus, pipe_grpc_stream};
use velda_router::GrpcRouteRequest;

use crate::error::EdgeError;
use crate::runtime::SharedRuntime;

/// Dispatches an accepted downstream gRPC stream.
///
/// If routing succeeds, pipes the stream in full-duplex streaming mode to the selected upstream backend.
/// If routing fails, responds immediately with standard Trailers-Only gRPC status.
pub async fn handle_grpc_stream<IO>(stream: IO, context: ComposerContext, runtime: SharedRuntime)
where
    IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let mut conn = match GrpcServerConnection::handshake(stream).await {
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

    let timeout_duration = std::time::Duration::from_millis(context.limits.request_timeout_ms);

    while let Ok(accept_result) = tokio::time::timeout(timeout_duration, conn.accept()).await {
        match accept_result {
            Ok(Some(server_stream)) => {
                let ctx = context.clone();
                let rt = runtime.clone();

                tokio::spawn(async move {
                    dispatch_grpc_request_stream(server_stream, &ctx, &rt).await;
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
    server_stream: GrpcServerStream,
    context: &ComposerContext,
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
        let _ = server_stream.send_trailers_only(
            GrpcStatus::Unimplemented,
            Some("no route matched for empty path"),
        );
        return;
    };

    let rt = runtime.load();
    let Some(route) = rt.router.route_grpc(&context.listener_id, &grpc_req) else {
        let _ = server_stream.send_trailers_only(
            GrpcStatus::Unimplemented,
            Some("no route matched for service"),
        );
        return;
    };

    let target = if let Some(up) = rt.upstreams.grpc.get(&route.upstream_name) {
        up.select_target().or_else(|| route.select_target())
    } else {
        route.select_target()
    };

    let Some(target) = target else {
        tracing::error!(
            listener = %context.listener_id,
            route = %route.id,
            upstream = %route.upstream_name,
            "No healthy backend endpoints available for gRPC upstream"
        );
        let _ = server_stream.send_trailers_only(
            GrpcStatus::Unavailable,
            Some("no healthy upstream endpoints"),
        );
        return;
    };

    tracing::debug!(
        listener = %context.listener_id,
        route = %route.id,
        upstream = %route.upstream_name,
        target = %target,
        service = %grpc_req.service,
        method = ?grpc_req.method,
        "Piping gRPC full-duplex stream to upstream backend"
    );

    if let Err(e) = pipe_grpc_stream(server_stream, target).await {
        tracing::warn!(
            error = %e,
            target = %target,
            upstream = %route.upstream_name,
            "gRPC stream pipe terminated with error"
        );
    }
}

/// Helper for UDP L7 (HTTP/3) or single request dispatch returning an `L7Response`.
pub async fn process_grpc_request(
    req: &L7Request,
    context: &ComposerContext,
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

    let target = if let Some(up) = rt.upstreams.grpc.get(&route.upstream_name) {
        up.select_target().or_else(|| route.select_target())
    } else {
        route.select_target()
    };

    let Some(target) = target else {
        return GrpcStatus::Unavailable.to_l7_response(Some("no healthy upstream endpoints"));
    };

    match forward_grpc_unary_request(req, target, &context.limits).await {
        Ok(resp) => resp,
        Err(e) => GrpcStatus::Unavailable.to_l7_response(Some(&format!("upstream error: {e}"))),
    }
}

/// Forwards an in-memory L7Request to upstream backend via HTTP/2 and returns L7Response.
pub async fn forward_grpc_unary_request(
    req: &L7Request,
    target: SocketAddr,
    limits: &velda_core::IngressLimits,
) -> Result<L7Response, EdgeError> {
    GrpcUpstreamConnector::forward_unary(req, target, limits)
        .await
        .map_err(|e| EdgeError::Internal(format!("Failed to forward gRPC unary request: {e}")))
}
