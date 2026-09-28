//! HTTP stream worker dispatching requests across HTTP versions.

use tokio::io::{AsyncRead, AsyncWrite};
use velda_composer::ComposerContext;
use velda_http::{HttpConnection, HttpVersion, bind_connection};

/// Asynchronous stream worker dispatching incoming HTTP requests across HTTP/1 and HTTP/2 connections.
pub async fn handle_http_stream<IO>(stream: IO, version: HttpVersion, context: ComposerContext)
where
    IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    match bind_connection(stream, version).await {
        Ok(HttpConnection::Http1(mut conn)) => {
            while let Ok(Some(req)) = conn.next_request().await {
                tracing::debug!(
                    method = %req.method,
                    path = %req.path(),
                    version = ?req.version,
                    listener = %context.listener_id,
                    "Decoded HTTP/1 request"
                );
                let response = velda_core::L7Response::from_bytes(
                    http::StatusCode::OK,
                    b"{\"gateway\":\"velda-edge\",\"protocol\":\"http1\",\"status\":\"active\"}"
                        .to_vec(),
                );
                if let Err(e) = conn.send_response(&response).await {
                    tracing::warn!(error = %e, "Failed to send HTTP/1 response");
                    break;
                }
                if conn.is_closed() {
                    break;
                }
            }
        }
        Ok(HttpConnection::Http2(mut conn)) => {
            while let Ok(Some((req, responder))) = conn.accept_request().await {
                tracing::debug!(
                    method = %req.method,
                    path = %req.path(),
                    version = ?req.version,
                    listener = %context.listener_id,
                    "Decoded HTTP/2 request"
                );
                let response = velda_core::L7Response::from_bytes(
                    http::StatusCode::OK,
                    b"{\"gateway\":\"velda-edge\",\"protocol\":\"h2\",\"status\":\"active\"}"
                        .to_vec(),
                );
                if let Err(e) = responder.send_response(&response) {
                    tracing::warn!(error = %e, "Failed to send HTTP/2 response");
                }
            }
        }
        Err(e) => {
            tracing::warn!(error = %e, "Failed to bind HTTP connection");
        }
    }
}
